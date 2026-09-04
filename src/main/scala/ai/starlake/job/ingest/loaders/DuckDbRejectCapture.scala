package ai.starlake.job.ingest.loaders

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.SchemaInfo
import com.typesafe.scalalogging.LazyLogging

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import scala.util.Using

/** Reads back the lines DuckDB refused while loading.
  *
  * `reject_scans` and `reject_errors` are session scoped temporary tables populated by
  * `read_csv(..., store_rejects = true)`. They only exist on the connection that ran the scan, and
  * a ROLLBACK discards them, so they must be read on that same connection and before any rollback.
  */
object DuckDbRejectCapture extends LazyLogging {

  /** DuckDB records one row per bad column, so a line with two bad columns yields two rows. The
    * GROUP BY collapses them into one entry per input line, which keeps the resulting count
    * comparable to the accepted row count.
    */
  private val captureCsvRejectsSql =
    """SELECT s.file_path AS file, e.line AS line, e.csv_line AS raw_line,
      |       string_agg(CAST(e.error_type AS VARCHAR) || ': ' || e.error_message, ' | ') AS error
      |FROM reject_errors e JOIN reject_scans s USING (scan_id, file_id)
      |WHERE e.scan_id = (SELECT max(scan_id) FROM reject_scans)
      |GROUP BY 1, 2, 3
      |ORDER BY 1, 2""".stripMargin

  def captureCsvRejects(conn: Connection): List[RejectedLine] = {
    val rejected = ListBuffer[RejectedLine]()
    Using.resource(conn.createStatement()) { statement =>
      Using.resource(statement.executeQuery(captureCsvRejectsSql)) { rs =>
        while (rs.next()) {
          rejected += RejectedLine(
            file = rs.getString("file"),
            line = Some(rs.getLong("line")),
            rawLine = rs.getString("raw_line"),
            error = rs.getString("error")
          )
        }
      }
    }
    if (rejected.nonEmpty) {
      logger.warn(s"DuckDB rejected ${rejected.size} line(s) while reading the input files")
    }
    rejected.toList
  }

  /** Builds the (predicate, message) pairs that decide whether a fixed width line is rejected.
    * Positions are zero based and inclusive, so an attribute at [first, last] is sliced as
    * SUBSTR(value, first + 1, last - first + 1) and a complete record needs at least last + 1
    * characters.
    */
  def positionRejectClauses(
    schema: SchemaInfo,
    ddlTypesByAttribute: Map[String, String]
  ): List[(String, String)] = {
    val positioned =
      schema.exceptIgnoreScriptAndTransformAttributes().filter(_.position.isDefined)
    if (positioned.isEmpty) {
      Nil
    } else {
      val minimumLength = positioned.flatMap(_.position).map(_.last).max + 1
      val shortLine =
        (
          s"length(value) < $minimumLength",
          s"line is shorter than the $minimumLength characters required by the declared positions"
        )
      val castClauses = positioned.flatMap { attr =>
        val position = attr.position.get
        ddlTypesByAttribute.get(attr.name).filterNot(SchemaInfo.isStringLikeDdlType).map {
          ddlType =>
            val slice =
              s"SUBSTR(value, ${position.first + 1}, ${position.last - position.first + 1})"
            (
              s"(TRIM($slice) <> '' AND TRY_CAST($slice AS $ddlType) IS NULL)",
              s"${attr.name}: cannot cast to $ddlType"
            )
        }
      }
      shortLine :: castClauses
    }
  }

  /** Collects the fixed width lines that cannot be projected into the target columns, then deletes
    * them from the temporary table so the second step inserts only good rows.
    */
  def capturePositionRejects(
    conn: Connection,
    tableName: String,
    filePath: String,
    schema: SchemaInfo,
    ddlTypesByAttribute: Map[String, String]
  ): List[RejectedLine] = {
    val clauses = positionRejectClauses(schema, ddlTypesByAttribute)
    if (clauses.isEmpty) {
      Nil
    } else {
      val where = clauses.map(_._1).mkString(" OR ")
      val errorExpression = clauses
        .map { case (predicate, message) =>
          s"CASE WHEN $predicate THEN '${message.replace("'", "''")}' END"
        }
        .mkString("concat_ws(' | ', ", ", ", ")")
      val selectSql =
        s"SELECT value, $errorExpression AS sl_reject_error FROM $tableName WHERE $where"
      val rejected = ListBuffer[RejectedLine]()
      Using.resource(conn.createStatement()) { statement =>
        Using.resource(statement.executeQuery(selectSql)) { rs =>
          while (rs.next()) {
            rejected += RejectedLine(
              file = filePath,
              line = None,
              rawLine = rs.getString("value"),
              error = rs.getString("sl_reject_error")
            )
          }
        }
      }
      if (rejected.nonEmpty) {
        logger.warn(s"Rejecting ${rejected.size} fixed width line(s) from $filePath")
        JdbcDbUtils.execute(s"DELETE FROM $tableName WHERE $where", conn)
      }
      rejected.toList
    }
  }
}
