package ai.starlake.job.ingest.loaders

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.SchemaInfo
import com.typesafe.scalalogging.LazyLogging

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.{Connection, ResultSet}
import scala.collection.mutable.ListBuffer
import scala.util.Using
import scala.util.control.NonFatal

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

  def captureCsvRejects(conn: Connection)(implicit settings: Settings): RejectCapture = {
    val captured = capture(conn, captureCsvRejectsSql, "raw_line") { (rs, rawLine) =>
      // wasNull() reports on the column just read, so the line number is read on its own
      // line: a SQL NULL there means "unknown line", not line 0.
      val line = rs.getLong("line")
      val lineNumber = if (rs.wasNull()) None else Some(line)
      RejectedLine(
        file = rs.getString("file"),
        line = lineNumber,
        rawLine = rawLine,
        error = rs.getString("error")
      )
    }
    if (captured.nonEmpty) {
      logger.warn(s"DuckDB rejected ${captured.count} line(s) while reading the input files")
    }
    captured
  }

  /** Builds the (predicate, message) pairs that decide whether a fixed width line is rejected. The
    * slice expression and the cast eligibility rule come from `SchemaInfo.positionSlice` and
    * `SchemaInfo.castableDdlType`, the same two helpers `buildSecondStepSqlSelectOnLoad` projects
    * with, so this predicate cannot drift from what the second step actually reads.
    *
    * The line length is only checked against the REQUIRED positioned attributes: a line has to
    * reach the end of every required field, but a fixed width source that right trims blanks
    * legitimately stops short when its trailing optional fields are empty, and rejecting those
    * lines would abort loads that worked before reject capture existed. The slice of an absent
    * optional field comes back as the empty string, the TRIM guard below then suppresses its cast
    * clause and the second step loads NULL, which is exactly what the Spark POSITION path does with
    * an empty optional cell. When no positioned attribute is required there is no length clause at
    * all, only the cast clauses.
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
      val requiredPositions = positioned.filter(_.resolveRequired()).flatMap(_.position)
      // An empty input line comes back from the first step as value = NULL, not as the empty
      // string, so a bare length(value) would evaluate to NULL and the line would be neither
      // rejected nor deleted. coalesce makes it fall on the reject side of the predicate.
      val shortLine = Option.when(requiredPositions.nonEmpty) {
        val minimumLength = requiredPositions.map(_.last).max + 1
        (
          s"coalesce(length(value), 0) < $minimumLength",
          s"line is shorter than the $minimumLength characters that cover every required attribute"
        )
      }
      val castClauses = positioned.flatMap { attr =>
        val position = attr.position.get
        SchemaInfo.castableDdlType(ddlTypesByAttribute, attr.name).map { ddlType =>
          val slice = SchemaInfo.positionSlice(position)
          (
            s"(TRIM($slice) <> '' AND TRY_CAST($slice AS $ddlType) IS NULL)",
            s"${attr.name}: cannot cast to $ddlType"
          )
        }
      }
      shortLine.toList ::: castClauses
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
  )(implicit settings: Settings): RejectCapture = {
    val clauses = positionRejectClauses(schema, ddlTypesByAttribute)
    if (clauses.isEmpty) {
      RejectCapture.empty
    } else {
      val where = clauses.map(_._1).mkString(" OR ")
      val errorExpression = clauses
        .map { case (predicate, message) =>
          s"CASE WHEN $predicate THEN '${message.replace("'", "''")}' END"
        }
        .mkString("concat_ws(' | ', ", ", ", ")")
      val selectSql =
        s"SELECT value, $errorExpression AS sl_reject_error FROM $tableName WHERE $where"
      val captured = capture(conn, selectSql, "value") { (rs, rawLine) =>
        RejectedLine(
          file = filePath,
          line = None,
          rawLine = rawLine,
          error = rs.getString("sl_reject_error")
        )
      }
      if (captured.nonEmpty) {
        logger.warn(s"Rejecting ${captured.count} fixed width line(s) from $filePath")
        JdbcDbUtils.execute(s"DELETE FROM $tableName WHERE $where", conn)
      }
      captured
    }
  }

  /** Runs the rejected lines query twice on the open connection: once to count them exactly, once
    * to read them.
    *
    * A load that reads a huge file with the wrong delimiter rejects every line of it, so nothing
    * here may grow with the number of rejects. The count comes from a COUNT over the same query, so
    * it is exact whatever is kept; at most `audit.maxErrors` lines are materialized, which is all
    * the audit rejected table can hold anyway; and every raw line goes straight to a local spill
    * file that the replay writer streams to its output without ever holding it as a String.
    *
    * Both statements run on the connection that ran the scan and before any rollback, because
    * `reject_errors` is session scoped and a rollback discards it.
    */
  private def capture(
    conn: Connection,
    sql: String,
    rawLineColumn: String
  )(rejectedLine: (ResultSet, String) => RejectedLine)(implicit
    settings: Settings
  ): RejectCapture = {
    val count = countOf(conn, sql)
    if (count == 0L) {
      RejectCapture.empty
    } else {
      val maxSample = settings.appConfig.audit.maxErrors
      val sample = ListBuffer[RejectedLine]()
      val spillFile = Files.createTempFile("sl-reject-", ".spill")
      try {
        Using.resource(Files.newBufferedWriter(spillFile, StandardCharsets.UTF_8)) { writer =>
          Using.resource(conn.createStatement()) { statement =>
            Using.resource(statement.executeQuery(sql)) { rs =>
              while (rs.next()) {
                // the raw line column is SQL NULL for an empty input line, and for the CSV error
                // types that carry no usable line, invalid encoding and line size over maximum
                // among them. The replay file has to carry the line as it was, so an empty line
                // rather than the literal string "null".
                val rawLine = Option(rs.getString(rawLineColumn)).getOrElse("")
                if (sample.size < maxSample) {
                  sample += rejectedLine(rs, rawLine)
                }
                writer.write(rawLine)
                // a plain \n, never the platform separator: these bytes are the replay file
                writer.write('\n')
              }
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          // the caller only learns about the spill file through the capture it never gets
          Files.deleteIfExists(spillFile)
          throw e
      }
      RejectCapture(count, sample.toList, List(spillFile))
    }
  }

  /** The exact number of rejected input lines, counted over the same grouped query the capture
    * reads, so it cannot drift from what the replay file holds.
    */
  private def countOf(conn: Connection, sql: String): Long =
    Using.resource(conn.createStatement()) { statement =>
      Using.resource(statement.executeQuery(s"SELECT count(*) AS cnt FROM ($sql)")) { rs =>
        rs.next()
        rs.getLong("cnt")
      }
    }
}
