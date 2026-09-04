package ai.starlake.job.ingest.loaders

import com.typesafe.scalalogging.LazyLogging

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import scala.util.Using

/** Reads back the lines DuckDB refused while loading.
  *
  * `reject_scans` and `reject_errors` are session scoped temporary tables populated by
  * `read_csv(..., store_rejects = true)`. They only exist on the connection that ran the
  * scan, and a ROLLBACK discards them, so they must be read on that same connection and
  * before any rollback.
  */
object DuckDbRejectCapture extends LazyLogging {

  /** DuckDB records one row per bad column, so a line with two bad columns yields two
    * rows. The GROUP BY collapses them into one entry per input line, which keeps the
    * resulting count comparable to the accepted row count.
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
}
