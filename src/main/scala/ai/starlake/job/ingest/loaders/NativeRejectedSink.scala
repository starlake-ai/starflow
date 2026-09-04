package ai.starlake.job.ingest.loaders

import ai.starlake.config.Settings
import ai.starlake.job.ingest.{AuditTaskBuilder, RejectedRecord}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.ConnectionType
import ai.starlake.utils.GcpUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/** Writes rejected input lines into the audit `rejected` table without Spark, by running a literal
  * SELECT ... UNION ALL through an AutoTask. Same approach as AuditLog, so it works for every audit
  * sink a native load can target.
  */
object NativeRejectedSink extends LazyLogging {

  def sink(
    applicationId: String,
    domainName: String,
    tableName: String,
    rejected: RejectCapture,
    paths: List[Path],
    timestamp: Timestamp,
    scheduledDate: Option[String],
    accessToken: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Unit] = Try {
    if (rejected.nonEmpty && settings.appConfig.audit.isActive()) {
      val limited = rejected.sample.take(settings.appConfig.audit.maxErrors)
      if (limited.size < rejected.count) {
        logger.warn(
          s"Only ${limited.size} of ${rejected.count} rejected lines were written to the " +
          s"audit rejected table, capped by audit.maxErrors"
        )
      }
      val rejectedPathName = paths.map(_.toString).mkString(",")
      val auditTimestamp = new Timestamp(timestamp.getTime)
      auditTimestamp.setNanos(0)
      val records = limited.map { line =>
        val location = line.line.map(l => s"${line.file}:$l").getOrElse(line.file)
        RejectedRecord(
          jobid = applicationId,
          timestamp = auditTimestamp,
          domain = domainName,
          schema = tableName,
          error = s"$location: ${line.error}",
          path = rejectedPathName
        )
      }

      settings.appConfig.audit.getSink().getConnectionType() match {
        case ConnectionType.GCPLOG =>
          // Cloud Logging is not a SQL sink, so the rows are sent as log entries instead of
          // being inlined into a SELECT. Same routing, log name and payload shape as
          // IngestionUtil.sinkRejected, which is the Spark equivalent of this sink.
          GcpUtils.sinkToGcpCloudLogging(
            records.map(_.asMap()),
            "rejected",
            settings.appConfig.audit.getDomainRejected()
          )

        case _ =>
          // Values are inlined into the SQL, so they go through the same escaping as AuditLog.
          // DuckDB embeds the offending value verbatim in its error messages, so arbitrary
          // input data reaches the Jinja pass AutoTask runs on this statement.
          val selectSql = records
            .map { record =>
              s"""SELECT '${AuditTaskBuilder.escapeLiteral(record.jobid)}' AS JOBID,
                 |  CAST('${record.timestamp.toString}' AS TIMESTAMP) AS TIMESTAMP,
                 |  '${AuditTaskBuilder.escapeLiteral(record.domain)}' AS DOMAIN,
                 |  '${AuditTaskBuilder.escapeLiteral(record.schema)}' AS SCHEMA,
                 |  '${AuditTaskBuilder.escapeLiteral(record.error)}' AS ERROR,
                 |  '${AuditTaskBuilder.escapeLiteral(record.path)}' AS PATH""".stripMargin
            }
            .mkString("\nUNION ALL\n")

          val task = AuditTaskBuilder.buildTask(
            name = s"rejected-$applicationId-$domainName-$tableName",
            auditTableName = "rejected",
            selectSql = selectSql,
            applicationId = applicationId,
            scheduledDate = scheduledDate,
            accessToken = accessToken
          )

          task.run() match {
            case Success(_) => ()
            case Failure(e) =>
              logger.error("Failed to write the audit rejected rows", e)
              throw e
          }
      }
    }
  }
}
