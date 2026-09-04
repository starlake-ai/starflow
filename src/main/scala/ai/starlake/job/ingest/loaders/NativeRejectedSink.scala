package ai.starlake.job.ingest.loaders

import ai.starlake.config.Settings
import ai.starlake.job.transform.TransformContext
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, Engine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/** Writes rejected input lines into the audit `rejected` table without Spark, by running a literal
  * SELECT ... UNION ALL through an AutoTask. Same approach as AuditLog, so it works for every audit
  * sink a native load can target.
  */
object NativeRejectedSink extends LazyLogging {

  /** Values are inlined into the SQL, so quotes and newlines are neutralized the same way AuditLog
    * does it.
    */
  private def literal(value: String): String =
    value.replaceAll("'", "-").replaceAll("\n", " ")

  def sink(
    applicationId: String,
    domainName: String,
    tableName: String,
    rejected: List[RejectedLine],
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
      val limited = rejected.take(settings.appConfig.audit.maxErrors)
      if (limited.size < rejected.size) {
        logger.warn(
          s"Only ${limited.size} of ${rejected.size} rejected lines were written to the " +
          s"audit rejected table, capped by audit.maxErrors"
        )
      }
      val rejectedPathName = paths.map(_.toString).mkString(",")
      val auditTimestamp = new Timestamp(timestamp.getTime)
      auditTimestamp.setNanos(0)
      val selectSql = limited
        .map { line =>
          val location = line.line.map(l => s"${line.file}:$l").getOrElse(line.file)
          val error = literal(s"$location: ${line.error}")
          s"""SELECT '${literal(applicationId)}' AS JOBID,
             |  CAST('${auditTimestamp.toString}' AS TIMESTAMP) AS TIMESTAMP,
             |  '${literal(domainName)}' AS DOMAIN,
             |  '${literal(tableName)}' AS SCHEMA,
             |  '$error' AS ERROR,
             |  '${literal(rejectedPathName)}' AS PATH""".stripMargin
        }
        .mkString("\nUNION ALL\n")

      val taskDesc = AutoTaskInfo(
        name = s"rejected-$applicationId-$domainName-$tableName",
        sql = Some(selectSql),
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.getDomain(),
        table = "rejected",
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        _auditTableName = Some("rejected"),
        connectionRef = settings.appConfig.audit.sink.connectionRef,
        parseSQL = Some(true),
        taskTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
      )

      val context = TransformContext(
        appId = Option(applicationId),
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        accessToken = accessToken,
        resultPageSize = 200,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = scheduledDate,
        syncSchema = false
      )(settings, storageHandler, schemaHandler)

      val engine =
        if (taskDesc.getSinkConnection().isJdbcUrl()) Engine.JDBC
        else taskDesc.getSinkConnection().getEngine()

      context.toTask(engine).run() match {
        case Success(_) => ()
        case Failure(e) =>
          logger.error("Failed to write the audit rejected rows", e)
          throw e
      }
    }
  }
}
