package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.transform.{SparkAutoTask, TransformContext}
import ai.starlake.job.validator.SimpleRejectedRecord
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.GcpUtils
import com.google.cloud.bigquery.LegacySQLTypeName
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try, Using}
object IngestionUtil {

  private val rejectedCols = List(
    ("jobid", LegacySQLTypeName.STRING, StringType),
    ("timestamp", LegacySQLTypeName.TIMESTAMP, TimestampType),
    ("domain", LegacySQLTypeName.STRING, StringType),
    ("schema", LegacySQLTypeName.STRING, StringType),
    ("error", LegacySQLTypeName.STRING, StringType),
    ("path", LegacySQLTypeName.STRING, StringType)
  )

  def sinkRejected(
    applicationId: String,
    session: SparkSession,
    rejectedDS: Dataset[SimpleRejectedRecord],
    domainName: String,
    schemaName: String,
    now: Timestamp,
    paths: List[Path],
    scheduledDate: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[(Dataset[Row], Path)] = {
    import session.implicits._
    val rejectedPathName = paths.map(_.toString).mkString(",")
    // We need to save first the application ID
    // referencing it inside the worker (ds.map) below would fail.
    val rejectedTypedDS = rejectedDS.map { err =>
      RejectedRecord(
        applicationId,
        now,
        domainName,
        schemaName,
        err.errors, // TODO: Wondering if we should remove file name in errors and rely on err.path for rejectedPathName instead
        rejectedPathName
      )
    }
    val limitedRejectedTypedDS = rejectedTypedDS.limit(settings.appConfig.audit.maxErrors)
    val rejectedDF =
      limitedRejectedTypedDS.toDF(rejectedCols.map { case (attrName, _, _) => attrName }: _*)

    val auditSink = settings.appConfig.audit.getSink()
    auditSink.getConnectionType() match {
      case ConnectionType.GCPLOG =>
        val logName = settings.appConfig.audit.getDomainRejected()
        GcpUtils.sinkToGcpCloudLogging(
          ArraySeq.unsafeWrapArray(limitedRejectedTypedDS.collect()).map(_.asMap()),
          "rejected",
          logName
        )
        Success(rejectedDF, paths.head)

      case _ =>
        val taskDesc =
          AutoTaskInfo(
            name = s"rejected-$applicationId-$domainName-$schemaName",
            sql = None,
            database = settings.appConfig.audit.getDatabase(),
            domain = settings.appConfig.audit.getDomain(),
            table = "rejected",
            presql = Nil,
            postsql = Nil,
            sink = Some(settings.appConfig.audit.sink),
            _auditTableName = Some("rejected"),
            connectionRef = settings.appConfig.audit.sink.connectionRef
          )

        val autoTask = new SparkAutoTask(
          appId = Option(applicationId),
          taskDesc = taskDesc,
          commandParameters = Map.empty,
          interactive = None,
          truncate = false,
          test = false,
          logExecution = false,
          resultPageSize = 200,
          resultPageNumber = 1,
          scheduledDate = scheduledDate,
          syncSchema = false
        )
        val res = autoTask.sink(rejectedDF)
        if (res) {
          Success(rejectedDF, paths.head)
        } else {
          Failure(new Exception("Failed to save rejected"))
        }
    }
  }

  /** Writes the rejected records of a native load to the audit `rejected` table, at most
    * `audit.maxErrors` of them, as [[sinkRejected]] writes those of a Spark load (#1856). A
    * BigQuery, JDBC or Cloud Logging audit sink is written without Spark, with parameters rather
    * than SQL literals: the errors hold quotes, line breaks and braces. A file sink is written with
    * Spark.
    */
  def sinkRejectedRecords(
    applicationId: String,
    records: List[RejectedRecord],
    paths: List[Path],
    scheduledDate: Option[String],
    accessToken: Option[String],
    session: => SparkSession
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Unit] = {
    val limited = records.take(settings.appConfig.audit.maxErrors)
    limited.headOption match {
      case None => Success(())
      case Some(first) =>
        settings.appConfig.audit.getSink().getConnectionType() match {
          case ConnectionType.GCPLOG =>
            Try(
              GcpUtils.sinkToGcpCloudLogging(
                limited.map(_.asMap()),
                "rejected",
                settings.appConfig.audit.getDomainRejected()
              )
            )
          case ConnectionType.BQ =>
            Try(sinkRejectedToBigQuery(limited, accessToken))
          case ConnectionType.JDBC =>
            Try(sinkRejectedToJdbc(limited, accessToken, scheduledDate))
          case _ =>
            val sparkSession = session
            import sparkSession.implicits._
            sinkRejected(
              applicationId,
              sparkSession,
              limited.map(record => SimpleRejectedRecord(record.error, record.path)).toDS(),
              first.domain,
              first.schema,
              first.timestamp,
              paths,
              scheduledDate
            ).map(_ => ())
        }
    }
  }

  private def sinkRejectedToJdbc(
    records: List[RejectedRecord],
    accessToken: Option[String],
    scheduledDate: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Unit = {
    val audit = settings.appConfig.audit
    val taskDesc = AutoTaskInfo(
      name = s"rejected-${records.head.jobid}",
      sql = None,
      database = audit.getDatabase(),
      domain = audit.getDomain(),
      table = "rejected",
      presql = Nil,
      postsql = Nil,
      sink = Some(audit.sink),
      _auditTableName = Some("rejected"),
      connectionRef = audit.sink.connectionRef
    )
    val task = TransformContext.createJdbcTask(
      TransformContext(
        appId = Some(records.head.jobid),
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
      )
    )
    // Creates the audit table when it does not exist
    task.tableExists
    JdbcDbUtils.withJDBCConnection(
      schemaHandler.dataBranch(),
      audit.getSink().getConnection().withAccessToken(accessToken).options
    ) { connection =>
      Using.resource(
        connection.prepareStatement(
          s"INSERT INTO ${task.fullTableName} (JOBID, TIMESTAMP, DOMAIN, SCHEMA, ERROR, PATH) VALUES (?, ?, ?, ?, ?, ?)"
        )
      ) { statement =>
        records.foreach { record =>
          statement.setString(1, record.jobid)
          statement.setTimestamp(2, record.timestamp)
          statement.setString(3, record.domain)
          statement.setString(4, record.schema)
          statement.setString(5, record.error)
          statement.setString(6, record.path)
          statement.addBatch()
        }
        statement.executeBatch()
      }
    }
  }

  private def sinkRejectedToBigQuery(records: List[RejectedRecord], accessToken: Option[String])(
    implicit settings: Settings
  ): Unit = {
    val audit = settings.appConfig.audit
    val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
      audit.getDatabase(),
      audit.getDomain(),
      "rejected",
      audit
        .getSink()
        .getConnection()
        .options
        .get("projectId")
        .orElse(settings.appConfig.getDefaultDatabase())
    )
    val job = new BigQueryNativeJob(
      BigQueryLoadConfig(
        connectionRef = Some(audit.getConnectionRef()),
        outputTableId = Some(tableId),
        outputDatabase = audit.getDatabase(),
        accessToken = accessToken
      ),
      ""
    )
    val table = BigQueryJobBase.getBqTableForNative(tableId)
    if (!job.tableExists(tableId)) {
      job.getOrCreateDataset(None).get
      val createSql = settings.appConfig
        .jdbcEngines("bigquery")
        .tables("rejected")
        .createSql
        .richFormat(
          Map("table" -> table, "writeFormat" -> settings.appConfig.defaultWriteFormat),
          Map.empty
        )
      job.runBigQueryJob(Some(createSql)).get
    }
    RejectedRecord.batches(records).foreach { group =>
      val (insert, parameters) = RejectedRecord.bigQueryInsert(table, group)
      job.runBigQueryJob(Some(insert), queryParameters = parameters).get
    }
  }
}

case class BqLoadInfo(
  totalAcceptedRows: Long,
  totalRejectedRows: Long,
  paths: List[String],
  jobResult: BigQueryJobResult
) {
  val totalRows: Long = totalAcceptedRows + totalRejectedRows
}
