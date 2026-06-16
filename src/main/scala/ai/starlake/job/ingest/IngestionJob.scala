package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.loaders.{
  BigQueryNativeLoader,
  DuckDbNativeLoader,
  NativeLoader,
  SnowflakeNativeLoader
}
import ai.starlake.job.sink.bigquery.*
import ai.starlake.job.validator.{GenericRowValidator, SimpleRejectedRecord}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.*
import com.google.cloud.bigquery.TableId
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.*

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

trait IngestionJob
    extends SparkJob
    with IngestionAudit
    with IngestionExpectations
    with IngestionAcl
    with SparkIngestionPipeline {
  val accessToken: Option[String]
  val test: Boolean
  val scheduledDate: Option[String]
  private def loadGenericValidator(validatorClass: String): GenericRowValidator = {
    val validatorClassName = loader.toLowerCase() match {
      case "native" =>
        logger.warn(s"Unexpected '$loader' loader !!!")
        validatorClass
      case "spark" => validatorClass
      case _ =>
        throw new Exception(s"Unexpected '$loader' loader !!!")
    }
    Utils.loadInstance[GenericRowValidator](validatorClassName)
  }

  protected lazy val rowValidator: GenericRowValidator =
    loadGenericValidator(settings.appConfig.rowValidatorClass)

  def domain: DomainInfo

  def schema: SchemaInfo

  def storageHandler: StorageHandler

  def schemaHandler: SchemaHandler

  def types: List[Type]

  def path: List[Path]

  def options: Map[String, String]

  def name: String =
    s"""${domain.name}-${schema.name}-${path.headOption.map(_.getName).mkString(",")}"""

  lazy val strategy: WriteStrategy = {
    val s = mergedMetadata.getStrategyOptions()
    val startTs = s.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = s.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    s.copy(startTs = Some(startTs), endTs = Some(endTs))
  }

  def targetTableName: String = domain.finalName + "." + schema.finalName

  val now: Timestamp = java.sql.Timestamp.from(Instant.now)

  /** Merged metadata
    */
  lazy val mergedMetadata: Metadata = schema.mergedMetadata(domain.metadata)
  lazy val loader: String = mergedMetadata.loader.getOrElse(settings.appConfig.loader)

  private val accessTokenOptions: Map[String, String] =
    accessToken.map(token => Map("gcpAccessToken" -> token)).getOrElse(Map.empty)

  protected val sparkOptions: Map[String, String] =
    mergedMetadata.getOptions() ++ accessTokenOptions

  override protected def withExtraSparkConf(sourceConfig: SparkConf): SparkConf = {
    val conf = super.withExtraSparkConf(sourceConfig)
    conf.set("spark.sql.parser.escapedStringLiterals", "true")
  }

  protected def bqNativeJob(tableId: TableId, sql: String)(implicit
    settings: Settings
  ): BigQueryNativeJob = {
    val bqConfig = BigQueryLoadConfig(
      connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
      outputDatabase = schemaHandler.getDatabase(domain),
      outputTableId = Some(tableId),
      accessToken = accessToken
    )
    new BigQueryNativeJob(bqConfig, sql)
  }

  def runPrePostSql(engine: Engine, sqls: List[String]): Unit = {
    engine match {

      case Engine.BQ =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          schemaHandler.getDatabase(domain),
          domain.finalName,
          schema.finalName,
          connection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
        )

        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          bqNativeJob(tableId, compiledSql).runBigQueryJob()
        }

      case Engine.JDBC =>
        val connection = settings.appConfig.connections(mergedMetadata.getSinkConnectionRef())
        JdbcDbUtils.withJDBCConnection(
          this.schemaHandler.dataBranch(),
          connection.withAccessToken(accessToken).options
        ) { conn =>
          sqls.foreach { sql =>
            val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
            JdbcDbUtils.executeUpdate(compiledSql, conn)
          }
        }

      case _ =>
        val exists = SparkUtils.tableExists(session, targetTableName)
        if (exists)
          session.sql(s"select * from $targetTableName").createOrReplaceTempView("SL_THIS")
        sqls.foreach { sql =>
          val compiledSql = sql.richFormat(schemaHandler.activeEnvVars(), options)
          SparkUtils.sql(session, compiledSql)
        }
    }
  }

  private def selectLoader(): String = {
    val sinkConn = mergedMetadata.getSinkConnection()
    val dbName = sinkConn.targetDatawareHouse()
    val nativeCandidate: Boolean = isNativeCandidate(dbName)
    logger.info(s"Native candidate: $nativeCandidate")

    val loader =
      if (nativeCandidate) {
        val loaders = Set("bigquery", "duckdb", "spark", "snowflake")
        if (loaders.contains(dbName))
          dbName
        else
          "spark"
      } else {
        "spark"
      }
    logger.info(s"Using $loader as ingestion engine")
    loader
  }

  private val nativeSupportedFormats: Map[String, Set[Format]] = Map(
    "bigquery"  -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.POSITION),
    "duckdb"    -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT),
    "snowflake" -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.XML, Format.PARQUET),
    "redshift"  -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
  )

  private def isNativeCandidate(dbName: String): Boolean = {
    val isNative = mergedMetadata.loader
      .orElse(mergedMetadata.getSinkConnection().loader)
      .getOrElse(settings.appConfig.loader)
      .equalsIgnoreCase("native")
    isNative && {
      val format = mergedMetadata.resolveFormat()
      // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
      val arrayOk = dbName != "bigquery" || !mergedMetadata.resolveArray()
      nativeSupportedFormats.get(dbName).exists(_.contains(format)) && arrayOk
    }
  }

  def buildListOfSQLStatementsAsMap(orchestrator: String): Map[String, Object] = {
    require(
      orchestrator == "snowflake",
      s"buildListOfSQLStatementsAsMap only supports snowflake, got $orchestrator"
    )
    new NativeLoader(this, None).buildSQLStatements()
  }

  def run(): Try[JobResult] = {
    val jobResult = selectLoader() match {
      case "bigquery"  => new BigQueryNativeLoader(this, accessToken).run()
      case "snowflake" => new SnowflakeNativeLoader(this).run()
      case "duckdb"    => new DuckDbNativeLoader(this).run()
      case "spark"     => ingestWithSpark()
      case other =>
        logger.warn(s"Unsupported engine $other, falling back to spark")
        ingestWithSpark()
    }
    jobResult
      .recoverWith { case exception =>
        logLoadFailureInAudit(now, exception)
      }
      .map { (counterResults: List[IngestionCounters]) =>
        val validCounterResults = counterResults.filter(!_.ignore)
        logLoadInAudit(now, validCounterResults) match {
          case Failure(exception) => throw exception
          case Success(auditLogs) =>
            if (auditLogs.exists(!_.success)) {
              throw new DisallowRejectRecordException()
            }
        }
        runExpectations() match {
          case Failure(exception) if settings.appConfig.expectations.failOnError =>
            throw exception
          case _ =>
        }
        val globalCounters = IngestionCounters
          .aggregate(validCounterResults, applicationId())
          .orElse(
            Some(
              IngestionCounters(
                inputCount = -1,
                acceptedCount = -1,
                rejectedCount = -1,
                paths = path.map(_.toString),
                jobid = applicationId()
              )
            )
          )
        SparkJobResult(None, globalCounters)
      }
  }

  def loadDataSet(): Try[DataFrame]

  def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row]

  protected def saveRejected(
    errMessagesDS: Dataset[SimpleRejectedRecord],
    rejectedLinesDS: DataFrame
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Path] = {
    logger.whenDebugEnabled {
      logger.debug(s"rejected SIZE ${errMessagesDS.count()}")
      errMessagesDS
        .take(100)
        .foreach(rejected => logger.debug(rejected.errors.replaceAll("\n", "|")))
    }
    val domainName = domain.name
    val schemaName = schema.name

    val start = Timestamp.from(Instant.now())
    val formattedDate = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(start)

    if (settings.appConfig.sinkReplayToFile && !rejectedLinesDS.isEmpty) {
      val replayArea = DatasetArea.replay(domainName)
      val targetPath =
        new Path(replayArea, s"$domainName.$schemaName.$formattedDate.replay")
      val formattedRejectedLinesDF: DataFrameWriter[Row] = defineOutputAsOriginalFormat(
        rejectedLinesDS.repartition(1)
      )
      formattedRejectedLinesDF
        .save(targetPath.toString)
      storageHandler.moveSparkPartFile(
        targetPath,
        "0000" // When saving as text file, no extension is added.
      )
    }

    IngestionUtil.sinkRejected(
      applicationId(),
      session,
      errMessagesDS,
      domainName,
      schemaName,
      now,
      path,
      scheduledDate
    ) match {
      case Success((rejectedDF, rejectedPath)) =>
        Success(rejectedPath)
      case Failure(exception) =>
        logger.error("Failed to save Rejected", exception)
        Failure(exception)
    }
  }
}
