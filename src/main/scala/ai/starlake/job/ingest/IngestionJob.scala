package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.exceptions.DisallowRejectRecordException
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.loaders.{
  BigQueryNativeLoader,
  DuckDbNativeLoader,
  NativeLoader,
  ReplayFileWriter,
  SnowflakeNativeLoader
}
import ai.starlake.job.sink.bigquery.*
import ai.starlake.job.validator.{GenericRowValidator, SimpleRejectedRecord}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.*
import com.google.cloud.bigquery.TableId
import com.typesafe.scalalogging.LazyLogging
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
      // A table configured with the native loader is loaded with Spark when its engine, format or
      // attributes cannot be loaded natively
      case "native" | "spark" => validatorClass
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

  private[ingest] def selectLoader(): String =
    IngestionJob.selectLoader(
      mergedMetadata,
      schema.attributes,
      targetTableName,
      schemaHandler.types(),
      IngestionJob.unconvertedParquetFields(schema, schemaHandler),
      path
    )

  /** Spark, DuckDB and Snowflake decimals have a precision of 38 at most: a wider decimal would
    * lose digits
    */
  private def withoutWideDecimals(loader: String)(
    load: => Try[List[IngestionCounters]]
  ): Try[List[IngestionCounters]] =
    Try(requireNoWideDecimals(loader)).flatMap(_ => load)

  private def requireNoWideDecimals(loader: String): Unit = {
    val wideDecimals = IngestionJob.wideDecimalAttributes(schema.attributes, schemaHandler)
    if (wideDecimals.nonEmpty)
      throw new IllegalArgumentException(
        s"$targetTableName cannot be loaded with $loader, whose decimals have a precision of ${PrimitiveType.decimal.maxSparkPrecision} at most: the precision of ${wideDecimals
            .mkString(", ")} is above. Only the native BigQuery loader loads such a decimal, into a BIGNUMERIC column, from DSV, JSON, POSITION or Parquet files: use a decimal type of precision ${PrimitiveType.decimal.maxSparkPrecision} at most for other loads"
      )
  }

  def buildListOfSQLStatementsAsMap(orchestrator: String): Map[String, Object] = {
    require(
      orchestrator == "snowflake",
      s"buildListOfSQLStatementsAsMap only supports snowflake, got $orchestrator"
    )
    requireNoWideDecimals(s"the $orchestrator orchestrator")
    // The SQL runs in the orchestrator, where Spark cannot apply the privacy instead
    val privacyAttributes = IngestionJob.nonSqlPrivacyAttributes(schema.attributes)
    if (privacyAttributes.nonEmpty)
      throw new IllegalArgumentException(
        s"$targetTableName cannot be loaded by the $orchestrator orchestrator, which only applies SQL privacy, because of the privacy of ${privacyAttributes
            .mkString(", ")}. Use a SQL privacy (privacy: \"SQL: ...\") or another orchestrator template"
      )
    new NativeLoader(this, None).buildSQLStatements()
  }

  def run(): Try[JobResult] = {
    // A failure to select the loader is logged in the audit, as a failure to load
    val jobResult = Try(selectLoader()).flatMap {
      case "bigquery" => new BigQueryNativeLoader(this, accessToken).run()
      case "snowflake" =>
        withoutWideDecimals("the native Snowflake loader")(new SnowflakeNativeLoader(this).run())
      case "duckdb" =>
        withoutWideDecimals("the native DuckDB loader")(new DuckDbNativeLoader(this).run())
      case "spark" => withoutWideDecimals("Spark")(ingestWithSpark())
      case other =>
        logger.warn(s"Unsupported engine $other, falling back to spark")
        withoutWideDecimals("Spark")(ingestWithSpark())
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

    if (settings.appConfig.sinkReplayToFile && !rejectedLinesDS.isEmpty) {
      val replayArea = DatasetArea.replay(domainName)
      // named through the same helper as the native loaders: the timestamp only resolves to the
      // second, so the jobid and input file discriminator is what keeps two same second loads of
      // the same table from clobbering each other's replay file
      val targetPath =
        new Path(
          replayArea,
          ReplayFileWriter.fileName(
            domainName,
            schemaName,
            start,
            applicationId(),
            path.headOption.map(_.getName)
          )
        )
      val formattedRejectedLinesDF: DataFrameWriter[Row] = defineOutputAsOriginalFormat(
        rejectedLinesDS.repartition(1)
      )
      formattedRejectedLinesDF
        .save(targetPath.toString)
      storageHandler.moveSparkPartFile(targetPath)
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

object IngestionJob extends LazyLogging {

  private val nativeSupportedFormats: Map[String, Set[Format]] = Map(
    "bigquery" -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.POSITION, Format.PARQUET),
    "duckdb"   -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.POSITION),
    "snowflake" -> Set(
      Format.DSV,
      Format.JSON,
      Format.JSON_FLAT,
      Format.XML,
      Format.PARQUET,
      Format.POSITION
    ),
    "redshift" -> Set(Format.DSV, Format.JSON, Format.JSON_FLAT)
  )

  /** Loader effectively used to ingest a table with this merged metadata: "bigquery", "snowflake"
    * or "duckdb" for warehouse-native loads, "spark" otherwise.
    *
    * @param unconvertedParquetFields
    *   the nested or repeated `JSON`, `INTERVAL` and `RANGE` fields of the first step of a native
    *   BigQuery Parquet load (see [[unconvertedParquetFields]]), only evaluated for such a load
    * @param paths
    *   the files to load: a native BigQuery Parquet load of local files does not read the Parquet
    *   `LIST` columns as repeated fields
    */
  def selectLoader(
    mergedMetadata: Metadata,
    attributes: List[TableAttribute],
    tableName: String,
    types: List[Type],
    unconvertedParquetFields: => List[String] = Nil,
    paths: List[Path] = Nil
  )(implicit settings: Settings): String = {
    val sinkConn = mergedMetadata.getSinkConnection()
    val dbName = sinkConn.targetDatawareHouse()
    val nativeCandidate: Boolean =
      isNativeCandidate(
        mergedMetadata,
        attributes,
        tableName,
        types,
        dbName,
        unconvertedParquetFields,
        paths
      )
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

  /** True when loading this table requires a Spark session, in which case a server that cannot
    * share its Spark session must run the load in a separate process.
    */
  def loadRequiresSpark(domain: DomainInfo, table: SchemaInfo)(implicit
    settings: Settings
  ): Boolean = {
    val schemaHandler = settings.schemaHandler()
    selectLoader(
      table.mergedMetadata(domain.metadata),
      table.attributes,
      domain.finalName + "." + table.finalName,
      schemaHandler.types(),
      unconvertedParquetFields(table, schemaHandler)
    ) == "spark"
  }

  /** @return
    *   the nested or repeated `JSON`, `INTERVAL` and `RANGE` fields of the first step schema of a
    *   native BigQuery Parquet load of this table, with the mappings, which a column of the target
    *   table may keep
    */
  def unconvertedParquetFields(table: SchemaInfo, schemaHandler: SchemaHandler): List[String] =
    BigQueryNativeLoader.unconvertedParquetFields(
      BigQueryNativeLoader.firstStepSchema(
        table,
        None,
        schemaHandler,
        fromDdlMapping = true,
        log = false
      )
    )

  private def isNativeCandidate(
    mergedMetadata: Metadata,
    attributes: List[TableAttribute],
    tableName: String,
    types: List[Type],
    dbName: String,
    unconvertedParquetFields: => List[String],
    paths: List[Path]
  )(implicit settings: Settings): Boolean = {
    val isNative = mergedMetadata.loader
      .orElse(mergedMetadata.getSinkConnection().loader)
      .getOrElse(settings.appConfig.loader)
      .equalsIgnoreCase("native")
    isNative && isNativelySupported(
      dbName,
      mergedMetadata.resolveFormat(),
      mergedMetadata.resolveArray()
    ) && {
      // The native loaders only apply SQL privacy, computed by the SQL of the load
      val privacyAttributes = nonSqlPrivacyAttributes(attributes)
      if (privacyAttributes.nonEmpty)
        logger.warn(
          s"Loading $tableName with Spark instead of the native loader, which does not apply the privacy of ${privacyAttributes
              .mkString(", ")}. Its scripts, SQL privacy and filter run as Spark SQL"
        )
      privacyAttributes.isEmpty
    } && {
      val binaryAttributes =
        binaryAttributesNotDecodedNatively(mergedMetadata, attributes, types, dbName)
      if (binaryAttributes.nonEmpty)
        logger.warn(
          s"Loading $tableName with Spark instead of the native $dbName loader, which would not decode the bytes of ${binaryAttributes
              .mkString(", ")} from the encoding of their type. Its scripts, SQL privacy and filter run as Spark SQL"
        )
      binaryAttributes.isEmpty
    } && {
      // Parquet tables were loaded with Spark before the native BigQuery loader read Parquet: they
      // keep doing so when it would load them differently
      val unsupported =
        if (dbName == "bigquery" && mergedMetadata.resolveFormat() == Format.PARQUET)
          parquetFeaturesNotLoadedByBigQuery(
            mergedMetadata,
            attributes,
            paths,
            unconvertedParquetFields
          )
        else Nil
      if (unsupported.nonEmpty)
        logger.warn(
          s"Loading $tableName with Spark instead of the native bigquery loader, which does not load Parquet files with ${unsupported
              .mkString(", ")}. Its scripts, SQL privacy and filter run as Spark SQL"
        )
      unsupported.isEmpty
    }
  }

  /** @return
    *   the loaded binary attributes whose text the native loader of the engine does not decode with
    *   the encoding of their type: it would store other bytes. BigQuery reads base64 in CSV and
    *   JSON loads, Snowflake reads hex by default, DuckDB reads its own escaped text.
    */
  private def binaryAttributesNotDecodedNatively(
    mergedMetadata: Metadata,
    attributes: List[TableAttribute],
    types: List[Type],
    dbName: String
  ): List[String] = {
    val format = mergedMetadata.resolveFormat()
    // A Parquet file holds the bytes themselves
    def decodedNatively(encoding: String): Boolean =
      format == Format.PARQUET ||
      dbName == "bigquery" && encoding == "base64" && format != Format.POSITION ||
      dbName == "snowflake" && encoding == "hex"
    def binaryLeaves(attributes: List[TableAttribute], prefix: String): List[String] =
      attributes
        .filterNot(attribute => attribute.resolveIgnore() || attribute.script.isDefined)
        .flatMap { attribute =>
          val path = prefix + attribute.getFinalName()
          if (attribute.attributes.nonEmpty) binaryLeaves(attribute.attributes, path + ".")
          else
            types
              .find(_.name == attribute.`type`)
              .filter(tpe =>
                tpe.primitiveType == PrimitiveType.binary &&
                !decodedNatively(tpe.pattern.toLowerCase(java.util.Locale.ROOT))
              )
              .map(_ => path)
              .toList
        }
    binaryLeaves(attributes, "")
  }

  /** @param array
    *   true if the files are JSON arrays (the `array` metadata), which does not apply to Parquet
    *   files
    * @return
    *   true if the native loader of the datawarehouse can load files of this format
    */
  def isNativelySupported(dbName: String, format: Format, array: Boolean): Boolean = {
    // BigQuery load jobs read newline-delimited JSON, not JSON arrays
    // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
    val arrayOk = dbName != "bigquery" || format == Format.PARQUET || !array
    nativeSupportedFormats.get(dbName).exists(_.contains(format)) && arrayOk
  }

  /** @return
    *   the features of a Parquet table that a BigQuery load job would not apply as the Spark loader
    *   does, with the attributes they concern, if any:
    *   - Spark read options (`options`): Spark reads the files with them, in another format (ORC,
    *     Avro...) for `format`, while a load job ignores them;
    *   - array attributes in local files: they are uploaded without the Parquet options, so list
    *     inference does not read the Parquet `LIST` columns as repeated fields;
    *   - a rename below the top level: the second step only renames top level columns;
    *   - `default` and `trim`, which no native loader applies;
    *   - nested or repeated `JSON`, `INTERVAL` and `RANGE` fields, that the second step does not
    *     convert from strings.
    * @param unconvertedFields
    *   the nested or repeated `JSON`, `INTERVAL` and `RANGE` fields of the first step schema
    */
  def parquetFeaturesNotLoadedByBigQuery(
    metadata: Metadata,
    attributes: List[TableAttribute],
    paths: List[Path],
    unconvertedFields: List[String]
  ): List[String] = {
    // Every attribute, at every nesting level, with its path
    def withPaths(
      attributes: List[TableAttribute],
      prefix: String
    ): List[(String, TableAttribute)] =
      attributes.flatMap { attribute =>
        val path = prefix + attribute.name
        (path, attribute) :: withPaths(attribute.attributes, path + ".")
      }
    // Scripted attributes are computed by the second step, ignored ones are not loaded
    val loaded = withPaths(
      attributes.filterNot(attribute => attribute.resolveIgnore() || attribute.script.nonEmpty),
      ""
    )
    def feature(name: String, paths: List[String]): Option[String] =
      if (paths.isEmpty) None else Some(s"$name (${paths.mkString(", ")})")
    // Same test of local files as BigQueryNativeJob.loadPathsToBQ
    val localFiles = paths.exists(_.toString.startsWith("file:"))
    List(
      feature(
        "Spark read options",
        metadata.getOptions().toList.sorted.collect {
          case ("format", format) if !format.equalsIgnoreCase("parquet") => s"format $format"
          case (option, _) if option != "format"                         => option
        }
      ),
      feature(
        "array attributes in local files",
        if (localFiles)
          // Ignored attributes are in the files: they are loaded into the temporary table
          withPaths(attributes.filterNot(_.script.nonEmpty), "").collect {
            case (path, attribute) if attribute.resolveArray() => path
          }
        else Nil
      ),
      feature(
        "nested renames",
        loaded.collect {
          case (path, attribute)
              if path.contains('.') && attribute.getFinalName() != attribute.name =>
            path
        }
      ),
      feature(
        "default values",
        loaded.collect { case (path, attribute) if attribute.default.nonEmpty => path }
      ),
      feature(
        "trim",
        loaded.collect {
          case (path, attribute) if attribute.trim.exists(_ != Trim.NONE) => path
        }
      ),
      feature("nested or repeated JSON, INTERVAL or RANGE fields", unconvertedFields)
    ).flatten
  }

  /** @return
    *   the paths of the loaded leaf attributes, at every nesting level, scripted ones included, of
    *   a decimal type whose precision is above the one of Spark decimals
    */
  def wideDecimalAttributes(
    attributes: List[TableAttribute],
    schemaHandler: SchemaHandler,
    prefix: String = ""
  ): List[String] =
    attributes
      .filterNot(_.resolveIgnore())
      .flatMap { attribute =>
        val path = prefix + attribute.name
        if (attribute.attributes.nonEmpty)
          wideDecimalAttributes(attribute.attributes, schemaHandler, path + ".")
        else
          attribute
            .`type`(schemaHandler)
            .filter(PrimitiveType.decimal.isWide)
            .map(_ => path)
            .toList
      }

  /** @return
    *   the paths of the loaded leaf attributes, at every nesting level, whose privacy is not SQL.
    *   Ignored attributes are not loaded, and the privacy of scripted ones is not applied by any
    *   loader.
    */
  def nonSqlPrivacyAttributes(attributes: List[TableAttribute], prefix: String = ""): List[String] =
    attributes
      .filterNot(attribute => attribute.resolveIgnore() || attribute.script.nonEmpty)
      .flatMap { attribute =>
        val path = prefix + attribute.name
        val privacy = attribute.resolvePrivacy()
        if (attribute.attributes.nonEmpty)
          nonSqlPrivacyAttributes(attribute.attributes, path + ".")
        else if (privacy != TransformInput.None && !privacy.sql)
          List(path)
        else
          Nil
      }
}
