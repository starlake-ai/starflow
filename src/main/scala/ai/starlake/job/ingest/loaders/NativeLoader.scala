package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.job.common.{TaskSQLStatements, WorkflowStatements}
import ai.starlake.job.ingest.{AuditLog, IngestionJob, Step}
import ai.starlake.job.metrics.{ExpectationJob, JdbcExpectationAssertionHandler}
import ai.starlake.job.transform.{AutoTask, TransformContext}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{SparkUtils, Utils}
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import java.nio.charset.Charset
import java.sql.Timestamp
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Random, Success, Using}

class NativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  val settings: Settings
) extends LazyLogging {

  val domain: DomainInfo = ingestionJob.domain

  val starlakeSchema: SchemaInfo = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: WriteStrategy = ingestionJob.mergedMetadata.getStrategyOptions()

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  lazy val database: Option[String] = schemaHandler.getDatabase(domain)

  val targetFullTableName = s"${domain.finalName}.${starlakeSchema.finalName}"

  lazy val sink = mergedMetadata.getSink()

  lazy val sinkConnection: Settings.ConnectionInfo =
    sink
      .getConnection()
      .copy(sparkFormat = None) // we are forcing native load.

  lazy val tempTableName: String = SQLUtils.temporaryTableName(starlakeSchema.finalName)

  lazy val scheduledDate: Option[String] = ingestionJob.scheduledDate

  protected def requireTwoSteps(schema: SchemaInfo): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    schema.filter.nonEmpty ||
    schema.attributes.exists(_.rename.isDefined) ||
    settings.appConfig.archiveTable || settings.appConfig.audit.detailedLoadAudit && path.size > 1
  }

  lazy val twoSteps: Boolean = requireTwoSteps(effectiveSchema)

  lazy val (createDisposition: String, writeDisposition: String) = Utils.getDBDisposition(
    strategy.toWriteMode()
  )

  lazy val engine: Engine = sinkConnection.getEngine()

  lazy val engineName: Engine = sinkConnection.getJdbcEngineName()

  protected def archiveTableTask(
    database: String,
    schema: String,
    table: String,
    firstStepTableInfo: TableInfo
  ): Option[AutoTask] = {
    if (settings.appConfig.archiveTable) {
      val (
        archiveDatabaseName: Option[String],
        archiveDomainName: String,
        archiveTableName: String
      ) = getArchiveTableComponents()

      val targetTable = OutputRef(
        database = database,
        domain = schema,
        table = table
      ).toSQLString(sinkConnection)

      val firstStepFields = firstStepTableInfo.maybeSchema
        .map { schema =>
          schema.getFields.asScala.map(_.getName)
        }
        .getOrElse(
          throw new Exception(
            "Should never happen in Ingestion mode. We know the fields we are loading using the yml files"
          )
        )
      val req =
        s"SELECT ${firstStepFields.mkString(",")}, '${ingestionJob.applicationId()}' as JOBID FROM $targetTable"
      val taskDesc = AutoTaskInfo(
        s"archive-${ingestionJob.applicationId()}",
        Some(req),
        database = archiveDatabaseName,
        archiveDomainName,
        archiveTableName,
        sink = Some(mergedMetadata.getSink().toAllSinks()),
        connectionRef = Option(mergedMetadata.getSinkConnectionRef())
      )

      val context = TransformContext(
        appId = Option(ingestionJob.applicationId()),
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = true,
        accessToken = accessToken,
        resultPageSize = 200,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = scheduledDate,
        syncSchema = false
      )(settings, storageHandler, schemaHandler)
      val autoTask = context.toTask(engine)
      Some(autoTask)
    } else {
      None
    }
  }

  private def getArchiveTableComponents(): (Option[String], String, String) = {
    val fullArchiveTableName = Utils.parseJinja(
      settings.appConfig.archiveTablePattern,
      Map("domain" -> domain.finalName, "table" -> starlakeSchema.finalName)
    )
    val archiveTableComponents = fullArchiveTableName.split('.')
    val (archiveDatabaseName, archiveDomainName, archiveTableName) =
      if (archiveTableComponents.length == 3) {
        (
          Some(archiveTableComponents(0)),
          archiveTableComponents(1),
          archiveTableComponents(2)
        )
      } else if (archiveTableComponents.length == 2) {
        (
          schemaHandler.getDatabase(domain),
          archiveTableComponents(0),
          archiveTableComponents(1)
        )
      } else {
        throw new Exception(
          s"Archive table name must be in the format <domain>.<table> but got $fullArchiveTableName"
        )
      }
    (archiveDatabaseName, archiveDomainName, archiveTableName)
  }

  protected def computeEffectiveInputSchema(): SchemaInfo = {
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        val fileForSchemaExtract = path.map(_.toString).headOption
        (mergedMetadata.resolveWithHeader(), fileForSchemaExtract) match {
          case (java.lang.Boolean.TRUE, Some(sourceFile)) =>
            val csvHeaders = storageHandler.readAndExecute(
              new Path(sourceFile),
              Charset.forName(mergedMetadata.resolveEncoding())
            ) { is =>
              Using.resource(is) { reader =>
                require(
                  mergedMetadata.resolveQuote().length <= 1,
                  "quote must be a single character"
                )
                require(
                  mergedMetadata.resolveEscape().length <= 1,
                  "escape must be a single character"
                )
                val csvParserSettings = new CsvParserSettings()
                val format = new CsvFormat()
                format.setDelimiter(mergedMetadata.resolveSeparator())
                mergedMetadata.resolveQuote().headOption.foreach(format.setQuote)
                mergedMetadata.resolveEscape().headOption.foreach(format.setQuoteEscape)
                csvParserSettings.setFormat(format)
                // allocate twice the declared columns. If fail a strange exception is thrown: https://github.com/uniVocity/univocity-parsers/issues/247
                csvParserSettings.setMaxColumns(starlakeSchema.attributes.length * 2)
                csvParserSettings.setNullValue(mergedMetadata.resolveNullValue())
                csvParserSettings.setHeaderExtractionEnabled(true)
                csvParserSettings.setMaxCharsPerColumn(-1)
                val csvParser = new CsvParser(csvParserSettings)
                csvParser.beginParsing(reader)
                // call this in order to get the headers even if there is no record
                csvParser.parseNextRecord()
                csvParser.getRecordMetadata.headers().toList
              }
            }
            val attributesMap = starlakeSchema.attributes.map(attr => attr.name -> attr).toMap
            val csvAttributesInOrders =
              csvHeaders.flatMap(h =>
                Option(h) match {
                  case Some(value) if value.nonEmpty =>
                    Some(
                      attributesMap.getOrElse(
                        h,
                        TableAttribute(h, ignore = Some(true), required = None)
                      )
                    )
                  case _ =>
                    logger.warn(
                      s"Found an non defined header in $fileForSchemaExtract. It has been ignored automatically."
                    )
                    None
                }
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ starlakeSchema.attributes.diff(csvAttributesInOrders)
            starlakeSchema.copy(attributes = effectiveAttributes)
          case _ => starlakeSchema
        }
      case _ => starlakeSchema
    }
  }

  def secondStepSQLTask(
    firstStepTempTableNames: List[String]
  ): AutoTask = {
    // val incomingSparkSchema = starlakeSchema.targetSparkSchemaWithoutIgnore(schemaHandler)

    val tempTable =
      if (firstStepTempTableNames.length == 1) firstStepTempTableNames.head
      else
        firstStepTempTableNames
          .map("SELECT * FROM " + _)
          .mkString("(", " UNION ALL ", ")")

    val queryEngine = settings.appConfig.jdbcEngines.get(engineName.toString)
    val sqlWithTransformedFields =
      starlakeSchema.buildSecondStepSqlSelectOnLoad(tempTable, queryEngine)

    val taskDesc = AutoTaskInfo(
      name = starlakeSchema.finalName,
      sql = Some(sqlWithTransformedFields),
      database = schemaHandler.getDatabase(domain),
      domain = domain.finalName,
      table = starlakeSchema.finalName,
      presql = starlakeSchema.presql,
      postsql = starlakeSchema.postsql,
      sink = mergedMetadata.sink,
      rls = starlakeSchema.rls,
      expectations = starlakeSchema.expectations,
      acl = starlakeSchema.acl,
      comment = starlakeSchema.comment,
      tags = starlakeSchema.tags,
      writeStrategy = mergedMetadata.writeStrategy,
      parseSQL = Some(true),
      connectionRef = Option(mergedMetadata.getSinkConnectionRef())
    )
    val context = TransformContext(
      appId = Option(ingestionJob.applicationId()),
      taskDesc = taskDesc,
      commandParameters = Map.empty,
      interactive = None,
      truncate = false,
      test = false,
      logExecution = true,
      accessToken = None,
      resultPageSize = 200,
      resultPageNumber = 1,
      dryRun = false,
      scheduledDate = scheduledDate,
      syncSchema = false
    )(settings, storageHandler, schemaHandler)
    context.toTask(engine)
  }

  def secondStepSQL(
    firstStepTempTableNames: List[String]
  ): WorkflowStatements = {
    val task = secondStepSQLTask(firstStepTempTableNames)
    WorkflowStatements(
      task.buildListOfSQLStatements(), // main
      task.expectationStatements(), // expectations
      task.auditStatements(), // audit
      task.aclSQL() // acl
    )
  }

  def expectationStatements(): List[ExpectationSQL] = {
    if (settings.appConfig.expectations.active) {
      // TODO Implement Expectations
      new ExpectationJob(
        Option("ignore"),
        database,
        domain.finalName,
        starlakeSchema.finalName,
        starlakeSchema.expectations,
        storageHandler,
        schemaHandler,
        new JdbcExpectationAssertionHandler(sinkConnection.options),
        false
      ).buildStatementsList() match {
        case Success(expectations) =>
          expectations
        case Failure(e) =>
          throw e
      }
    } else {
      List.empty
    }
  }

  val now = new Timestamp(System.currentTimeMillis())
  val auditLog = AuditLog(
    jobid = "ignore",
    paths = Some(targetFullTableName),
    domain = domain.finalName,
    schema = starlakeSchema.finalName,
    success = true,
    count = 0,
    countAccepted = -1,
    countRejected = -1,
    timestamp = now,
    duration = 0,
    message = "",
    step = Step.LOAD.toString,
    database = database,
    tenant = settings.appConfig.tenant,
    test = false,
    scheduledDate = scheduledDate
  )
  def auditStatements(): Option[TaskSQLStatements] = {
    if (settings.appConfig.audit.active.getOrElse(true)) {
      val auditStatements =
        AuditLog.buildListOfSQLStatements(List(auditLog), accessToken)(
          settings,
          storageHandler,
          schemaHandler
        )
      auditStatements
    } else {
      None
    }
  }

  lazy val tempStage = s"starlake_load_stage_${Random.alphanumeric.take(10).mkString("")}"

  def getIncomingDir(): String = domain.resolveDirectory()

  def buildSQLStatements(): Map[String, Object] = {
    val twoSteps = this.twoSteps
    val targetFullTableName = s"${domain.finalName}.${starlakeSchema.finalName}"
    val tempTableName = s"${domain.finalName}.${this.tempTableName}"
    val incomingDir = getIncomingDir()
    val pattern = starlakeSchema.pattern.toString
    val format = mergedMetadata.resolveFormat()

    val ddlMap: Map[String, Map[String, String]] =
      schemaHandler.getDdlMapping(starlakeSchema.attributes)
    val options =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, targetFullTableName, sinkConnection.options)

    val connectionPreActions =
      sinkConnection.options.get("preActions").map(_.split(';')).getOrElse(Array.empty).toList

    val tempSparkSchema =
      starlakeSchema.sparkSchemaWithIgnoreAndScript(schemaHandler, withFinalName = false)
    val finalSparkSchema =
      starlakeSchema.sparkSchemaWithIgnoreAndScript(schemaHandler, withFinalName = true)

    val stepMap =
      if (twoSteps) {
        val (tempCreateSchemaSql, tempCreateTableSql, _) = SparkUtils.buildCreateTableSQL(
          sinkConnection.getJdbcEngineName().toString,
          tempTableName,
          tempSparkSchema,
          caseSensitive = false,
          temporaryTable = true,
          options,
          ddlMap
        )
        val schemaString = SparkUtils.sqlSchemaString(
          finalSparkSchema,
          caseSensitive = false,
          options.url,
          ddlMap,
          0
        )

        val firstSTepCreateTableSqls = List(tempCreateSchemaSql, tempCreateTableSql)
        val extraFileNameColumn =
          s"ALTER TABLE $tempTableName ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '{{sl_input_file_name}}';"
        val workflowStatements = this.secondStepSQL(List(tempTableName))

        val dropFirstStepTableSql = s"DROP TABLE IF EXISTS $tempTableName;"
        val loadTaskSQL = Map(
          "steps"               -> "2",
          "incomingDir"         -> incomingDir,
          "pattern"             -> pattern,
          "format"              -> format,
          "firstStep"           -> firstSTepCreateTableSqls.asJava,
          "extraFileNameColumn" -> List(extraFileNameColumn).asJava,
          "secondStep"          -> workflowStatements.task.asMap().asJava,
          "dropFirstStep"       -> dropFirstStepTableSql,
          "tempTableName"       -> tempTableName,
          "targetTableName"     -> targetFullTableName,
          "domain"              -> domain.finalName,
          "table"               -> starlakeSchema.finalName,
          "writeStrategy"       -> writeDisposition,
          "schemaString"        -> schemaString,
          "preActions"          -> connectionPreActions.asJava
        )

        workflowStatements
          .asMap()
          .updated(
            "statements",
            loadTaskSQL.asJava
          )
      } else {
        val (createSchemaSql, createTableSql, _) = SparkUtils.buildCreateTableSQL(
          sinkConnection.getJdbcEngineName().toString,
          targetFullTableName,
          finalSparkSchema,
          caseSensitive = false,
          temporaryTable = false,
          options,
          ddlMap
        )
        val createTableSqls = List(createSchemaSql, createTableSql)
        val workflowStatements = this.secondStepSQL(List(targetFullTableName))
        val loadTaskSQL =
          Map(
            "steps"           -> "1",
            "incomingDir"     -> incomingDir,
            "pattern"         -> pattern,
            "format"          -> format.toString,
            "createTable"     -> createTableSqls.asJava,
            "targetTableName" -> targetFullTableName,
            "domain"          -> domain.finalName,
            "table"           -> starlakeSchema.finalName,
            "writeStrategy"   -> writeDisposition,
            "preActions"      -> connectionPreActions.asJava
          )
        workflowStatements
          .asMap()
          .updated(
            "statements",
            loadTaskSQL.asJava
          )
      }
    val engine = settings.appConfig.jdbcEngines(engineName.toString)

    val tempStage = s"starlake_load_stage_${Random.alphanumeric.take(10).mkString("")}"
    val commonOptionsMap = Map(
      "schema"     -> starlakeSchema.asMap().asJava,
      "sink"       -> sink.asMap(engine).asJava,
      "fileSystem" -> settings.appConfig.fileSystem,
      "tempStage"  -> tempStage,
      "connection" -> sinkConnection.asMap(),
      "variant" -> starlakeSchema.attributes
        .exists(
          _.primitiveType(schemaHandler).getOrElse(PrimitiveType.string) == PrimitiveType.variant
        )
        .toString
    )
    val result = stepMap ++ commonOptionsMap
    result
  }

  lazy val effectiveSchema: SchemaInfo = computeEffectiveInputSchema()
  lazy val schemaWithMergedMetadata: SchemaInfo =
    effectiveSchema.copy(metadata = Some(mergedMetadata))

}
