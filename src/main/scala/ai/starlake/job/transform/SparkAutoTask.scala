package ai.starlake.job.transform

import ai.starlake.config.{Settings, SparkSessionBuilder}
import ai.starlake.extract.{JdbcDbUtils, SparkExtractorJob}
import ai.starlake.job.metrics.{ExpectationAssertionHandler, SparkExpectationAssertionHandler}
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.job.sink.es.{ESLoadConfig, ESLoadJob}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io.{BufferedReader, FileReader}
import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try, Using}

/** SparkAutoTask executes SQL transformations using Apache Spark.
  *
  * ==Overview==
  * This is the most versatile AutoTask implementation, capable of:
  *   - Reading from Hive/Spark tables, JDBC databases, BigQuery, and filesystems
  *   - Writing to Hive/Spark tables, JDBC databases, BigQuery, Elasticsearch, and Kafka
  *   - Executing PySpark scripts
  *   - Delegating to BigQueryAutoTask or JdbcAutoTask when native execution is required
  *
  * ==Execution Modes==
  *   - '''Spark-to-Spark''': Both source and sink are Spark/Hive tables (fastest)
  *   - '''Spark-to-Any''': Source is Spark, sink is external (JDBC, BigQuery, etc.)
  *   - '''Any-to-Spark''': Source is external, sink is Spark (uses Spark connectors)
  *
  * ==Delegation Pattern==
  * When the sink requires native execution (e.g., BigQuery MERGE, JDBC transactions), SparkAutoTask
  * delegates to the appropriate native task using the `delegateContext()` helper method and
  * `TransformContext` factory methods.
  *
  * ==Key Methods==
  *   - `run()` - Main entry point, dispatches to appropriate execution method
  *   - `runSparkOnSpark()` - Executes SQL entirely within Spark
  *   - `runSparkOnAny()` - Builds DataFrame and sinks to external target
  *   - `sinkToBigQuery()` - Delegates to BigQueryAutoTask for native BQ operations
  *   - `sinkToJDBC()` - Delegates to JdbcAutoTask for native JDBC operations
  *
  * @param schema
  *   Optional Starlake schema for the target table (used for type mapping)
  */
class SparkAutoTask(
  appId: Option[String],
  taskDesc: AutoTaskInfo,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int,
  resultPageNumber: Int,
  schema: Option[SchemaInfo] = None,
  scheduledDate: Option[String],
  syncSchema: Boolean
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
      truncate,
      resultPageSize,
      resultPageNumber,
      accessToken,
      None,
      scheduledDate,
      syncSchema
    ) {

  /** Main entry point for task execution.
    *
    * ==Execution Flow==
    *   1. If interactive mode: Execute SQL and return results without materializing 2. If FS-to-FS
    *      (Spark-to-Spark):
    *      - Export mode: Use runSparkOnAny() to write to filesystem
    *      - Table mode: Use runSparkOnSpark() for native Spark execution 3. Otherwise: Use
    *        runSparkOnAny() which handles external sinks
    */
  override def run(): Try[JobResult] = {
    val result =
      interactive match {
        case Some(_) =>
          // Interactive mode: just execute the query and return results
          runSparkOnSpark(taskDesc.getSql())
        case None =>
          // Batch mode: determine execution path based on source and sink types
          (taskDesc.getRunConnection().`type`, sinkConnection.`type`) match {
            case (
                  ConnectionType.FS,
                  ConnectionType.FS
                ) =>
              // Both source and sink are filesystem/Spark - most efficient path
              if (sinkConfig match { case fs: FsSink => fs.isExport(); case _ => false }) {
                runSparkOnAny() // Export to filesystem (CSV, JSON, Parquet files)
              } else {
                runSparkOnSpark(taskDesc.getSql()) // Native Spark table operations
              }

            case _ =>
              // Mixed source/sink types - build DataFrame and sink to external target
              runSparkOnAny()
          }
      }
    result
  }

  def applyHiveTableAcl(): Try[Unit] =
    Try {
      val isGrantSupported =
        if (SparkSessionBuilder.isSparkConnectActive) true
        else
          Try(
            session.sessionState.sqlParser.parseExpression(
              "grant select on table secured_table to role my_role"
            )
          ).isSuccess
      if (isGrantSupported) {
        if (settings.appConfig.accessPolicies.apply) {
          val sqls = this.aclSQL()
          sqls.foreach { sql =>
            logger.info(sql)
            SparkUtils.sql(session, sql)
          }
        }
      } else {
        logger.warn(
          "GRANT are not supported in this version of Spark."
        )
      }
    }

  val fullTableName = s"${taskDesc.domain}.${taskDesc.table}"

  private def sinkToES(dataframe: DataFrame): Try[JobResult] = {
    val sink: EsSink = this.taskDesc.sink
      .map(_.getSink())
      .collect { case es: EsSink => es }
      .getOrElse(
        throw new Exception("Sink of type ES must be specified when loading data to ES !!!")
      )
    val esConfig =
      ESLoadConfig(
        timestamp = sink.timestamp,
        id = sink.id,
        format = settings.appConfig.defaultWriteFormat,
        domain = taskDesc.domain,
        schema = taskDesc.table,
        dataset = Some(Right(dataframe)),
        options = sink.getOptions()
      )

    val res = new ESLoadJob(esConfig, storageHandler, schemaHandler).run()
    res

  }

  /** @param dataframe
    * @param schema
    *   this schema to set the JSON metadata in the BQ table as a JSON column
    * @return
    */
  def sink(dataframe: DataFrame, schema: Option[SchemaInfo] = None): Boolean = {
    val sink = this.sinkConfig
    logger.info(s"Sinking data to $sink")
    Utils.printOut(s"""
        |Table/View: $fullTableName
        |Connection: ${sink.connectionRef}(${taskDesc.getSinkConnectionType()})
        |""".stripMargin)
    val result =
      sink match {
        case _: EsSink =>
          sinkToES(dataframe)

        case _: FsSink =>
          sinkToFile(dataframe)

        case _: BigQuerySink =>
          sinkToBQ(dataframe, schema)

        case _: JdbcSink =>
          sinkToJDBC(dataframe)

        case _: KafkaSink =>
          sinkToKafka(dataframe)
        case _ =>
          dataframe.write.format("console").save()
          throw new Exception(
            s"No supported Sink is activated for this job $sink, dumping to console"
          )

      }
    Utils.throwFailure(result, logger)
  }

  private def sinkToKafka(mergedDF: DataFrame): Try[DataFrame] = Try {
    Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
      kafkaClient.sinkToTopic(
        settings.appConfig.kafka.topics(taskDesc.table),
        mergedDF
      )
    }
    mergedDF
  }

  def buildDataframeFromBigQuery(): Option[DataFrame] = {
    val config = BigQueryLoadConfig(
      connectionRef = Some(this.taskDesc.getRunConnectionRef()),
      accessToken = accessToken
    )
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    val result = new BigQuerySparkJob(config).query(sqlWithParameters)
    result match {
      case Success(df) => Some(df)
      case Failure(e) =>
        logger.error("BigQuery query failed", e)
        throw e
    }
  }

  def buildDataframeFromJdbc(): Option[DataFrame] = {
    val runConnection = taskDesc.getRunConnection()
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    val res = session.read
      .format(
        runConnection.sparkDatasource().getOrElse("jdbc")
      )
      .option("query", sqlWithParameters)
      .options(runConnection.options)
      .load()
    Some(res)
  }

  def runSparkOnAny(): Try[SparkJobResult] = Try {
    val dataFrameToSink = buildDataFrameToSink()
    if (interactive.isEmpty) {
      dataFrameToSink.map { df =>
        sink(df)
      }
    }
    SparkJobResult(dataFrameToSink, None)
  }

  def buildDataframeFromFS(): Option[DataFrame] = {
    val sqlWithParameters =
      schemaHandler.substituteRefTaskMainSQL(taskDesc.getSql(), taskDesc.getRunConnection())
    runSqls(List(sqlWithParameters), "Main")
  }

  private def buildDataFrameToSink(): Option[DataFrame] = {
    // PySpark tasks handle their own data loading and register result as SL_THIS
    taskDesc.python match {
      case Some(pythonFile) =>
        return runPySpark(pythonFile)
      case None => // continue with SQL-based DataFrame building
    }

    val runConnectionType = taskDesc.getRunConnectionType()
    val runEngine: Engine = taskDesc.getRunEngine()

    val dataframe =
      (runEngine, runConnectionType) match {
        case (Engine.SPARK, ConnectionType.FS) =>
          buildDataframeFromFS()
        case (Engine.SPARK, ConnectionType.BQ) =>
          buildDataframeFromBigQuery()
        case (Engine.SPARK, ConnectionType.JDBC) =>
          buildDataframeFromJdbc()
        case (Engine.BQ, ConnectionType.BQ) =>
          buildDataframeFromBigQuery()
        case (Engine.JDBC, ConnectionType.JDBC) =>
          buildDataframeFromJdbc()
        case _ =>
          throw new Exception(
            s"Unsupported engine $runEngine and connection type $runConnectionType"
          )
      }
    dataframe
  }

  def runSparkOnSpark(sql: String): Try[SparkJobResult] = {
    val start = Timestamp.from(Instant.now())
    Try {
      if (taskDesc._dbComment.nonEmpty || taskDesc.tags.nonEmpty) {
        val domainComment = taskDesc._dbComment.getOrElse("")
        val tableTagPairs = Utils.extractTags(taskDesc.tags) + ("comment" -> domainComment)
        val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
        SparkUtils.sql(
          session,
          s"CREATE SCHEMA IF NOT EXISTS ${taskDesc.domain} WITH DBPROPERTIES($tagsAsString)"
        )
      } else if (taskDesc.domain != "__ignore__") {
        SparkUtils.createSchema(session, taskDesc.domain)
      }
      val jobResult = interactive match {
        case Some(_) =>
          // we replace any ref in the sql
          val sqlNoRefs = schemaHandler.substituteRefTaskMainSQL(
            sql,
            taskDesc.getRunConnection(),
            allVars
          )
          val pageSize = if (resultPageSize > settings.appConfig.maxInteractiveRecords) {
            settings.appConfig.maxInteractiveRecords
          } else {
            resultPageSize
          }
          // just run the request and return the dataframe
          val df =
            SparkUtils
              .sql(session, sqlNoRefs)
              .limit(pageSize)
              .offset((resultPageNumber - 1) * pageSize)
          SparkJobResult(Some(df), None)
        case None =>
          runSqls(preSql, "Pre")
          val jobResult =
            (sql, taskDesc.python) match {
              case (_, None) =>
                // we need to generate the insert / merge / create table except when exporting to csv & xls
                val sqlToRun = buildAllSQLQueriesMerged(Some(sql))
                val result = runSqls(sqlToRun.splitSql(), "Main")
                result

              case ("", Some(pythonFile)) =>
                runPySpark(pythonFile)
              case (_, _) =>
                throw new Exception(
                  s"Only one of 'sql' or 'python' attribute may be defined ${taskDesc.name}"
                )
            }
          runSqls(postSql, "Post")
          if (taskDesc._auditTableName.isEmpty) {
            if (taskDesc.comment.nonEmpty || taskDesc.tags.nonEmpty) {
              val tableComment = taskDesc.comment.getOrElse("")
              val tableTagPairs = Utils.extractTags(taskDesc.tags) + ("comment" -> tableComment)
              val tagsAsString = tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
              SparkUtils.sql(
                session,
                s"ALTER TABLE $fullTableName SET TBLPROPERTIES($tagsAsString)"
              )
              if (settings.appConfig.autoExportSchema) {
                val domains = new SparkExtractorJob(Map(taskDesc.domain -> List(taskDesc.table)))
                  .schemasAndTables()
                domains match {
                  case Success(domains) =>
                    schemaHandler.saveToExternals(domains)
                  case Failure(e) =>
                    logger.error(s"Failed to extract domain", e)
                }
              }
            }
            applyHiveTableAcl()
          }
          if (settings.appConfig.expectations.active && !taskDesc.isAuditTable()) {
            runAndSinkExpectations()
          }
          applyHiveTableAcl()
          SparkJobResult(jobResult, None)
      }
      val end = Timestamp.from(Instant.now())
      if (logExecution)
        logAuditSuccess(start, end, -1, test)
      jobResult
    } recoverWith { case e: Exception =>
      val end = Timestamp.from(Instant.now())
      logAuditFailure(start, end, e, test)
      Failure(e)
    }
  }

  override protected def expectationAssertionHandler: ExpectationAssertionHandler =
    new SparkExpectationAssertionHandler(session)

  private def runPySpark(pythonFile: Path): Option[DataFrame] = {
    if (SparkSessionBuilder.isSparkConnectActive) {
      throw new UnsupportedOperationException(
        "PySpark execution is not supported with Spark Connect. Use a native Spark cluster instead."
      )
    }
    SparkUtils.runPySpark(pythonFile, commandParameters)
    val exists = SparkUtils.tableExists(session, "SL_THIS")
    if (exists)
      Some(session.table("SL_THIS"))
    else
      None
  }

  def runSqls(sqls: List[String], typ: String): Option[DataFrame] = {
    if (sqls.nonEmpty) {
      val df = sqls.map { sql =>
        SparkUtils.sql(session, sql)
      }.lastOption
      df
    } else {
      None
    }
  }

  def createAuditTable(): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    logger.info(s"Table ${taskDesc.table} not found in ${taskDesc.domain}")
    val entry = taskDesc._auditTableName.getOrElse(
      throw new Exception(
        s"audit table for output ${taskDesc.table} is not defined in engine $jdbcSinkEngineName"
      )
    )
    val scriptTemplate = jdbcSinkEngine.tables(entry).createSql
    val script = scriptTemplate.richFormat(
      Map("table" -> fullTableName, "writeFormat" -> settings.appConfig.defaultWriteFormat),
      Map.empty
    )

    SparkUtils.createSchema(session, fullDomainName)
    Try(SparkUtils.sql(session, script)).isSuccess

  }

  /** Creates a TransformContext for delegating to other AutoTask types.
    *
    * ==Delegation Pattern==
    * When SparkAutoTask needs to delegate to BigQueryAutoTask or JdbcAutoTask (e.g., for native
    * MERGE operations or JDBC transactions), this helper method creates a TransformContext with the
    * current task's parameters.
    *
    * This eliminates code duplication - instead of repeating 15+ constructor parameters for each
    * delegation, we create a context and use factory methods:
    * {{{
    * // Before (20+ lines of duplicated code):
    * val bqJob = new BigQueryAutoTask(appId, taskDesc, commandParameters, ...)
    *
    * // After (2 lines):
    * val bqJob = TransformContext.createBigQueryTask(delegateContext())
    * }}}
    *
    * @param taskDescOverride
    *   Override the task description (e.g., for second-step tasks)
    * @param commandParametersOverride
    *   Override command parameters
    * @param interactiveOverride
    *   Override interactive mode
    * @param truncateOverride
    *   Override truncate flag
    * @return
    *   TransformContext configured for delegation
    */
  private def delegateContext(
    taskDescOverride: AutoTaskInfo = this.taskDesc,
    commandParametersOverride: Map[String, String] = Map.empty,
    interactiveOverride: Option[String] = None,
    truncateOverride: Boolean = false
  ): TransformContext = {
    TransformContext(
      appId = Option(applicationId()),
      taskDesc = taskDescOverride,
      commandParameters = commandParametersOverride,
      interactive = interactiveOverride,
      truncate = truncateOverride,
      test = test,
      logExecution = logExecution,
      accessToken = this.accessToken,
      resultPageSize = resultPageSize,
      resultPageNumber = resultPageNumber,
      dryRun = false,
      scheduledDate = scheduledDate,
      syncSchema = false
    )(settings, storageHandler, schemaHandler)
  }

  /** Checks if the target table exists in the sink.
    *
    * Delegates to the appropriate engine-specific check based on sink type:
    *   - FsSink: Uses SparkUtils.tableExists (Hive metastore)
    *   - BigQuerySink: Delegates to BigQueryAutoTask
    *   - JdbcSink: Delegates to JdbcAutoTask
    */
  override def tableExists: Boolean = {
    val sink = this.sinkConfig
    val result = {
      Try {
        sink match {
          case _: FsSink =>
            val exists = SparkUtils.tableExists(session = session, tableName = fullTableName)
            if (!exists && taskDesc._auditTableName.isDefined) {
              createAuditTable()
            } else
              exists

          case _: BigQuerySink =>
            val bqJob = TransformContext.createBigQueryTask(delegateContext())
            bqJob.tableExists
          case _: JdbcSink =>
            val jdbcJob = TransformContext.createJdbcTask(delegateContext())
            jdbcJob.tableExists
          case other =>
            throw new Exception(
              s"No supported on $other"
            )

        }
      }
    }
    Utils.throwFailure(result, logger)
    logger.info(s"tableExists $fullTableName: $result")
    result.getOrElse(false)
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// SPARK SINK ///////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  private def updateSparkTableSchema(incomingSchema: StructType): Unit = {
    val incomingSchemaWithSCD2Support =
      if (writeStrategy.getEffectiveType() == WriteStrategyType.SCD2) {
        val startTs = writeStrategy.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTs = writeStrategy.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)

        val scd2FieldsFound =
          incomingSchema.fields.exists(_.name.toLowerCase() == startTs.toLowerCase())

        if (!scd2FieldsFound) {
          val incomingSchemaWithScd2 =
            incomingSchema
              .add(
                StructField(
                  startTs,
                  TimestampType,
                  nullable = true
                )
              )
              .add(
                StructField(
                  endTs,
                  TimestampType,
                  nullable = true
                )
              )
          incomingSchemaWithScd2
        } else {
          incomingSchema
        }

      } else {
        incomingSchema
      }

    if (tableExists) {
      // Load from accepted area
      // We provide the accepted DF schema since partition columns types are inferred when parquet is loaded and might not match with the DF being ingested
      val existingTableSchema = session.table(fullTableName).schema
      val newFields =
        MergeUtils.computeNewColumns(existingTableSchema, incomingSchemaWithSCD2Support)
      if (newFields.nonEmpty) {
        val colsAsString = newFields
          .map(field =>
            s"""${field.name} ${field.dataType.sql} comment "${field
                .getComment()
                .getOrElse("")}" """
          )
          .mkString(",")

        SparkUtils.sql(session, s"ALTER TABLE $fullTableName ADD COLUMNS ($colsAsString)")
      }
    } else {
      val sink = sinkConfig match {
        case fs: FsSink => fs
        case _ =>
          throw new IllegalStateException(
            s"Expected FsSink but got ${sinkConfig.getClass.getSimpleName}"
          )
      }

      val comment = taskDesc.comment.map(c => s"COMMENT '$c'").getOrElse("")
      val tableTagPairs = Utils.extractTags(taskDesc.tags)
      val tblProperties = {
        if (tableTagPairs.isEmpty) ""
        else
          tableTagPairs.map { case (k, v) => s"'$k'='$v'" }.mkString("TBLPROPERTIES(", ",", ")")
      }

      val fields = incomingSchemaWithSCD2Support.fields
        .map(field =>
          s"""${field.name} ${field.dataType.sql} comment "${field.getComment().getOrElse("")}" """
        )
        .mkString(",")

      SparkUtils.createSchema(session, taskDesc.domain)

      val ddlTable =
        s"""CREATE TABLE $fullTableName($fields)
           |USING ${sink.getStorageFormat()}
           |${sink.sparkTableOptionsClause()}
           |${sink.sparkPartitionByClauseSQL()}
           |${sink.sparkClusterByClauseSQL()}
           |$comment
           |$tblProperties
           |""".stripMargin
      logger.info(s"Creating table $fullTableName with DDL $ddlTable")
      session.sql(ddlTable)
    }
  }

  protected def effectiveSinkToFile(dataset: DataFrame): Try[JobResult] = {
    val allAttributes = dataset.schema.fieldNames.mkString(",")
    dataset.createOrReplaceTempView("SL_INTERNAL_VIEW")
    runSparkOnSpark(s"SELECT $allAttributes FROM SL_INTERNAL_VIEW")
  }

  private def sinkToFile(
    dataset: DataFrame
  ): Try[JobResult] = {
    // Ingestion done with Spark but not yet sinked.
    // This is called by sinkRejected and sinkAccepted
    // We check if the table exists before updating the table schema below
    val incomingSchema = dataset.schema
    val fsSink = sinkConfig match {
      case fs: FsSink => fs
      case _ =>
        throw new IllegalStateException(
          s"Expected FsSink but got ${sinkConfig.getClass.getSimpleName}"
        )
    }
    if (taskDesc._auditTableName.isEmpty && !fsSink.isExport()) {
      // We are not writing to an audit table. We are writing to the final table
      // Update the table schema and create it if required
      updateSparkTableSchema(incomingSchema)
    }

    val hasColumns: Boolean = dataset.columns.length > 0
    if (!hasColumns) {
      Success(SparkJobResult(None, None))
    } else {
      effectiveSinkToFile(dataset)
    }
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// BQ SINK //////////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////

  /** Sinks a DataFrame to BigQuery.
    *
    * ==Two-Step Process for MERGE Operations==
    * When using MERGE/SCD2 write strategies, BigQuery requires a two-step process:
    *   1. Write DataFrame to a temporary table using Spark BigQuery connector 2. Execute native
    *      BigQuery MERGE statement via BigQueryAutoTask
    *
    * ==Direct vs Indirect Write==
    *   - '''Direct''': Uses BigQuery Storage Write API (faster, but no variant support)
    *   - '''Indirect''': Uses GCS staging (slower, but supports all types)
    *
    * ==Variant Type Handling==
    * BigQuery's JSON type (variant) requires indirect write method. The code validates this and
    * throws an error if direct write is attempted with variants.
    *
    * @param loadedDF
    *   The DataFrame to sink to BigQuery
    * @param slSchema
    *   Optional Starlake schema for type mapping (especially variants)
    */
  private def sinkToBQ(loadedDF: DataFrame, slSchema: Option[SchemaInfo] = None): Try[JobResult] = {
    val twoSteps = writeStrategy.isMerge()
    val isDirect = sinkConnection.options.getOrElse("writeMethod", "direct") == "direct"
    val forceIndirect =
      slSchema.exists { schema =>
        val variantAttribute =
          schema.attributes.find(
            _.primitiveType(settings.schemaHandler())
              .getOrElse(PrimitiveType.string) == PrimitiveType.variant
          )
        (isDirect, variantAttribute) match {
          case (true, Some(attribute)) =>
            logger.warn(
              s"""Setting writeMethod to indirect as
                 |direct write method is not supported for BigQuery when sinking file with variant attribute.
                 |Please use indirect write method instead.
                 |schema: ${taskDesc.domain}.${schema.name}
                 |variant attribute: ${attribute.name}: ${attribute.`type`}
                 |set writeMethod: indirect
                 |in Connection: ${sinkConnection.options}
                 |""".stripMargin
            )
            true
          case _ =>
            false
        }
      }

    if (twoSteps) {
      val (overwriteCreateDisposition: String, overwriteWriteDisposition: String) =
        Utils.getDBDisposition(WriteMode.OVERWRITE)

      val bqTableSchema = Some(BigQuerySchemaConverters.toBigQuerySchema(loadedDF.schema))
      val tempTablePartName = SQLUtils.temporaryTableName(taskDesc.table)
      val firstStepTemplateTableId =
        BigQueryJobBase.extractProjectDatasetAndTable(
          taskDesc.database,
          taskDesc.domain,
          tempTablePartName,
          sinkConnection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
        )

      val config = BigQueryLoadConfig(
        connectionRef = Some(sinkConnectionRef),
        source = Right(loadedDF),
        outputTableId = Some(firstStepTemplateTableId),
        sourceFormat = settings.appConfig.defaultWriteFormat,
        createDisposition = overwriteCreateDisposition,
        writeDisposition = overwriteWriteDisposition,
        days = Some(1),
        outputDatabase = taskDesc.database,
        accessToken = accessToken,
        forceIndirect = forceIndirect
      )

      val sparkBigQueryJob = new BigQuerySparkJob(config, bqTableSchema, None)
      val firstStepJobResult = sparkBigQueryJob.run()

      firstStepJobResult match {
        case Success(_) =>
          val allAttributeNames = loadedDF.schema.fields.map(_.name)
          val attributesSelectAsString = allAttributeNames.mkString(",")

          val secondStepTaskDesc = taskDesc.copy(
            name = fullTableName,
            sql = Some(
              s"SELECT $attributesSelectAsString FROM ${taskDesc.domain}.$tempTablePartName"
            )
          )
          val secondStepContext = delegateContext(
            taskDescOverride = secondStepTaskDesc,
            commandParametersOverride = commandParameters,
            interactiveOverride = interactive,
            truncateOverride = truncate
          )
          val secondStepTask = TransformContext.createBigQueryTask(secondStepContext)
          val secondStepJobResult = secondStepTask.runNativeOnLoad(loadedDF.schema)
          sparkBigQueryJob.dropTable(firstStepTemplateTableId)
          secondStepJobResult
        case Failure(e) =>
          Failure(e)
      }
    } else {
      val secondStepDesc = taskDesc.copy(
        name = fullTableName,
        sql = None,
        attributes = slSchema.map(_.attributes).getOrElse(Attributes.from(loadedDF.schema))
      )
      // Update table schema
      val secondStepContext = delegateContext(
        taskDescOverride = secondStepDesc,
        commandParametersOverride = commandParameters,
        interactiveOverride = interactive,
        truncateOverride = truncate
      )
      val secondStepTask = TransformContext.createBigQueryTask(secondStepContext)

      val sparkSchema = loadedDF.schema
      val updateSchema = slSchema
        .map { slSchema =>
          val fields =
            sparkSchema.fields.map { field =>
              val slField = slSchema.attributes.find { attr =>
                attr.getFinalName().equalsIgnoreCase(field.name) &&
                attr.`type`.toLowerCase() == "variant"
              }
              slField
                .map { attr =>
                  val metadataBuilder = new MetadataBuilder()
                  metadataBuilder.putString("sqlType", "JSON")
                  attr.comment.foreach(metadataBuilder.putString("description", _))
                  val jsonField = field.copy(metadata = metadataBuilder.build())
                  jsonField
                }
                .getOrElse(field)
            }
          val updatedSchema = StructType(fields)
          updatedSchema
        }
        .getOrElse(sparkSchema)
      secondStepTask.runOnDFOnLoad(loadedDF, Some(updateSchema))

    }
  }

  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////
  //////////// JDBC SINK ///////////////////////////
  ///////////////////////////////////////////////////
  ///////////////////////////////////////////////////

  /** Sinks a DataFrame to a JDBC database.
    *
    * ==Two-Step Process for MERGE Operations==
    * When using MERGE/SCD2 write strategies, JDBC requires a two-step process:
    *   1. Write DataFrame to a temporary table using Spark JDBC writer 2. Execute native MERGE
    *      statement via JdbcAutoTask
    *
    * ==DuckDB Special Handling==
    * DuckDB connections require special initialization to create the database file if it doesn't
    * exist.
    *
    * ==PostgreSQL COPY Optimization==
    * For PostgreSQL with APPEND strategy, uses COPY command for bulk loading which is significantly
    * faster than row-by-row INSERT.
    *
    * @param loadedDF
    *   The DataFrame to sink to the JDBC database
    */
  def sinkToJDBC(loadedDF: DataFrame): Try[JobResult] = {
    val sinkConnectionRefOptions = sinkConnection.options

    val allAttributeNames = loadedDF.schema.fields.map(_.name)
    val attributesSelectAsString = allAttributeNames.mkString(",")

    /*
    val targetTableName = s"${taskDesc.domain}.${taskDesc.table}"
    val targetTableExists: Boolean = { // TODO declaration is never used
      JdbcDbUtils.withJDBCConnection(sinkConnectionRefOptions) { conn =>
        val url = sinkConnection.options("url")
        JdbcDbUtils.tableExists(conn, url, targetTableName)
      }
    }
     */

    // do nothing just create the database
    if (sinkConnection.isDuckDb()) {
      JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnectionRefOptions) {
        _ =>
      }
    }

    val twoSteps = writeStrategy.isMerge()
    val result =
      if (twoSteps) {
        val tablePartName = SQLUtils.temporaryTableName(taskDesc.table)
        val firstStepTempTable = s"${taskDesc.domain}.$tablePartName"

        if (settings.appConfig.createSchemaIfNotExists) {
          JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkConnectionRefOptions
          ) { conn =>
            JdbcDbUtils.createSchema(conn, taskDesc.domain)
          }
        }
        val jdbcUrl = sinkConnectionRefOptions("url")

        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnectionRefOptions) {
          conn =>
            SparkUtils.createTable(
              sinkConnection.getJdbcEngineName().toString,
              conn,
              firstStepTempTable,
              loadedDF.schema,
              caseSensitive = false,
              temporaryTable = false,
              new JdbcOptionsInWrite(jdbcUrl, firstStepTempTable, sinkConnectionRefOptions),
              attDdl()
            )
        }

        val timestamp = java.time.LocalDateTime
          .now()
          .format(
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
          )
        val tablePath =
          new Path(s"${settings.appConfig.datasets}/${firstStepTempTable}_$timestamp")

        if (settings.storageHandler().exists(tablePath)) {
          settings.storageHandler().delete(tablePath)
        }

        val isIndirectWriteMethod =
          sinkConnection.options.getOrElse("writeMethod", "direct") == "indirect"

        if (sinkConnection.isPostgreSql() && isIndirectWriteMethod) {
          val separator = sinkConnection.options.getOrElse("separator", ",")
          loadedDF.write
            .format("csv")
            .mode(SaveMode.Overwrite) // truncate done above if requested
            .option("separator", separator)
            .save(tablePath.toString)
          JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
            conn =>
              val manager = new CopyManager(conn.unwrap(classOf[BaseConnection]))
              storageHandler
                .list(tablePath, recursive = false, extension = "csv")
                .foreach { file =>
                  val localFile = StorageHandler.localFile(file.path).toString()
                  val copySql =
                    s"COPY $firstStepTempTable FROM '$localFile' DELIMITER ',' CSV HEADER"
                  Using.resource(new BufferedReader(new FileReader(localFile))) { reader =>
                    manager.copyIn(copySql, reader)
                  }
                }
          }
          settings.storageHandler().delete(tablePath)
        } else if (sinkConnection.isDuckDb()) {
          val colNames = loadedDF.schema.fields.map(_.name)
          loadedDF.write
            .format("parquet")
            .mode(SaveMode.Overwrite) // truncate done above if requested
            .save(tablePath.toString)
          JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkConnection.options
          ) { conn =>
            val quotedColumns =
              SQLUtils.quoteCols(colNames.toList, "\"")
            val sql =
              s"INSERT INTO $firstStepTempTable SELECT ${quotedColumns.mkString(",")} FROM '$tablePath/*.parquet'"
            JdbcDbUtils.executeUpdate(sql, conn)
          }
          settings.storageHandler().delete(tablePath)
        } else {
          loadedDF.write
            .format(sinkConnection.sparkDatasource().getOrElse("jdbc"))
            .option("dbtable", firstStepTempTable)
            .mode(SaveMode.Append) // truncate done above if requested
            .options(sinkConnection.options)
            .save()
        }

        logger.info(s"JDBC save done to table $firstStepTempTable")

        // We now have a table in the database.
        // We can now run the merge statement using the native SQL capabilities
        val secondStepTaskDesc =
          taskDesc.copy(
            name = fullTableName,
            sql = Some(s"SELECT $attributesSelectAsString FROM $firstStepTempTable")
          )

        val secondStepAutoTask =
          TransformContext.createJdbcTask(delegateContext(taskDescOverride = secondStepTaskDesc))
        if (tableExists)
          secondStepAutoTask.updateJdbcTableSchema(
            incomingSchema = loadedDF.schema,
            tableName = fullTableName,
            syncStrategy = TableSync.ALL,
            createIfAbsent = false
          )
        createDucklakePartitions()
        val jobResult = secondStepAutoTask.runJDBC(None)
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnectionRefOptions) {
          conn =>
            JdbcDbUtils.dropTable(conn, firstStepTempTable)
        }
        jobResult
      } else {
        // We have a OVERWRITE / APPEND strategy, we can write directly to the direct table using Spark.
        // No need to pass by an intermediary table

        val secondStepDesc = taskDesc.copy(
          name = fullTableName,
          sql = None
        )
        val secondAutoStepTask =
          TransformContext.createJdbcTask(delegateContext(taskDescOverride = secondStepDesc))
        secondAutoStepTask.updateJdbcTableSchema(
          incomingSchema = loadedDF.schema,
          tableName = fullTableName,
          syncStrategy = TableSync.ALL,
          createIfAbsent = true
        )
        createDucklakePartitions()
        val jobResult = secondAutoStepTask.runJDBC(Some(loadedDF))
        jobResult
      }
    result
  }

  private def createDucklakePartitions(): Unit = {
    if (sinkConnection.isDucklake()) {
      schema.foreach { slSchema =>
        val jdbcEngine = settings.appConfig.jdbcEngines("duckdb")
        val partitionClause =
          slSchema.metadata
            .flatMap(_.sink.flatMap(_.toAllSinks().getPartitionByClauseSQL(jdbcEngine)))
        partitionClause foreach { partitionClause =>
          val sql = s"ALTER TABLE $fullTableName SET $partitionClause;"
          JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkConnection.options
          ) { conn =>
            JdbcDbUtils.execute(sql, conn)
          }
        }
      }
    }
  }

  override def buildRLSQueries(): List[String] = ???
}

object SparkAutoTask extends LazyLogging {
  def executeUpdate(
    sql: String,
    connectionName: String,
    accessToken: Option[String]
  )(implicit iSettings: Settings): Try[Boolean] = {
    val job = new SparkJob {
      override def name: String = "BigQueryTablesInfo"

      override implicit def settings: Settings = iSettings

      /** Just to force any job to implement its entry point using within the "run" method
        *
        * @return
        *   : Spark Dataframe for Spark Jobs None otherwise
        */
      override def run(): Try[JobResult] = Try {
        val df = session.sql(sql)
        SparkJobResult(Option(df), None)
      }
    }

    val jobResult = job.run()
    jobResult match {
      case Success(_) =>
        Success(true)
      case Failure(e) =>
        logger.error("Error while executing SQL", e)
        Failure(e)
    }
  }
}
