package ai.starlake.extract

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.core.utils.StringUtils
import ai.starlake.exceptions.DataExtractionException
import ai.starlake.extract.JdbcDbUtils._
import ai.starlake.extract.LastExportUtils._
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{JDBCSchema, JDBCTable, PrimitiveType}
import ai.starlake.utils.Formatter.*
import ai.starlake.utils.{Utils, YamlSerde}
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.conversions.Conversions
import com.univocity.parsers.csv.{CsvFormat, CsvRoutines, CsvWriterSettings}
import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.sql.{Connection as SQLConnection, Date, PreparedStatement, ResultSet, Timestamp}
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try, Using}

case class InsertLastExportParams(deltaRow: DeltaRow, partitionColumnType: PrimitiveType)

class ExtractDataJob(schemaHandler: SchemaHandler) extends ExtractPathHelper with LazyLogging {

  @nowarn
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = {
    ExtractDataCmd.run(args, schemaHandler).map(_ => ())
  }

  /** Generate YML file from JDBC Schema stored in a YML file
    *
    * @param jdbcMapFile
    *   : Yaml File containing the JDBC Schema to extract
    * @param ymlOutputDir
    *   : Where to output the YML file. The generated filename will be in the for
    *   TABLE_SCHEMA_NAME.yml
    * @param settings
    *   : Application configuration file
    */
  def run(
    userConfig: UserExtractDataConfig
  )(implicit settings: Settings): Unit = {
    val extractConfigPath = mappingPath(DatasetArea.extract, userConfig.extractConfig)
    Try(settings.storageHandler().exists(extractConfigPath)) match {
      case Failure(_) | Success(false) =>
        throw new FileNotFoundException(
          s"Could not found extract userConfig ${userConfig.extractConfig}. Please check its existence."
        )
      case _ => // do nothing
    }
    val content = settings
      .storageHandler()
      .read(extractConfigPath)
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val jdbcSchemas =
      YamlSerde.deserializeYamlExtractConfig(content, userConfig.extractConfig)
    val dataConnectionSettings = jdbcSchemas.connectionRef match {
      case Some(connectionRef) => settings.appConfig.getConnection(connectionRef)
      case None                => throw new Exception(s"No connectionRef defined for jdbc schemas.")
    }
    val auditConnectionRef =
      jdbcSchemas.auditConnectionRef.getOrElse(settings.appConfig.audit.getConnectionRef())

    val auditConnectionSettings = settings.appConfig.getConnection(auditConnectionRef)
    val fileFormat = jdbcSchemas.output.getOrElse(FileFormat()).fillWithDefault()
    logger.info(s"Extraction will be formatted following $fileFormat")

    implicit val implicitSchemaHandler: SchemaHandler = schemaHandler
    ParUtils.withExecutor(userConfig.parallelism) { implicit extractEC =>
      implicit val extractExecutionContext =
        new ExtractExecutionContext(extractEC)
      val auditColumns = initExportAuditTable(auditConnectionSettings)
      ParUtils
        .runInParallel(
          jdbcSchemas.jdbcSchemas
            .getOrElse(Nil)
            .filter { s =>
              (userConfig.includeSchemas, userConfig.excludeSchemas) match {
                case (Nil, Nil) => true
                case (inc, Nil) => inc.map(_.toLowerCase).contains(s.schema.toLowerCase)
                case (Nil, exc) => !exc.map(_.toLowerCase).contains(s.schema.toLowerCase)
                case (_, _) =>
                  throw new RuntimeException(
                    "You can't specify includeShemas and excludeSchemas at the same time"
                  )
              }
            },
          userConfig.parallelism
        ) { jdbcSchema =>
          require(userConfig.numPartitions > 0)
          extractData(
            ExtractJdbcDataConfig(
              jdbcSchema,
              dataOutputDir(userConfig.outputDir),
              userConfig.limit,
              userConfig.numPartitions,
              userConfig.parallelism,
              userConfig.fullExport,
              userConfig.ifExtractedBefore
                .map(userTimestamp => lastTimestamp => lastTimestamp < userTimestamp),
              userConfig.ignoreExtractionFailure,
              userConfig.cleanOnExtract,
              userConfig.includeTables,
              userConfig.excludeTables ++ jdbcSchema.exclude,
              fileFormat,
              dataConnectionSettings.mergeOptionsWith(jdbcSchema.connectionOptions),
              auditConnectionSettings
            ),
            auditColumns
          )
        }
    }
  }

  /** Extract data and save to output directory
    *
    * @param schemaHandler
    * @param jdbcSchema
    *   All tables referencede here will be saved
    * @param connectionOptions
    *   jdbc connection options for the schema. specific conneciton options may be specified at the
    *   table level.
    * @param baseOutputDir
    *   data is saved in this directory. suffixed by datetime and parition index if any
    * @param limit
    *   For dev mode, it may be useful to extract only a subset of the data
    * @param separator
    *   data are saved as CSV files. this is the separator to use.
    * @param defaultNumPartitions
    *   Parallelism level for the extraction process
    * @param parallelism
    *   number of thread used during extraction
    * @param fullExportCli
    *   fullExport flag coming from cli. Has higher precedence than config files.
    * @param settings
    */
  private def extractData(
    extractConfig: ExtractJdbcDataConfig,
    auditColumns: Columns
  )(implicit
    settings: Settings,
    schemaHandler: SchemaHandler,
    dbExtractEC: ExtractExecutionContext
  ): Unit = {
    implicit val storageHandler = settings.storageHandler()

    // Some database accept strange chars (aka DB2). We get rid of them
    val domainName = extractConfig.jdbcSchema.sanitizeName match {
      case Some(true) =>
        StringUtils.replaceNonAlphanumericWithUnderscore(extractConfig.jdbcSchema.schema)
      case _ => extractConfig.jdbcSchema.schema
    }
    val tableOutputDir = createDomainOutputDir(extractConfig.baseOutputDir, domainName)

    val filteredJdbcSchema: JDBCSchema =
      computeEligibleTablesForExtraction(
        extractConfig.jdbcSchema,
        extractConfig.includeTables,
        extractConfig.excludeTables
      )

    val doTablesExtraction =
      isProcessTablesExtraction(extractConfig, filteredJdbcSchema)
    if (doTablesExtraction) {
      // Map tables to columns and primary keys
      val selectedTablesAndColumns: Map[TableName, ExtractTableAttributes] =
        JdbcDbUtils.extractJDBCTables(
          filteredJdbcSchema.copy(exclude = extractConfig.excludeTables.toList),
          extractConfig.data,
          skipRemarks = true,
          keepOriginalName = true,
          includeColumns = true
        )
      val globalStart = System.currentTimeMillis()
      val extractionResults: List[Try[Unit]] =
        ParUtils
          .runInParallel(selectedTablesAndColumns.toList, extractConfig.parallelism) {
            case (tableName, tableAttrs) =>
              Try {
                val context = s"[${extractConfig.jdbcSchema.schema}.$tableName]"

                // Get the current table partition column and  connection options if any
                val currentTableDefinition =
                  resolveTableDefinition(extractConfig.jdbcSchema, tableName)
                val currentTableConnectionOptions =
                  currentTableDefinition.map(_.connectionOptions).getOrElse(Map.empty)
                // get cols to extract and frame colums names with quotes to handle cols that are keywords in the target database
                val fullExport = isTableFullExport(extractConfig, currentTableDefinition)
                val fetchSize =
                  computeTableFetchSize(extractConfig, currentTableDefinition)
                val partitionConfig =
                  computePartitionConfig(
                    extractConfig,
                    domainName,
                    tableName,
                    currentTableDefinition,
                    tableAttrs
                  )
                val tableExtractDataConfig = TableExtractDataConfig(
                  domainName,
                  tableName,
                  currentTableDefinition.flatMap(_.sql),
                  tableAttrs.columNames,
                  fullExport,
                  fetchSize,
                  tableOutputDir,
                  tableAttrs.filterOpt,
                  partitionConfig
                )

                if (
                  isExtractionNeeded(
                    extractConfig,
                    tableExtractDataConfig,
                    auditColumns
                  )
                ) {
                  if (extractConfig.cleanOnExtract) {
                    cleanTableFiles(storageHandler, tableOutputDir, tableName)
                  }

                  extractTableData(
                    extractConfig.copy(data =
                      extractConfig.data.mergeOptionsWith(currentTableConnectionOptions)
                    ),
                    tableExtractDataConfig,
                    context,
                    auditColumns,
                    sinkPartitionToFile(
                      extractConfig.outputFormat,
                      tableExtractDataConfig,
                      _,
                      _,
                      _
                    )
                  )
                } else {
                  logger.info(s"Extraction skipped. $domainName.$tableName data is fresh enough.")
                  Success(())
                }
              }.flatten
          }
          .toList
      val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(globalStart)
      val nbFailures = extractionResults.count(_.isFailure)
      val dataExtractionFailures = extractionResults
        .flatMap {
          case Failure(e: DataExtractionException) => Some(s"${e.domain}.${e.table}")
          case _                                   => None
        }
        .mkString(", ")
      nbFailures match {
        case 0 =>
          logger.info(s"Extracted successfully all tables in $elapsedTime")
        case nb if extractConfig.ignoreExtractionFailure && dataExtractionFailures.nonEmpty =>
          logger.warn(s"Failed to extract $nb tables: $dataExtractionFailures")
        case nb if dataExtractionFailures.nonEmpty =>
          throw new RuntimeException(s"Failed to extract $nb tables: $dataExtractionFailures")
        case nb =>
          throw new RuntimeException(s"Encountered $nb failures during extraction")
      }
    } else {
      logger.info("Tables extraction skipped")
    }
  }

  private def computePartitionConfig(
    extractConfig: ExtractJdbcDataConfig,
    domainName: String,
    tableName: TableName,
    currentTableDefinition: Option[JDBCTable],
    tableAttrs: ExtractTableAttributes
  ): Option[PartitionConfig] = {
    resolvePartitionColumn(extractConfig, currentTableDefinition)
      .map { partitionColumn =>
        val numPartitions = resolveNumPartitions(extractConfig, currentTableDefinition)
        // Partition column type is useful in order to know how to compare values since comparing numeric, big decimal, date and timestamps are not the same
        val partitionColumnType = tableAttrs.columNames
          .find(_.name.equalsIgnoreCase(partitionColumn))
          .flatMap(attr => schemaHandler.types().find(_.name == attr.`type`))
          .map(_.primitiveType)
          .getOrElse(
            throw new Exception(
              s"Could not find column type for partition column $partitionColumn in table $domainName.$tableName"
            )
          )
        val stringPartitionFuncTpl =
          resolveTableStringPartitionFunc(extractConfig, currentTableDefinition)
        PartitionConfig(
          partitionColumn,
          partitionColumnType,
          stringPartitionFuncTpl,
          numPartitions
        )
      }
  }

  private[extract] def resolveTableStringPartitionFunc(
    extractConfig: ExtractJdbcDataConfig,
    currentTableDefinition: Option[JDBCTable]
  ): Option[TableName] = {
    currentTableDefinition
      .flatMap(_.stringPartitionFunc)
      .orElse(extractConfig.jdbcSchema.stringPartitionFunc)
      .orElse(
        getStringPartitionFunc(extractConfig.data.getJdbcEngineName().toString)
      )
  }

  private[extract] def resolveNumPartitions(
    extractConfig: ExtractJdbcDataConfig,
    currentTableDefinition: Option[JDBCTable]
  ): Int = {
    currentTableDefinition
      .flatMap { tbl =>
        tbl.numPartitions
      }
      .orElse(extractConfig.jdbcSchema.numPartitions)
      .getOrElse(extractConfig.numPartitions)
  }

  private[extract] def resolvePartitionColumn(
    extractConfig: ExtractJdbcDataConfig,
    currentTableDefinition: Option[JDBCTable]
  ) = {
    currentTableDefinition
      .flatMap(_.partitionColumn)
      .orElse(extractConfig.jdbcSchema.partitionColumn)
  }

  private[extract] def cleanTableFiles(
    storageHandler: StorageHandler,
    tableOutputDir: Path,
    tableName: TableName
  ): Unit = {
    logger.info(s"Deleting all files of $tableName")
    storageHandler
      .list(tableOutputDir, recursive = false)
      .filter(fileInfo =>
        s"^$tableName-\\d{14}[\\.\\-].*".r.pattern
          .matcher(fileInfo.path.getName)
          .matches()
      )
      .foreach { f =>
        storageHandler.delete(f.path)
        logger.debug(f"${f.path} deleted")
      }
  }

  /** Table fetch size precedence from higher to lower is:
    *   - table definition (may be propagated with * config)
    *   - schema definition (propagated with default one)
    *
    * If not set, default from JDBC driver is used
    */
  private[extract] def computeTableFetchSize(
    extractConfig: ExtractJdbcDataConfig,
    currentTableDefinition: Option[JDBCTable]
  ): Option[Int] = {
    currentTableDefinition
      .flatMap(_.fetchSize)
      .orElse(extractConfig.jdbcSchema.fetchSize)
  }

  /** fullExport precedence from higher to lower is:
    *   - cli config (can only force all tables to be incremental. Impact tables with
    *     partitionColumn define)
    *   - table definition (may be propagated with * config)
    *   - schema definition (propagated with default one)
    *   - true if not defined
    */
  private[extract] def isTableFullExport(
    extractConfig: ExtractJdbcDataConfig,
    currentTableDefinition: Option[JDBCTable]
  ): Boolean = {
    extractConfig.cliFullExport
      .orElse(
        currentTableDefinition
          .flatMap(_.fullExport)
      )
      .orElse(extractConfig.jdbcSchema.fullExport)
      .getOrElse(
        true
      )
  }

  /** With includeSchemas we may have tables empty so we should ignore tables. If empty, by default
    * we fetch all tables.
    * @param extractConfig
    * @param filteredJdbcSchema
    * @return
    */
  private def isProcessTablesExtraction(
    extractConfig: ExtractJdbcDataConfig,
    filteredJdbcSchema: JDBCSchema
  ) = {
    extractConfig.jdbcSchema.tables.isEmpty || filteredJdbcSchema.tables.nonEmpty
  }

  def isExtractionNeeded(
    extractDataConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    auditColumns: Columns
  )(implicit settings: Settings, dbExtractEC: ExtractExecutionContext): Boolean = {
    extractDataConfig.extractionPredicate
      .flatMap { predicate =>
        ParUtils
          .runOneInExecutionContext {
            withJDBCConnection(
              this.schemaHandler.dataBranch(),
              extractDataConfig.audit.options
            ) { connection =>
              LastExportUtils.getMaxTimestampFromSuccessfulExport(
                connection,
                extractDataConfig,
                tableExtractDataConfig,
                "start_ts",
                auditColumns
              )
            }
          }(dbExtractEC.executionContext)
          .map(t => predicate(t.getTime))
      }
      .getOrElse(true)
  }

  private[extract] def resolveTableDefinition(
    jdbcSchema: JDBCSchema,
    tableName: TableName
  ): Option[JDBCTable] = {
    jdbcSchema.tables
      .flatMap { table =>
        if (table.name == "*" || table.name.equalsIgnoreCase(tableName)) {
          Some(table)
        } else {
          None
        }
      }
      .sortBy(
        // Table with exact name has precedence over *
        _.name.equalsIgnoreCase(tableName)
      )(Ordering.Boolean.reverse)
      .headOption
  }

  private def getQuotedPartitionColumn(
    connection: ConnectionInfo,
    partitionColumn: String
  ): String = {
    connection.quoteIdentifier(partitionColumn)
  }

  private[extract] def extractTableData(
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    context: String,
    auditColumns: Columns,
    extractedDataConsumer: (String, ResultSet, Option[Int]) => Try[Long]
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[Unit] = {
    // Get the boundaries of each partition that will be handled by a specific thread.
    val (boundaries, boundDef) =
      getBoundaries(extractConfig, tableExtractDataConfig, context, auditColumns)

    val tableStart = System.currentTimeMillis()
    // Export in parallel mode
    val tableCount = new AtomicLong()
    val extractionResults: List[Try[Unit]] =
      ParUtils
        .runInExecutionContext(boundaries) { case (bounds, index) =>
          Try {
            val boundaryContext = bounds match {
              case _: Boundary => s"$context[$index]"
              case Unbounded   => context
            }
            logger.info(s"$boundaryContext bounds = $bounds")

            withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.data.options) {
              connection =>
                Using.resource(
                  computePrepareStatement(
                    extractConfig,
                    tableExtractDataConfig,
                    bounds,
                    boundaryContext,
                    connection
                  )
                ) { statement =>
                  val partitionStart = System.currentTimeMillis()
                  val count = {
                    Using.resource(statement.executeQuery()) { rs =>
                      extractedDataConsumer(
                        boundaryContext,
                        rs,
                        bounds match {
                          case _: Boundary => Some(index)
                          case Unbounded   => None
                        }
                      )
                    } match {
                      case Failure(exception) =>
                        logger.error(f"$boundaryContext Encountered an error during extraction.")
                        Utils.logException(logger, exception)
                        throw exception
                      case Success(value) =>
                        value
                    }
                  }
                  val currentTableCount = tableCount.addAndGet(count)

                  boundDef match {
                    case b: Bounds =>
                      val lineLength = 100
                      val progressPercent =
                        if (b.count == 0) lineLength
                        else (currentTableCount * lineLength / b.count).toInt
                      val progressPercentFilled = (0 until progressPercent).map(_ => "#").mkString
                      val progressPercentUnfilled =
                        (progressPercent until lineLength).map(_ => " ").mkString
                      val progressBar =
                        s"[$progressPercentFilled$progressPercentUnfilled] $progressPercent %"
                      val partitionEnd = System.currentTimeMillis()
                      val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(tableStart)
                      logger.info(
                        s"$context $progressBar. Elapsed time: $elapsedTime"
                      )
                      tableExtractDataConfig.partitionConfig match {
                        case Some(p: PartitionConfig) =>
                          val deltaRow = DeltaRow(
                            domain = extractConfig.jdbcSchema.schema,
                            schema = tableExtractDataConfig.table,
                            lastExport = b.max,
                            start = new Timestamp(partitionStart),
                            end = new Timestamp(partitionEnd),
                            duration = (partitionEnd - partitionStart).toInt,
                            count = count,
                            success = true,
                            message = p.partitionColumn,
                            step = index.toString
                          )
                          Some(InsertLastExportParams(deltaRow, p.partitionColumnType))
                        case None =>
                          throw new RuntimeException(
                            "Should never happen since we are fetching an interval"
                          )
                      }
                    case NoBound =>
                      None
                    // do nothing
                  }
                }
            }
          }.recoverWith { case err: Exception =>
            logger.error(err.getMessage, err)
            Failure(
              new DataExtractionException(
                extractConfig.jdbcSchema.schema,
                tableExtractDataConfig.table
              )
            )
          }.map { insertLastExportParams =>
            // We insert here right after the connection has been closed (duckdb does not support multiple connections)
            insertLastExportParams.foreach { insertLastExportParams =>
              withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.audit.options) {
                connection =>
                  LastExportUtils.insertNewLastExport(
                    connection,
                    insertLastExportParams.deltaRow,
                    Some(insertLastExportParams.partitionColumnType),
                    extractConfig.audit,
                    auditColumns
                  )
              }
            }
          }
        }(dbExtractEC.executionContext)
        .toList

    val success = if (extractionResults.exists(_.isFailure)) {
      logger.error(s"$context An error occured during extraction.")
      extractionResults.foreach {
        case Failure(exception) =>
          Utils.logException(logger, exception)
        case Success(_) => // do nothing
      }
      false
    } else {
      true
    }
    val tableEnd = System.currentTimeMillis()
    val duration = (tableEnd - tableStart).toInt
    val elapsedTime = ExtractUtils.toHumanElapsedTime(duration)
    if (success)
      logger.info(s"$context Extracted all lines in $elapsedTime")
    else
      logger.info(s"$context Extraction took $elapsedTime")
    ParUtils.runOneInExecutionContext {
      boundDef match {
        case b: Bounds =>
          tableExtractDataConfig.partitionConfig match {
            case Some(p: PartitionConfig) =>
              val deltaRow = DeltaRow(
                domain = extractConfig.jdbcSchema.schema,
                schema = tableExtractDataConfig.table,
                lastExport = b.max,
                start = new Timestamp(tableStart),
                end = new Timestamp(tableEnd),
                duration = duration,
                count = b.count,
                success = success,
                message = p.partitionColumn,
                step = "ALL"
              )
              // Audit for partition
              withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.audit.options) {
                connection =>
                  LastExportUtils.insertNewLastExport(
                    connection,
                    deltaRow,
                    Some(p.partitionColumnType),
                    extractConfig.audit,
                    auditColumns
                  )
              }
            case _ =>
              throw new RuntimeException("Should never happen since we are fetching an interval")
          }
        case NoBound =>
          val deltaRow = DeltaRow(
            domain = extractConfig.jdbcSchema.schema,
            schema = tableExtractDataConfig.table,
            lastExport = tableStart,
            start = new Timestamp(tableStart),
            end = new Timestamp(tableEnd),
            duration = (tableEnd - tableStart).toInt,
            count = tableCount.get(),
            success = tableCount.get() >= 0,
            message = "FULL",
            step = "ALL"
          )
          // Full audit
          withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.audit.options) {
            connection =>
              LastExportUtils.insertNewLastExport(
                connection,
                deltaRow,
                None,
                extractConfig.audit,
                auditColumns
              )
          }
      }
    }(dbExtractEC.executionContext)
    if (success)
      Success(())
    else
      Failure(new RuntimeException(s"$context An error occured during extraction."))
  }

  /** @param columnExprToDistribute
    *   expression to use in order to distribute data.
    */
  def sqlFetchQuery(
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    boundary: BoundaryDef,
    columnToDistribute: Option[String] = None
  ): String = {
    val dataColumnsProjection = tableExtractDataConfig.columnsProjectionQuery(extractConfig.data)
    val extraCondition = tableExtractDataConfig.filterOpt.map(w => s"and $w").getOrElse("")
    val boundCondition = boundary match {
      case b: Boundary =>
        val columnExprToDistribute =
          columnToDistribute.getOrElse(
            getQuotedPartitionColumn(extractConfig.data, tableExtractDataConfig.partitionColumn)
          )
        val (lowerOperator, upperOperator) = b match {
          case Boundary(_: InclusiveBound, _: InclusiveBound) => ">=" -> "<="
          case Boundary(_: InclusiveBound, _: ExclusiveBound) => ">=" -> "<"
          case Boundary(_: ExclusiveBound, _: InclusiveBound) => ">"  -> "<="
          case Boundary(_: ExclusiveBound, _: ExclusiveBound) => ">"  -> "<"
        }
        s"$columnExprToDistribute $upperOperator ? AND $columnExprToDistribute $lowerOperator ?"
      case Unbounded =>
        "1=1"
    }
    val dataSource = tableExtractDataConfig.sql
      .map("(" + _ + ") sl_data_source")
      .getOrElse(s"${extractConfig.data.quoteIdentifier(
          tableExtractDataConfig.domain
        )}.${extractConfig.data.quoteIdentifier(tableExtractDataConfig.table)}")
    s"""select $dataColumnsProjection
       |from $dataSource
       |where $boundCondition $extraCondition""".stripMargin
  }

  private def computePrepareStatement(
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    bounds: BoundaryDef,
    boundaryContext: String,
    connection: SQLConnection
  )(implicit settings: Settings) = {
    val (effectiveSql, statementFiller) =
      bounds match {
        case Unbounded =>
          sqlFetchQuery(extractConfig, tableExtractDataConfig, bounds) -> ((_: PreparedStatement) =>
            ()
          )
        case bounds: Boundary =>
          tableExtractDataConfig.partitionColumnType match {
            case PrimitiveType.int | PrimitiveType.long | PrimitiveType.short =>
              sqlFetchQuery(extractConfig, tableExtractDataConfig, bounds) -> (
                (st: PreparedStatement) => {
                  st.setLong(1, bounds.upper.value.asInstanceOf[Long])
                  st.setLong(2, bounds.lower.value.asInstanceOf[Long])
                }
              )
            case PrimitiveType.decimal =>
              sqlFetchQuery(extractConfig, tableExtractDataConfig, bounds) -> (
                (st: PreparedStatement) => {
                  st.setBigDecimal(1, bounds.upper.value.asInstanceOf[java.math.BigDecimal])
                  st.setBigDecimal(2, bounds.lower.value.asInstanceOf[java.math.BigDecimal])
                }
              )

            case PrimitiveType.date =>
              sqlFetchQuery(extractConfig, tableExtractDataConfig, bounds) -> (
                (st: PreparedStatement) => {
                  st.setDate(1, bounds.upper.value.asInstanceOf[Date])
                  st.setDate(2, bounds.lower.value.asInstanceOf[Date])
                }
              )

            case PrimitiveType.timestamp =>
              sqlFetchQuery(extractConfig, tableExtractDataConfig, bounds) -> (
                (st: PreparedStatement) => {
                  st.setTimestamp(1, bounds.upper.value.asInstanceOf[Timestamp])
                  st.setTimestamp(2, bounds.lower.value.asInstanceOf[Timestamp])
                }
              )
            case PrimitiveType.string if tableExtractDataConfig.hashFunc.isDefined =>
              tableExtractDataConfig.hashFunc match {
                case Some(tpl) =>
                  val stringPartitionFunc =
                    Utils.parseJinjaTpl(
                      tpl,
                      Map(
                        "col" -> getQuotedPartitionColumn(
                          extractConfig.data,
                          tableExtractDataConfig.partitionColumn
                        ),
                        "nb_partitions" -> tableExtractDataConfig.nbPartitions.toString
                      )
                    )
                  sqlFetchQuery(
                    extractConfig,
                    tableExtractDataConfig,
                    bounds,
                    Some(stringPartitionFunc)
                  ) -> ((st: PreparedStatement) => {
                    st.setLong(1, bounds.upper.value.asInstanceOf[Long])
                    st.setLong(2, bounds.lower.value.asInstanceOf[Long])
                  })
                case None =>
                  throw new RuntimeException(
                    "Should never happen since stringPartitionFuncTpl is always defined here"
                  )
              }
            case _ =>
              throw new Exception(
                s"type ${tableExtractDataConfig.partitionColumnType} not supported for partition columnToDistribute"
              )
          }
      }
    logger.info(s"$boundaryContext SQL: $effectiveSql")
    Try { connection.setAutoCommit(false) } // ignore error if note supported aka clickhouse
    val statement = connection.prepareStatement(effectiveSql)
    statementFiller(statement)
    tableExtractDataConfig.fetchSize.foreach(fetchSize => statement.setFetchSize(fetchSize))

    statement.setMaxRows(extractConfig.limit)
    statement
  }

  private def getBoundaries(
    extractConfig: ExtractJdbcDataConfig,
    tableExtractDataConfig: TableExtractDataConfig,
    context: TableName,
    auditColumns: Columns
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): (List[(BoundaryDef, Int)], BoundariesDef) = {
    tableExtractDataConfig.partitionConfig match {
      case None =>
        List(Unbounded -> 0) -> NoBound
      case Some(_: PartitionConfig) =>
        ParUtils.runOneInExecutionContext {
          withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.data.options) {
            connection =>
              def getBoundariesWith(auditConnection: SQLConnection): Bounds = {
                auditConnection.setAutoCommit(false)
                LastExportUtils.getBoundaries(
                  connection,
                  auditConnection,
                  extractConfig,
                  tableExtractDataConfig,
                  auditColumns
                )
              }

              val boundaries = if (extractConfig.data.options == extractConfig.audit.options) {
                getBoundariesWith(connection)
              } else {
                withJDBCConnection(this.schemaHandler.dataBranch(), extractConfig.audit.options) {
                  auditConnection =>
                    getBoundariesWith(auditConnection)
                }
              }
              logger.info(s"$context Boundaries : $boundaries")
              boundaries.partitions.zipWithIndex -> boundaries
          }
        }(dbExtractEC.executionContext)
    }
  }

  /** @return
    *   an updated jdbcSchema.tables with config propagation if includeTables is specified. Table
    *   given in includeTables may not exist in database.
    */
  private[extract] def computeEligibleTablesForExtraction(
    jdbcSchema: JDBCSchema,
    includeTables: Seq[TableName],
    excludedTables: Seq[TableName]
  ): JDBCSchema = {
    val updatedJdbcSchema = if (includeTables.nonEmpty) {
      val additionalTables = jdbcSchema.tables.find(_.name.trim == "*") match {
        case Some(allTableDef) =>
          // Contains * table, meaning that tables in includeTables but not declared in jdbcSchema.tables inherit from * config
          includeTables
            .filterNot(t => jdbcSchema.tables.exists(_.name.equalsIgnoreCase(t)))
            .filterNot(t => excludedTables.exists(n => n.equalsIgnoreCase(t)))
            .map(n => allTableDef.copy(name = n))
        case None =>
          if (jdbcSchema.tables.isEmpty) {
            // having jdbcSchema.tables empty means that we have an implicit * without special config
            includeTables
              .filterNot(t => excludedTables.exists(n => n.equalsIgnoreCase(t)))
              .map(JDBCTable(_, None, Nil, None, None, Map.empty, None, None))
          } else {
            Nil
          }
      }
      val tablesToFetch = {
        // we keep tables with specific config and union with previous generated tables from *
        jdbcSchema.tables.filter(t =>
          includeTables.exists(_.equalsIgnoreCase(t.name))
        ) ++ additionalTables
      }
      jdbcSchema.copy(tables = tablesToFetch)
    } else {
      // we keep as is
      jdbcSchema
    }
    updatedJdbcSchema
  }

  private[extract] def createDomainOutputDir(baseOutputDir: Path, domainName: TableName)(implicit
    storageHandler: StorageHandler
  ): Path = {
    val outputDir = new Path(baseOutputDir, domainName)
    storageHandler.mkdirs(outputDir)
    outputDir
  }

  /** Create audit export's table
    */
  private[extract] def initExportAuditTable(
    connectionSettings: ConnectionInfo
  )(implicit settings: Settings, dbExtractEC: ExtractExecutionContext): Columns = {
    val auditSchema = settings.appConfig.audit.domain.getOrElse("audit")
    ParUtils.runOneInExecutionContext {
      withJDBCConnection(this.schemaHandler.dataBranch(), connectionSettings.options) {
        connection =>
          val existLastExportTable =
            tableExists(
              connection,
              connectionSettings.jdbcUrl,
              s"${auditSchema}.$lastExportTableName"
            )
          if (!existLastExportTable && settings.appConfig.createSchemaIfNotExists) {
            createSchema(connection, auditSchema)
            val jdbcEngineName = connectionSettings.getJdbcEngineName()
            settings.appConfig.jdbcEngines.get(jdbcEngineName.toString).foreach { jdbcEngine =>
              val createTableSql = jdbcEngine
                .tables("extract")
                .createSql
                .richFormat(
                  Map(
                    "table"       -> s"$auditSchema.$lastExportTableName",
                    "writeFormat" -> settings.appConfig.defaultWriteFormat
                  ),
                  Map.empty
                )
              execute(createTableSql, connection)
            }
          }
      }
    }(dbExtractEC.executionContext)
    JdbcDbUtils
      .extractJDBCTables(
        JDBCSchema(
          schema = auditSchema,
          tables = List(
            JDBCTable(name = lastExportTableName, None, List(), None, None, Map.empty, None, None)
          ),
          tableTypes = List("TABLE")
        ),
        connectionSettings,
        skipRemarks = true,
        keepOriginalName = true,
        includeColumns = true
      )
      .find { case (tableName, _) =>
        tableName.equalsIgnoreCase(lastExportTableName)
      }
      .map { case (_, tableAttrs) =>
        tableAttrs.columNames
      }
      .getOrElse(
        throw new RuntimeException(s"$lastExportTableName table not found. Please create it.")
      )

  }

  private[extract] def getStringPartitionFunc(dbType: String): Option[String] = {
    val hashFunctions = Map(
      "sqlserver"  -> "abs( binary_checksum({{col}}) % {{nb_partitions}} )",
      "postgresql" -> "abs( hashtext({{col}}) % {{nb_partitions}} )",
      "h2"         -> "ora_hash({{col}}, {{nb_partitions}} - 1)",
      "mysql"      -> "crc32({{col}}) % {{nb_partitions}}",
      "oracle"     -> "ora_hash({{col}}, {{nb_partitions}} - 1)"
    )
    hashFunctions.get(dbType)
  }

  private[extract] def sinkPartitionToFile(
    outputFormat: FileFormat,
    tableExtractDataConfig: TableExtractDataConfig,
    context: String,
    rs: ResultSet,
    index: Option[Int]
  )(implicit storageHandler: StorageHandler): Try[Long] = {
    val filename = index
      .map(index =>
        tableExtractDataConfig.table + s"-${tableExtractDataConfig.extractionDateTime}-$index.csv"
      )
      .getOrElse(
        tableExtractDataConfig.table + s"-${tableExtractDataConfig.extractionDateTime}.csv"
      )
    val outFile = new Path(tableExtractDataConfig.tableOutputDir, filename)
    Try {
      logger.info(s"$context Starting extraction into $filename")
      Using.resource(storageHandler.output(outFile)) { outFileWriter =>
        val writerSettings = new CsvWriterSettings()
        val format = new CsvFormat()
        outputFormat.quote.flatMap(_.headOption).foreach { q =>
          format.setQuote(q)
        }
        outputFormat.escape.flatMap(_.headOption).foreach(format.setQuoteEscape)
        outputFormat.separator.foreach(format.setDelimiter)
        writerSettings.setFormat(format)
        outputFormat.nullValue.foreach(writerSettings.setNullValue)
        outputFormat.withHeader.foreach(writerSettings.setHeaderWritingEnabled)
        val csvRoutines = new CsvRoutines(writerSettings)

        val extractionStartMs = System.currentTimeMillis()
        val objectRowWriterProcessor = new SLObjectRowWriterProcessor(context)
        outputFormat.datePattern.foreach(dtp =>
          objectRowWriterProcessor.convertType(classOf[java.sql.Date], Conversions.toDate(dtp))
        )
        outputFormat.timestampPattern.foreach(tp =>
          objectRowWriterProcessor.convertType(classOf[java.sql.Timestamp], Conversions.toDate(tp))
        )
        writerSettings.setQuoteAllFields(true)
        writerSettings.setRowWriterProcessor(objectRowWriterProcessor)
        csvRoutines.write(
          rs,
          outFileWriter,
          outputFormat.encoding.getOrElse(StandardCharsets.UTF_8.name())
        )
        val elapsedTime = ExtractUtils.toHumanElapsedTimeFrom(extractionStartMs)
        logger.info(
          s"$context Extracted ${objectRowWriterProcessor.getRecordsCount()} rows and saved into $outFile in $elapsedTime"
        )
        objectRowWriterProcessor.getRecordsCount()
      }
    }.recoverWith { case e =>
      storageHandler.delete(outFile)
      Failure(e)
    }
  }
}
