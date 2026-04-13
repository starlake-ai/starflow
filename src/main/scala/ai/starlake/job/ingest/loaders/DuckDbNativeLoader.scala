package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.TransformContext
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import java.nio.charset.Charset
import java.sql.Connection
import scala.util.{Try, Using}

class DuckDbNativeLoader(ingestionJob: IngestionJob)(implicit
  val settings: Settings
) extends LazyLogging {

  val domain: DomainInfo = ingestionJob.domain

  val schema: SchemaInfo = ingestionJob.schema

  val storageHandler: StorageHandler = ingestionJob.storageHandler

  val schemaHandler: SchemaHandler = ingestionJob.schemaHandler

  val path: List[Path] = ingestionJob.path

  val options: Map[String, String] = ingestionJob.options

  val strategy: WriteStrategy = ingestionJob.mergedMetadata.getStrategyOptions()

  lazy val mergedMetadata: Metadata = ingestionJob.mergedMetadata

  lazy val sinkConnection = mergedMetadata.getSinkConnection()

  lazy val scheduledDate: Option[String] = ingestionJob.scheduledDate

  lazy val engineName: Engine = sinkConnection.getJdbcEngineName()

  private def requireTwoSteps(schema: SchemaInfo): Boolean = {
    // renamed attribute can be loaded directly so it's not in the condition
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    !schema.isVariant() ||
    schema.filter.nonEmpty ||
    settings.appConfig.archiveTable
  }
  lazy val effectiveSchema: SchemaInfo = computeEffectiveInputSchema()
  lazy val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))

  def run(): Try[List[IngestionCounters]] = {
    Try {
      val initialRowCount =
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
          conn =>
            // the two lines below are used to initialize the duckdb database
            val stmtExternal = conn.createStatement()
            stmtExternal.close()
            val tableExists =
              JdbcDbUtils.tableExists(
                conn,
                sinkConnection.jdbcUrl,
                domain.finalName + "." + schema.finalName
              )
            if (tableExists) {
              // get line count from table
              val countSql =
                s"SELECT COUNT(*) AS cnt FROM ${domain.finalName}.${schema.finalName};"
              val res = JdbcDbUtils.executeQueryAsMap(countSql, conn)
              val count = res.head("cnt").toInt
              logger
                .info(
                  s"Table ${domain.finalName}.${schema.finalName} already exists with $count records"
                )
              count
            } else {
              logger.info(
                s"Table ${domain.finalName}.${schema.finalName} does not exist and will be created"
              )
              0
            }
        }
      val twoSteps = requireTwoSteps(effectiveSchema)
      if (twoSteps) {
        val tempTables =
          path.map { p =>
            logger.info(s"Loading $p to temporary table")
            val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
            singleStepLoad(domain.finalName, tempTable, schemaWithMergedMetadata, List(p))
            val escapedPath = p.toString.replace("'", "''")
            val filenameSQL =
              s"ALTER TABLE ${domain.finalName}.$tempTable ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '$escapedPath';"

            JdbcDbUtils.withJDBCConnection(
              this.schemaHandler.dataBranch(),
              sinkConnection.options
            ) { conn =>
              JdbcDbUtils.execute(filenameSQL, conn)

            }
            tempTable
          }

        try {
          val unionTempTables = tempTables
            .map(s"SELECT * FROM ${domain.finalName}." + _)
            .mkString("(", " UNION ALL ", ")")
          val targetFullTableName = s"${domain.finalName}.${schema.finalName}"
          val sqlWithTransformedFields = schema.buildSecondStepSqlSelectOnLoad(unionTempTables)

          val taskDesc = AutoTaskInfo(
            name = schema.finalName,
            sql = Some(sqlWithTransformedFields),
            database = schemaHandler.getDatabase(domain),
            domain = domain.finalName,
            table = schema.finalName,
            presql = schema.presql,
            postsql = schema.postsql,
            sink = mergedMetadata.sink,
            rls = schema.rls,
            expectations = schema.expectations,
            acl = schema.acl,
            comment = schema.comment,
            tags = schema.tags,
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
            accessToken = ingestionJob.accessToken,
            resultPageSize = 200,
            resultPageNumber = 1,
            dryRun = false,
            scheduledDate = scheduledDate,
            syncSchema = false
          )(settings, storageHandler, schemaHandler)
          val job = TransformContext.createJdbcTask(context, None)
          val incomingSchema = schema.sparkSchemaWithoutIgnore(
            schemaHandler,
            withFinalName = true
          )
          job.updateJdbcTableSchema(
            incomingSchema = incomingSchema,
            tableName = targetFullTableName,
            syncStrategy = TableSync.ALL,
            createIfAbsent = true
          )
          job.run()
        } finally {
          tempTables.foreach { tempTable =>
            Try {
              JdbcDbUtils.withJDBCConnection(
                this.schemaHandler.dataBranch(),
                sinkConnection.options
              ) { conn =>
                JdbcDbUtils.dropTable(conn, s"${domain.finalName}.$tempTable")
              }
            }.recover { case e =>
              logger.warn(
                s"Failed to drop temporary table ${domain.finalName}.$tempTable: ${e.getMessage}"
              )
            }
          }
        }
      } else {
        singleStepLoad(domain.finalName, schema.finalName, schemaWithMergedMetadata, path)
      }
      initialRowCount
    }.map { initialRowCount =>
      val countSql =
        s"SELECT COUNT(*) AS cnt FROM ${domain.finalName}.${schema.finalName};"

      val currentRowCount =
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
          conn =>
            val res = JdbcDbUtils.executeQueryAsMap(countSql, conn)
            val count = res.head("cnt").toInt
            logger.info(
              s"Table ${domain.finalName}.${schema.finalName} now has $count records"
            )
            count
        }
      List(
        IngestionCounters(
          inputCount = currentRowCount - initialRowCount,
          acceptedCount = currentRowCount - initialRowCount,
          rejectedCount = -1,
          paths = path.map(_.toString),
          jobid = ingestionJob.applicationId()
        )
      )
    }
  }

  private def computeEffectiveInputSchema(): SchemaInfo = {
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        (mergedMetadata.resolveWithHeader(), path.map(_.toString).headOption) match {
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
                csvParserSettings.setMaxColumns(schema.attributes.length * 2)
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
            val attributesMap = schema.attributes.map(attr => attr.name -> attr).toMap
            val csvAttributesInOrders =
              csvHeaders.map(h =>
                attributesMap.getOrElse(h, TableAttribute(h, ignore = Some(true), required = None))
              )
            // attributes not in csv input file must not be required but we don't force them to optional.
            val effectiveAttributes =
              csvAttributesInOrders ++ schema.attributes.diff(csvAttributesInOrders)
            if (effectiveAttributes.length > schema.attributes.length) {
              logger.warn(
                s"Attributes in the CSV file are bigger from the schema. " +
                s"Schema will be updated to match the CSV file. " +
                s"Schema: ${schema.attributes.map(_.name).mkString(",")}. " +
                s"CSV: ${csvHeaders.mkString(",")}"
              )
              schema.copy(attributes = effectiveAttributes.take(schema.attributes.length))

            } else {
              schema.copy(attributes = effectiveAttributes)
            }

          case _ => schema
        }
      case _ => schema
    }
  }

  private def setPartition(connection: Connection, domainAndTableName: String) = {
    if (sinkConnection.isDucklake()) {
      val jdbcEngine = settings.appConfig.jdbcEngines("duckdb")
      val partitionClause =
        mergedMetadata.getSink().toAllSinks().getPartitionByClauseSQL(jdbcEngine)
      partitionClause.foreach { partitionClause =>
        logger.info(s"Setting partition on $domainAndTableName : $partitionClause")
        val sql = s"ALTER TABLE $domainAndTableName  SET $partitionClause;"
        JdbcDbUtils.execute(sql, connection)
      }
    }
  }

  private def singleStepLoad(
    domain: String,
    table: String,
    schema: SchemaInfo,
    path: List[Path]
  ) = {
    val isTemporary = table.startsWith("zztmp_")
    val incomingSparkSchema =
      schema.sparkSchemaWithIgnoreAndScript(schemaHandler, !isTemporary)
    val domainAndTableName = domain + "." + table
    val optionsWrite =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, domainAndTableName, sinkConnection.options)
    val ddlMap = schemaHandler.getDdlMapping(schema.attributes)
    val attrsWithDDLTypes = schemaHandler.getAttributesWithDDLType(schema, "duckdb")

    // Create or update table schema first
    JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
      conn =>
        // the two lines below are intentional to initialize the database
        val stmtExternal = conn.createStatement()
        stmtExternal.close()
        val tableExists = JdbcDbUtils.tableExists(conn, sinkConnection.jdbcUrl, domainAndTableName)
        JdbcDbUtils.createSchema(conn, domain)
        strategy.getEffectiveType() match {
          case WriteStrategyType.APPEND =>
            if (tableExists) {
              SparkUtils.updateJdbcTableSchema(
                "duckdb",
                conn,
                sinkConnection.options,
                domainAndTableName,
                incomingSparkSchema,
                attrsWithDDLTypes.toMap
              )
            } else {
              SparkUtils.createTable(
                "duckdb",
                conn,
                domainAndTableName,
                incomingSparkSchema,
                caseSensitive = true,
                temporaryTable = false,
                optionsWrite,
                ddlMap
              )
              if (!isTemporary) {
                setPartition(conn, domainAndTableName)
              }
            }
          case _ => //  WriteStrategyType.OVERWRITE or first step of other strategies
            JdbcDbUtils.dropTable(conn, domainAndTableName)
            SparkUtils.createTable(
              "duckdb",
              conn,
              domainAndTableName,
              incomingSparkSchema,
              caseSensitive = true,
              temporaryTable = false,
              optionsWrite,
              ddlMap
            )
            if (!isTemporary) {
              setPartition(conn, domainAndTableName)
            }
        }
        val columnsString =
          attrsWithDDLTypes
            .map { case (attr, ddlType) =>
              s"'$attr': '$ddlType'"
            }
            .mkString(", ")
        val paths =
          path
            .map { p =>
              val ps = p.toString
              if (ps.startsWith("file:"))
                StorageHandler.localFile(p).pathAsString
              else if (ps.contains("://")) {
                // For accessing secured storage like S3 with DuckDB HTTPFS extension,
                // user needs to have created a secret with the proper configuration
                JdbcDbUtils.execute("INSTALL httpfs;", conn)
                JdbcDbUtils.execute("LOAD httpfs;", conn)
                ps
              } else {
                ps
              }
            }
            .mkString("['", "','", "']")
        mergedMetadata.resolveFormat() match {
          case Format.DSV =>
            val nullstr =
              if (Option(mergedMetadata.resolveNullValue()).isEmpty)
                ""
              else
                s"nullstr = '${mergedMetadata.resolveNullValue()}',"
            val options = mergedMetadata.getOptions()
            val extraOptions =
              if (options.nonEmpty)
                options
                  .map { case (k, v) =>
                    s"$k = '$v'"
                  }
                  .mkString("", ",", ",")
              else
                ""

            val sql = s"""INSERT INTO $domainAndTableName SELECT
               | * FROM read_csv(
               | ${paths},
               | delim = '${mergedMetadata.resolveSeparator()}',
               | header = ${mergedMetadata.resolveWithHeader()},
               | quote = '${mergedMetadata.resolveQuote()}',
               | escape = '${mergedMetadata.resolveEscape()}',
               | $nullstr
               | $extraOptions
               | columns = { $columnsString});""".stripMargin
            JdbcDbUtils.execute(sql, conn)

          case Format.JSON_FLAT | Format.JSON =>
            val format =
              if (mergedMetadata.resolveArray()) "array"
              else if (mergedMetadata.resolveMultiline())
                "unstructured"
              else
                "newline_delimited"
            if (schema.isFlat()) {
              val sql =
                s"""INSERT INTO  $domainAndTableName SELECT * FROM read_json($paths, format = '$format', columns = { $columnsString});"""
              JdbcDbUtils.execute(sql, conn)
            } else {
              schema.attributes.head.primitiveType(schemaHandler) match {
                case Some(PrimitiveType.variant) =>
                  val sql =
                    s"""INSERT INTO $domainAndTableName SELECT * FROM read_json_objects($paths, format = '$format');"""
                  JdbcDbUtils.execute(sql, conn)
                case _ =>
                  s"""INSERT INTO $domainAndTableName SELECT * FROM read_json($paths, auto_detect = true, format = '$format');"""
              }
            }
          case _ =>
        }
    }
  }
}
