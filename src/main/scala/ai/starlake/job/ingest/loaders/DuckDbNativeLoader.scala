package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.{IngestionJob, IngestionUtil, RejectedRecord}
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
import scala.collection.mutable.ListBuffer
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
    // The rows of each Parquet file are checked while loading it into a table of the first step,
    // before the second step writes the target (#1952)
    mergedMetadata.resolveFormat() == Format.PARQUET ||
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
                domain.finalName + "." + schema.finalName,
                sinkConnection.getJdbcEngineName().toString
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
      val rejectedCount = if (twoSteps) {
        // Dropped whatever happens, a failure on a later file included
        val tempTables = ListBuffer.empty[String]
        try {
          val rejectedRows =
            path.map { p =>
              logger.info(s"Loading $p to temporary table")
              val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
              tempTables += tempTable
              val rejected =
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
              rejected
            }
          val targetFullTableName = s"${domain.finalName}.${schema.finalName}"
          val rejected = rejectedRows.map(_.count).sum
          if (rejected > 0) {
            // As a load in one step, over the files of the load
            val error = BigQueryNativeLoader.rejectedRowsError(
              rejected,
              settings.appConfig.rejectAllOnError,
              settings.appConfig.rejectMaxRecords
            )
            val message = s"$rejected rows of $targetFullTableName are rejected"
            logger.warn(s"$message. ${error.getOrElse("The others are loaded")}")
            // As the Spark loads, the rejected rows are kept before the load fails because of them,
            // once every file is loaded: a retry of a load that failed on a later file would write
            // them again
            if (settings.appConfig.sinkReplayToFile)
              rejectedRows.flatMap(_.replay).foreach(writeReplayFile)
            sinkRejected(rejectedRows.flatMap(_.records))
            error.foreach(error => throw new IllegalArgumentException(s"$message. $error"))
          }
          val unionTempTables = tempTables
            .map(s"SELECT * FROM ${domain.finalName}." + _)
            .mkString("(", " UNION ALL ", ")")
          // Quoted: a column may be named like a keyword
          val sqlWithTransformedFields = schema.buildSecondStepSqlSelectOnLoad(
            unionTempTables,
            jdbcEngine = Some(settings.appConfig.jdbcEngines("duckdb"))
          )

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
            expectations = Nil, // IngestionJob runs them, once the load is audited
            acl = schema.acl,
            comment = schema.comment,
            tags = schema.tags,
            writeStrategy = mergedMetadata.writeStrategy.map(
              _.withDeclaredValidityTypes(schema.attributes)
            ),
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
            // IngestionJob writes the LOAD audit row, as for Spark loads: a TRANSFORM row would
            // duplicate it. A failure still writes one, as a Spark sink task does (#1838)
            logExecution = false,
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
          // A failed second step fails the load, whose files must not be archived
          ingestionJob.writingWith(job)(job.run().get)
          rejected
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
        singleStepLoad(domain.finalName, schema.finalName, schemaWithMergedMetadata, path).count
      }
      (initialRowCount, rejectedCount)
    }.map { case (initialRowCount, rejectedCount) =>
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
      // Only the Parquet loads find their rejected rows (#1952)
      val countsRejected = mergedMetadata.resolveFormat() == Format.PARQUET
      List(
        IngestionCounters(
          inputCount = currentRowCount - initialRowCount + rejectedCount,
          acceptedCount = currentRowCount - initialRowCount,
          rejectedCount = if (countsRejected) rejectedCount else -1,
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
  ): RejectedRows = {
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
        val tableExists = JdbcDbUtils.tableExists(
          conn,
          sinkConnection.jdbcUrl,
          domainAndTableName,
          sinkConnection.getJdbcEngineName().toString
        )
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
            .map(_.replace("'", "''"))
            .mkString("['", "','", "']")
        val rejected = mergedMetadata.resolveFormat() match {
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
            RejectedRows.none

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
            RejectedRows.none
          case Format.PARQUET =>
            // Always in two steps: one file, into a table of the first step
            loadParquet(conn, domainAndTableName, schema, paths, path, attrsWithDDLTypes.toMap)
          case _ => RejectedRows.none
        }
        // Inside the block, before the postActions of the connection, which run once it succeeded
        if (!isTemporary) ingestionJob.markRowsWritten()
        rejected
    }
  }

  /** The rows of a file that the native loader rejects: their number, the first `audit.maxErrors`
    * of them, for the audit, and the query of all of them, for a replay file
    */
  private case class RejectedRows(
    count: Long,
    records: List[RejectedRecord],
    replay: Option[String]
  )

  private object RejectedRows {
    val none: RejectedRows = RejectedRows(0, Nil, None)
  }

  /** Loads the rows of a Parquet file that its checks accept into a table of the first step, whose
    * columns have the source names of the attributes, and finds the others, which the load keeps as
    * the Spark loads keep their rejected rows (#1952)
    *
    * @param file
    *   the file, as the list of paths that DuckDB reads
    * @param ddlTypes
    *   the DuckDB types of the columns of the table, by attribute name
    */
  private def loadParquet(
    conn: Connection,
    table: String,
    schema: SchemaInfo,
    file: String,
    path: List[Path],
    ddlTypes: Map[String, String]
  ): RejectedRows = {
    // As Spark, read the columns of the attributes by name: the other columns of the file are
    // dropped, and an attribute that it does not hold has no value. Selecting a column that the
    // file does not hold would fail with an obscure error of the driver.
    val fileTypes = JdbcDbUtils
      .executeQueryAsMap(s"DESCRIBE SELECT * FROM read_parquet($file);", conn)
      .map(row => row("column_name") -> row("column_type"))
      .toMap
    val columns = schema.attributes.filter(_.script.isEmpty).map { attribute =>
      DuckDbParquetRows.Column(
        attribute,
        attribute
          .`type`(schemaHandler)
          .getOrElse(throw new IllegalArgumentException(s"Unknown type ${attribute.`type`}")),
        ddlTypes(attribute.name),
        fileTypes.get(attribute.name)
      )
    }
    if (!columns.exists(_.fileType.nonEmpty))
      throw new IllegalArgumentException(
        s"No attribute of ${schema.name} found in ${path.mkString(", ")}. Attributes: ${schema.attributes
            .map(_.name)
            .mkString(", ")}. Columns: ${fileTypes.keys.toList.sorted.mkString(", ")}"
      )
    val rows = DuckDbParquetRows(
      columns,
      // As Spark
      emptyIsNull = mergedMetadata.emptyIsNull.getOrElse(settings.appConfig.emptyIsNull),
      withValue = settings.appConfig.rejectWithValue
    )
    JdbcDbUtils.execute(rows.insertSql(table, file), conn)
    val rejected =
      if (rows.checks.isEmpty) 0L
      else
        JdbcDbUtils
          .executeQueryAsMap(rows.rejectedCountSql(file), conn)
          .head("rejected")
          .toLong
    if (rejected == 0) RejectedRows.none
    else {
      val maxErrors = settings.appConfig.audit.maxErrors
      val records =
        if (maxErrors <= 0) Nil
        else
          JdbcDbUtils.executeQueryAsMap(rows.rejectedRowsSql(file, maxErrors), conn).map { row =>
            rejectedRecord(
              row("errors").split('\n').toList.filter(_.nonEmpty),
              path.mkString(","),
              row.get("row_json")
            )
          }
      RejectedRows(rejected, records, Some(rows.replaySql(file)))
    }
  }

  /** Writes rejected rows to a Parquet replay file of the replay area of the domain, as the Spark
    * loads write theirs, so that the table loads them again. DuckDB writes a local file, which is
    * copied to the replay area, wherever it is. A failure fails the load, as with Spark.
    *
    * @param sql
    *   the query of the rejected rows of a file
    */
  private def writeReplayFile(sql: String): Unit = {
    val local = java.io.File.createTempFile("starlake-replay-", ".parquet")
    try {
      local.delete()
      JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
        conn =>
          JdbcDbUtils.execute(
            s"COPY ($sql) TO '${local.getPath.replace("'", "''")}' (FORMAT parquet);",
            conn
          )
      }
      val replayArea = DatasetArea.replay(domain.name)
      storageHandler.mkdirs(replayArea)
      val target =
        new Path(replayArea, s"${IngestionJob.replayName(domain.name, schema.name)}.parquet")
      // A path with a scheme: the storage handler would read a path without one on its own file
      // system
      storageHandler.copyFromLocal(new Path(local.toURI), target)
      logger.info(s"Replay file of ${domain.finalName}.${schema.finalName} written to $target")
    } finally local.delete()
  }

  /** Writes the rejected records of the load to the audit `rejected` table: a failure to write them
    * fails the load, as with Spark
    */
  private def sinkRejected(records: List[RejectedRecord]): Unit =
    IngestionUtil
      .sinkRejectedRecords(
        ingestionJob.applicationId(),
        records,
        path,
        scheduledDate,
        ingestionJob.accessToken,
        ingestionJob.session
      )(settings, storageHandler, schemaHandler)
      .get

  private def rejectedRecord(errors: List[String], file: String, value: Option[String]) =
    RejectedRecord(
      ingestionJob.applicationId(),
      ingestionJob.now,
      domain.name,
      schema.name,
      RejectedRecord.errorText(errors, file, value),
      RejectedRecord.limited(path.map(_.toString).mkString(","))
    )
}
