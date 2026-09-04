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
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.BufferedReader
import java.nio.charset.Charset
import java.sql.Connection
import scala.util.control.NonFatal
import scala.util.{Failure, Try, Using}

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
    // renamed attribute can be loaded directly so it's not in the condition.
    // POSITION always needs two steps: the first step loads each line as a single VARCHAR
    // column, the second step slices it via SUBSTR into the target columns.
    schema
      .hasTransformOrIgnoreOrScriptColumns() ||
    strategy.isMerge() ||
    !schema.isVariant() ||
    schema.filter.nonEmpty ||
    mergedMetadata.resolveFormat() == Format.POSITION ||
    settings.appConfig.archiveTable
  }
  lazy val effectiveSchema: SchemaInfo = computeEffectiveInputSchema()
  lazy val schemaWithMergedMetadata = effectiveSchema.copy(metadata = Some(mergedMetadata))
  private lazy val twoSteps: Boolean = requireTwoSteps(effectiveSchema)

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
      val rejected: List[RejectedLine] = if (twoSteps) {
        val tempTablesWithRejects =
          path.map { p =>
            logger.info(s"Loading $p to temporary table")
            val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
            val rejects =
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
            (tempTable, rejects)
          }
        val tempTables = tempTablesWithRejects.map(_._1)
        val rejectedLines = tempTablesWithRejects.flatMap(_._2)

        try {
          if (rejectThresholdBreached(rejectedLines.size)) {
            // The target table has not been written yet at this point, so aborting here
            // leaves it untouched. The temp tables are dropped by the finally block below.
            throw new RejectThresholdExceededException(
              rejectedLines,
              rejectThresholdMessage(rejectedLines.size)
            )
          }
          // Rejects first, target rows second, the ordering SparkIngestionPipeline.ingest
          // uses. If the replay area cannot be written or the audit connection is down, this
          // throws before anything lands in the target, so the failed load leaves nothing
          // behind and the same input can be loaded again without double appending.
          reportRejects(rejectedLines)
          val unionTempTables = tempTables
            .map(s"SELECT * FROM ${domain.finalName}." + _)
            .mkString("(", " UNION ALL ", ")")
          val targetFullTableName = s"${domain.finalName}.${schema.finalName}"
          // Per-attribute DDL type, used by POSITION to wrap each SUBSTR projection in
          // TRY_CAST (DuckDB's SAFE_CAST) so malformed cells yield NULL instead of
          // aborting the whole INSERT.
          val ddlTypesByAttribute: Map[String, String] = Try(
            schemaHandler.getAttributesWithDDLType(schema, "duckdb").toMap
          ).getOrElse(Map.empty)
          val sqlWithTransformedFields = schema.buildSecondStepSqlSelectOnLoad(
            unionTempTables,
            ddlTypesByAttribute = ddlTypesByAttribute,
            safeCastFunction = "TRY_CAST"
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
          // A failed second step is a real error, so it has to be propagated. Dropping the Try
          // reported it as a successful load, which with the counters below reads "success,
          // 0 accepted, N rejected, replay file written" for a load that may have written
          // nothing. Throw instead: the finally below still drops the temporary tables, and
          // the exception is not a RejectThresholdExceededException, so the recoverWith at
          // the end of run() leaves it alone and the load fails.
          // A failure does not always mean the rows never reached the target. It does for the
          // main SQL and the postsql, which are rolled back, but not for what runs after
          // conn.commit() in JdbcAutoTask: runExpectations calls Utils.parseJinja and
          // transpileSql outside its per expectation Try, so a broken expectation template
          // throws once the rows are already committed. Under APPEND, retrying a load reported
          // as failed for that reason would duplicate the committed rows.
          job.run().get
          rejectedLines
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
      (initialRowCount, rejected)
    }.map { case (initialRowCount, rejected) =>
      // The two step path has already reported them, before it wrote its target rows, so
      // only the single step path is left here. That path is reachable only for a variant
      // schema, which in practice means JSON, and DuckDB captures no rejects for JSON, so
      // this call is a no-op today. It is left where it is rather than reordered on
      // speculation: there is no reject to lose, and the single step INSERT is what would
      // have to move.
      if (!twoSteps) reportRejects(rejected)
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
      // OVERWRITE replaces the target's previous contents (singleStepLoad drops and
      // recreates the table, and the two step path's AutoTask replaces the rows), so
      // initialRowCount no longer describes what was there before this load's rows landed.
      // currentRowCount alone is the accepted count in that case. Do not turn this back into
      // a delta: for OVERWRITE the delta undercounts, or goes negative, whenever the
      // replaced table held rows before this load.
      // Known gap, pre-existing and out of scope here: OVERWRITE_BY_PARTITION,
      // DELETE_THEN_INSERT and SCD2 can shrink the table too, so they can still report a
      // negative delta, which inputCount now carries as well.
      val acceptedCount =
        if (strategy.getEffectiveType() == WriteStrategyType.OVERWRITE) currentRowCount.toLong
        else (currentRowCount - initialRowCount).toLong
      // JSON has no reject capture in DuckDB, so it keeps reporting an unknown count.
      val format = mergedMetadata.resolveFormat()
      val rejectsSupported = format == Format.DSV || format == Format.POSITION
      val rejectedCount = if (rejectsSupported) rejected.size.toLong else -1L
      val inputCount = if (rejectsSupported) acceptedCount + rejectedCount else acceptedCount
      List(
        IngestionCounters(
          inputCount = inputCount,
          acceptedCount = acceptedCount,
          rejectedCount = rejectedCount,
          paths = path.map(_.toString),
          jobid = ingestionJob.applicationId()
        )
      )
    }.recoverWith { case e: RejectThresholdExceededException =>
      // The load is going to fail, but the user still gets the replay file and the audit
      // rejected rows so they can fix the input and load it again. recoverWith takes a
      // throwing partial function, so reporting runs in a Try of its own: whatever happens
      // to it, the failure the user sees is the threshold breach that aborted the load, not
      // the IO error that followed it.
      Try(reportRejects(e.rejected)).failed.foreach { reportFailure =>
        logger.error(
          "Failed to report the rejected lines of a load aborted on the reject threshold",
          reportFailure
        )
      }
      Failure(e)
    }
  }

  /** The first physical line of the first input file, so the replay file can be ingested again by a
    * table that declares a header. Read verbatim rather than rebuilt from the attribute names, so
    * it survives renamed or reordered source columns.
    */
  private def replayHeaderLine(): Option[String] = {
    val isDsvWithHeader =
      mergedMetadata.resolveFormat() == Format.DSV &&
      mergedMetadata.resolveWithHeader().booleanValue()
    if (isDsvWithHeader) {
      path.headOption.flatMap { p =>
        Try {
          storageHandler.readAndExecute(p, Charset.forName(mergedMetadata.resolveEncoding())) {
            reader => Option(new BufferedReader(reader).readLine())
          }
        }.recover { case NonFatal(e) =>
          logger.warn(s"Could not read the header line of $p for the replay file", e)
          None
        }.get
      }
    } else {
      None
    }
  }

  /** Writes the rejected lines where the user can act on them: the replay file when
    * sinkReplayToFile is set, and the audit rejected table.
    */
  private def reportRejects(rejected: List[RejectedLine]): Unit = {
    if (rejected.nonEmpty && settings.appConfig.sinkReplayToFile) {
      ReplayFileWriter.write(
        domainName = domain.finalName,
        tableName = schema.finalName,
        rejectedLines = rejected,
        header = replayHeaderLine(),
        encoding = mergedMetadata.resolveEncoding(),
        timestamp = ingestionJob.now
      )(settings, storageHandler)
    }
    if (rejected.nonEmpty) {
      NativeRejectedSink
        .sink(
          applicationId = ingestionJob.applicationId(),
          domainName = domain.finalName,
          tableName = schema.finalName,
          rejected = rejected,
          paths = path,
          timestamp = ingestionJob.now,
          scheduledDate = scheduledDate,
          accessToken = ingestionJob.accessToken
        )(settings, storageHandler, schemaHandler)
        .get
    }
  }

  /** A load is aborted when it produces more rejects than allowed, or any reject at all when the
    * user asked for all or nothing.
    */
  private def rejectThresholdBreached(rejectedCount: Int): Boolean =
    rejectedCount > settings.appConfig.rejectMaxRecords ||
    (settings.appConfig.rejectAllOnError && rejectedCount > 0)

  /** Shared by the two step and the single step abort sites so the two messages cannot drift.
    */
  private def rejectThresholdMessage(rejectedCount: Int): String =
    s"$rejectedCount rejected record(s) exceeds the allowed threshold"

  /** The read_csv options the reject capture owns. `store_rejects` is injected by the DSV load
    * below and the rejected lines are read back from the session scoped `reject_errors` table, so a
    * user option that turns error skipping off or renames the reject tables breaks a load that
    * worked before reject capture existed: `ignore_errors = false` makes DuckDB refuse
    * `store_rejects` outright, and `rejects_table` moves the table the capture reads from.
    */
  private val reservedReadCsvOptions =
    Set("ignore_errors", "store_rejects", "rejects_table", "rejects_scan")

  /** Drops the options above from what the user declared, naming each one so the setting is not
    * silently ignored.
    */
  private def readCsvOptionsWithoutReserved(
    options: Map[String, String]
  ): Map[String, String] = {
    val (reserved, kept) = options.partition { case (key, _) =>
      reservedReadCsvOptions.contains(key.trim.toLowerCase)
    }
    reserved.foreach { case (key, value) =>
      logger.warn(
        s"Ignoring the read_csv option $key = $value declared on " +
        s"${domain.finalName}.${schema.finalName}: the DuckDB native loader sets the error " +
        s"handling of read_csv itself so that malformed lines are captured as rejects"
      )
    }
    kept
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

  // Map the Java charset name from the metadata to DuckDB's read_csv encoding
  // names. UTF-8 is DuckDB's default and is omitted; unknown charsets are passed
  // through lowercased so DuckDB fails loudly instead of silently mis-decoding.
  private def duckDbEncoding(charsetName: String): Option[String] = {
    charsetName.toUpperCase() match {
      case "UTF-8" | "US-ASCII"                => None
      case "ISO-8859-1" | "LATIN-1" | "LATIN1" => Some("latin-1")
      case "UTF-16" | "UTF-16LE" | "UTF-16BE"  => Some("utf-16")
      case other                               => Some(other.toLowerCase())
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
  ): List[RejectedLine] = {
    val isTemporary = table.startsWith("zztmp_")
    // For POSITION format the first step loads each line as a single VARCHAR
    // column named `value`; the second step slices it via SUBSTR.
    val isPosition = mergedMetadata.resolveFormat() == Format.POSITION
    val incomingSparkSchema =
      if (isPosition)
        StructType(Seq(StructField("value", StringType)))
      else
        schema.sparkSchemaWithIgnoreAndScript(schemaHandler, !isTemporary)
    val domainAndTableName = domain + "." + table
    val optionsWrite =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, domainAndTableName, sinkConnection.options)
    val ddlMap = schemaHandler.getDdlMapping(schema.attributes)
    val attrsWithDDLTypes = schemaHandler.getAttributesWithDDLType(schema, "duckdb")

    // Create or update table schema first
    JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
      conn =>
        val previousAutoCommit = conn.getAutoCommit
        conn.setAutoCommit(false)
        try {
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
              .mkString("['", "','", "']")
          val rejected = mergedMetadata.resolveFormat() match {
            case Format.DSV =>
              val nullstr =
                if (Option(mergedMetadata.resolveNullValue()).isEmpty)
                  ""
                else
                  s"nullstr = '${mergedMetadata.resolveNullValue()}',"
              val options = readCsvOptionsWithoutReserved(mergedMetadata.getOptions())
              val extraOptions =
                if (options.nonEmpty)
                  options
                    .map { case (k, v) =>
                      s"$k = '$v'"
                    }
                    .mkString("", ",", ",")
                else
                  ""

              // store_rejects makes DuckDB skip and record malformed lines instead of
              // aborting the whole INSERT. The rejected lines are read back below, on this
              // same connection, because reject_errors is a session scoped temp table.
              val sql = s"""INSERT INTO $domainAndTableName SELECT
             | * FROM read_csv(
             | ${paths},
             | delim = '${mergedMetadata.resolveSeparator()}',
             | header = ${mergedMetadata.resolveWithHeader()},
             | quote = '${mergedMetadata.resolveQuote()}',
             | escape = '${mergedMetadata.resolveEscape()}',
             | store_rejects = true,
             | $nullstr
             | $extraOptions
             | columns = { $columnsString});""".stripMargin
              JdbcDbUtils.execute(sql, conn)
              DuckDbRejectCapture.captureCsvRejects(conn)

            case Format.POSITION =>
              // Load each line of the fixed-width file as a single VARCHAR column named
              // `value`. The field delimiter is set to SOH (\x01) — a control char
              // vanishingly unlikely to appear in fixed-width data, so each line becomes
              // one field. Quote and escape are disabled so any `"` or `\` in the data is
              // treated as a literal byte. Same approach as the BigQuery native loader.
              val skip =
                if (mergedMetadata.resolveWithHeader()) "skip = 1," else ""
              val encoding =
                duckDbEncoding(mergedMetadata.resolveEncoding())
                  .map(enc => s"encoding = '$enc',")
                  .getOrElse("")
              val sql = s"""INSERT INTO $domainAndTableName SELECT
             | * FROM read_csv(
             | ${paths},
             | delim = e'\\x01',
             | quote = '',
             | escape = '',
             | header = false,
             | $skip
             | $encoding
             | columns = {'value': 'VARCHAR'});""".stripMargin
              JdbcDbUtils.execute(sql, conn)
              DuckDbRejectCapture.capturePositionRejects(
                conn = conn,
                tableName = domainAndTableName,
                filePath = path.map(_.toString).mkString(","),
                schema = schema,
                ddlTypesByAttribute = attrsWithDDLTypes.toMap
              )

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
                    val sql =
                      s"""INSERT INTO $domainAndTableName SELECT * FROM read_json($paths, auto_detect = true, format = '$format');"""
                    JdbcDbUtils.execute(sql, conn)
                }
              }
              List.empty[RejectedLine]
            case _ => List.empty[RejectedLine]
          }
          if (!isTemporary && rejectThresholdBreached(rejected.size)) {
            // The rejects have already been captured into Scala values above, and the
            // exception carries them, so the rollback performed by the catch block below
            // cannot cost us the replay file even if it fails. That ordering matters
            // because a ROLLBACK also discards the session scoped reject_errors table.
            throw new RejectThresholdExceededException(
              rejected,
              rejectThresholdMessage(rejected.size)
            )
          }
          // Known limitation: SparkUtils.updateJdbcTableSchema (called above for the APPEND
          // strategy) calls JdbcDbUtils.executeAlterTable, which commits everything pending on
          // the connection, not just the ALTER. At that point only the CREATE SCHEMA and the
          // metadata reads are pending, since the INSERT runs later, so the data rollback
          // guarantee still holds. Schema changes themselves are not rolled back.
          conn.commit()
          rejected
        } catch {
          case NonFatal(e) =>
            Try(conn.rollback())
            throw e
        } finally {
          Try(conn.setAutoCommit(previousAutoCommit))
        }
    }
  }
}
