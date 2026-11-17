package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.{AuditTaskBuilder, IngestionJob, IngestionUtil, RejectedRecord}
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
import java.nio.file.Files
import java.sql.Connection
import scala.collection.mutable.ListBuffer
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
    // The rows of each Parquet file are checked while loading it into a table of the first step,
    // before the second step writes the target (#1952)
    mergedMetadata.resolveFormat() == Format.PARQUET ||
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

  /** The local temporary files the reject captures spilled their raw lines into, deleted once the
    * load is over whatever became of it. They are only ever read by the replay writer, which runs
    * inside the load.
    */
  private val spillFiles = ListBuffer[java.nio.file.Path]()

  /** The rows of each Parquet file that the first step rejects, in the order of the files (#1952)
    */
  private val parquetRejects = ListBuffer[RejectedRows]()

  def run(): Try[List[IngestionCounters]] = {
    try runLoad()
    finally deleteSpillFiles()
  }

  private def runLoad(): Try[List[IngestionCounters]] = {
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
      val rejected: RejectCapture = if (twoSteps) {
        // Registered before each CREATE so a first-step failure (create ok but insert
        // failed, or a later file's load failed) still drops every zztmp table already
        // left behind; dropTable is IF EXISTS, so a name whose CREATE never ran is a no-op.
        val createdTempTables = scala.collection.mutable.ListBuffer[String]()
        try {
          val tempTablesWithRejects =
            path.map { p =>
              logger.info(s"Loading $p to temporary table")
              val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
              createdTempTables += tempTable
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
          val rejectedLines =
            tempTablesWithRejects.map(_._2).foldLeft(RejectCapture.empty)(_ ++ _)
          val targetFullTableName = s"${domain.finalName}.${schema.finalName}"
          val parquetRejected = parquetRejects.map(_.count).sum
          if (parquetRejected > 0) {
            // As a load in one step, over the files of the load
            val error = BigQueryNativeLoader.rejectedRowsError(
              parquetRejected,
              settings.appConfig.rejectAllOnError,
              settings.appConfig.rejectMaxRecords
            )
            val message = s"$parquetRejected rows of $targetFullTableName are rejected"
            logger.warn(s"$message. ${error.getOrElse("The others are loaded")}")
            // As the Spark loads, the rejected rows are kept before the load fails because of them,
            // once every file is loaded: a retry of a load that failed on a later file would write
            // them again
            if (settings.appConfig.sinkReplayToFile)
              parquetRejects.flatMap(_.replay).foreach(writeReplayFile)
            sinkRejected(parquetRejects.flatMap(_.records).toList)
            error.foreach(error => throw new IllegalArgumentException(s"$message. $error"))
          }

          if (rejectThresholdBreached(rejectedLines.count)) {
            // The target table has not been written yet at this point, so aborting here
            // leaves it untouched. The temp tables are dropped by the finally block below.
            throw new RejectThresholdExceededException(
              rejectedLines,
              rejectThresholdMessage(rejectedLines.count)
            )
          }
          // Rejects first, target rows second, the ordering SparkIngestionPipeline.ingest
          // uses. If the replay area cannot be written or the audit connection is down, this
          // throws before anything lands in the target, so the failed load leaves nothing
          // behind and the same input can be loaded again without double appending.
          val replayFilePath = reportRejects(rejectedLines)
          val unionTempTables = tempTables
            .map(s"SELECT * FROM ${domain.finalName}." + _)
            .mkString("(", " UNION ALL ", ")")
          // Per-attribute DDL type, used by POSITION to wrap each SUBSTR projection in
          // TRY_CAST (DuckDB's SAFE_CAST) so malformed cells yield NULL instead of
          // aborting the whole INSERT.
          val ddlTypesByAttribute: Map[String, String] = Try(
            schemaHandler.getAttributesWithDDLType(schema, "duckdb").toMap
          ).getOrElse(Map.empty)
          // Quoted: a column may be named like a keyword
          val sqlWithTransformedFields = schema.buildSecondStepSqlSelectOnLoad(
            unionTempTables,
            jdbcEngine = Some(settings.appConfig.jdbcEngines("duckdb")),
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
          try {
            // A failed second step fails the load, whose files must not be archived
            ingestionJob.writingWith(job)(job.run().get)
          } catch {
            case NonFatal(e) =>
              // The rejects were reported above, before the second step ran. Nothing about the
              // input data is wrong with a load that fails here, so this attempt's replay file
              // and audit rejected rows describe an attempt that never happened: leaving them
              // would make the retry, once the broken SQL is fixed, count the same rejects
              // twice and stack a second replay file next to the first.
              cleanupRejectArtifacts(replayFilePath, rejectedLines.nonEmpty)
              throw e
          }
          rejectedLines
        } finally {
          createdTempTables.foreach { tempTable =>
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
      // JSON has no reject capture in DuckDB, so it keeps reporting an unknown count. The
      // Parquet loads find their rejected rows in the first step (#1952)
      val format = mergedMetadata.resolveFormat()
      val rejectsSupported =
        format == Format.DSV || format == Format.POSITION || format == Format.PARQUET
      val rejectedCount =
        if (format == Format.PARQUET) parquetRejects.map(_.count).sum
        else if (rejectsSupported) rejected.count
        else -1L
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
    * it survives renamed or reordered source columns. DSV and POSITION both drop that line on the
    * way in, DSV through the read_csv header option and POSITION through skip = 1, so both need it
    * back on the way out or a re-ingest would silently drop the first rejected record.
    */
  private def replayHeaderLine(): Option[String] = {
    val format = mergedMetadata.resolveFormat()
    val hasHeaderLine =
      (format == Format.DSV || format == Format.POSITION) &&
      mergedMetadata.resolveWithHeader().booleanValue()
    if (hasHeaderLine) {
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
    *
    * The declared names are used, not the final ones. `IngestionAudit.buildAuditLog` writes
    * `domain.name` and `schema.name` into audit.audit, and the Spark reject path
    * (`IngestionJob.saveRejected`) writes the same declared names into audit.rejected and into the
    * replay file name. A table carrying a `rename` would otherwise leave audit.rejected rows that
    * no longer join audit.audit on (jobid, domain, schema), and a replay file under a different
    * domain area than the Spark loader writes to.
    *
    * @return
    *   the replay file this call wrote, so a caller that then fails can take it back out again
    */
  private def reportRejects(rejected: RejectCapture): Option[Path] = {
    val replayFilePath = if (rejected.nonEmpty && settings.appConfig.sinkReplayToFile) {
      ReplayFileWriter.write(
        domainName = domain.name,
        tableName = schema.name,
        rejected = rejected,
        header = replayHeaderLine(),
        encoding = mergedMetadata.resolveEncoding(),
        timestamp = ingestionJob.now,
        jobid = ingestionJob.applicationId(),
        inputFileName = path.headOption.map(_.getName)
      )(settings, storageHandler)
    } else {
      None
    }
    if (rejected.nonEmpty) {
      NativeRejectedSink
        .sink(
          applicationId = ingestionJob.applicationId(),
          domainName = domain.name,
          tableName = schema.name,
          rejected = rejected,
          paths = path,
          timestamp = ingestionJob.now,
          scheduledDate = scheduledDate,
          accessToken = ingestionJob.accessToken
        )(settings, storageHandler, schemaHandler)
        .get
    }
    replayFilePath
  }

  /** Remembers the spill files a capture owns, so the load can delete them once the replay file has
    * been written, whatever happened in between.
    */
  private def recordSpills(capture: RejectCapture): RejectCapture = {
    spillFiles ++= capture.spillFiles
    capture
  }

  private def deleteSpillFiles(): Unit = {
    spillFiles.foreach { spillFile =>
      Try(Files.deleteIfExists(spillFile)).failed.foreach { e =>
        logger.warn(s"Failed to delete the temporary reject spill file $spillFile", e)
      }
    }
    spillFiles.clear()
  }

  /** Takes back the reject artifacts of the attempt that just failed its second step.
    *
    * Best effort on purpose: the caller is already on its way out with the real failure, and a
    * cleanup that cannot run is a stale replay file and a few audit rows, not a reason to replace
    * the error the user needs to read. Whatever happens here is logged and swallowed.
    *
    * Only the second step failure path calls this. A load aborted on the reject threshold keeps its
    * artifacts: there the data is what is wrong and the replay file is the user's repair tool.
    */
  private def cleanupRejectArtifacts(replayFilePath: Option[Path], hadRejects: Boolean): Unit = {
    Try {
      replayFilePath.foreach { replayFile =>
        logger.info(s"Deleting the replay file $replayFile of the load attempt that just failed")
        storageHandler.delete(replayFile)
      }
      if (hadRejects) {
        deleteAuditRejectedRows()
      }
    }.failed.foreach { e =>
      logger.warn(
        "Failed to clean up the reject artifacts of the load attempt that just failed. The " +
        "replay file or the audit rejected rows it wrote may be counted twice when the load " +
        "is retried.",
        e
      )
    }
  }

  /** Deletes this attempt's rows from the audit rejected table, through
    * `NativeRejectedSink.deleteSql`, which is the exact inverse of what that sink wrote.
    *
    * Only JDBC audit sinks are cleaned up. On a Cloud Logging sink the entries are immutable, and
    * on any other non JDBC sink, BigQuery among them, there is no connection here to run a DELETE
    * through, so on those sinks the rejected rows of a failed attempt stay and the retry adds its
    * own next to them. That is the behavior this cleanup was added to avoid, so it is warned about
    * rather than silently accepted.
    */
  private def deleteAuditRejectedRows(): Unit = {
    val audit = settings.appConfig.audit
    if (audit.isActive()) {
      val auditConnection = audit.getConnection()
      if (audit.getSink().getConnectionType() == ConnectionType.GCPLOG) {
        // Cloud Logging entries are immutable, so the rows of the failed attempt stay there and
        // the retry adds its own next to them.
        logger.warn(
          "The audit sink is Cloud Logging, whose entries cannot be deleted: the rejected lines " +
          "of the load attempt that just failed stay logged and the retry will log them again"
        )
      } else if (!auditConnection.isJdbcUrl()) {
        logger.warn(
          s"The audit sink connection ${audit.getConnectionRef()} is not a JDBC one, so the " +
          "rejected rows of the load attempt that just failed cannot be deleted and the retry " +
          "will write its own next to them"
        )
      } else {
        val sql = NativeRejectedSink.deleteSql(
          applicationId = ingestionJob.applicationId(),
          domainName = domain.name,
          tableName = schema.name,
          paths = path
        )
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), auditConnection.options) {
          conn =>
            JdbcDbUtils.execute(sql, conn)
        }
      }
    }
  }

  /** A load is aborted when it produces more rejects than allowed, or any reject at all when the
    * user asked for all or nothing.
    */
  private def rejectThresholdBreached(rejectedCount: Long): Boolean =
    rejectedCount > settings.appConfig.rejectMaxRecords ||
    (settings.appConfig.rejectAllOnError && rejectedCount > 0)

  /** Shared by the two step and the single step abort sites so the two messages cannot drift.
    */
  private def rejectThresholdMessage(rejectedCount: Long): String =
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
  ): RejectCapture = {
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
              .map(_.replace("'", "''"))
              .mkString("['", "','", "']")
          // recorded before the threshold check below, which throws with the capture in hand:
          // the spill file has to be deletable however this load ends
          val rejected = recordSpills(mergedMetadata.resolveFormat() match {
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
              // When they cannot be read back (Flight SQL) the lines are not skipped either:
              // DuckDB parses strictly and a malformed line fails the load.
              val captureRejects = DuckDbRejectCapture.csvRejectsReadable(sinkConnection)
              val storeRejects = if (captureRejects) "store_rejects = true," else ""
              val sql = s"""INSERT INTO $domainAndTableName SELECT
             | * FROM read_csv(
             | ${paths},
             | delim = '${mergedMetadata.resolveSeparator()}',
             | header = ${mergedMetadata.resolveWithHeader()},
             | quote = '${mergedMetadata.resolveQuote()}',
             | escape = '${mergedMetadata.resolveEscape()}',
             | $storeRejects
             | $nullstr
             | $extraOptions
             | columns = { $columnsString});""".stripMargin
              JdbcDbUtils.execute(sql, conn)
              if (captureRejects) DuckDbRejectCapture.captureCsvRejects(conn)
              else RejectCapture.empty

            case Format.POSITION =>
              // Load each line of the fixed-width file as a single VARCHAR column named
              // `value`. The field delimiter is set to SOH (\x01), a control char
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
                    // BY NAME: read_json's auto-detected columns come back in file order while
                    // the target table follows the inferred schema's (alphabetical) order, so a
                    // positional SELECT * would map fields to the wrong columns.
                    val sql =
                      s"""INSERT INTO $domainAndTableName BY NAME SELECT * FROM read_json($paths, auto_detect = true, format = '$format');"""
                    JdbcDbUtils.execute(sql, conn)
                }
              }
              RejectCapture.empty
            case Format.PARQUET =>
              // Always in two steps: one file, into a table of the first step. Its rejected rows
              // are kept as the Spark loads keep theirs, not as the lines of a text file
              parquetRejects += loadParquet(
                conn,
                domainAndTableName,
                schema,
                paths,
                path,
                attrsWithDDLTypes.toMap
              )
              RejectCapture.empty
            case _ => RejectCapture.empty
          })
          if (!isTemporary && rejectThresholdBreached(rejected.count)) {
            // The rejects have already been read off the connection above, counted and
            // spilled to a local file, and the exception carries that capture, so the
            // rollback performed by the catch block below cannot cost us the replay file
            // even if it fails. That ordering matters because a ROLLBACK also discards the
            // session scoped reject_errors table.
            throw new RejectThresholdExceededException(
              rejected,
              rejectThresholdMessage(rejected.count)
            )
          }
          // Known limitation: SparkUtils.updateJdbcTableSchema (called above for the APPEND
          // strategy) calls JdbcDbUtils.executeAlterTable, which commits everything pending on
          // the connection, not just the ALTER. At that point only the CREATE SCHEMA and the
          // metadata reads are pending, since the INSERT runs later, so the data rollback
          // guarantee still holds. Schema changes themselves are not rolled back.
          conn.commit()
          // Inside the block, before the postActions of the connection, which run once it
          // succeeded
          if (!isTemporary) ingestionJob.markRowsWritten()
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
