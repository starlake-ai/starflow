package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, DatasetArea, Settings}
import ai.starlake.extract.ParUtils
import ai.starlake.job.ingest.{BqLoadInfo, IngestionJob, IngestionUtil, RejectedRecord}
import ai.starlake.job.sink.bigquery.{
  BigQueryBranchHandler,
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryLoadJobException,
  BigQueryNativeJob
}
import ai.starlake.job.transform.BigQueryAutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.conversion.{
  BigQueryNativeSchema,
  BigQueryPositionValues,
  BigQueryType,
  BigQueryUtils
}
import ai.starlake.utils.{IngestionCounters, JobResult, Utils}
import com.google.cloud.bigquery
import com.google.cloud.bigquery.{
  BigQueryError,
  Field,
  FieldList,
  JobInfo,
  StandardSQLTypeName,
  StandardTableDefinition,
  TableDefinition,
  TableId
}
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.Storage
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.io.{BufferedWriter, OutputStreamWriter, Writer}
import java.nio.charset.Charset
import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, accessToken)
    with LazyLogging {

  lazy val targetTableId: TableId = {
    val originalTableId = BigQueryJobBase.extractProjectDatasetAndTable(
      schemaHandler.getDatabase(domain),
      domain.finalName,
      effectiveSchema.finalName,
      sinkConnection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
    )
    schemaHandler.dataBranch(sinkConnection.options) match {
      case Some(branchName) =>
        logger.info(
          s"Branch '$branchName' active: redirecting load target from ${domain.finalName}.${effectiveSchema.finalName} to $branchName.${effectiveSchema.finalName}"
        )
        val projectId = Option(originalTableId.getProject).getOrElse(
          BigQueryJobBase.projectId(
            sinkConnection.options.get("projectId"),
            settings.appConfig.getDefaultDatabase()
          )
        )
        val bqService = BigQueryJobBase.bigquery(
          connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
          accessToken = accessToken,
          outputDatabase = schemaHandler.getDatabase(domain)
        )
        val location = sinkConnection.options.getOrElse(
          "location",
          throw new Exception("location is required for BigQuery branching")
        )
        BigQueryBranchHandler
          .prepareBranch(
            branchName,
            bqService,
            originalTableId,
            "", // No source SQL for loads
            location,
            sinkConnection.options
          )
          .branchTargetTableId
      case None => originalTableId
    }
  }

  private lazy val format: Format = mergedMetadata.resolveFormat()

  override protected def requireTwoSteps(schema: SchemaInfo): Boolean =
    super.requireTwoSteps(schema) || format == Format.PARQUET && (
      // Local files are uploaded through a single stream per load job: the first step loads
      // them one by one, since their concatenation is not a Parquet file
      BigQueryNativeLoader.requiresOneLoadPerFile(format, path) ||
      // The first step reads every column as nullable, and the second step writes into the
      // target table, which fails on a null value of a required attribute
      BigQueryNativeLoader.hasRequiredAttributes(schema.attributes) ||
      // Run by the second step, as Spark ran them: a load in one step does not
      schema.presql.nonEmpty || schema.postsql.nonEmpty ||
      // The first step reads them as strings, that the second step converts. The target table
      // is not read yet: one of its columns can keep a mapped type even when the mappings are
      // disabled, so they are applied
      BigQueryNativeLoader
        .parquetConversions(
          BigQueryNativeLoader.firstStepSchema(
            schema,
            None,
            schemaHandler,
            fromDdlMapping = true,
            log = false
          )
        )
        .nonEmpty
    )

  private def firstStepSchema(
    schema: SchemaInfo,
    existingTargetSchema: Option[bigquery.Schema],
    log: Boolean
  ): bigquery.Schema =
    BigQueryNativeLoader.firstStepSchema(
      schema,
      existingTargetSchema,
      schemaHandler,
      settings.appConfig.bigqueryNativeSchemaFromDdlMapping,
      log
    )

  def run(): Try[List[IngestionCounters]] = {
    Try {
      val bqSink = mergedMetadata.getSink().asInstanceOf[BigQuerySink]

      val targetConfig =
        BigQueryLoadConfig(
          connectionRef = Some(mergedMetadata.getSinkConnectionRef()),
          source = Left(path.map(_.toString).mkString(",")),
          outputTableId = Some(targetTableId),
          sourceFormat = settings.appConfig.defaultWriteFormat,
          createDisposition = createDisposition,
          writeDisposition = writeDisposition,
          outputPartition = bqSink.getPartitionColumn(),
          outputClustering = bqSink.clustering.getOrElse(Nil),
          days = bqSink.days,
          requirePartitionFilter = bqSink.requirePartitionFilter.getOrElse(false),
          rls = effectiveSchema.rls,
          acl = effectiveSchema.acl,
          partitionsToUpdate = Nil,
          starlakeSchema = Some(schemaWithMergedMetadata),
          domainTags = domain.tags,
          domainDescription = domain.comment,
          outputDatabase = schemaHandler.getDatabase(domain),
          accessToken = accessToken
        )
      // Read once: the types of its existing columns are kept by every step
      val existingTargetSchema = targetSchema(targetConfig)
      if (twoSteps) {
        val (loadResults, tempTableIds, tableInfos) =
          ParUtils
            .runInParallel(path.map(_.toString).zipWithIndex, Some(settings.appConfig.maxParTask)) {
              case (sourceUri, index) =>
                val firstStepTempTable =
                  BigQueryJobBase.extractProjectDatasetAndTable(
                    schemaHandler.getDatabase(domain),
                    domain.finalName,
                    tempTableName + "_" + index,
                    sinkConnection.options
                      .get("projectId")
                      .orElse(settings.appConfig.getDefaultDatabase())
                  )
                val firstStepConfig =
                  BigQueryNativeLoader.firstStepConfig(targetConfig, sourceUri, firstStepTempTable)

                val firstStepBigqueryJob = new BigQueryNativeJob(firstStepConfig, "")
                // For POSITION format the first step loads each line as a single VARCHAR
                // column named `value`; the second step slices it via SUBSTR.
                val isPosition = format == Format.POSITION
                val toBQSchema: SchemaInfo => bigquery.Schema =
                  if (isPosition)
                    _ =>
                      bigquery.Schema.of(
                        Field
                          .newBuilder("value", StandardSQLTypeName.STRING)
                          .setMode(Field.Mode.NULLABLE)
                          .build()
                      )
                  else
                    schema => {
                      val patched =
                        firstStepSchema(
                          schema,
                          existingTargetSchema,
                          // Logged once per load, before the first load job
                          log = index == 0
                        )
                      if (format == Format.PARQUET)
                        BigQueryNativeLoader.parquetFirstStepSchema(patched)
                      else patched
                    }
                val firstStepTableInfo = firstStepBigqueryJob.getTableInfo(
                  firstStepTempTable,
                  toBQSchema
                )

                val enrichedTableInfo = withInputFileName(firstStepTableInfo, sourceUri)

                // TODO What if type is changed by transform ? we need to use safe_cast to have the same behavior as in SPARK.
                val firstStepResult =
                  firstStepBigqueryJob
                    .loadPathsToBQ(firstStepTableInfo, Some(enrichedTableInfo))
                (
                  firstStepResult,
                  firstStepTempTable,
                  enrichedTableInfo
                )
            }
            .foldLeft[(List[Try[BqLoadInfo]], List[TableId], List[TableInfo])]((Nil, Nil, Nil)) {
              case (
                    (loadResultList, tempTableIdList, tableInfoList),
                    (firstStepResult, firstStepTempTable, enrichedTableInfo)
                  ) =>
                (
                  loadResultList :+ firstStepResult,
                  tempTableIdList :+ firstStepTempTable,
                  tableInfoList :+ enrichedTableInfo
                )
            }
        def tryListSequence[A](list: List[Try[A]]): Try[List[A]] = {
          list.foldRight(Try(List.empty[A])) { (tryElem, acc) =>
            for {
              elem <- tryElem
              rest <- acc
            } yield elem :: rest
          }
        }

        keepBadRecordsOfFailedJobs(loadResults.collect {
          case Failure(exception: BigQueryLoadJobException) => exception
        })
        val output: Try[List[BqLoadInfo]] =
          applyBigQuerySecondStep(
            targetConfig,
            existingTargetSchema,
            List(
              BigQueryJobBase.extractProjectDatasetAndTable(
                schemaHandler.getDatabase(domain),
                domain.finalName,
                tempTableName + "_*",
                sinkConnection.options
                  .get("projectId")
                  .orElse(settings.appConfig.getDefaultDatabase())
              )
            ),
            tempTableIds,
            tryListSequence(loadResults)
          )

        try {
          tempTableIds.zip(tableInfos).foreach { case (id, info) =>
            val database = Option(id.getProject()).getOrElse("")
            val schema = Option(id.getDataset()).getOrElse("")
            val table = Option(id.getTable()).getOrElse("")
            archiveTableTask(database, schema, table, info).foreach(_.run())
          }
        } finally {
          Try(ParUtils.runInParallel(tempTableIds, Some(settings.appConfig.maxParTask)) { tableId =>
            BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
              new BigQueryNativeJob(targetConfig, "").dropTable(tableId)
            }
          }).recover { case exception =>
            Utils.logException(logger, exception)
          }
        }
        output
      } else {
        // Single step ingestion
        val bigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val tableInfo = bigqueryJob
          .getTableInfo(
            targetTableId,
            schema =>
              BigQueryNativeSchema.patch(
                schema.bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true),
                schema.attributes.filterNot(_.resolveIgnore()),
                _.getFinalName(),
                // A load that replaces the table can change the type of its columns
                if (writeDisposition == JobInfo.WriteDisposition.WRITE_TRUNCATE.name()) None
                else existingTargetSchema,
                schemaHandler,
                settings.appConfig.bigqueryNativeSchemaFromDdlMapping
              )
          )
        // A Parquet load job neither adds nor relaxes columns: the target table gets them first
        if (
          format == Format.PARQUET &&
          writeDisposition == JobInfo.WriteDisposition.WRITE_APPEND.name()
        )
          for {
            existing <- existingTargetSchema
            incoming <- tableInfo.maybeSchema
            aligned  <- BigQueryNativeLoader.schemaForParquetAppend(existing, incoming)
          } {
            logger.info(s"Adding or relaxing the columns of $targetTableId before the load")
            val table = bigqueryJob.getTable(targetTableId).get
            table.toBuilder
              .setDefinition(
                table.getDefinition[StandardTableDefinition].toBuilder.setSchema(aligned).build()
              )
              .build()
              .update()
          }
        bigqueryJob
          .loadPathsToBQ(tableInfo)
          .flatMap { loadInfo =>
            // The bad records that a one-step load job skipped, known once it loaded the others: a
            // failure to keep them does not fail the load, whose rows are written. Two-step loads
            // keep them before their second step.
            Try(keepBadRecords(targetConfig, tableInfo.maybeSchema, List(loadInfo))).failed
              .foreach { exception =>
                logger.error(
                  s"Could not write the rejected records of $targetFullTableName to the audit",
                  exception
                )
              }
            // Then the ACL, row access policies and policy tags of the table: a failure fails the
            // load, whose rows are written, as with Spark and the second step of two-step loads
            BigQueryJobBase
              .recoverBigqueryException(settings.appConfig.onExceptionRetries)(
                bigqueryJob.applyRLSAndCLS().get
              )
              .map(_ => List(loadInfo))
          }
          .recoverWith { case exception: BigQueryLoadJobException =>
            keepBadRecordsOfFailedJobs(List(exception))
            Failure(exception)
          }
      }
    }.map {
      case Success(results) =>
        results.foreach {
          _.jobResult.job.flatMap(j => Option(j.getStatus.getExecutionErrors)).foreach { errors =>
            errors.forEach(err => logger.error(f"${err.getReason} - ${err.getMessage}"))
          }
        }
        val bqLoadInfoOutput = if (settings.appConfig.audit.detailedLoadAudit) {
          results
        } else {
          def combineStats(bqLoadInfo1: BqLoadInfo, bqLoadInfo2: BqLoadInfo): BqLoadInfo = {
            BqLoadInfo(
              totalAcceptedRows = bqLoadInfo1.totalAcceptedRows + bqLoadInfo2.totalAcceptedRows,
              totalRejectedRows = bqLoadInfo1.totalRejectedRows + bqLoadInfo2.totalRejectedRows,
              paths = bqLoadInfo1.paths ++ bqLoadInfo2.paths,
              jobResult = BigQueryJobResult(
                None,
                bqLoadInfo1.jobResult.totalBytesProcessed + bqLoadInfo2.jobResult.totalBytesProcessed,
                None
              )
            )
          }
          List(results.reduce(combineStats))
        }
        bqLoadInfoOutput.map { bli =>
          IngestionCounters(
            inputCount = bli.totalRows,
            acceptedCount = bli.totalAcceptedRows,
            rejectedCount = bli.totalRejectedRows,
            paths = bli.paths,
            jobid = ingestionJob.applicationId()
          )
        }
      case Failure(exception) =>
        Utils.logException(logger, exception)
        throw exception
    }
  }

  /** @return
    *   the schema of the target table, if it exists
    */
  private def targetSchema(targetConfig: BigQueryLoadConfig): Option[bigquery.Schema] = {
    // A failure to read the table fails the load: its column types would be ignored otherwise.
    // BigQuery returns null when the table does not exist.
    Option(new BigQueryNativeJob(targetConfig, "").getTable(targetTableId).get)
      .flatMap(table => Option(table.getDefinition[TableDefinition]().getSchema))
  }

  private def applyBigQuerySecondStep(
    targetConfig: BigQueryLoadConfig,
    existingTargetSchema: Option[bigquery.Schema],
    firstStepTempTable: List[TableId],
    firstStepTables: List[TableId],
    firstStepResult: Try[List[BqLoadInfo]]
  ): Try[List[BqLoadInfo]] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: ${loadFileResult.map(_.toString).mkString(", ")}")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val tempTableNames = firstStepTempTable.map(t => BigQueryUtils.tableIdToTableName(t))
        val result = for {
          // The bad records that the first step skipped, as BigQuery reports them, before
          // anything is loaded into the target: a failure to keep them fails the load, as with
          // Spark
          _ <- Try(
            keepBadRecords(
              targetConfig,
              Some(firstStepSchema(schemaWithMergedMetadata, existingTargetSchema, log = false)),
              loadFileResult
            )
          )
          incomingSchema <- Try(this.incomingSchema(existingTargetSchema))
          castTypes = BigQueryNativeSchema.castTypes(
            incomingSchema,
            starlakeSchema.attributes,
            schemaHandler
          )
          checks = firstStepChecks(castTypes)
          (rejectedByFile, filteredByFile) <- checks
            .map(checks =>
              rejectRows(
                targetBigqueryJob,
                tempTableNames,
                firstStepTables.map(BigQueryUtils.tableIdToTableName),
                checks
              )
            )
            .getOrElse(Success((Map.empty[Int, Long], Map.empty[Int, Long])))
          _ <- targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              applyBigQuerySecondStepSQL(
                tempTableNames,
                existingTargetSchema,
                incomingSchema,
                castTypes,
                checks.filter(_.checks.nonEmpty).map(_.condition)
              )
            }
            .getOrElse(throw new IllegalStateException("Should never happen"))
        } yield BigQueryNativeLoader.withRejectedRows(
          loadFileResult,
          rejectedByFile,
          filteredByFile
        )
        result
      case res @ Failure(_) =>
        res
    }
  }

  /** @return
    *   the schema of the rows that the second step writes into the target table
    */
  private def incomingSchema(existingTargetSchema: Option[bigquery.Schema]): bigquery.Schema =
    BigQueryNativeSchema.patch(
      starlakeSchema.bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true),
      starlakeSchema.attributes.filterNot(_.resolveIgnore()),
      _.getFinalName(),
      existingTargetSchema,
      schemaHandler,
      settings.appConfig.bigqueryNativeSchemaFromDdlMapping,
      // Logged by the first step
      log = false
    )

  /** @param castTypes
    *   the types that the second step casts the values of POSITION files to, instead of their
    *   mapping
    * @return
    *   the checks of the rows of the first step that the second step rejects, if any: the first
    *   step of a Parquet load reads every column as nullable, and the rows without a value for a
    *   required attribute are rejected; the first step of a POSITION load reads each line as a
    *   string, and the rows with a value that does not convert to the type of its attribute, or
    *   without a value for a required attribute, are rejected (#1886)
    */
  private def firstStepChecks(
    castTypes: Map[String, String]
  ): Option[BigQueryNativeLoader.FirstStepChecks] =
    format match {
      case Format.PARQUET =>
        BigQueryNativeLoader.requiredValuesChecks(
          starlakeSchema.attributes,
          settings.appConfig.rejectWithValue
        )
      case Format.POSITION =>
        val ddlTypes = ddlTypesByAttribute(castTypes)
        // The Spark fallback reads the mappings: a column that kept another type may need a
        // pattern that BigQuery cannot apply, which would reject every row
        val unreadable =
          starlakeSchema.exceptIgnoreScriptAndTransformAttributes().flatMap { attribute =>
            typesByAttribute
              .get(attribute.name)
              .flatMap(BigQueryPositionValues.unreadable(_, ddlTypes.get(attribute.name)))
              .map(reason =>
                s"${attribute.name} ($reason, read into a ${ddlTypes.getOrElse(attribute.name, "")} column)"
              )
          }
        if (unreadable.nonEmpty)
          throw new IllegalArgumentException(
            s"The native bigquery loader does not read the values of ${unreadable.mkString(", ")} of $targetFullTableName: load it with Spark (loader: spark)"
          )
        val checks = BigQueryNativeLoader.positionChecks(
          starlakeSchema.attributes,
          ddlTypes,
          typesByAttribute,
          settings.appConfig.rejectWithValue
        )
        // The rows that the filter drops are neither rejected nor counted, even without checks
        starlakeSchema.filter.fold(checks) { filter =>
          // Rendered as the second step renders its query, but for the references to tables
          val vars = schemaHandler.activeEnvVars()
          def render(sql: String) =
            Utils.parseJinja(
              SQLUtils.instantiateMacrosInSql(
                SQLUtils.stripComments(sql),
                schemaHandler.allMacros,
                vars
              ),
              vars
            )
          Some(
            checks
              .getOrElse(BigQueryNativeLoader.FirstStepChecks("TRUE", Nil, None, ""))
              .copy(scope =
                Some(
                  BigQueryNativeLoader.RowScope(
                    render(
                      starlakeSchema
                        .secondStepProjections(queryEngine, ddlTypes, typesByAttribute)
                        .mkString(", ")
                    ),
                    render(filter)
                  )
                )
              )
          )
        }
      case _ => None
    }

  /** @param tempTableNames
    *   the wildcard name of the temporary tables of the first step, whose suffix is the index of
    *   their file in the load
    * @return
    *   the number of rows of the first step that the checks reject, and of those that the filter
    *   drops, by index of their file. The rejected rows are rejected as BigQuery rejects the bad
    *   records of the other formats, or the load fails (see
    *   [[BigQueryNativeLoader.rejectedRowsError]]); the filtered rows are not counted.
    */
  private def rejectRows(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    tables: List[String],
    checks: BigQueryNativeLoader.FirstStepChecks
  ): Try[(Map[Int, Long], Map[Int, Long])] =
    job
      .runBigQueryJob(Some(tempTableNames.map(checks.countsSql).mkString(" UNION ALL ")))
      .map { result =>
        val counts = result.tableResult.toList
          .flatMap(_.iterateAll().asScala)
          .map(row =>
            (row.get(0).getStringValue.toInt, row.get(1).getLongValue, row.get(2).getLongValue)
          )
        def byFile(count: ((Int, Long, Long)) => Long): Map[Int, Long] =
          counts.groupMapReduce(_._1)(count)(_ + _).filter { case (_, rows) => rows > 0 }
        val rejectedByFile = byFile(_._2)
        val filteredByFile = byFile(_._3)
        if (filteredByFile.nonEmpty)
          logger.info(
            s"${filteredByFile.values.sum} rows of $targetFullTableName are dropped by its filter"
          )
        val rejected = rejectedByFile.values.sum
        if (rejected > 0) {
          val files = rejectedByFile.toList.sorted.map { case (index, count) =>
            s"${path.lift(index).map(_.toString).getOrElse(index.toString)} ($count)"
          }
          val message =
            s"$rejected rows of $targetFullTableName ${checks.reason}, in ${files.take(10).mkString(", ")}${if (files.size > 10)
                s" and ${files.size - 10} other files"
              else ""}"
          val error = BigQueryNativeLoader.rejectedRowsError(
            rejected,
            settings.appConfig.rejectAllOnError,
            settings.appConfig.rejectMaxRecords
          )
          logger.warn(s"$message. ${error.getOrElse("They are rejected")}")
          // As the Spark loads, the rejected rows are kept before the load fails because of them
          keepRejectedRows(job, tempTableNames, tables, checks)
          error.foreach(error => throw new IllegalArgumentException(s"$message. $error"))
        }
        (rejectedByFile, filteredByFile)
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
        accessToken,
        ingestionJob.session
      )(settings, storageHandler, schemaHandler)
      .get

  private def rejectedRecord(errors: List[String], file: String, value: Option[String]) =
    RejectedRecord(
      ingestionJob.applicationId(),
      ingestionJob.now,
      domain.name,
      starlakeSchema.name,
      RejectedRecord.errorText(errors, file, value),
      RejectedRecord.limited(path.map(_.toString).mkString(","))
    )

  /** The bad records that a load job skipped, as BigQuery reports them, without their rows (#1857).
    * They hold the bad values in BigQuery's words, whatever `rejectWithValue`.
    */
  private def badRecords(job: bigquery.Job, paths: List[String]): List[RejectedRecord] =
    BigQueryNativeLoader
      .badRecords(
        paths,
        Option(job.getStatus.getExecutionErrors).map(_.asScala.toList).getOrElse(Nil),
        Option(job.getStatus.getError)
      )
      .map { case (file, message) => rejectedRecord(List(message), file, None) }

  private def badRecordsOf(loadInfo: BqLoadInfo): List[RejectedRecord] =
    loadInfo.jobResult.job.toList.flatMap(badRecords(_, loadInfo.paths))

  /** Keeps the bad records of the load jobs that failed because of them, as Spark keeps the
    * rejected rows of a load that fails: the load fails anyway, with the error of its job
    */
  private def keepBadRecordsOfFailedJobs(failures: List[BigQueryLoadJobException]): Unit =
    if (failures.nonEmpty)
      Try(sinkRejected(failures.flatMap(failure => badRecords(failure.job, failure.paths)))).failed
        .foreach { exception =>
          logger.error(
            s"Could not write the rejected records of $targetFullTableName to the audit",
            exception
          )
        }

  /** @return
    *   the table info of a load of one file, whose table has a column with the name of the file
    */
  private def withInputFileName(tableInfo: TableInfo, sourceUri: String): TableInfo =
    tableInfo.copy(
      maybeSchema = tableInfo.maybeSchema.map((schema: bigquery.Schema) =>
        bigquery.Schema.of(
          (schema.getFields.asScala :+
          Field
            .newBuilder(CometColumns.cometInputFileNameColumn, StandardSQLTypeName.STRING)
            .setDefaultValueExpression(
              s"'${sourceUri.replace("\\", "\\\\").replace("'", "\\'")}'"
            )
            .build()).asJava
        )
      ),
      maybeDurationMs = Some(Duration.ofHours(24).toMillis)
    )

  /** Keeps the bad records that the load jobs skipped: the rejected rows of CSV and JSON files,
    * found by [[findRejectedRows]] (#1857), or else the bad records that BigQuery reports. A
    * failure to write them to the audit deletes their replay file.
    *
    * @param loadSchema
    *   the schema of the load jobs
    */
  private def keepBadRecords(
    targetConfig: BigQueryLoadConfig,
    loadSchema: => Option[bigquery.Schema],
    loadInfos: List[BqLoadInfo]
  ): Unit = {
    val withBadRecords = loadInfos.filter(_.totalRejectedRows > 0)
    val found =
      if (
        withBadRecords.nonEmpty && BigQueryRejectedRows.supports(format) &&
        (settings.appConfig.audit.maxErrors > 0 || settings.appConfig.sinkReplayToFile)
      )
        loadSchema.flatMap { schema =>
          Try(findRejectedRows(targetConfig, schema, withBadRecords)) match {
            case Success(found) => Some(found)
            case Failure(exception) =>
              logger.warn(
                s"Could not find the rejected rows of $targetFullTableName: the bad records that BigQuery reports are kept instead",
                exception
              )
              None
          }
        }
      else None
    found match {
      case Some(found) =>
        try sinkRejected(found.records)
        catch {
          case exception: Exception =>
            found.replay.foreach(path => Try(storageHandler.delete(path)))
            throw exception
        }
      case None => sinkRejected(loadInfos.flatMap(badRecordsOf))
    }
  }

  /** The rejected rows of load jobs, found by [[findRejectedRows]]
    *
    * @param records
    *   the records to write to the audit
    * @param replay
    *   their replay file, if written
    */
  private case class FoundRows(records: List[RejectedRecord], replay: Option[Path])

  /** Finds the rejected rows of load jobs of CSV or JSON files, to keep them as the Spark loads
    * keep theirs. The files are loaded again into diagnostic tables, every CSV value or JSON line
    * as a string, which are queried with the rules of the load jobs for the types of the load
    * schema, then dropped.
    *
    * The rejected rows of a load job are exact when there are as many as its bad records that the
    * diagnostic loads did not reject, whose errors are structural. The records are the first
    * `audit.maxErrors` rejected rows, then the bad records of the diagnostic loads as BigQuery
    * reports them, and for the load jobs whose rows are not exact, their own bad records as
    * BigQuery reports them. With `sinkReplayToFile`, all the rejected rows are written to a replay
    * file, as lines of the input files, when the rows of every load job are exact: so that no
    * loaded row is loaded again.
    *
    * @param loadInfos
    *   the load jobs, with bad records
    */
  private def findRejectedRows(
    targetConfig: BigQueryLoadConfig,
    loadSchema: bigquery.Schema,
    loadInfos: List[BqLoadInfo]
  ): FoundRows = {
    val withValue = settings.appConfig.rejectWithValue
    val maxErrors = settings.appConfig.audit.maxErrors
    val diagnosticMetadata = BigQueryRejectedRows.diagnosticMetadata(mergedMetadata)
    val diagnosticSchema = BigQueryRejectedRows.diagnosticSchema(format, loadSchema)
    val name = SQLUtils.temporaryTableName(starlakeSchema.finalName)
    // In the dataset of the target, the dataset of its branch included
    def tableId(suffix: String): TableId =
      Option(targetTableId.getProject)
        .map(TableId.of(_, targetTableId.getDataset, s"${name}_$suffix"))
        .getOrElse(TableId.of(targetTableId.getDataset, s"${name}_$suffix"))
    val files = loadInfos.zipWithIndex.flatMap { case (loadInfo, job) =>
      loadInfo.paths.map(job -> _)
    }
    val tableIds = files.indices.toList.map(index => tableId(index.toString))
    try {
      val diagnosticLoads =
        ParUtils
          .runInParallel(files.zip(tableIds), Some(settings.appConfig.maxParTask)) {
            case ((job, file), tableId) =>
              val diagnosticJob = new BigQueryNativeJob(
                targetConfig.copy(
                  source = Left(file),
                  outputTableId = Some(tableId),
                  outputTableDesc =
                    Some("Temporary table created to find the rejected rows of a load."),
                  createDisposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(),
                  writeDisposition = JobInfo.WriteDisposition.WRITE_APPEND.name(),
                  outputPartition = None,
                  outputClustering = Nil,
                  days = None,
                  requirePartitionFilter = false,
                  // A row access policy would hide rows from the query. The column-level security
                  // of the attributes still protects their raw values
                  rls = Nil,
                  acl = Nil,
                  starlakeSchema =
                    Some(schemaWithMergedMetadata.copy(metadata = Some(diagnosticMetadata)))
                ),
                ""
              )
              val tableInfo = diagnosticJob.getTableInfo(tableId, _ => diagnosticSchema)
              val loaded = diagnosticJob
                .loadPathsToBQ(tableInfo, Some(withInputFileName(tableInfo, file)))
                .get
              // The policy tags of the attributes protect their raw values: the rows are searched
              // even when they cannot be applied
              diagnosticJob.applyRLSAndCLS().failed.foreach { exception =>
                logger
                  .warn(s"Could not apply the policy tags of the attributes to $tableId", exception)
              }
              job -> loaded
          }
          .toList
      // Rows for the audit only
      val limited = !settings.appConfig.sinkReplayToFile
      val sql = BigQueryRejectedRows.rejectedRowsSql(
        BigQueryUtils.tableIdToTableName(tableId("*")),
        files.map { case (job, _) => job },
        format,
        loadSchema,
        withValue,
        effectiveSchema.attributes,
        if (limited) Some(maxErrors) else None,
        emptyIsNull = mergedMetadata.nullValue.forall(_.isEmpty)
      )
      val rows = new BigQueryNativeJob(targetConfig, "")
        .runBigQueryJob(Some(sql), pageSize = Some(1000))
        .get
        .tableResult
        .getOrElse(throw new IllegalStateException(s"No result for $sql"))
      val names = loadSchema.getFields.asScala.toList.map(_.getName)
      // Not in the input files
      val scripted =
        effectiveSchema.attributes.filter(_.script.nonEmpty).map(_.name.toLowerCase).toSet
      val hidden = effectiveSchema.attributes
        .filter(a => a.resolveIgnore() || a.resolvePrivacy() != TransformInput.None)
        .flatMap(a => List(a.name, a.getFinalName()))
        .map(_.toLowerCase)
        .toSet ++ scripted
      val replayColumns =
        names.zipWithIndex.filterNot { case (name, _) => scripted(name.toLowerCase) }.map(_._2)
      val replay =
        if (settings.appConfig.sinkReplayToFile) Some(new ReplayFile(replayColumns.map(names)))
        else None
      val records = ListBuffer.empty[RejectedRecord]
      val counts = mutable.Map.empty[Int, Long].withDefaultValue(0L)
      try {
        rows.iterateAll().forEach { fieldValues =>
          val row = BigQueryRejectedRows.rejectedRow(
            fieldValues,
            format,
            names,
            hidden,
            withValue,
            counted = limited
          )
          counts(row.job) = row.jobCount.getOrElse(counts(row.job) + 1)
          if (records.size < maxErrors) records += rejectedRecord(row.errors, row.file, row.value)
          replay.foreach { file =>
            if (format == Format.DSV) {
              val values = row.values.toVector
              file.write(replayColumns.map(values))
            } else file.write(row.values)
          }
        }
      } catch {
        case exception: Exception =>
          replay.foreach(_.abort())
          throw exception
      }
      val structural = diagnosticLoads.groupMapReduce(_._1)(_._2.totalRejectedRows)(_ + _)
      val inexact = loadInfos.zipWithIndex.flatMap { case (loadInfo, job) =>
        val count = counts(job)
        val structuralCount = structural.getOrElse(job, 0L)
        val files = loadInfo.paths.mkString(", ")
        if (loadInfo.totalRejectedRows - structuralCount == count) {
          logger.info(
            s"Found the $count rows of $files rejected by the load of $targetFullTableName" +
            (if (structuralCount > 0)
               s", and $structuralCount other bad records with structural errors"
             else "")
          )
          None
        } else {
          logger.warn(
            s"Found $count rows of $files rejected by the load of $targetFullTableName, which rejected ${loadInfo.totalRejectedRows} bad records, $structuralCount with structural errors: the rules of the native loader differ from those of BigQuery for some values. The rows found are written to the audit, with the bad records that BigQuery reports${if (settings.appConfig.sinkReplayToFile) ", and no replay file is written, so that no loaded row is loaded again"
              else ""}"
          )
          Some(job)
        }
      }.toSet
      val replayed = replay.flatMap { file =>
        if (inexact.isEmpty) file.close()
        else {
          file.abort()
          None
        }
      }
      FoundRows(
        records.toList ++
        diagnosticLoads.collect {
          case (job, loadInfo) if !inexact(job) => badRecordsOf(loadInfo)
        }.flatten ++
        loadInfos.zipWithIndex.collect {
          case (loadInfo, job) if inexact(job) => badRecordsOf(loadInfo)
        }.flatten,
        replayed
      )
    } finally {
      Try(ParUtils.runInParallel(tableIds, Some(settings.appConfig.maxParTask)) { id =>
        BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
          new BigQueryNativeJob(targetConfig, "").dropTable(id)
        }
      }).failed.foreach(exception => Utils.logException(logger, exception))
    }
  }

  /** @return
    *   the name of new replay files of the domain, without extension: unique, so that two loads of
    *   the table in the same second keep their files
    */
  private def replayName(): String = {
    val timestamp = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.systemDefault())
      .format(Instant.now())
    s"${domain.name}.${starlakeSchema.name}.$timestamp.replay-${UUID.randomUUID().toString.take(8)}"
  }

  /** The replay file of the rejected rows of CSV, JSON or POSITION files, in the replay area of the
    * domain, created with its first row: the lines of a JSON or POSITION file, the rows of a CSV
    * file with its header, separator, quote and null value, in their encoding, so that the table
    * loads it again. A failure to write it deletes it, with a `WARN`: the rows are kept in the
    * audit.
    *
    * @param names
    *   the names of the columns of a CSV file, or of the attributes of a POSITION file
    */
  private class ReplayFile(names: List[String]) {
    private val csv = format == Format.DSV
    private val extension = format match {
      case Format.DSV      => "csv"
      case Format.POSITION => "txt"
      case _               => "json"
    }
    private val path = new Path(DatasetArea.replay(domain.name), s"${replayName()}.$extension")
    // JSON load jobs read UTF-8 files only, the others are written in their encoding
    private val charset = Charset.forName(
      if (format == Format.POSITION) mergedMetadata.resolveEncoding()
      else BigQueryRejectedRows.diagnosticMetadata(mergedMetadata).resolveEncoding()
    )
    private var writer: Option[Writer] = None
    private var started = false
    private var failed = false

    private def line(values: List[Option[String]]): String =
      if (csv)
        BigQueryRejectedRows.csvLine(
          values,
          mergedMetadata.resolveSeparator(),
          // BigQuery's default
          mergedMetadata.quote.getOrElse("\""),
          mergedMetadata.nullValue
        )
      else values.headOption.flatten.getOrElse("")

    def write(values: List[Option[String]]): Unit =
      if (!failed)
        Try {
          val output = writer.getOrElse {
            started = true
            storageHandler.mkdirs(path.getParent)
            val created =
              new BufferedWriter(new OutputStreamWriter(storageHandler.output(path), charset))
            writer = Some(created)
            if (csv && mergedMetadata.resolveWithHeader())
              created.write(line(names.map(Some(_))) + "\n")
            // The load skips the header of a POSITION file without keeping it: any line will do
            else if (format == Format.POSITION && mergedMetadata.resolveWithHeader())
              created.write(names.mkString(" ") + "\n")
            created
          }
          output.write(line(values) + "\n")
        }.failed.foreach(fail)

    private def fail(exception: Throwable): Unit = {
      logger.warn(s"Could not write the replay file $path of $targetFullTableName", exception)
      abort()
      failed = true
    }

    /** @return
      *   the file, if written
      */
    def close(): Option[Path] =
      writer.filterNot(_ => failed).flatMap { output =>
        Try(output.close()) match {
          case Success(_) =>
            logger.info(s"Replay file of $targetFullTableName written to $path")
            Some(path)
          case Failure(exception) =>
            fail(exception)
            None
        }
      }

    /** Deletes the file, written in part */
    def abort(): Unit = {
      writer.foreach(output => Try(output.close()))
      writer = None
      if (started) Try(storageHandler.delete(path))
    }
  }

  /** Keeps the rows of the first step that the checks reject, as the Spark loads keep their
    * rejected rows: the first `audit.maxErrors` of them in the audit `rejected` table, and all of
    * them in replay files with `sinkReplayToFile`
    */
  private def keepRejectedRows(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    tables: List[String],
    checks: BigQueryNativeLoader.FirstStepChecks
  ): Unit = {
    val withValue = checks.value.isDefined
    val maxErrors = settings.appConfig.audit.maxErrors
    if (maxErrors > 0) {
      val sql = BigQueryNativeLoader.rejectedRowsSql(tempTableNames, checks, maxErrors)
      val records = job
        .runBigQueryJob(Some(sql))
        .get
        .tableResult
        .toList
        .flatMap(_.iterateAll().asScala)
        .map { row =>
          def text(index: Int): Option[String] =
            Option(row.get(index)).filterNot(_.isNull).map(_.getStringValue)
          val (errors, file, value) = BigQueryNativeLoader.rejectedRowFields(
            text(0),
            text(1),
            if (withValue) text(2) else None
          )
          rejectedRecord(errors, file, value)
        }
      sinkRejected(records)
    }
    if (settings.appConfig.sinkReplayToFile) {
      if (format == Format.POSITION) writeLineReplayFile(job, tempTableNames, checks)
      else writeReplayFiles(job, tables, checks.condition)
    }
  }

  /** Writes all the rejected lines of a POSITION load to a replay file of the replay area of the
    * domain, as they are in the files, so that the table loads them again (see [[ReplayFile]]). A
    * failure to read them deletes it, with a `WARN`: the rows are kept in the audit.
    */
  private def writeLineReplayFile(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    checks: BigQueryNativeLoader.FirstStepChecks
  ): Unit = {
    val replay = new ReplayFile(starlakeSchema.attributes.filter(_.position.isDefined).map(_.name))
    Try {
      job
        .runBigQueryJob(
          Some(BigQueryNativeLoader.rejectedLinesSql(tempTableNames, checks)),
          pageSize = Some(1000)
        )
        .get
        .tableResult
        .foreach(_.iterateAll().forEach { row =>
          replay.write(List(Option(row.get(0)).filterNot(_.isNull).map(_.getStringValue)))
        })
    } match {
      case Success(_) => replay.close()
      case Failure(exception) =>
        logger.warn(
          s"Could not read the rejected lines of $targetFullTableName: no replay file is written",
          exception
        )
        replay.abort()
    }
  }

  /** Writes all the rejected rows to Parquet replay files of the replay area of the domain, as the
    * Spark loads write theirs. BigQuery exports them to Cloud Storage only: when the replay area is
    * elsewhere, they are exported to the temporary bucket of the connection, then moved.
    */
  /** @param tables
    *   the temporary tables of the first step: EXPORT DATA does not read wildcard tables
    */
  private def writeReplayFiles(
    job: BigQueryNativeJob,
    tables: List[String],
    condition: String
  ): Unit = {
    val replayArea = DatasetArea.replay(domain.name)
    val name = replayName()
    val onCloudStorage = replayArea.toString.startsWith("gs://")
    val exportFolder =
      if (onCloudStorage) Some(replayArea.toString.stripSuffix("/") + "/")
      else
        BigQueryNativeLoader
          .temporaryBucket(
            sinkConnection.options,
            settings.appConfig.internal.flatMap(_.temporaryGcsBucket)
          )
          .map(bucket => s"gs://$bucket/starlake-replay/${UUID.randomUUID()}/")
    exportFolder match {
      case None =>
        logger.warn(
          s"The replay files of the rejected rows of $targetFullTableName are not written: BigQuery exports them to Cloud Storage only, and the replay area $replayArea is not on Cloud Storage, nor is there a temporaryGcsBucket or gcsBucket in the connection, or a TEMPORARY_GCS_BUCKET"
        )
      case Some(folder) =>
        job
          .runBigQueryJob(
            Some(
              BigQueryNativeLoader.replayExportSql(
                s"$folder$name-*.parquet",
                tables,
                condition,
                // Not in the input files
                starlakeSchema.attributes.filter(_.script.nonEmpty).map(_.name)
              )
            )
          )
          .get
        if (onCloudStorage) logger.info(s"Replay files of $targetFullTableName written to $folder")
        else {
          val storage = job.storageClient(accessToken)
          val Array(bucket, prefix) = folder.stripPrefix("gs://").split("/", 2)
          def exported() =
            storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll().asScala.toList
          try {
            storageHandler.mkdirs(replayArea)
            exported().foreach { blob =>
              val target = new Path(replayArea, blob.getName.split('/').last)
              Try(Using.resource(storageHandler.output(target))(blob.downloadTo(_))) match {
                case Success(_) =>
                  logger.info(s"Replay file of $targetFullTableName written to $target")
                case Failure(exception) =>
                  Try(storageHandler.delete(target))
                  throw exception
              }
            }
          } finally {
            // The exported files are temporary
            Try(exported().foreach(blob => storage.delete(blob.getBlobId))).failed.foreach {
              exception =>
                logger.warn(s"Could not delete the temporary replay files of $folder", exception)
            }
          }
        }
    }
  }

  /** @param condition
    *   the condition of the rows of the first step that the second step loads, if they are checked
    */
  private def applyBigQuerySecondStepSQL(
    firstStepTempTableTableNames: List[String],
    existingTargetSchema: Option[bigquery.Schema],
    incomingSchema: bigquery.Schema,
    castTypes: Map[String, String],
    condition: Option[String]
  ): Try[JobResult] = {
    // The values that the first step of a Parquet load reads as strings
    val conversions =
      if (format == Format.PARQUET)
        BigQueryNativeLoader.parquetConversions(
          firstStepSchema(schemaWithMergedMetadata, existingTargetSchema, log = false)
        )
      else Nil
    val task = this.secondStepSQLTask(
      firstStepTempTableTableNames.map(
        BigQueryNativeLoader.withConversions(_, conversions, condition)
      ),
      expectations = Nil,
      castTypes = castTypes
    )
    val bqTask = task.asInstanceOf[BigQueryAutoTask]
    bqTask.updateBigQueryTableSchemaWith(
      incomingSchema = Some(incomingSchema),
      createIfAbsent = true
    )
    val jobResult = bqTask.run()
    jobResult
  }

}

object BigQueryNativeLoader {

  /** @return
    *   the config of the load of a file into a temporary table of the first step
    */
  def firstStepConfig(
    targetConfig: BigQueryLoadConfig,
    sourceUri: String,
    tempTableId: TableId
  ): BigQueryLoadConfig =
    targetConfig.copy(
      source = Left(sourceUri),
      outputTableId = Some(tempTableId),
      outputTableDesc = Some("Temporary table created during data ingestion."),
      // force first step to be write append, otherwise write_truncate overwrite the
      // created structure with default values, making second step query to fail if it relies on
      // technical column such as comet_input_filename.
      writeDisposition = JobInfo.WriteDisposition.WRITE_APPEND.name(),
      // The second step reads every row and column of the temporary tables, to which no access
      // policy is applied: a row access policy would hide rows from it, a policy tag would deny or
      // mask columns, and a required partition filter would reject its queries. The second step
      // applies the ACL and row access policies to the target
      requirePartitionFilter = false,
      rls = Nil,
      acl = Nil
    )

  /** @return
    *   the schema of the temporary table of the first step, with the source names of the attributes
    *   and the types of the target columns
    */
  def firstStepSchema(
    schema: SchemaInfo,
    existingTargetSchema: Option[bigquery.Schema],
    schemaHandler: SchemaHandler,
    fromDdlMapping: Boolean,
    log: Boolean
  ): bigquery.Schema =
    BigQueryNativeSchema.patch(
      schema.bigquerySchemaWithIgnoreAndScript(schemaHandler, withFinalName = false),
      // The raw values of the ignored, transformed and scripted attributes are not
      // loaded into the target: they keep their derived type, except the decimals
      // wider than Spark, whose derived type would not hold them
      schema.attributes.filterNot(attribute =>
        (attribute
          .resolveIgnore() || attribute.transform.isDefined || attribute.script.isDefined) &&
        !BigQueryNativeSchema.isWideDecimal(attribute, schemaHandler)
      ),
      _.name,
      existingTargetSchema,
      schemaHandler,
      fromDdlMapping,
      log
    )

  /** @return
    *   true if each file must be loaded by its own load job: local files are uploaded through a
    *   single stream per job, and the concatenation of Parquet files is not a Parquet file
    */
  def requiresOneLoadPerFile(format: Format, paths: List[Path]): Boolean =
    // Same test of local files as BigQueryNativeJob.loadPathsToBQ
    format == Format.PARQUET && paths.size > 1 && paths.exists(_.toString.startsWith("file:"))

  /** @return
    *   the schema of an existing table, with the fields of the incoming schema that it lacks, at
    *   any level, and with its required fields that the incoming schema makes nullable relaxed, if
    *   this changes it. A Parquet load job can neither add nor relax fields: `ALLOW_FIELD_ADDITION`
    *   and `ignoreUnknownValues` cannot both be set for self-describing formats.
    */
  def schemaForParquetAppend(
    existing: bigquery.Schema,
    incoming: bigquery.Schema
  ): Option[bigquery.Schema] = {
    def key(field: Field): String = field.getName.toLowerCase(java.util.Locale.ROOT)
    def merge(existing: List[Field], incoming: List[Field]): List[Field] = {
      val incomingByName = incoming.map(field => key(field) -> field).toMap
      val existingNames = existing.map(key).toSet
      existing.map { field =>
        incomingByName.get(key(field)) match {
          case Some(incomingField) =>
            val builder = field.toBuilder
            if (
              field.getMode == Field.Mode.REQUIRED && incomingField.getMode != Field.Mode.REQUIRED
            )
              builder.setMode(Field.Mode.NULLABLE)
            (Option(field.getSubFields), Option(incomingField.getSubFields)) match {
              case (Some(subFields), Some(incomingSubFields)) =>
                builder.setType(
                  field.getType.getStandardType,
                  FieldList.of(
                    merge(subFields.asScala.toList, incomingSubFields.asScala.toList).asJava
                  )
                )
              case _ =>
            }
            builder.build()
          case None => field
        }
      } ++ incoming.filterNot(field => existingNames(key(field))).map { field =>
        // BigQuery adds no required field to an existing table
        if (field.getMode == Field.Mode.REQUIRED)
          field.toBuilder.setMode(Field.Mode.NULLABLE).build()
        else field
      }
    }
    val aligned = bigquery.Schema.of(
      merge(
        Option(existing.getFields).map(_.asScala.toList).getOrElse(Nil),
        Option(incoming.getFields).map(_.asScala.toList).getOrElse(Nil)
      ).asJava
    )
    if (aligned == existing) None else Some(aligned)
  }

  /** @return
    *   true if an attribute is required, at any nesting level
    */
  def hasRequiredAttributes(attributes: List[TableAttribute]): Boolean =
    attributes.exists(attribute =>
      attribute.resolveRequired() || hasRequiredAttributes(attribute.attributes)
    )

  /** @return
    *   the schema of the first step of a Parquet load, whose columns are matched by name with it.
    *   It is always explicit, so that the types of the YAML schema are kept, where a schema-less
    *   load would infer them from the files (a `DATETIME` as a `TIMESTAMP`, a `BIGNUMERIC` as a
    *   `NUMERIC`). Its fields are nullable: a column of the files can be optional while its
    *   attribute is required. Its top level `JSON`, `INTERVAL` and `RANGE` fields are strings,
    *   converted by the second step (see [[parquetConversions]]).
    */
  def parquetFirstStepSchema(schema: bigquery.Schema): bigquery.Schema = {
    def nullable(field: Field): Field = {
      val builder = field.toBuilder
      if (field.getMode == Field.Mode.REQUIRED)
        builder.setMode(Field.Mode.NULLABLE)
      Option(field.getSubFields).foreach { subFields =>
        builder.setType(
          field.getType.getStandardType,
          FieldList.of(subFields.asScala.map(nullable).asJava)
        )
      }
      builder.build()
    }
    val converted = parquetConversions(schema).map { case (name, _) => name }.toSet
    bigquery.Schema.of(
      schema.getFields.asScala.map { field =>
        if (converted.contains(field.getName))
          Field
            .newBuilder(field.getName, StandardSQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .setDescription(field.getDescription)
            .build()
        else nullable(field)
      }.asJava
    )
  }

  /** @return
    *   the top level, non repeated fields of the first step schema that a Parquet load reads as
    *   strings, with the SQL that converts their value: `EXPORT DATA` does not export `JSON` and
    *   `INTERVAL` columns to Parquet, so they are exported as strings, and BigQuery documents no
    *   conversion to these types in Parquet loads. An invalid value fails the second step.
    */
  def parquetConversions(schema: bigquery.Schema): List[(String, String)] =
    schema.getFields.asScala.toList
      .filter(_.getMode != Field.Mode.REPEATED)
      .flatMap { field =>
        val column = s"`${field.getName}`"
        Option(field.getType.getStandardType).collect {
          case StandardSQLTypeName.JSON =>
            field.getName -> s"PARSE_JSON($column)"
          case StandardSQLTypeName.INTERVAL =>
            field.getName -> s"CAST($column AS INTERVAL)"
          case StandardSQLTypeName.RANGE if field.getRangeElementType != null =>
            field.getName -> BigQueryNativeSchema.rangeFromText(
              column,
              field.getRangeElementType.getType,
              safe = false
            )
        }
      }

  /** @return
    *   the paths of the nested or repeated `JSON`, `INTERVAL` and `RANGE` fields of the first step
    *   schema: a Parquet load does not convert them from strings, and BigQuery may not read them
    *   from Parquet files
    */
  def unconvertedParquetFields(schema: bigquery.Schema): List[String] = {
    def unconverted(field: Field): Boolean =
      Option(field.getType.getStandardType).exists(
        Set(StandardSQLTypeName.JSON, StandardSQLTypeName.INTERVAL, StandardSQLTypeName.RANGE)
      )
    def subFields(field: Field, path: String): List[String] =
      Option(field.getSubFields).toList.flatMap(_.asScala).flatMap { subField =>
        val subPath = s"$path.${subField.getName}"
        (if (unconverted(subField)) List(subPath) else Nil) ++ subFields(subField, subPath)
      }
    schema.getFields.asScala.toList.flatMap { field =>
      (if (field.getMode == Field.Mode.REPEATED && unconverted(field)) List(field.getName)
       else Nil) ++ subFields(field, field.getName)
    }
  }

  /** @return
    *   the temporary table of the first step, with the values that it holds as strings converted to
    *   the type of their column, so that the scripts, transforms and filter of the second step see
    *   them as in the other formats
    */
  def withConversions(
    table: String,
    conversions: List[(String, String)],
    condition: Option[String] = None
  ): String =
    if (conversions.isEmpty && condition.isEmpty) table
    else {
      val replace =
        if (conversions.isEmpty) ""
        else
          conversions
            .map { case (name, sql) => s"$sql AS `$name`" }
            .mkString(" REPLACE (", ", ", ")")
      s"(SELECT *$replace FROM $table${condition.map(" WHERE " + _).getOrElse("")})"
    }

  /** @return
    *   why the load fails with these rejected rows, if it does: as BigQuery with the bad records of
    *   the other formats, when every record is rejected on error (`rejectAllOnError`), or when
    *   there are more than `rejectMaxRecords` of them, over the files of the load
    */
  def rejectedRowsError(
    rejected: Long,
    rejectAllOnError: Boolean,
    rejectMaxRecords: Int
  ): Option[String] =
    if (rejected == 0) None
    else if (rejectAllOnError) Some("No row is loaded: rejectAllOnError is true")
    else if (rejected > rejectMaxRecords)
      Some(s"No row is loaded: rejectMaxRecords is $rejectMaxRecords")
    else None

  /** @param rejectedByFile
    *   the number of rejected rows by index of their file, the index of its load info
    * @param filteredByFile
    *   the number of rows that the filter drops by index of their file
    * @return
    *   the load infos of the files, with their rejected rows counted as rejected instead of
    *   accepted, and their filtered rows not counted, as Spark counts them
    */
  def withRejectedRows(
    loadInfos: List[BqLoadInfo],
    rejectedByFile: Map[Int, Long],
    filteredByFile: Map[Int, Long] = Map.empty
  ): List[BqLoadInfo] = {
    (rejectedByFile.keys ++ filteredByFile.keys)
      .find(index => !loadInfos.indices.contains(index))
      .foreach { index =>
        throw new IllegalStateException(
          s"Rows counted for the file $index of a load of ${loadInfos.size} files"
        )
      }
    loadInfos.zipWithIndex.map { case (loadInfo, index) =>
      val rejected = rejectedByFile.getOrElse(index, 0L)
      loadInfo.copy(
        totalAcceptedRows =
          loadInfo.totalAcceptedRows - rejected - filteredByFile.getOrElse(index, 0L),
        totalRejectedRows = loadInfo.totalRejectedRows + rejected
      )
    }
  }

  /** @return
    *   the paths of the required attributes that a load checks, at every level: neither ignored nor
    *   scripted, and not repeated, since a repeated field cannot be null
    */
  def requiredAttributePaths(attributes: List[TableAttribute], prefix: String = ""): List[String] =
    attributes.filterNot(a => a.resolveIgnore() || a.script.nonEmpty).flatMap { attribute =>
      val path = prefix + attribute.name
      (if (attribute.resolveRequired() && !attribute.resolveArray()) List(path) else Nil) ++
      requiredAttributePaths(attribute.attributes, path + ".")
    }

  /** @return
    *   the SQL condition, on the columns of the first step, of the rows with a value for every
    *   required attribute, at any level, if any: a required field of a null record has no value to
    *   check, and every element of a repeated record is checked. Ignored attributes are not loaded,
    *   and scripted ones are not in the files.
    */
  def requiredValuesCondition(attributes: List[TableAttribute]): Option[String] = {
    // The conditions on the attributes of `parent`, the SQL of a record, or of the row if None
    def conditions(
      attributes: List[TableAttribute],
      parent: Option[String],
      depth: Int
    ): List[String] =
      attributes.filterNot(a => a.resolveIgnore() || a.script.nonEmpty).flatMap { attribute =>
        val column = parent.map(_ + ".").getOrElse("") + s"`${attribute.name}`"
        val own =
          if (attribute.resolveRequired() && !attribute.resolveArray())
            List(
              parent
                .map(p => s"($p IS NULL OR $column IS NOT NULL)")
                .getOrElse(s"$column IS NOT NULL")
            )
          else Nil
        val nested =
          if (attribute.attributes.isEmpty) Nil
          else if (attribute.resolveArray()) {
            val element = s"sl_element_$depth"
            conditions(attribute.attributes, Some(element), depth + 1) match {
              case Nil => Nil
              case elementConditions =>
                List(
                  s"NOT EXISTS (SELECT 1 FROM UNNEST($column) AS $element WHERE NOT (${elementConditions
                      .mkString(" AND ")}))"
                )
            }
          } else conditions(attribute.attributes, Some(column), depth + 1)
        own ++ nested
      }
    conditions(attributes, None, 0) match {
      case Nil        => None
      case conditions => Some(conditions.mkString(" AND "))
    }
  }

  /** @return
    *   for each required attribute that a load checks, its path and the SQL condition, on the
    *   columns of the first step, of the rows with a value for it: their conjunction is
    *   [[requiredValuesCondition]]
    */
  def requiredValueTerms(attributes: List[TableAttribute]): List[(String, String)] = {
    def terms(
      attributes: List[TableAttribute],
      parent: Option[String],
      prefix: String,
      depth: Int
    ): List[(String, String)] =
      attributes.filterNot(a => a.resolveIgnore() || a.script.nonEmpty).flatMap { attribute =>
        val column = parent.map(_ + ".").getOrElse("") + s"`${attribute.name}`"
        val path = prefix + attribute.name
        val own =
          if (attribute.resolveRequired() && !attribute.resolveArray())
            List(
              path -> parent
                .map(p => s"($p IS NULL OR $column IS NOT NULL)")
                .getOrElse(s"$column IS NOT NULL")
            )
          else Nil
        val nested =
          if (attribute.attributes.isEmpty) Nil
          else if (attribute.resolveArray()) {
            val element = s"sl_element_$depth"
            terms(attribute.attributes, Some(element), path + ".", depth + 1).map {
              case (elementPath, condition) =>
                elementPath -> s"NOT EXISTS (SELECT 1 FROM UNNEST($column) AS $element WHERE NOT ($condition))"
            }
          } else terms(attribute.attributes, Some(column), path + ".", depth + 1)
        own ++ nested
      }
    terms(attributes, None, "", 0)
  }

  /** A BigQuery string literal, double quoted: the apostrophes of the messages need no escape */
  def bigQueryLiteral(value: String): String =
    "\"" + value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r") + "\""

  /** A check of the rows of the first step of a load
    *
    * @param condition
    *   its SQL condition, true for a row that it accepts and never NULL
    * @param error
    *   the SQL of the error line of a row that it rejects, ending with a line break
    */
  case class RowCheck(condition: String, error: String)

  /** The checks of the rows of the first step that the second step rejects
    *
    * @param condition
    *   the SQL condition of the rows that the second step loads, never NULL: the conjunction of the
    *   checks
    * @param checks
    *   the checks, which give the error lines of a rejected row: none when only the rows that the
    *   filter drops are counted, with the condition `TRUE`
    * @param value
    *   the SQL of a rejected row as JSON, without its hidden values, with `rejectWithValue`
    * @param reason
    *   why the rows are rejected, for the log
    * @param scope
    *   the rows that the checks apply to, if not all
    */
  case class FirstStepChecks(
    condition: String,
    checks: List[RowCheck],
    value: Option[String],
    reason: String,
    scope: Option[RowScope] = None
  ) {

    /** @param table
      *   the wildcard name of the temporary tables of the first step
      * @return
      *   the SQL of the rows of the tables that the checks apply to, and of the index of their file
      */
    def rows(table: String): (String, String) =
      scope match {
        case None => (table, "_TABLE_SUFFIX")
        case Some(scope) =>
          (
            s"(SELECT `$lineColumn`, `$inputFileColumn`, sl_suffix FROM ${scope.lines(table)} WHERE sl_kept IS NOT FALSE)",
            "sl_suffix"
          )
      }

    /** @param table
      *   the wildcard name of the temporary tables of the first step
      * @return
      *   the SQL of the number of rows of the tables that the checks reject (`rejected`), and of
      *   those that the filter drops (`filtered`), by index of their file (`suffix`). The second
      *   step loads a row when the checks and the filter keep it.
      */
    def countsSql(table: String): String =
      scope match {
        case None =>
          s"SELECT _TABLE_SUFFIX AS suffix, COUNT(*) AS rejected, 0 AS filtered FROM $table WHERE NOT ($condition) GROUP BY 1"
        case Some(scope) =>
          s"SELECT sl_suffix AS suffix, COUNTIF(sl_kept IS NOT FALSE AND NOT ($condition)) AS rejected, COUNTIF(sl_kept IS FALSE OR (sl_kept IS NULL AND ($condition))) AS filtered FROM ${scope
              .lines(table)} GROUP BY 1"
      }
  }

  private val inputFileColumn = CometColumns.cometInputFileNameColumn
  private val lineColumn = BigQueryPositionValues.lineColumn

  /** The rows of the first step of a POSITION load that its filter keeps, or may keep: the filter
    * of the second step reads the values of the attributes, which a rejected row may lack. The rows
    * that it drops are neither rejected nor counted, as with Spark, which filters the lines before
    * reading them.
    *
    * @param columns
    *   the SQL of the columns that the second step selects from the rows of its first step
    * @param filter
    *   the filter of the table, on these columns
    */
  case class RowScope(columns: String, filter: String) {

    /** @return
      *   the SQL of the rows of the tables, with the index of their file (`sl_suffix`) and the
      *   value of the filter on their columns (`sl_kept`), where a value that does not convert is
      *   NULL, as in the second step. The line and the name of its file are renamed among the
      *   columns, which an attribute may name.
      */
    def lines(table: String): String =
      s"(SELECT sl_line AS `$lineColumn`, sl_file AS `$inputFileColumn`, sl_suffix, ($filter) AS sl_kept FROM (SELECT _TABLE_SUFFIX AS sl_suffix, `$lineColumn` AS sl_line, `$inputFileColumn` AS sl_file, $columns FROM $table) AS SL_INTERNAL_FROM_SELECT)"
  }

  /** The alias of a row of the first step in the queries of the rejected rows */
  val rejectedRow = "sl_rejected_row"

  /** @return
    *   the checks of the rows of the first step of a Parquet load, if a required attribute is
    *   checked (see [[requiredValuesCondition]]): one error line per attribute without a value, as
    *   the Spark validators write them, and with `withValue` the row as JSON, without the values of
    *   [[hiddenValuePaths]]
    */
  def requiredValuesChecks(
    attributes: List[TableAttribute],
    withValue: Boolean
  ): Option[FirstStepChecks] =
    requiredValuesCondition(attributes).map { condition =>
      val file = CometColumns.cometInputFileNameColumn
      // Wide numbers as strings, as TO_JSON_STRING writes them: TO_JSON fails on them otherwise
      val value =
        if (withValue) {
          val removed = (s"""$$."$file"""" :: hiddenValuePaths(attributes))
            .map(path => s"'$path'")
            .mkString(", ")
          Some(
            s"TO_JSON_STRING(JSON_REMOVE(TO_JSON($rejectedRow, stringify_wide_numbers => TRUE), $removed))"
          )
        } else None
      FirstStepChecks(
        condition,
        requiredValueTerms(attributes).map { case (path, term) =>
          RowCheck(term, bigQueryLiteral(s"`$path`  can't be null\n"))
        },
        value,
        s"have no value for one of its required attributes (${requiredAttributePaths(attributes).mkString(", ")})"
      )
    }

  /** @param ddlTypes
    *   the types of the columns of the attributes, by attribute name
    * @param types
    *   the types of the attributes, by attribute name
    * @return
    *   the checks of the lines of the first step of a POSITION load (#1886), if any:
    *   - a required attribute has a slice that is not blank (see [[BigQueryPositionValues.blank]]);
    *   - a slice that is not blank converts to the type of its column, read with the pattern of its
    *     type if it has one (see [[BigQueryPositionValues.value]]). A blank slice of an optional
    *     attribute is NULL, or the slice itself for a string, where the Spark loader nulls an empty
    *     value only and checks the others. A `byte` value is not checked: the Spark loader reads
    *     any character, and loads NULL when it is not a number.
    *
    * One error line per invalid attribute, with its value with `withValue`, and the row as the JSON
    * object of its slices. The values of the attributes with a privacy are not shown. The slices of
    * the ignored, scripted and transformed attributes are not loaded, and not checked.
    */
  def positionChecks(
    attributes: List[TableAttribute],
    ddlTypes: Map[String, String],
    types: Map[String, Type],
    withValue: Boolean
  ): Option[FirstStepChecks] = {
    def shown(attribute: TableAttribute): Boolean =
      attribute.resolvePrivacy() == TransformInput.None
    val loaded = attributes.filter(attribute =>
      !attribute.resolveIgnore() && attribute.script.isEmpty && attribute.transform.isEmpty
    )
    val checksByAttribute = loaded.flatMap { attribute =>
      attribute.position.map { position =>
        val slice = BigQueryPositionValues.slice(position)
        val blank = BigQueryPositionValues.blank(slice)
        val ddlType = ddlTypes.get(attribute.name).map(_.trim)
        val value = BigQueryPositionValues.value(slice, ddlType, types.get(attribute.name))
        val required =
          if (attribute.resolveRequired())
            List(
              RowCheck(s"NOT ($blank)", bigQueryLiteral(s"`${attribute.name}`  can't be null\n"))
            )
          else Nil
        // A string without a maximum length is the slice itself
        val converted =
          if (
            value == slice ||
            types.get(attribute.name).exists(_.primitiveType == PrimitiveType.byte)
          ) Nil
          else {
            val pattern = types
              .get(attribute.name)
              .filter(BigQueryPositionValues.readsPattern(ddlType.flatMap(BigQueryType.parse), _))
              .map(tpe => s" with the pattern `${tpe.pattern}`")
              .getOrElse("")
            val message = s"`${attribute.name}` is not a valid ${ddlType.getOrElse("")}$pattern"
            val error =
              if (withValue && shown(attribute))
                s"CONCAT(${bigQueryLiteral(message + ": ")}, $slice, ${bigQueryLiteral("\n")})"
              else bigQueryLiteral(message + "\n")
            List(RowCheck(s"($blank OR $value IS NOT NULL)", error))
          }
        attribute.name -> (required ++ converted)
      }
    }
    val checks = checksByAttribute.flatMap { case (_, checks) => checks }
    if (checks.isEmpty) None
    else {
      val value =
        if (withValue) {
          val slices = attributes
            .filter(a => !a.resolveIgnore() && a.script.isEmpty && shown(a))
            .flatMap(a => a.position.map(p => s"${BigQueryPositionValues.slice(p)} AS `${a.name}`"))
          Some(
            if (slices.isEmpty) "'{}'" else slices.mkString("TO_JSON_STRING(STRUCT(", ", ", "))")
          )
        } else None
      val checked = checksByAttribute.collect { case (name, checks) if checks.nonEmpty => name }
      Some(
        FirstStepChecks(
          checks.map(_.condition).mkString(" AND "),
          checks,
          value,
          s"have a value that does not convert to the type of its attribute, or no value for a required attribute (${checked
              .mkString(", ")})"
        )
      )
    }
  }

  /** @return
    *   the query of the first `limit` rows of the first step that the checks reject: their file,
    *   their error lines, and with `rejectWithValue` the row as JSON
    */
  def rejectedRowsSql(
    tempTableNames: List[String],
    checks: FirstStepChecks,
    limit: Int
  ): String = {
    val file = CometColumns.cometInputFileNameColumn
    val errors = checks.checks
      .map(check => s"IF(${check.condition}, '', ${check.error})")
      .mkString("CONCAT(", ", ", ")")
    val value = checks.value.map(value => s", $value AS row_json").getOrElse("")
    val selects = tempTableNames.map(table =>
      s"SELECT $rejectedRow.`$file` AS file, $errors AS errors$value FROM ${checks.rows(table)._1} AS $rejectedRow WHERE NOT (${checks.condition})"
    )
    s"SELECT * FROM (${selects.mkString(" UNION ALL ")}) LIMIT $limit"
  }

  /** @return
    *   the query of all the lines of the first step of a POSITION load that the checks reject
    */
  def rejectedLinesSql(tempTableNames: List[String], checks: FirstStepChecks): String =
    tempTableNames
      .map(table =>
        s"SELECT `${BigQueryPositionValues.lineColumn}` FROM ${checks.rows(table)._1} WHERE NOT (${checks.condition})"
      )
      .mkString(" UNION ALL ")

  /** @return
    *   the JSON paths of the values that a rejected row does not show: those of the ignored
    *   attributes and of the attributes with a privacy, still in clear in the first step, or of the
    *   repeated record that holds them
    */
  def hiddenValuePaths(attributes: List[TableAttribute], prefix: String = "$"): List[String] = {
    def hidden(attribute: TableAttribute): Boolean =
      attribute.resolveIgnore() || attribute.resolvePrivacy() != TransformInput.None
    def holdsHidden(attribute: TableAttribute): Boolean =
      hidden(attribute) || attribute.attributes.exists(holdsHidden)
    attributes.flatMap { attribute =>
      val path = s"""$prefix."${attribute.name}""""
      if (hidden(attribute) || (attribute.resolveArray() && holdsHidden(attribute))) List(path)
      else if (holdsHidden(attribute)) hiddenValuePaths(attribute.attributes, path)
      else Nil
    }
  }

  /** @return
    *   the errors, the file and the value of a rejected row that [[rejectedRowsSql]] read
    */
  def rejectedRowFields(
    file: Option[String],
    errors: Option[String],
    value: Option[String]
  ): (List[String], String, Option[String]) =
    (
      errors
        .map(_.split("\n").toList.filter(_.nonEmpty))
        .filter(_.nonEmpty)
        .getOrElse(List("A required attribute has no value")),
      file.getOrElse(""),
      value
    )

  /** @return
    *   the statement exporting to Parquet files all the rows of the first step without a value for
    *   a required attribute, with the columns of the input files, so that the table loads them
    *   again. EXPORT DATA does not read wildcard tables ("cannot reference meta tables"): the
    *   tables are the temporary tables of the first step.
    */
  def replayExportSql(
    uri: String,
    tables: List[String],
    condition: String,
    excluded: List[String] = Nil
  ): String = {
    val columns = (CometColumns.cometInputFileNameColumn :: excluded).map(c => s"`$c`")
    // The tables have the same columns, in the same order
    val rows = tables.map(table => s"SELECT * FROM $table").mkString("\nUNION ALL\n")
    s"""EXPORT DATA OPTIONS (uri = ${bigQueryLiteral(
        uri
      )}, format = 'PARQUET', compression = 'SNAPPY', overwrite = true) AS
       |SELECT * EXCEPT (${columns.mkString(", ")}) FROM (
       |$rows
       |) WHERE NOT ($condition)""".stripMargin
  }

  /** @return
    *   the bucket, and its optional folder, where a load exports the files that BigQuery writes to
    *   Cloud Storage only: the temporary bucket of the connection, or its bucket, or the temporary
    *   bucket of the application
    */
  def temporaryBucket(
    connectionOptions: Map[String, String],
    applicationBucket: Option[String]
  ): Option[String] =
    List(
      connectionOptions.get("temporaryGcsBucket"),
      connectionOptions.get("gcsBucket"),
      applicationBucket
    ).flatten
      .map(_.trim.stripPrefix("gs://").stripSuffix("/"))
      .find(_.nonEmpty)

  /** The messages of a load job that summarize its bad records */
  private val jobSummary = "(?i)encountered too many errors|errors\\[\\] collection".r

  /** @return
    *   the file and the message of each bad record that a load job skipped, as BigQuery reports
    *   them: the error of a failed job, and its summary of the bad records, are not about a record
    */
  def badRecords(
    paths: List[String],
    executionErrors: List[BigQueryError],
    jobError: Option[BigQueryError]
  ): List[(String, String)] =
    executionErrors
      .filterNot(error =>
        jobError.contains(error) ||
        Option(error.getMessage).exists(message => jobSummary.findFirstIn(message).isDefined)
      )
      .map { error =>
        val file =
          Option(error.getLocation).filter(_.contains("://")).getOrElse(paths.mkString(","))
        file -> Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getReason)
      }
}
