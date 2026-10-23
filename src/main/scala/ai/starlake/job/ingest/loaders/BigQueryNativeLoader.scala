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
import ai.starlake.utils.conversion.{BigQueryNativeSchema, BigQueryUtils}
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

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, accessToken)
    with LazyLogging {

  lazy val targetTableId: TableId = {
    val originalTableId = BigQueryNativeLoader.originalTableId(
      schemaHandler.getDatabase(domain),
      domain.finalName,
      effectiveSchema.finalName,
      sinkConnection.options
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
                  targetConfig
                    .copy(
                      source = Left(sourceUri),
                      outputTableId = Some(firstStepTempTable),
                      outputTableDesc = Some("Temporary table created during data ingestion."),
                      // force first step to be write append, otherwise write_truncate overwrite the
                      // created structure with default values, making second step query to fail if it relies on
                      // technical column such as comet_input_filename.
                      writeDisposition = JobInfo.WriteDisposition.WRITE_APPEND.name()
                    )

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

                val enrichedTableInfo = firstStepTableInfo.copy(
                  maybeSchema = firstStepTableInfo.maybeSchema.map((schema: bigquery.Schema) =>
                    bigquery.Schema.of(
                      (schema.getFields.asScala :+
                      Field
                        .newBuilder(
                          CometColumns.cometInputFileNameColumn,
                          StandardSQLTypeName.STRING
                        )
                        .setDefaultValueExpression(s"'${sourceUri.replace("'", "\\\\'")}'")
                        .build()).asJava
                    )
                  ),
                  maybeDurationMs = Some(Duration.ofHours(24).toMillis)
                )

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
        bigqueryJob.loadPathsToBQ(tableInfo).map(List(_)).recoverWith {
          case exception: BigQueryLoadJobException =>
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
        // The bad records that a one-step load job skipped, known once it loaded the others: a
        // failure to keep them does not fail the load, whose rows are written. Two-step loads keep
        // them before their second step.
        if (!twoSteps)
          Try(sinkRejected(results.flatMap(badRecordsOf))).failed.foreach { exception =>
            logger.error(
              s"Could not write the rejected records of $targetFullTableName to the audit",
              exception
            )
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
    firstStepResult: Try[List[BqLoadInfo]]
  ): Try[List[BqLoadInfo]] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: ${loadFileResult.map(_.toString).mkString(", ")}")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val tempTableNames = firstStepTempTable.map(t => BigQueryUtils.tableIdToTableName(t))
        // The first step of a Parquet load reads every column as nullable: the rows without a
        // value for a required attribute are rejected by the second step
        val requiredCondition =
          if (format == Format.PARQUET)
            BigQueryNativeLoader.requiredValuesCondition(starlakeSchema.attributes)
          else None
        val result = for {
          // The bad records that the first step skipped, as BigQuery reports them, before
          // anything is loaded into the target: a failure to keep them fails the load, as with
          // Spark
          _ <- Try(sinkRejected(loadFileResult.flatMap(badRecordsOf)))
          rejectedByFile <- requiredCondition
            .map(condition =>
              rejectRowsWithoutRequiredValues(targetBigqueryJob, tempTableNames, condition)
            )
            .getOrElse(Success(Map.empty[Int, Long]))
          _ <- targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              applyBigQuerySecondStepSQL(tempTableNames, existingTargetSchema, requiredCondition)
            }
            .getOrElse(throw new IllegalStateException("Should never happen"))
        } yield BigQueryNativeLoader.withRejectedRows(loadFileResult, rejectedByFile)
        result
      case res @ Failure(_) =>
        res
    }
  }

  /** @param tempTableNames
    *   the wildcard name of the temporary tables of the first step, whose suffix is the index of
    *   their file in the load
    * @return
    *   the number of rows of the first step without a value for a required attribute, by index of
    *   their file. They are rejected as BigQuery rejects the bad records of the other formats, or
    *   the load fails (see [[BigQueryNativeLoader.rejectedRowsError]]).
    */
  private def rejectRowsWithoutRequiredValues(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    condition: String
  ): Try[Map[Int, Long]] =
    job
      .runBigQueryJob(
        Some(
          tempTableNames
            .map(table =>
              s"SELECT _TABLE_SUFFIX AS suffix, COUNT(*) AS rejected FROM $table WHERE NOT ($condition) GROUP BY 1"
            )
            .mkString(" UNION ALL ")
        )
      )
      .map { result =>
        val rejectedByFile = result.tableResult.toList
          .flatMap(_.iterateAll().asScala)
          .map(row => row.get(0).getStringValue.toInt -> row.get(1).getLongValue)
          .toMap
        val rejected = rejectedByFile.values.sum
        if (rejected > 0) {
          val files = rejectedByFile.toList.sorted.map { case (index, count) =>
            s"${path.lift(index).map(_.toString).getOrElse(index.toString)} ($count)"
          }
          val message =
            s"$rejected rows of $targetFullTableName have no value for one of its required attributes (${BigQueryNativeLoader
                .requiredAttributePaths(starlakeSchema.attributes)
                .mkString(", ")}), in ${files.take(10).mkString(", ")}${if (files.size > 10)
                s" and ${files.size - 10} other files"
              else ""}"
          val error = BigQueryNativeLoader.rejectedRowsError(
            rejected,
            settings.appConfig.rejectAllOnError,
            settings.appConfig.rejectMaxRecords
          )
          logger.warn(s"$message. ${error.getOrElse("They are rejected")}")
          // As the Spark loads, the rejected rows are kept before the load fails because of them
          keepRowsWithoutRequiredValues(job, tempTableNames, condition)
          error.foreach(error => throw new IllegalArgumentException(s"$message. $error"))
        }
        rejectedByFile
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

  /** Keeps the rows of the first step without a value for a required attribute, as the Spark loads
    * keep their rejected rows: the first `audit.maxErrors` of them in the audit `rejected` table,
    * and all of them in replay files with `sinkReplayToFile`
    */
  private def keepRowsWithoutRequiredValues(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    condition: String
  ): Unit = {
    val withValue = settings.appConfig.rejectWithValue
    val maxErrors = settings.appConfig.audit.maxErrors
    if (maxErrors > 0) {
      val sql = BigQueryNativeLoader.rejectedRowsSql(
        tempTableNames,
        condition,
        BigQueryNativeLoader.requiredValueTerms(starlakeSchema.attributes),
        if (withValue) Some(BigQueryNativeLoader.hiddenValuePaths(starlakeSchema.attributes))
        else None,
        maxErrors
      )
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
    if (settings.appConfig.sinkReplayToFile)
      writeReplayFiles(job, tempTableNames, condition)
  }

  /** Writes all the rejected rows to Parquet replay files of the replay area of the domain, as the
    * Spark loads write theirs. BigQuery exports them to Cloud Storage only: when the replay area is
    * elsewhere, they are exported to the temporary bucket of the connection, then moved.
    */
  private def writeReplayFiles(
    job: BigQueryNativeJob,
    tempTableNames: List[String],
    condition: String
  ): Unit = {
    val replayArea = DatasetArea.replay(domain.name)
    val timestamp = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.systemDefault())
      .format(Instant.now())
    // Unique, so that two loads of the table in the same second keep their files
    val name =
      s"${domain.name}.${starlakeSchema.name}.$timestamp.replay-${UUID.randomUUID().toString.take(8)}"
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
                tempTableNames,
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

  private def applyBigQuerySecondStepSQL(
    firstStepTempTableTableNames: List[String],
    existingTargetSchema: Option[bigquery.Schema],
    requiredCondition: Option[String]
  ): Try[JobResult] = {
    val incomingSchema = BigQueryNativeSchema.patch(
      starlakeSchema.bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true),
      starlakeSchema.attributes.filterNot(_.resolveIgnore()),
      _.getFinalName(),
      existingTargetSchema,
      schemaHandler,
      settings.appConfig.bigqueryNativeSchemaFromDdlMapping,
      // Logged by the first step
      log = false
    )
    // The values that the first step of a Parquet load reads as strings
    val conversions =
      if (format == Format.PARQUET)
        BigQueryNativeLoader.parquetConversions(
          firstStepSchema(schemaWithMergedMetadata, existingTargetSchema, log = false)
        )
      else Nil
    val task = this.secondStepSQLTask(
      firstStepTempTableTableNames.map(
        BigQueryNativeLoader.withConversions(_, conversions, requiredCondition)
      ),
      BigQueryNativeSchema.castTypes(incomingSchema, starlakeSchema.attributes, schemaHandler)
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

  /** The table a native BigQuery load writes to when no data branch is active. On a branch, the
    * load writes to the table of the same name in the branch dataset of this table's project (see
    * BigQueryBranchHandler.prepareBranch).
    */
  def originalTableId(
    database: Option[String],
    domain: String,
    table: String,
    sinkOptions: Map[String, String]
  )(implicit settings: Settings): TableId =
    BigQueryJobBase.extractProjectDatasetAndTable(
      database,
      domain,
      table,
      sinkOptions.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
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
    * @return
    *   the load infos of the files, with their rejected rows counted as rejected instead of
    *   accepted
    */
  def withRejectedRows(
    loadInfos: List[BqLoadInfo],
    rejectedByFile: Map[Int, Long]
  ): List[BqLoadInfo] = {
    rejectedByFile.keys.find(index => !loadInfos.indices.contains(index)).foreach { index =>
      throw new IllegalStateException(
        s"Rejected rows counted for the file $index of a load of ${loadInfos.size} files"
      )
    }
    loadInfos.zipWithIndex.map { case (loadInfo, index) =>
      val rejected = rejectedByFile.getOrElse(index, 0L)
      loadInfo.copy(
        totalAcceptedRows = loadInfo.totalAcceptedRows - rejected,
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

  /** @return
    *   the query of the first `limit` rows of the first step without a value for a required
    *   attribute: their file, their errors, one line per attribute without a value as the Spark
    *   validators write them, and with `withValue` the row as JSON
    */
  def rejectedRowsSql(
    tempTableNames: List[String],
    condition: String,
    terms: List[(String, String)],
    hiddenPaths: Option[List[String]],
    limit: Int
  ): String = {
    val file = CometColumns.cometInputFileNameColumn
    val row = "sl_rejected_row"
    val errors = terms
      .map { case (path, term) =>
        s"IF($term, '', ${bigQueryLiteral(s"`$path`  can't be null\n")})"
      }
      .mkString("CONCAT(", ", ", ")")
    // Wide numbers as strings, as TO_JSON_STRING writes them: TO_JSON fails on them otherwise
    val value = hiddenPaths
      .map { paths =>
        val removed = (s"""$$."$file"""" :: paths).map(path => s"'$path'").mkString(", ")
        s", TO_JSON_STRING(JSON_REMOVE(TO_JSON($row, stringify_wide_numbers => TRUE), $removed)) AS row_json"
      }
      .getOrElse("")
    val selects = tempTableNames.map(table =>
      s"SELECT $row.`$file` AS file, $errors AS errors$value FROM $table AS $row WHERE NOT ($condition)"
    )
    s"SELECT * FROM (${selects.mkString(" UNION ALL ")}) LIMIT $limit"
  }

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
    *   again
    */
  def replayExportSql(
    uri: String,
    tempTableNames: List[String],
    condition: String,
    excluded: List[String] = Nil
  ): String = {
    val columns = (CometColumns.cometInputFileNameColumn :: excluded).map(c => s"`$c`")
    val selects = tempTableNames.map(table =>
      s"SELECT * EXCEPT (${columns.mkString(", ")}) FROM $table WHERE NOT ($condition)"
    )
    s"EXPORT DATA OPTIONS (uri = ${bigQueryLiteral(uri)}, format = 'PARQUET', compression = 'SNAPPY', overwrite = true) AS\n${selects
        .mkString("\nUNION ALL\n")}"
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
