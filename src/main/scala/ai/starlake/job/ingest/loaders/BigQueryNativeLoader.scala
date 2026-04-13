package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.ParUtils
import ai.starlake.job.ingest.{BqLoadInfo, IngestionJob}
import ai.starlake.job.sink.bigquery.{
  BigQueryJobBase,
  BigQueryJobResult,
  BigQueryLoadConfig,
  BigQueryNativeJob
}
import ai.starlake.job.transform.BigQueryAutoTask
import ai.starlake.schema.model._
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{IngestionCounters, JobResult, Utils}
import com.google.cloud.bigquery
import com.google.cloud.bigquery.{Field, JobInfo, StandardSQLTypeName, TableId}
import com.typesafe.scalalogging.LazyLogging

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class BigQueryNativeLoader(ingestionJob: IngestionJob, accessToken: Option[String])(implicit
  settings: Settings
) extends NativeLoader(ingestionJob, accessToken)
    with LazyLogging {

  lazy val targetTableId: TableId =
    BigQueryJobBase.extractProjectDatasetAndTable(
      schemaHandler.getDatabase(domain),
      domain.finalName,
      effectiveSchema.finalName,
      sinkConnection.options.get("projectId").orElse(settings.appConfig.getDefaultDatabase())
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
                val firstStepTableInfo = firstStepBigqueryJob.getTableInfo(
                  firstStepTempTable,
                  _.bigquerySchemaWithIgnoreAndScript(schemaHandler, withFinalName = false)
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

        val output: Try[List[BqLoadInfo]] =
          applyBigQuerySecondStep(
            targetConfig,
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
        bigqueryJob
          .loadPathsToBQ(
            bigqueryJob
              .getTableInfo(
                targetTableId,
                _.bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true)
              )
          )
          .map(List(_))
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

  private def applyBigQuerySecondStep(
    targetConfig: BigQueryLoadConfig,
    firstStepTempTable: List[TableId],
    firstStepResult: Try[List[BqLoadInfo]]
  ): Try[List[BqLoadInfo]] = {
    firstStepResult match {
      case Success(loadFileResult) =>
        logger.info(s"First step result: ${loadFileResult.map(_.toString).mkString(", ")}")
        val targetBigqueryJob = new BigQueryNativeJob(targetConfig, "")
        val secondStepResult =
          targetBigqueryJob.cliConfig.outputTableId
            .map { _ =>
              applyBigQuerySecondStepSQL(
                firstStepTempTable.map(t => BigQueryUtils.tableIdToTableName(t))
              )
            }
            .getOrElse(throw new IllegalStateException("Should never happen"))

        secondStepResult
          .flatMap { _ =>
            firstStepResult
          }
      case res @ Failure(_) =>
        res
    }
  }

  private def applyBigQuerySecondStepSQL(
    firstStepTempTableTableNames: List[String]
  ): Try[JobResult] = {
    val task = this.secondStepSQLTask(firstStepTempTableTableNames)
    val bqTask = task.asInstanceOf[BigQueryAutoTask]
    val incomingSparkSchema =
      starlakeSchema.sparkSchemaWithoutIgnore(schemaHandler, withFinalName = true)
    bqTask.updateBigQueryTableSchema(
      incomingSparkSchema = Some(incomingSparkSchema),
      createIfAbsent = true
    )
    val jobResult = bqTask.run()
    jobResult
  }

}
