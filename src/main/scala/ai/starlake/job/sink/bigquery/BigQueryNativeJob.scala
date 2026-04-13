package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.job.ingest.BqLoadInfo
import ai.starlake.schema.model.{
  ClusteringInfo,
  FieldPartitionInfo,
  Format,
  Materialization,
  SchemaInfo,
  TableInfo as SLTableInfo
}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{JobBase, JobResult, Utils}
import better.files.File
import com.google.cloud.bigquery.BigQuery.QueryResultsOption
import com.google.cloud.bigquery.JobInfo.{CreateDisposition, SchemaUpdateOption, WriteDisposition}
import com.google.cloud.bigquery.JobStatistics.{LoadStatistics, QueryStatistics}
import com.google.cloud.bigquery.QueryJobConfiguration.Priority
import com.google.cloud.bigquery.{Schema as BQSchema, *}
import com.google.cloud.{PageImpl, RetryOption}
import com.manticore.jsqlformatter.JSQLFormatter

import java.net.URI
import java.nio.channels.Channels
import java.nio.file.Files
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try, Using}

class BigQueryNativeJob(
  override val cliConfig: BigQueryLoadConfig,
  sql: String,
  resultPageSize: Long = 1,
  jobTimeoutMs: scala.Option[Long] = None
)(implicit val settings: Settings)
    extends JobBase
    with BigQueryJobBase {
  override def name: String = s"bqload-${bqNativeTable}"

  logger.debug(s"BigQuery Config $cliConfig")

  def loadPathsToBQ(
    tableInfo: SLTableInfo,
    tableInfoWithDefaultColumn: scala.Option[SLTableInfo] = None
  ): Try[BqLoadInfo] = {
    // have default column in another tableInfo otherwise load doesn't fill with default value. Column in load schema are then null if they doesn't exist.
    getOrCreateDataset(cliConfig.domainDescription) match {
      case Failure(exception) =>
        logger.error(
          s"Error while getting or creating dataset ${cliConfig.outputDatabase}",
          exception
        )
        Failure(exception)
      case Success(_) =>
        getOrCreateTable(
          tableInfoWithDefaultColumn.getOrElse(tableInfo),
          None
        ).flatMap { _ =>
          Try {
            val bqSchema =
              tableInfo.maybeSchema.getOrElse(throw new RuntimeException("Should never happen"))
            logger.info(s"BigQuery Schema: $bqSchema")
            val formatOptions: FormatOptions = bqLoadFormatOptions()
            cliConfig.source match {
              case Left(sourceURIs) =>
                val sourceURIList = sourceURIs.split(",").toList
                val uri = sourceURIList.head

                // We upload local files first.
                val localFiles = uri.startsWith("file:")
                BigQueryJobBase
                  .recoverBigqueryException(settings.appConfig.onExceptionRetries) {
                    val job = {
                      if (localFiles) {
                        loadLocalFilePathsToBQ(bqSchema, formatOptions, sourceURIList)
                      } else {
                        val loadConfig: LoadJobConfiguration =
                          bqLoadConfig(bqSchema, formatOptions, sourceURIs)
                        // Load data from a GCS CSV file into the table
                        val jobId = newJobIdWithLocation()
                        bigquery(accessToken = cliConfig.accessToken).create(
                          JobInfo.newBuilder(loadConfig).setJobId(jobId).build()
                        )
                      }
                    }
                    job
                  }
                  .flatMap { job =>
                    logger.info(s"Waiting for job ${job.getJobId}")
                    // Blocks until this load table job completes its execution, either failing or succeeding.
                    BigQueryJobBase.recoverBigqueryException(
                      settings.appConfig.onExceptionRetries
                    )(
                      job.waitFor(
                        RetryOption.totalTimeout(
                          org.threeten.bp.Duration.ofMillis(
                            jobTimeoutMs.getOrElse(settings.appConfig.longJobTimeoutMs)
                          )
                        )
                      )
                    )
                  }
                  .map { jobResult =>
                    if (scala.Option(jobResult.getStatus.getError()).isEmpty) {
                      val stats = jobResult.getStatistics.asInstanceOf[LoadStatistics]
                      applyRLSAndCLS().recover { case e =>
                        Utils.logException(logger, e)
                        throw e
                      }
                      BqLoadInfo(
                        stats.getOutputRows,
                        stats.getBadRecords,
                        sourceURIList,
                        BigQueryJobResult(None, stats.getInputBytes, Some(jobResult))
                      )
                    } else
                      throw new Exception(
                        "BigQuery was unable to load into the table due to an error:" + jobResult.getStatus
                          .getError()
                      )
                  }
              case Right(_) =>
                throw new Exception("Should never happen")
            }
          }.flatten
        }
    }

  }

  private def loadLocalFilePathsToBQ(
    bqSchema: BQSchema,
    formatOptions: FormatOptions,
    sourceURIs: Iterable[String]
  ): Job = {
    val loadConfig = bqLoadLocaFileConfig(bqSchema, formatOptions)
    val jobId: JobId = newJobIdWithLocation()
    Using(bigquery(accessToken = cliConfig.accessToken).writer(jobId, loadConfig)) { writer =>
      val outputStream = Channels.newOutputStream(writer)
      sourceURIs
        .foreach(uri => Files.copy(File(new URI(uri)).path, outputStream))
    }
    bigquery(accessToken = cliConfig.accessToken).getJob(jobId)
  }

  private def newJobIdWithLocation(): JobId = {
    val jobName = UUID.randomUUID().toString
    val jobIdBuilder = JobId.newBuilder().setJob(jobName);
    jobIdBuilder.setProject(
      BigQueryJobBase.projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase)
    )
    jobIdBuilder.setLocation(
      connectionOptions.getOrElse(
        "location",
        throw new Exception(
          s"location is required but not present in connection $connectionName"
        )
      )
    )
    val jobId = jobIdBuilder.build()
    jobId
  }

  def getTableInfo(tableId: TableId, toBQSchema: SchemaInfo => BQSchema): SLTableInfo = {
    SLTableInfo(
      tableId,
      cliConfig.outputTableDesc,
      cliConfig.starlakeSchema.map(toBQSchema),
      cliConfig.outputPartition.map { partitionField =>
        FieldPartitionInfo(
          partitionField,
          cliConfig.days,
          cliConfig.requirePartitionFilter
        )
      },
      cliConfig.outputClustering match {
        case Nil => None
        case _   => Some(ClusteringInfo(cliConfig.outputClustering.toList))
      }
    )
  }

  private def bqLoadConfig(
    bqSchema: BQSchema,
    formatOptions: FormatOptions,
    sourceURIs: String
  ): LoadJobConfiguration = {
    val loadConfig =
      LoadJobConfiguration.newBuilder(tableId, sourceURIs.split(",").toList.asJava, formatOptions)
    configureBqLoad(loadConfig, bqSchema).build()
  }

  private def bqLoadLocaFileConfig(
    bqSchema: BQSchema,
    formatOptions: FormatOptions
  ): WriteChannelConfiguration = {
    val loadConfig = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(formatOptions)
    configureBqLoad(loadConfig, bqSchema).build()
  }

  private def configureBqLoad[T <: LoadConfiguration.Builder](
    loadConfig: T,
    bqSchema: BQSchema
  ): T = {
    loadConfig
      .setIgnoreUnknownValues(true)
      .setCreateDisposition(JobInfo.CreateDisposition.valueOf(cliConfig.createDisposition))
      .setWriteDisposition(JobInfo.WriteDisposition.valueOf(cliConfig.writeDisposition))
      .setSchema(bqSchema)
    if (cliConfig.writeDisposition == JobInfo.WriteDisposition.WRITE_APPEND.toString) {
      loadConfig.setSchemaUpdateOptions(
        List(
          SchemaUpdateOption.ALLOW_FIELD_ADDITION,
          SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ).asJava
      )
    }
    if (!settings.appConfig.rejectAllOnError)
      loadConfig.setMaxBadRecords(settings.appConfig.rejectMaxRecords)

    cliConfig.outputPartition match {
      case Some(partitionField) =>
        // Generating schema from YML to get the descriptions in BQ
        val partitioning =
          timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
            .build()
        loadConfig.setTimePartitioning(partitioning)
      case None =>
    }
    cliConfig.outputClustering match {
      case Nil =>
      case fields =>
        val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
        loadConfig.setClustering(clustering)
    }
    cliConfig.starlakeSchema.flatMap(_.metadata) match {
      case Some(metadata) =>
        metadata.resolveFormat() match {
          case Format.DSV =>
            metadata.nullValue.foreach(loadConfig.setNullMarker)
          case _ =>
        }
      case _ =>
    }
    loadConfig
  }

  private def bqLoadFormatOptions(): FormatOptions = {
    val formatOptions = cliConfig.starlakeSchema.flatMap(_.metadata) match {
      case Some(metadata) =>
        metadata.resolveFormat() match {
          case Format.DSV =>
            val formatOptions =
              CsvOptions.newBuilder.setAllowQuotedNewLines(true).setAllowJaggedRows(true)
            if (metadata.resolveWithHeader())
              formatOptions.setSkipLeadingRows(1).build
            formatOptions.setEncoding(metadata.resolveEncoding())
            formatOptions.setFieldDelimiter(metadata.resolveSeparator())
            metadata.quote.map(quote => formatOptions.setQuote(quote))
            formatOptions.setAllowJaggedRows(true)
            formatOptions.build()
          case Format.JSON | Format.JSON_FLAT =>
            FormatOptions.json()
          case _ =>
            throw new Exception(s"Should never happen: ${metadata.resolveFormat()}")
        }
      case None =>
        throw new Exception("Should never happen")
    }
    formatOptions
  }

  def runBigQueryJob(
    thisSql: scala.Option[String] = None,
    pageSize: scala.Option[Long] = None,
    queryJobTimeoutMs: scala.Option[Long] = None,
    dryRun: Boolean = false
  ): Try[BigQueryJobResult] = {
    val targetSQL = thisSql.getOrElse(sql).trim()
    if (targetSQL.startsWith("DESCRIBE")) {
      val table = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
      val bqSchema = table.getDefinition[StandardTableDefinition].getSchema
      Success(
        BigQueryJobResult(
          Some(
            TableResult
              .newBuilder()
              .setTotalRows(0)
              .setPageNoSchema(
                new PageImpl[FieldValueList](null, null, null)
              )
              .setSchema(bqSchema)
              .build()
          ),
          -1,
          None
        )
      )
    } else {
      val queryConfig: QueryJobConfiguration.Builder =
        QueryJobConfiguration
          .newBuilder(targetSQL)
          .setAllowLargeResults(true)
          .setMaximumBytesBilled(
            connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
          )

      val sqlId = java.util.UUID.randomUUID.toString
      val formattedSQL = SQLUtils
        .format(targetSQL, JSQLFormatter.OutputFormat.PLAIN)
      logger.info(s"running BigQuery statement with Id $sqlId: $formattedSQL")

      val queryConfigWithUDF = addUDFToQueryConfig(queryConfig)
      val finalConfiguration =
        queryConfigWithUDF.setPriority(Priority.INTERACTIVE).setDryRun(dryRun).build()

      val result =
        BigQueryJobBase
          .recoverBigqueryException(settings.appConfig.onExceptionRetries) {
            val jobId = newJobIdWithLocation()
            val queryJob =
              bigquery(accessToken = cliConfig.accessToken).create(
                JobInfo.newBuilder(finalConfiguration).setJobId(jobId).build()
              )
            if (!dryRun) {
              logger.info(s"Waiting for job $jobId")
              queryJob.waitFor(
                RetryOption.maxAttempts(0),
                RetryOption.totalTimeout(
                  org.threeten.bp.Duration.ofMinutes(
                    queryJobTimeoutMs
                      .orElse(jobTimeoutMs)
                      .getOrElse(settings.appConfig.longJobTimeoutMs)
                  )
                )
              )
            } else {
              logger.info(s"Dry run $jobId")
              queryJob
            }
          }
          .map { jobResult =>
            val totalBytesProcessed = jobResult
              .getStatistics()
              .asInstanceOf[QueryStatistics]
              .getTotalBytesProcessed
            if (!dryRun) {
              val results = jobResult.getQueryResults(
                QueryResultsOption.pageSize(pageSize.getOrElse(this.resultPageSize))
              )
              logger.info(
                s"Query large results performed successfully on query with Id $sqlId: ${results.getTotalRows} rows returned."
              )
              BigQueryJobResult(Some(results), totalBytesProcessed, Some(jobResult))
            } else {
              BigQueryJobResult(None, totalBytesProcessed, Some(jobResult))
            }
          }
      result match {
        case Failure(e) =>
          logger.error(s"Error while running BigQuery query with id $sqlId", e.getMessage)
          Failure(e)
        case Success(jobResult) =>
          Success(jobResult)
      }
    }
  }

  private def addUDFToQueryConfig(
    queryConfig: QueryJobConfiguration.Builder
  ): QueryJobConfiguration.Builder = {
    settings.appConfig
      .getEffectiveUdfs()
      .foreach { udf =>
        if (udf.contains("://")) // make sure it's a URI
          queryConfig.setUserDefinedFunctions(List(UserDefinedFunction.fromUri(udf)).asJava)
      }
    queryConfig
  }

  /** Just to force any job to implement its entry point within the "run" method
    *
    * @return
    *   : job result
    */
  override def run(): Try[JobResult] = {
    // The very first time we update a audit table, we run it interactively to make sure there is not
    // risk running it in batch mode (batch mode cannot catch errors such as schema incompatibility).
    if (this.datasetId.getDataset() == settings.appConfig.audit.getDomain()) {
      if (settings.appConfig.internal.forall(_.bqAuditSaveInBatchMode)) {
        runBatchQuery().map(_ => BigQueryJobResult(None, 0L, None))
      } else {
        runAndSinkAsTable(queryJobTimeoutMs = jobTimeoutMs)
      }
    } else if (cliConfig.materialization == Materialization.TABLE) {
      runAndSinkAsTable(queryJobTimeoutMs = jobTimeoutMs)
    } else if (cliConfig.materialization == Materialization.MATERIALIZED_VIEW) {
      runAndSinkAsMaterializedView().map(table => BigQueryJobResult(None, 0L, None))
    } else {
      require(cliConfig.materialization == Materialization.VIEW)
      runAndSinkAsView().map(table => BigQueryJobResult(None, 0L, None))
    }
  }

  def runAndSinkAsView(thisSql: scala.Option[String] = None): Try[Table] = {
    getOrCreateDataset(None).flatMap { _ =>
      Try {
        val deleted = bigquery(accessToken = cliConfig.accessToken).delete(tableId)
        if (deleted) {
          logger.info(s"Deleted view $tableId")
        } else {
          logger.info(s"View $tableId does not exist yet")
        }
        val viewDefinitionBuilder =
          ViewDefinition.newBuilder(thisSql.getOrElse(sql))
        val table =
          bigquery(accessToken = cliConfig.accessToken)
            .create(TableInfo.of(tableId, viewDefinitionBuilder.build()))
        table
      }
    }
  }
  def runAndSinkAsMaterializedView(thisSql: scala.Option[String] = None): Try[Table] = {
    getOrCreateDataset(None).flatMap { _ =>
      Try {
        val materializedViewDefinitionBuilder =
          MaterializedViewDefinition.newBuilder(thisSql.getOrElse(sql))
        cliConfig.outputPartition match {
          case Some(partitionField) =>
            // Generating schema from YML to get the descriptions in BQ
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            materializedViewDefinitionBuilder.setTimePartitioning(partitioning)
          case None =>
        }
        cliConfig.outputClustering match {
          case Nil =>
          case fields =>
            val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
            materializedViewDefinitionBuilder.setClustering(clustering)
        }

        cliConfig.enableRefresh match {
          case Some(x) => materializedViewDefinitionBuilder.setEnableRefresh(x)
          case None    =>
        }
        cliConfig.refreshIntervalMs match {
          case Some(x) => materializedViewDefinitionBuilder.setRefreshIntervalMs(x.toLong)
          case None    =>
        }
        val table =
          bigquery(accessToken = cliConfig.accessToken)
            .create(TableInfo.of(tableId, materializedViewDefinitionBuilder.build()))
        setTagsOnTable(table)
        table
      }
    }
  }

  def runAndSinkAsTable(
    thisSql: scala.Option[String] = None,
    targetTableSchema: scala.Option[BQSchema] = None,
    queryJobTimeoutMs: scala.Option[Long] = None
  ): Try[BigQueryJobResult] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
        val queryConfig: QueryJobConfiguration.Builder =
          QueryJobConfiguration
            .newBuilder(thisSql.getOrElse(sql))
            .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
            .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
            .setDefaultDataset(targetDataset.getDatasetId)
            .setPriority(Priority.INTERACTIVE)
            .setMaximumBytesBilled(
              connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
            )
            .setUseLegacySql(false)
            .setAllowLargeResults(true)

        logger.info("Computing partitionning")
        val queryConfigWithPartition = cliConfig.outputPartition match {
          case Some(partitionField) =>
            // Generating schema from YML to get the descriptions in BQ
            val partitioning =
              timePartitioning(partitionField, cliConfig.days, cliConfig.requirePartitionFilter)
                .build()
            queryConfig.setTimePartitioning(partitioning)
          case None =>
            queryConfig
        }
        // Allow Field relaxation / addition in native job when appending to existing partitioned table
        val tableExists = Try(
          bigquery(accessToken = cliConfig.accessToken)
            .getTable(tableId)
            .exists()
        ).toOption.getOrElse(false)

        logger.info("Schema update options")
        val queryConfigWithPartitioningAndSchemaUpdate =
          if (cliConfig.writeDisposition == WriteDisposition.WRITE_APPEND.toString && tableExists)
            queryConfigWithPartition
              .setSchemaUpdateOptions(
                List(
                  SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                  SchemaUpdateOption.ALLOW_FIELD_RELAXATION
                ).asJava
              )
          else
            queryConfigWithPartition

        logger.info("Computing clustering")
        val queryConfigWithClustering = cliConfig.outputClustering match {
          case Nil =>
            queryConfigWithPartitioningAndSchemaUpdate
          case fields =>
            val clustering = Clustering.newBuilder().setFields(fields.asJava).build()
            queryConfigWithPartitioningAndSchemaUpdate.setClustering(clustering)
        }
        logger.info("Add user defined functions")
        val queryConfigWithUDF = addUDFToQueryConfig(queryConfigWithClustering)
        logger.info(s"Executing BQ Query $sql")
        val finalConfiguration = queryConfigWithUDF.setDestinationTable(tableId).build()
        BigQueryJobBase
          .recoverBigqueryException(settings.appConfig.onExceptionRetries) {
            val jobId = newJobIdWithLocation()
            val jobInfo =
              bigquery(accessToken = cliConfig.accessToken).create(
                JobInfo.newBuilder(finalConfiguration).setJobId(jobId).build()
              )
            logger.info(s"Waiting for job $jobId")
            jobInfo.waitFor(
              RetryOption.totalTimeout(
                org.threeten.bp.Duration.ofMinutes(
                  queryJobTimeoutMs
                    .orElse(jobTimeoutMs)
                    .getOrElse(settings.appConfig.longJobTimeoutMs)
                )
              )
            )
          }
          .map { jobResult =>
            val totalBytesProcessed = jobResult
              .getStatistics()
              .asInstanceOf[QueryStatistics]
              .getTotalBytesProcessed
            val results =
              jobResult.getQueryResults(QueryResultsOption.pageSize(this.resultPageSize))
            logger.info(
              s"Query large results performed successfully: ${results.getTotalRows} rows inserted and processed ${totalBytesProcessed} bytes."
            )
            val table = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
            setTagsOnTable(table)

            applyRLSAndCLS().recover { case e =>
              Utils.logException(logger, e)
              throw new Exception(e)
            }
            updateTableDescription(table, cliConfig.outputTableDesc.getOrElse(""))
            targetTableSchema
              .map(updateColumnsDescription)
              .getOrElse(
                updateColumnsDescription(
                  BigQueryJobBase.dictToBQSchema(getFieldsDescriptionSource(sql))
                )
              )
            BigQueryJobResult(Some(results), totalBytesProcessed, Some(jobResult))
          }
      }.flatten
    }
  }

  def getTable(tableId: TableId): Try[Table] = {
    Try {
      bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
    }
  }
  // run batch query use only (for auditing only)
  def runBatchQuery(
    thisSql: scala.Option[String] = None,
    wait: Boolean = false
  ): Try[Job] = {
    getOrCreateDataset(None).flatMap { targetDataset =>
      Try {
        val jobId = newJobIdWithLocation()
        val queryConfig =
          QueryJobConfiguration
            .newBuilder(thisSql.getOrElse(sql))
            .setCreateDisposition(CreateDisposition.valueOf(cliConfig.createDisposition))
            .setWriteDisposition(WriteDisposition.valueOf(cliConfig.writeDisposition))
            .setDefaultDataset(targetDataset.getDatasetId)
            .setDestinationTable(tableId)
            .setPriority(Priority.BATCH)
            .setUseLegacySql(false)
            .setSchemaUpdateOptions(
              List(
                SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                SchemaUpdateOption.ALLOW_FIELD_RELAXATION
              ).asJava
            )
            .setMaximumBytesBilled(
              connectionOptions.get("maximumBytesBilled").map(java.lang.Long.valueOf).orNull
            )
            .build()
        logger.info(s"Executing batch BQ Query $sql")
        val job =
          bigquery(accessToken = cliConfig.accessToken)
            .create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
        logger.info(
          s"Batch query wth jobId $jobId sent to BigQuery "
        )
        if (job == null)
          throw new Exception(s"Job for $sql not executed since it no longer exists.")
        else {
          if (wait) {
            logger.info(s"Waiting for job $jobId")
            job.waitFor(
              RetryOption.totalTimeout(
                org.threeten.bp.Duration
                  .ofMinutes(jobTimeoutMs.getOrElse(settings.appConfig.longJobTimeoutMs))
              )
            )
          } else {
            logger.info(s"${job.getJobId()} is running in background")
          }
          job
        }
      } match {
        case Failure(e) =>
          logger.error(s"Error while running batch query $sql", e)
          Failure(e)
        case Success(job) =>
          Success(job)
      }
    }
  }
}
