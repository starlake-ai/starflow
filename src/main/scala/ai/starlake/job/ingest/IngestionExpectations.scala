package ai.starlake.job.ingest

import ai.starlake.job.metrics._
import ai.starlake.job.ingest.loaders.BigQueryNativeLoader
import ai.starlake.job.sink.bigquery._
import ai.starlake.schema.handlers.SqlTransformations
import ai.starlake.schema.model.{BigQuerySink, JdbcSink}
import ai.starlake.utils.{JobResult, SparkJobResult}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

/** Expectations and metrics concerns for ingestion jobs. */
trait IngestionExpectations { self: IngestionJob =>

  protected def runExpectations(): Try[JobResult] = {
    mergedMetadata.getSink() match {
      case _: BigQuerySink =>
        val tableId = BigQueryJobBase.extractProjectDatasetAndTable(
          settings.appConfig.audit.getDatabase(),
          settings.appConfig.audit.getDomain(),
          "expectations",
          mergedMetadata
            .getSinkConnection()
            .options
            .get("projectId")
            .orElse(settings.appConfig.getDefaultDatabase())
        )
        runBigQueryExpectations(bqNativeJob(tableId, ""))
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnection().withAccessToken(accessToken).options
        runJdbcExpectations(options)
      case _ =>
        runSparkExpectations(session)
    }
  }

  private def runExpectationsWithHandler(handler: ExpectationAssertionHandler): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      expectationJob(handler).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  /** The table expectations check, as (database, domain, table): the loaded table, except for a
    * native BigQuery load on a data branch, which writes to the branch copy of the table. Spark
    * loads never branch.
    */
  private def expectationTarget: (Option[String], String, String) = {
    val database = schemaHandler.getDatabase(this.domain)
    val loadedTable = (database, this.domain.finalName, this.schema.finalName)
    if (selectLoader() == "bigquery") {
      val sinkOptions = mergedMetadata.getSink().getConnection().options
      schemaHandler.dataBranch(sinkOptions) match {
        case Some(branch) =>
          val tableId = BigQueryNativeLoader.originalTableId(
            database,
            this.domain.finalName,
            this.schema.finalName,
            sinkOptions
          )
          (Option(tableId.getProject), branch, tableId.getTable)
        case None => loadedTable
      }
    } else
      loadedTable
  }

  private[ingest] def expectationJob(handler: ExpectationAssertionHandler): ExpectationJob = {
    val (database, domainName, tableName) = expectationTarget
    new ExpectationJob(
      Option(applicationId()),
      database,
      domainName,
      tableName,
      this.schema.expectations,
      storageHandler,
      schemaHandler,
      handler,
      false,
      transpileSql = sql =>
        SqlTransformations.transpileIfNeeded(
          sql,
          mergedMetadata.getSinkConnection(),
          schemaHandler.activeEnvVars(),
          this.test
        )
    )
  }

  private def runJdbcExpectations(jdbcOptions: Map[String, String]): Try[JobResult] =
    runExpectationsWithHandler(new JdbcExpectationAssertionHandler(jdbcOptions))

  private def runSparkExpectations(session: SparkSession): Try[JobResult] =
    runExpectationsWithHandler(new SparkExpectationAssertionHandler(session))

  def runBigQueryExpectations(job: BigQueryNativeJob): Try[JobResult] =
    runExpectationsWithHandler(new BigQueryExpectationAssertionHandler(job))

  protected def runMetrics(acceptedDF: DataFrame): Unit = {
    if (settings.appConfig.metrics.active) {
      new MetricsJob(
        Option(applicationId()),
        this.domain,
        this.schema,
        this.storageHandler,
        this.schemaHandler
      )
        .run(acceptedDF, System.currentTimeMillis())
    }
  }
}
