package ai.starlake.job.ingest

import ai.starlake.job.metrics._
import ai.starlake.job.sink.bigquery._
import ai.starlake.schema.handlers.SqlTransformations
import ai.starlake.schema.model.{BigQuerySink, JdbcSink}
import ai.starlake.utils.{JobResult, SparkJobResult}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

/** Expectations and metrics concerns for ingestion jobs. */
trait IngestionExpectations { self: IngestionJob =>

  /** Runs the expectations of the table that the write of the load has not checked before its
    * commit
    *
    * @param variables
    *   the variables of their macros: the counters of the load
    */
  protected def runExpectations(variables: Map[String, Any] = Map.empty): Try[JobResult] = {
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
        runBigQueryExpectations(bqNativeJob(tableId, ""), variables)
      case _: JdbcSink =>
        val options = mergedMetadata.getSinkConnection().withAccessToken(accessToken).options
        runJdbcExpectations(options, variables)
      case _ =>
        runSparkExpectations(session, variables)
    }
  }

  private def runExpectationsWithHandler(
    handler: ExpectationAssertionHandler,
    variables: Map[String, Any]
  ): Try[JobResult] = {
    if (settings.appConfig.expectations.active) {
      new ExpectationJob(
        Option(applicationId()),
        schemaHandler.getDatabase(this.domain),
        this.domain.finalName,
        this.schema.finalName,
        expectationsAfterAudit,
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
          ),
        variables = variables
      ).run()
    } else {
      Success(SparkJobResult(None, None))
    }
  }

  private def runJdbcExpectations(
    jdbcOptions: Map[String, String],
    variables: Map[String, Any]
  ): Try[JobResult] =
    runExpectationsWithHandler(new JdbcExpectationAssertionHandler(jdbcOptions), variables)

  private def runSparkExpectations(
    session: SparkSession,
    variables: Map[String, Any]
  ): Try[JobResult] =
    runExpectationsWithHandler(new SparkExpectationAssertionHandler(session), variables)

  def runBigQueryExpectations(
    job: BigQueryNativeJob,
    variables: Map[String, Any] = Map.empty
  ): Try[JobResult] =
    runExpectationsWithHandler(new BigQueryExpectationAssertionHandler(job), variables)

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
