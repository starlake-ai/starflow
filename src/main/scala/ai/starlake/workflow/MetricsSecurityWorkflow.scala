package ai.starlake.workflow

import ai.starlake.config.Settings
import ai.starlake.job.ingest.{DummyIngestionJob, LoadConfig}
import ai.starlake.job.metrics.{MetricsConfig, MetricsJob}
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.{JobResult, StarlakeConfigException, Utils}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

trait MetricsSecurityWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def storageHandler: StorageHandler
  protected def schemaHandler: SchemaHandler
  implicit protected def settings: Settings

  protected def domainsToWatch(config: LoadConfig): List[DomainInfo]

  /** Runs the metrics job
    *
    * @param cliConfig
    *   : Client's configuration for metrics computing
    */
  def metric(cliConfig: MetricsConfig): Try[JobResult] = {
    val cmdArgs = for {
      domain <- schemaHandler.getDomain(cliConfig.domain)
      schema <- domain.tables.find(_.name == cliConfig.schema)
    } yield (domain, schema)

    cmdArgs match {
      case Some((domain: DomainInfo, schema: SchemaInfo)) =>
        val result = new MetricsJob(
          None,
          domain,
          schema,
          storageHandler,
          schemaHandler
        ).run()
        Utils.logFailure(result, logger)
      case None =>
        logger.error("The domain or schema you specified doesn't exist! ")
        Failure(new Exception("The domain or schema you specified doesn't exist! "))
    }
  }

  def applyIamPolicies(accessToken: Option[String]): Try[Unit] = {
    val ignore = BigQueryLoadConfig(
      connectionRef = None,
      outputDatabase = None,
      accessToken = accessToken
    )
    schemaHandler
      .iamPolicyTags()
      .map { iamPolicyTags =>
        new BigQuerySparkJob(ignore).applyIamPolicyTags(iamPolicyTags)
      }
      .getOrElse(Success(()))
  }

  def secure(config: LoadConfig): Try[Boolean] = {
    if (settings.appConfig.accessPolicies.apply) {
      val includedDomains = domainsToWatch(config)
      val result = includedDomains.flatMap { domain =>
        domain.tables.map { schema =>
          val metadata = schema.mergedMetadata(domain.metadata)
          val dummyIngestionJob = new DummyIngestionJob(
            domain,
            schema,
            schemaHandler.types(),
            Nil,
            storageHandler,
            schemaHandler,
            Map.empty,
            config.accessToken,
            config.test,
            config.scheduledDate
          )
          val sink = metadata.sink
            .map(_.getSink())
            .getOrElse(throw new StarlakeConfigException("Sink required"))

          val connectionName = sink.connectionRef
            .getOrElse(throw new StarlakeConfigException("JdbcSink requires a connectionRef"))
          val connection =
            settings.appConfig.connections(connectionName).withAccessToken(config.accessToken)
          sink match {
            case _: FsSink =>
              dummyIngestionJob.applyHiveTableAcl()
            case jdbcSink: JdbcSink =>
              dummyIngestionJob.applyJdbcAcl(connection)
            case _: BigQuerySink =>
              val database = schemaHandler.getDatabase(domain)
              val bqConfig = BigQueryLoadConfig(
                connectionRef = Some(metadata.getSinkConnectionRef()),
                outputTableId = Some(
                  BigQueryJobBase
                    .extractProjectDatasetAndTable(
                      database,
                      domain.name,
                      schema.finalName,
                      settings.appConfig
                        .connections(metadata.getSinkConnectionRef())
                        .options
                        .get("projectId")
                        .orElse(settings.appConfig.getDefaultDatabase())
                    )
                ),
                rls = schema.rls,
                acl = schema.acl,
                starlakeSchema = Some(schema),
                outputDatabase = database,
                accessToken = config.accessToken
              )
              val res = new BigQuerySparkJob(bqConfig).applyRLSAndCLS(forceApply = true)
              res.recover { case e =>
                Utils.logException(logger, e)
                throw e
              }

            case _ =>
              Success(true) // unknown sink, just ignore.
          }
        }
      }
      if (result.exists(_.isFailure))
        Failure(new Exception("Some errors occurred during secure"))
      else
        Success(true)
    } else {
      logger.info("Access policies are not applied, skipping secure step")
      Success(true)
    }
  }
}
