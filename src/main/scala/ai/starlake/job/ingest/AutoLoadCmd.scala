package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.job.infer.{InferSchemaCmd, InferSchemaConfig}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.JobResult
import com.typesafe.scalalogging.LazyLogging
import scopt.OParser

import scala.util.{Failure, Success, Try}

/** Command to automatically infer schemas and load data from the incoming directory.
  *
  * Usage: starlake autoload [options]
  */
trait AutoLoadCmd extends Cmd[AutoLoadConfig] with LazyLogging {

  def command = "autoload"

  override def pageDescription: String =
    "Automatically watch and load incoming data files into specified domains and tables with optional scheduling and substitutions."
  override def pageKeywords: Seq[String] =
    Seq("starlake autoload", "auto load", "data ingestion", "file watching", "ETL")

  val parser: OParser[Unit, AutoLoadConfig] = {
    val builder = OParser.builder[AutoLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Automatically infer schemas from files in the incoming directory and load them into the data warehouse in a single step. This combines the `infer-schema` and `load` commands, making it ideal for quick ingestion of new data sources. See [Autoload Guide](/starflow/guides/load/autoload)."""
      ),
      builder
        .opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to watch"),
      builder
        .opt[Unit]("clean")
        .optional()
        .action((_, c) => c.copy(clean = true))
        .text("Overwrite existing mapping files before starting"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[String]("scheduledDate")
        .action((x, c) => c.copy(scheduledDate = Some(x)))
        .text("Scheduled date for the job, in format yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Watch arguments to be used as substitutions"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[AutoLoadConfig] =
    OParser.parse(parser, args, AutoLoadConfig(accessToken = None, scheduledDate = None))

  override def run(config: AutoLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val incomingFile = StorageHandler.localFile(DatasetArea.incoming("dummy").getParent)
    if (incomingFile.exists && incomingFile.isDirectory) {
      val folders =
        incomingFile.list.filter(f =>
          f.isDirectory && !f.name.startsWith(".") && !f.name.startsWith("_")
        )
      val success = folders
        .map { folder =>
          val domainName = folder.name
          if (config.domains.nonEmpty && !config.domains.contains(domainName)) {
            logger.info(s"Skipping $domainName")
            false
          } else {
            logger.info(s"Processing ${folder.pathAsString}")
            InferSchemaCmd
              .run(
                InferSchemaConfig(
                  inputPath = folder.pathAsString,
                  clean = config.clean
                ),
                schemaHandler
              )
              .isSuccess
          }
        }
        .forall(identity)
      if (success) {
        val wf = workflow(schemaHandler)
        logger.info("All schemas inferred successfully")
        wf.stage(StageConfig(config.domains, config.tables)).map { _ =>
          logger.info("Staged successfully")
          wf.load(
            LoadConfig(
              config.domains,
              config.tables,
              config.options,
              config.accessToken,
              test = false,
              files = None,
              scheduledDate = config.scheduledDate
            )
          )
        } match {
          case Success(result) => result
          case Failure(exception) =>
            logger.error("Could not stage", exception)
            Failure(exception)
        }
      } else {
        throw new Exception("Some schemas failed to be inferred")
      }
    } else {
      throw new Exception(s"${DatasetArea.incoming("dummy").getParent} is not a directory")
    }
  }
}

object AutoLoadCmd extends AutoLoadCmd
