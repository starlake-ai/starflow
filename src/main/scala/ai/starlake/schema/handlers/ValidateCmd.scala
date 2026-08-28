package ai.starlake.schema.handlers

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.transform.AutoTask
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Failure, Success, Try}

/** Command to validate the project and connections.
  *
  * Usage: starlake validate [options]
  */
object ValidateCmd extends Cmd[ValidateConfig] {

  val command = "validate"

  override def pageDescription: String =
    "Validate your Starflow project configuration by reloading all YAML files from disk and checking for errors or warnings."
  override def pageKeywords: Seq[String] =
    Seq("starlake validate", "project validation", "YAML validation", "configuration check")

  val parser: OParser[Unit, ValidateConfig] = {
    val builder = OParser.builder[ValidateConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Validate the Starflow project configuration files and optionally test database connections."""
      ),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Reload all files from disk before starting validation. Always true regardless of the value set here."
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  override def parse(args: Seq[String]): Option[ValidateConfig] =
    OParser.parse(parser, args, ValidateConfig(), setup)

  override def run(config: ValidateConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    validateConnections()
    val errorsAndWarning = schemaHandler.checkValidity(config)
    errorsAndWarning match {
      case Failure(error) =>
        // scalastyle:off println
        println(error)
        error.printStackTrace()
      case Success((errorsAndWarning, errorCount, warningCount)) =>
        if (errorCount > 0) {
          // scalastyle:off println
          println(s"Found $errorCount errors")
        } else if (warningCount > 0) {
          // scalastyle:off println
          println(s"Found $warningCount warnings")
        } else
          // scalastyle:off println
          println("No errors or warnings found")
        errorsAndWarning.foreach { msg =>
          println(msg.toString())
        }
    }
    errorsAndWarning.map(_ => JobResult.empty)
  }

  def validateConnections()(implicit
    settings: Settings
  ) = {
    settings.appConfig.connections.keys.foreach { connectionName =>
      AutoTask
        .executeSelect(
          "__ignore__",
          "__ignore__",
          "SELECT 1",
          summarizeOnly = false,
          connectionName,
          None,
          test = false,
          parseSQL = false,
          pageSize = 200,
          pageNumber = 1,
          scheduledDate = None // No scheduled date for validate command
        )(
          settings,
          settings.storageHandler(),
          settings.schemaHandler()
        ) match {
        case Success(_) =>
          // scalastyle:off println
          println(s"SUCCESS: Connection $connectionName is valid")
        case Failure(exception) =>
          // scalastyle:off println
          println(s"ERROR: Could not connect to database using connection $connectionName ")
      }
    }
  }
}
