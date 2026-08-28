package ai.starlake.config

import ai.starlake.job.Cmd
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, Utils}
import scopt.OParser

import scala.util.{Failure, Success, Try}

/** Command to print settings or test a connection.
  *
  * Usage: starlake settings [options]
  */
trait SettingsCmd extends Cmd[SettingsConfig] {

  def command = "settings"

  override def pageDescription: String =
    "Display and validate Starflow project settings, including testing database and warehouse connection configurations."
  override def pageKeywords: Seq[String] =
    Seq("starlake settings", "project configuration", "connection test", "data warehouse setup")

  val parser: OParser[Unit, SettingsConfig] = {
    val builder = OParser.builder[SettingsConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Print the resolved Starflow settings or test a database connection."""
      ),
      builder
        .opt[String]("test-connection")
        .action((x, c) => c.copy(testConnection = Some(x)))
        .optional()
        .text("Test this connection"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[SettingsConfig] =
    OParser.parse(
      parser,
      args,
      SettingsConfig(testConnection = None)
    )

  override def run(config: SettingsConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    config.testConnection match {
      case Some(connectionName) =>
        settings.appConfig.connections.get(connectionName) match {
          case Some(connection) =>
            val checkResult = connection.checkValidity(connectionName)
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
            Success(JobResult.empty)
          case None =>
            Utils.printOut(s"Connection $connectionName does not exist")
            Failure(new Exception(s"Connection $connectionName does not exist"))
        }
      case None =>
        Success(JobResult.empty)
    }
  }
}

object SettingsCmd extends SettingsCmd
