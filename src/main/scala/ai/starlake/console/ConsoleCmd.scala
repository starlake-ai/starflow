package ai.starlake.console

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to start the Starflow console.
  *
  * Usage: starlake console [options]
  */
object ConsoleCmd extends Cmd[ConsoleConfig] {
  override def command: String = "console"

  override def pageDescription: String =
    "Launch the Starflow interactive console for exploring and managing your data project from a local web-based interface."
  override def pageKeywords: Seq[String] =
    Seq("starlake console", "interactive console", "web interface", "data exploration")

  val parser: OParser[Unit, ConsoleConfig] = {
    val builder = OParser.builder[ConsoleConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Start an interactive Starflow console for exploring metadata and running commands from a local web-based interface. Use this to browse domains, tables, and task definitions in your project."""
      ),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = c.options ++ x))
        .unbounded()
        .text("Options(ignored)"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  override def parse(args: Seq[String]): Option[ConsoleConfig] =
    OParser.parse(parser, args, ConsoleConfig(), setup)

  override def run(config: ConsoleConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    new Console().console()
    Try(JobResult.empty)
  }
}
