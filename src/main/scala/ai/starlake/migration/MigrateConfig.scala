package ai.starlake.migration

import ai.starlake.job.ReportFormatConfig
import ai.starlake.utils.CliConfig
import scopt.OParser

case class MigrateConfig(
  template: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig

object MigrateConfig extends CliConfig[MigrateConfig] {
  val command = "migrate"
  val parser: OParser[Unit, MigrateConfig] = {
    val builder = OParser.builder[MigrateConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(
        """Migrate the Starflow project configuration files to the latest version format."""
      ),
      opt[String]("reportFormat")
        .action((x, c) => c.copy(reportFormat = Some(x)))
        .text("Report format: console, json, html")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[MigrateConfig] =
    OParser.parse(parser, args, MigrateConfig(), setup)

}
