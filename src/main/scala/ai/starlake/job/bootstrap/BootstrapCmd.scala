package ai.starlake.job.bootstrap

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, Utils}
import scopt.OParser

import scala.util.{Success, Try}

/** Command to create a new project optionally based on a specific template.
  *
  * Usage: starlake bootstrap [options]
  */
object BootstrapCmd extends Cmd[BootstrapConfig] {

  override def command: String = "bootstrap"

  override def pageDescription: String =
    "Create a new Starflow project from scratch or from a template such as quickstart or userguide with a single command."
  override def pageKeywords: Seq[String] =
    Seq("starlake bootstrap", "project setup", "quickstart", "project template")

  val parser: OParser[Unit, BootstrapConfig] = {
    val builder = OParser.builder[BootstrapConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Create a new Starflow project from a template. Generates the required directory structure and sample configuration files to get started quickly. See [Project Setup Guide](/guides/project-setup/starlake-project-setup).
          |
          |Create a new project optionally based on a specific template eq. quickstart / userguide
          |""".stripMargin
      ),
      builder
        .opt[Unit]("no-exit")
        .action((_, c) => c.copy(noExit = Some(true)))
        .text("Should we exit after project creation ?")
        .optional(),
      builder
        .opt[String]("template")
        .action((x, c) => c.copy(template = Some(x)))
        .text("Template to use to bootstrap project")
        .optional(),
      builder
        .opt[String]("project-location")
        .action((x, c) => c.copy(projectLocation = Some(x)))
        .text("Absolute path where the project should be created")
        .optional(),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[BootstrapConfig] =
    OParser.parse(parser, args, BootstrapConfig(), setup)

  override def run(config: BootstrapConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Bootstrap.bootstrap(config.template, projectLocation = config.projectLocation)
    Utils.printOut(s"Project created based on ${config.template.getOrElse("default")} template")
    Success(JobResult.empty)
  }
}
