package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{DagGenerateJobResult, JobResult}
import scopt.OParser

import scala.util.Try

/** Command to generate DAGs.
  *
  * Usage: starlake dag-generate [options]
  */
object DagGenerateCmd extends Cmd[DagGenerateConfig] {
  val command = "dag-generate"

  override def pageDescription: String =
    "Generate DAG files for tasks and domains with optional tag filtering and role definitions for workflow orchestration tools."
  override def pageKeywords: Seq[String] =
    Seq("starlake dag-generate", "DAG generation", "Airflow", "workflow orchestration")

  val parser: OParser[Unit, DagGenerateConfig] = {
    val builder = OParser.builder[DagGenerateConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Generate Airflow or Dagster DAG files from Starflow task and domain definitions. The command analyzes SQL dependencies to produce correctly ordered workflow graphs, with optional tag filtering. See [Orchestration Guide](/guides/orchestrate/tutorial)."""
      ),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting DAG file(s).""".stripMargin
        ),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(clean = true))
        .optional()
        .text(
          """Clean Resulting DAG file output first ?""".stripMargin
        ),
      builder
        .opt[Seq[String]]("tags")
        .action((x, c) => c.copy(tags = x))
        .optional()
        .text(
          """Generate for these tags only""".stripMargin
        ),
      builder
        .opt[Unit]("tasks")
        .action((_, c) => c.copy(tasks = true))
        .optional()
        .text(
          """Whether to generate DAG file(s) for tasks or not""".stripMargin
        ),
      builder
        .opt[Unit]("domains")
        .action((_, c) => c.copy(domains = true))
        .optional()
        .text(
          """Whether to generate DAG file(s) for domains or not""".stripMargin
        ),
      builder
        .opt[Unit]("withRoles")
        .action((_, c) => c.copy(includeRoles = true))
        .optional()
        .text(
          """Generate role definitions""".stripMargin
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[DagGenerateConfig] =
    OParser.parse(parser, args, DagGenerateConfig(), setup)

  override def run(config: DagGenerateConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val result = Try {
      val cmd = new DagGenerateJob(schemaHandler)
      if (config.clean) {
        cmd.clean(config.outputDir)
      }
      if (config.domains) {
        cmd.generateDomainDags(config)
      }
      if (config.tasks) {
        cmd.generateTaskDags(config)
      }
      if (!config.tasks && !config.domains) {
        cmd.generateDomainDags(config)
        cmd.generateTaskDags(config)
      }
      val files = cmd.normalizeDagNames(config)
      DagGenerateJobResult(files)
    }
    result
  }
}
