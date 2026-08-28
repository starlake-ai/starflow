package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer}
import better.files.File
import scopt.OParser

import scala.util.Try

/** Command to generate task dependencies graph (lineage).
  *
  * Usage: starlake lineage [options]
  */
object AutoTaskDependenciesCmd extends Cmd[AutoTaskDependenciesConfig] {

  val command = "lineage"

  override def pageDescription: String =
    "Generate task dependency graphs as DOT, SVG, PNG or JSON to visualize data lineage across your Starflow project."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake lineage",
      "data lineage",
      "dependency graph",
      "task dependencies",
      "DAG visualization"
    )

  val parser: OParser[Unit, AutoTaskDependenciesConfig] = {
    val builder = OParser.builder[AutoTaskDependenciesConfig]
    import builder._
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      note(
        """Generate a task dependency graph showing how transformation tasks relate to each other.
          |
          |Generate Task dependencies graph""".stripMargin
      ),
      opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      opt[Seq[String]]("task")
        .action((x, c) => c.copy(tasks = Some(x)))
        .optional()
        .text("Compute dependencies of these tasks only. If not specified, compute all jobs."),
      opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      opt[Unit]("viz")
        .action((_, c) => c.copy(viz = true))
        .optional()
        .text("Should we generate a dot file ?"),
      opt[Unit]("svg")
        .action((_, c) => c.copy(svg = true, viz = true))
        .optional()
        .text(
          "Should we generate SVG files ?"
        ),
      opt[Unit]("json")
        .action((_, c) => c.copy(json = true))
        .optional()
        .text(
          "Should we generate JSON files ?"
        ),
      opt[Unit]("png")
        .action((_, c) => c.copy(png = true, viz = true))
        .optional()
        .text(
          "Should we generate PNG files ?"
        ),
      opt[Unit]("print")
        .action((_, c) => c.copy(print = true))
        .optional()
        .text("Print dependencies as text"),
      opt[Seq[String]]("objects")
        .action((x, c) => c.copy(objects = x))
        .optional()
        .text("comma separated list of objects to display: task, table, view, unknown"),
      opt[Unit]("all")
        .action { (_, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all tasks  in the dot file ? None by default"
        ),
      opt[Unit]("verbose")
        .action { (_, c) =>
          c.copy(verbose = true)
        }
        .optional()
        .text(
          "Add extra table properties"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[AutoTaskDependenciesConfig] =
    OParser.parse(parser, args, AutoTaskDependenciesConfig(), setup)

  override def run(config: AutoTaskDependenciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Try {
      val autoTaskDependencies =
        new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
      val allDependencies: List[DependencyContext] = autoTaskDependencies.tasks(config)
      if (config.json) {
        val diagram = autoTaskDependencies.jobAsDiagram(allDependencies, config)
        if (config.print) {
          val data =
            JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(diagram)
          println(data)
        }
      } else {
        if (config.print) autoTaskDependencies.jobsDependencyTree(allDependencies, config)
        if (config.viz) autoTaskDependencies.jobAsDot(allDependencies, config)
      }
    }.map(_ => JobResult.empty)
  }
}
