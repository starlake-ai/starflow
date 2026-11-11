package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

/** Command to generate table dependencies graph.
  *
  * Usage: starlake table-dependencies [options]
  */
object TableDependenciesCmd extends Cmd[TableDependenciesConfig] {

  val command = "table-dependencies"

  override def pageDescription: String =
    "Generate table dependency diagrams from domain and schema YAML files, as DOT or JSON."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake table-dependencies",
      "schema dependencies",
      "DOT",
      "entity relationship",
      "data modeling"
    )

  val parser: OParser[Unit, TableDependenciesConfig] = {
    val builder = OParser.builder[TableDependenciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Generate a table dependency graph showing foreign key and lineage relationships between tables.
          |
          |Generate DOT or JSON files from Domain / Schema YAML files""".stripMargin
      ),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      builder
        .opt[Unit]("all-attrs")
        .action((x, c) => c.copy(includeAllAttributes = true))
        .optional()
        .text(
          "Should we include all attributes in the dot file or only the primary and foreign keys ? true by default"
        ),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      builder
        .opt[Unit]("related")
        .action((_, c) => c.copy(related = true))
        .optional()
        .text(
          "Should we include only entities with relations to others ? false by default"
        ),
      builder
        .opt[Seq[String]]("tables")
        .action((x, c) => c.copy(tables = Some(x)))
        .optional()
        .text(
          "Which tables should we include in the dot file ?"
        ),
      builder
        .opt[Unit]("all")
        .action { (_, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all tables in the dot file ? All by default"
        ),
      builder
        .opt[Unit]("json")
        .action { (_, c) =>
          c.copy(json = true)
        }
        .optional()
        .text(
          "JSON output ?"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[TableDependenciesConfig] =
    OParser.parse(parser, args, TableDependenciesConfig(), setup)

  override def run(config: TableDependenciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try {
      if (config.json)
        new TableDependencies(schemaHandler).relationsAsDiagram(config)
      else
        new TableDependencies(schemaHandler).relationsAsDotFile(config)

    }.map(_ => JobResult.empty)
}
