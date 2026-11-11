package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

/** Command to generate ACL dependencies graph.
  *
  * Usage: starlake acl-dependencies [options]
  */
object AclDependenciesCmd extends Cmd[AclDependenciesConfig] {

  val command = "acl-dependencies"

  override def pageDescription: String =
    "Generate DOT or JSON files visualizing ACL dependencies from Domain and Schema YAML definitions."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake acl-dependencies",
      "ACL dependencies",
      "DOT",
      "access control",
      "data governance"
    )

  val parser: OParser[Unit, AclDependenciesConfig] = {
    val builder = OParser.builder[AclDependenciesConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Generate an ACL (Access Control List) dependency graph showing which roles and permissions apply across tables and domains. Use this command to visualize access control relationships as DOT or JSON files.
          |
          |Generate DOT or JSON files from Domain / Schema YAML files""".stripMargin
      ),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(outputFile = Some(File(x))))
        .optional()
        .text("Where to save the generated dot file ? Output to the console by default"),
      builder
        .opt[Seq[String]]("grantees")
        .action { (x, c) =>
          c.copy(grantees = x.toList)
        }
        .optional()
        .text(
          "Which users should we include in the dot file ? All by default"
        ),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text(
          "Should we reload the domains first ?"
        ),
      builder
        .opt[Unit]("json")
        .action((_, c) => c.copy(json = true))
        .optional()
        .text(
          "Should we generate JSON files ?"
        ),
      builder
        .opt[Seq[String]]("tables")
        .action { (x, c) =>
          c.copy(tables = x.toList)
        }
        .optional()
        .text(
          "Which tables should we include in the dot file ? All by default"
        ),
      builder
        .opt[Unit]("all")
        .action { (x, c) =>
          c.copy(all = true)
        }
        .optional()
        .text(
          "Include all ACL in the dot file ? None by default"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[AclDependenciesConfig] =
    OParser.parse(parser, args, AclDependenciesConfig(), setup)

  override def run(config: AclDependenciesConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try {
      val aclDeps = new AclDependencies(schemaHandler)
      if (config.json)
        aclDeps.aclsAsDiagram(config)
      else
        aclDeps.aclsAsDotFile(config)
    }.map(_ => JobResult.empty)
}
