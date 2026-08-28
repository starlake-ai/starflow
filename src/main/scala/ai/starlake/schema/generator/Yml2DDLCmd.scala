package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to generate DDL from YML.
  *
  * Usage: starlake yml2ddl [options]
  */
object Yml2DDLCmd extends Cmd[Yml2DDLConfig] {

  val command = "yml2ddl"

  override def pageDescription: String =
    "Generate DDL statements from Starflow YAML schemas for your target data warehouse, with optional JDBC apply support."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake yml2ddl",
      "DDL generation",
      "schema to SQL",
      "data warehouse",
      "database migration"
    )

  val parser: OParser[Unit, Yml2DDLConfig] = {
    val builder = OParser.builder[Yml2DDLConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Generate SQL DDL statements (CREATE TABLE, etc.) from Starflow YAML table definitions."""
      ),
      builder
        .opt[String]("datawarehouse")
        .action((x, c) => c.copy(datawarehouse = x))
        .required()
        .text("target datawarehouse name (ddl mapping key in types.yml"),
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .optional()
        .text("JDBC connection name with at least read write on database schema"),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .optional()
        .text("Where to output the generated files. ./$datawarehouse/ by default"),
      builder
        .opt[String]("catalog")
        .action((x, c) => c.copy(catalog = Some(x)))
        .optional()
        .text("Database Catalog if any"),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = Some(x)))
        .optional()
        .text("Domain to create DDL for. All by default")
        .children(
          builder
            .opt[Seq[String]]("schemas")
            .action((x, c) => c.copy(schemas = Some(x)))
            .optional()
            .text("List of schemas to generate DDL for. All by default")
        ),
      builder
        .opt[Unit]("apply")
        .action((_, c) => c.copy(apply = true))
        .optional()
        .text("Does the file contain a header (For CSV files only)"),
      builder
        .opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level. By default equals to the available cores: ${Runtime.getRuntime.availableProcessors()}"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferDDLConfig.
    */
  def parse(args: Seq[String]): Option[Yml2DDLConfig] =
    OParser.parse(parser, args, Yml2DDLConfig(), setup)

  override def run(config: Yml2DDLConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).inferDDL(config).map(_ => JobResult.empty)
}
