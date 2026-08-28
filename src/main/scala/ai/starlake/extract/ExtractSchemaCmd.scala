package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.{Success, Try}

/** Command to extract schema from a database.
  *
  * Usage: starlake extract-schema [options]
  */
object ExtractSchemaCmd extends Cmd[ExtractSchemaConfig] {

  val command = "extract-schema"

  override def pageDescription: String =
    "Extract database table schemas and output them as YAML definition files with optional snake_case naming and parallelism control."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract-schema", "schema extraction", "database schema", "YAML generation")

  val parser: OParser[Unit, ExtractSchemaConfig] = {
    val builder = OParser.builder[ExtractSchemaConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Extract table schemas from a JDBC database and generate Starflow YAML configuration files. Use this to reverse-engineer existing database tables into domain definitions with optional snake_case naming. See [Extract Tutorial](/starflow/guides/extract/tutorial)."""
      ),
      builder
        .opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .optional()
        .text("Database tables & connection info"),
      builder
        .opt[Unit]("all")
        .action((x, c) => c.copy(all = true, external = true))
        .optional()
        .text("Should we extract all schemas and tables to external folder ?"),
      builder
        .opt[Seq[String]]("tables")
        .action((x, c) => c.copy(tables = x))
        .optional()
        .text("Database tables info"),
      builder
        .opt[String]("connectionRef")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .optional()
        .text("Database connection to use"),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to output YML files"),
      builder
        .opt[Unit]("external")
        .action((x, c) => c.copy(external = true))
        .optional()
        .text("Should we output YML files in the external folder"),
      builder
        .opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level of the extraction process. By default equals to the available cores"
        ),
      builder
        .opt[Unit]("snakecase")
        .action((_, c) => c.copy(snakeCase = true))
        .optional()
        .text(
          """Apply snake case when name sanitization is done""".stripMargin
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[ExtractSchemaConfig] =
    OParser.parse(parser, args, ExtractSchemaConfig(), setup)

  override def run(config: ExtractSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    new ExtractSchema(schemaHandler).run(config)
    Success(JobResult.empty)
  }
}
