package ai.starlake.dbt

import ai.starlake.config.Settings
import ai.starlake.job.{Cmd, ReportFormatConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

final case class DbtImportConfig(
  action: String = "",
  manifest: String = "",
  profiles: Option[String] = None,
  target: Option[String] = None,
  connection: Option[String] = None,
  vars: Option[String] = None,
  force: Boolean = false,
  introspect: Boolean = false,
  introspectMaxRows: Int = 10000,
  reportFormat: Option[String] = None
) extends ReportFormatConfig

/** `starlake dbt import`: Main finds commands by their first word, `dbt`, and `import` is the
  * action argument, as `starlake quack serve` is
  */
object DbtImportCmd extends Cmd[DbtImportConfig] {
  val command = "dbt"

  override def pageDescription: String =
    "Convert a dbt project into native Starlake YAML and SQL, once, from its manifest.json: models, seeds, sources, tests and docs."
  override def pageKeywords: Seq[String] =
    Seq("starlake dbt import", "dbt migration", "migrate from dbt", "dbt manifest")

  val parser: OParser[Unit, DbtImportConfig] = {
    val builder = OParser.builder[DbtImportConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "import", "[options]"),
      builder.note(
        """Convert a dbt project into the current Starlake project, from target/manifest.json (run `dbt deps && dbt parse` first).
          |Every macro is expanded; incremental models keep their two versions as `{% if sl_incremental %}`.
          |Writes dbt-import-report.md at the project root. Exit codes: 0 imported, 1 usage error, unreadable manifest, or existing files without --force.
          |example: starlake dbt import --manifest ../shop/target/manifest.json --profiles ~/.dbt/profiles.yml --target dev""".stripMargin
      ),
      builder
        .arg[String]("action")
        .required()
        .validate(a =>
          if (a == "import") Right(())
          else Left(s"unknown dbt action $a: only import")
        )
        .action((x, c) => c.copy(action = x))
        .text("import"),
      builder
        .opt[String]("manifest")
        .required()
        .action((x, c) => c.copy(manifest = x))
        .text("dbt's target/manifest.json"),
      builder
        .opt[String]("profiles")
        .optional()
        .action((x, c) => c.copy(profiles = Some(x)))
        .text(
          "dbt profiles.yml: the target renders target.*, and a DuckDB target becomes a connection"
        ),
      builder
        .opt[String]("target")
        .optional()
        .action((x, c) => c.copy(target = Some(x)))
        .text(
          "dbt target: target.name, and the output of --profiles (default: the profile's target, else dev)"
        ),
      builder
        .opt[String]("connection")
        .optional()
        .action((x, c) => c.copy(connection = Some(x)))
        .text("existing Starlake connection the imported objects run on"),
      builder
        .opt[String]("vars")
        .optional()
        .action((x, c) => c.copy(vars = Some(x)))
        .text("dbt --vars, a YAML or JSON map, over the vars of dbt_project.yml"),
      builder
        .opt[Unit]("force")
        .optional()
        .action((_, c) => c.copy(force = true))
        .text("overwrite the files the import writes; nothing is deleted"),
      builder
        .opt[Unit]("introspect")
        .optional()
        .action((_, c) => c.copy(introspect = true))
        .text(
          "answer the macros that read the warehouse (run_query, statement, adapter.get_columns_in_relation...) on the import's connection; read-only statements only"
        ),
      builder
        .opt[Int]("introspect-max-rows")
        .optional()
        .action((x, c) => c.copy(introspectMaxRows = x))
        .text(
          "with --introspect, the most rows a query may return before its model is skipped (default 10000)"
        )
    )
  }

  override def parse(args: Seq[String]): Option[DbtImportConfig] =
    OParser.parse(parser, args, DbtImportConfig(), setup)

  override def run(config: DbtImportConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = DbtImporter.run(config)
}
