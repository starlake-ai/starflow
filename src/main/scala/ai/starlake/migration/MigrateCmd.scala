package ai.starlake.migration

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to migrate the project to the latest version.
  *
  * Usage: starlake migrate [options]
  */
object MigrateCmd extends Cmd[MigrateConfig] {

  override def command: String = "migrate"

  override def pageDescription: String =
    "Migrate your Starflow project to the latest version, with warnings for breaking changes that require manual attention."
  override def pageKeywords: Seq[String] =
    Seq("starlake migrate", "project migration", "version upgrade", "schema evolution")

  val parser: OParser[Unit, MigrateConfig] = MigrateConfig.parser

  def parse(args: Seq[String]): Option[MigrateConfig] =
    OParser.parse(parser, args, MigrateConfig(), setup)

  override def run(config: MigrateConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    new ProjectMigrator().migrate()
  }
}
