package ai.starlake.job

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{CliConfig, JobResult, RedactedToString}
import ai.starlake.workflow.IngestionWorkflow
import scopt.{OParser, OParserBuilder}

import scala.util.{Failure, Try}

trait ReportFormatConfig extends RedactedToString {
  def reportFormat: Option[String]
}

trait Cmd[T <: ReportFormatConfig] extends CliConfig[T] {

  val shell: String = Main.shell

  final def run(
    args: Seq[String],
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Try[JobResult] = {
    parse(args) match {
      case Some(config) => run(config, schemaHandler)
      case None =>
        Failure(new IllegalArgumentException(usage()))
    }
  }

  def run(config: T, schemaHandler: SchemaHandler)(implicit settings: Settings): Try[JobResult]

  def workflow(schemaHandler: SchemaHandler)(implicit settings: Settings): IngestionWorkflow =
    new IngestionWorkflow(settings.storageHandler(), schemaHandler)

  def reportFormat(config: T): Option[String] = config.reportFormat

  def reportFormatOption(builder: OParserBuilder[T])(
    action: (T, Option[String]) => T
  ): OParser[String, T] = {
    builder
      .opt[String]("reportFormat")
      .action((x, c) => action(c, Some(x)))
      .text("Report format: console, json, html")
      .optional()
  }
}
