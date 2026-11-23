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
      case None         => onParseFailure()
    }
  }

  /** What a command returns when its arguments do not parse. Main reports a Failure with an
    * IllegalArgumentException as a usage error (exit 1); a command that needs another exit code
    * returns an ExitCodeJobResult instead.
    */
  def onParseFailure(): Try[JobResult] = Failure(new IllegalArgumentException(usage()))

  /** A read-only command neither writes to the project nor prints anything but its own output: Main
    * skips the creation of the metadata folders and the validate-on-load check for it.
    */
  def readOnly: Boolean = false

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
