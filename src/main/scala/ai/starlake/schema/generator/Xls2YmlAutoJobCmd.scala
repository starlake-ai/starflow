package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult

import scala.util.{Success, Try}

/** Command to convert XLS to YML job.
  *
  * Usage: starlake xls2ymljob [options]
  */
object Xls2YmlAutoJobCmd extends Xls2YmlCmd {

  override def command: String = "xls2ymljob"

  override def pageDescription: String =
    "Convert Excel files describing transform job definitions into Starflow YAML task configuration files."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake xls2ymljob",
      "Excel to YAML",
      "job generation",
      "task configuration",
      "transform definition"
    )

  override def run(config: Xls2YmlConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    Xls2YmlAutoJob.generateSchema(config.files.head, config.policyFile, config.outputPath)
    Success(JobResult.empty)
  }
}
