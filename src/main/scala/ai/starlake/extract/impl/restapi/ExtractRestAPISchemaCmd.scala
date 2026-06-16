package ai.starlake.extract.impl.restapi

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractPathHelper, ExtractSchemaConfig}
import ai.starlake.extract.spi.SchemaExtractorWorkflow
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, YamlSerde}
import ai.starlake.utils.Formatter.*
import scopt.OParser

import scala.util.{Success, Try}

/** Command to extract schema from REST API endpoints.
  *
  * Usage: starlake extract-rest-schema [options]
  */
object ExtractRestAPISchemaCmd extends Cmd[ExtractSchemaConfig] with ExtractPathHelper {

  val command = "extract-rest-schema"

  override def pageDescription: String =
    "Extract schemas from REST API endpoints by fetching sample data and inferring the structure."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract-rest-schema", "REST API", "schema extraction", "API schema")

  val parser: OParser[Unit, ExtractSchemaConfig] = {
    val builder = OParser.builder[ExtractSchemaConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Extract schemas from REST API endpoints by fetching sample responses and inferring
          |the data structure. Generates Starlake YAML configuration files (domain + table definitions)
          |that can be used for subsequent data extraction and ingestion.
          |
          |Examples
          |========
          |
          |  starlake.sh extract-rest-schema --config my-rest-api
          |  starlake.sh extract-rest-schema --config my-rest-api --outputDir /tmp/schemas
          |
          |""".stripMargin
      ),
      builder
        .opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("REST API extraction config file (in metadata/extract/)"),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Where to output YAML files"),
      builder
        .opt[String]("connectionRef")
        .action((x, c) => c.copy(connectionRef = Some(x)))
        .optional()
        .text("Connection reference name"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[ExtractSchemaConfig] =
    OParser.parse(parser, args, ExtractSchemaConfig(), setup)

  override def run(config: ExtractSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val extractConfigPath = mappingPath(DatasetArea.extract, config.extractConfig)
    val content = settings
      .storageHandler()
      .read(extractConfigPath)
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val extractSchemas =
      YamlSerde.deserializeYamlExtractConfig(content, config.extractConfig)
    SchemaExtractorWorkflow.run(config, extractSchemas)
    Success(JobResult.empty)
  }
}
