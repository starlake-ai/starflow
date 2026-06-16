package ai.starlake.extract.impl.restapi

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.ExtractPathHelper
import ai.starlake.job.{Cmd, ReportFormatConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, YamlSerde}
import ai.starlake.utils.Formatter.*
import org.apache.hadoop.fs.Path
import scopt.OParser

import scala.util.Try

/** CLI config for REST API data extraction */
case class UserExtractRestAPIDataConfig(
  extractConfig: String = "",
  outputDir: Option[String] = None,
  limit: Int = 0,
  parallelism: Option[Int] = None,
  incremental: Boolean = false,
  resume: Boolean = false,
  outputFormat: String = "csv",
  reportFormat: Option[String] = None
) extends ReportFormatConfig

/** Command to extract data from REST API endpoints.
  *
  * Usage: starlake extract-rest-data [options]
  */
object ExtractRestAPIDataCmd extends Cmd[UserExtractRestAPIDataConfig] with ExtractPathHelper {

  val command = "extract-rest-data"

  override def pageDescription: String =
    "Extract data from REST API endpoints into CSV files with support for pagination, authentication, and rate limiting."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract-rest-data", "REST API", "data extraction", "API data")

  val parser: OParser[Unit, UserExtractRestAPIDataConfig] = {
    val builder = OParser.builder[UserExtractRestAPIDataConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Extract data from REST API endpoints into CSV files. Supports pagination (offset, cursor,
          |link header, page number), authentication (bearer, API key, basic, OAuth2), rate limiting,
          |and parent-child endpoint relationships.
          |
          |The extracted CSV files can then be ingested using `starlake load`.
          |
          |Examples
          |========
          |
          |  starlake.sh extract-rest-data --config my-rest-api --outputDir /tmp/api-data
          |  starlake.sh extract-rest-data --config my-rest-api --outputDir /tmp/api-data --limit 1000
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
        .required()
        .text("Where to output CSV files"),
      builder
        .opt[Int]("limit")
        .action((x, c) => c.copy(limit = x))
        .optional()
        .text("Limit number of records per endpoint"),
      builder
        .opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"Parallelism level for endpoint extraction. Default: ${Runtime.getRuntime.availableProcessors()}"
        ),
      builder
        .opt[Unit]("incremental")
        .action((_, c) => c.copy(incremental = true))
        .optional()
        .text(
          "Only extract new data since last extraction. Uses incrementalField from endpoint config."
        ),
      builder
        .opt[Unit]("resume")
        .action((_, c) => c.copy(resume = true))
        .optional()
        .text(
          "Resume extraction from where a previous run failed, skipping already-extracted pages."
        ),
      builder
        .opt[String]("outputFormat")
        .action((x, c) => c.copy(outputFormat = x))
        .optional()
        .text("Output format: csv (default) or jsonl (JSON Lines, preserves nested structures)"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[UserExtractRestAPIDataConfig] =
    OParser.parse(parser, args, UserExtractRestAPIDataConfig(), setup)

  override def run(config: UserExtractRestAPIDataConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val extractConfigPath = mappingPath(DatasetArea.extract, config.extractConfig)
    val content = settings
      .storageHandler()
      .read(extractConfigPath)
      .richFormat(schemaHandler.activeEnvVars(), Map.empty)
    val extractSchemas =
      YamlSerde.deserializeYamlExtractConfig(content, config.extractConfig)

    extractSchemas.restAPI match {
      case Some(restAPIConfig) =>
        val outputDir = new Path(config.outputDir.getOrElse(DatasetArea.extract.toString))
        val dataExtractConfig = RestAPIDataExtractConfig(
          extractConfig = restAPIConfig,
          baseOutputDir = outputDir,
          limit = config.limit,
          parallelism = config.parallelism,
          incremental = config.incremental,
          resume = config.resume,
          outputFormat = RestAPIOutputFormat.fromString(config.outputFormat)
        )
        new RestAPIDataExtractor().run(dataExtractConfig).map(_ => JobResult.empty)
      case None =>
        throw new IllegalArgumentException(
          "No restAPI configuration found in the extract config file"
        )
    }
  }
}
