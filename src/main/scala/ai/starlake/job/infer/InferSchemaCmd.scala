package ai.starlake.job.infer

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, WriteMode}
import ai.starlake.utils.{JobResult, Utils}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import scopt.OParser

import java.nio.charset.Charset
import scala.util.{Failure, Success, Try}

/** Command to infer schema from a file.
  *
  * Usage: starlake infer-schema [options]
  */
object InferSchemaCmd extends Cmd[InferSchemaConfig] with LazyLogging {

  val command = "infer-schema"

  override def pageDescription: String =
    "Automatically infer and generate YAML schema definitions from input datasets in CSV, JSON, XML, or Parquet format."
  override def pageKeywords: Seq[String] =
    Seq("starlake infer-schema", "schema inference", "YAML generation", "data discovery")

  val parser: OParser[Unit, InferSchemaConfig] = {
    val builder = OParser.builder[InferSchemaConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Infer a Starflow table schema from a sample data file (CSV, JSON, etc.) and generate the corresponding YAML configuration. See [Load Tutorial](/guides/load/tutorial)."""
      ),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domainName = x))
        .text("Domain Name"),
      builder
        .opt[String]("table")
        .action((x, c) => c.copy(schemaName = x))
        .text("Table Name"),
      builder
        .opt[String]("input")
        .action((x, c) => c.copy(inputPath = x))
        .required()
        .text("Dataset Input Path"),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .optional()
        .text("Domain YAML Output Path"),
      builder
        .opt[String]("write")
        .action((x, c) => c.copy(write = Some(WriteMode.fromString(x))))
        .text(s"One of ${WriteMode.writes}")
        .optional(),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = Some(Format.fromString(x))))
        .optional()
        .text("Force input file format"),
      builder
        .opt[String]("rowTag")
        .action((x, c) => c.copy(rowTag = Some(x)))
        .optional()
        .text("row tag to use if detected format is XML"),
      builder
        .opt[Unit]("variant")
        .action((x, c) => c.copy(variant = Some(true)))
        .optional()
        .text("Infer schema as a single variant attribute"),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(clean = true))
        .optional()
        .text("Delete previous YML before writing"),
      builder
        .opt[String]("encoding")
        .action((x, c) => c.copy(encoding = Charset.forName(x)))
        .optional()
        .text("Input file encoding. Default to UTF-8"),
      builder
        .opt[Unit]("from-json-schema")
        .action((_, c) => c.copy(fromJsonSchema = true))
        .optional()
        .text("Input file is a valid JSON Schema"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class InferSchemaConfig.
    */
  def parse(args: Seq[String]): Option[InferSchemaConfig] =
    OParser.parse(parser, args, InferSchemaConfig(), setup)

  override def run(config: InferSchemaConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val inputFile = File(config.inputPath)
    val inputPaths =
      if (inputFile.isDirectory()) {
        inputFile.list.map(_.pathAsString).toList.filter(!_.startsWith(".")) // filter hidden files
      } else {
        List(config.inputPath)
      }

    val results = inputPaths.map { inputPath =>
      logger.info(s"Inferring schema for $inputPath")
      workflow(schemaHandler)
        .inferSchema(
          config.copy(inputPath = inputPath)
        ) match {
        case Success(result) =>
          val message =
            if (result.skipped)
              s"Skipped inference for ${result.domainName}.${result.tableName}: " +
              "definition already exists (use --clean to overwrite)"
            else
              s"Successfully inferred schema for $inputPath"
          logger.info(message)
          Utils.printOut(message)
          Success(JobResult.empty)
        case Failure(exception) =>
          logger.error(s"Failed to infer schema for $inputPath", exception)
          Failure(exception)
      }
    }
    val failures = results.filter(_.isFailure).map {
      case Failure(exception) => exception.getMessage
      case _                  => throw new IllegalStateException("This should never happen")
    }

    if (failures.isEmpty) {
      Success(JobResult.empty)
    } else {
      Failure(new Exception(failures.mkString("\n")))
    }
  }
}
