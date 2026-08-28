package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import org.joda.time.DateTime
import scopt.OParser

import scala.util.Try

/** Command to extract data from a database.
  *
  * Usage: starlake extract-data [options]
  */
trait ExtractDataCmd extends Cmd[UserExtractDataConfig] {

  val command = "extract-data"

  override def pageDescription: String =
    "Extract data from any database in parallel with support for partitioning, incremental exports, and configurable parallelism."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract-data", "data extraction", "database export", "parallel extraction")

  val parser: OParser[Unit, UserExtractDataConfig] = {
    val builder = OParser.builder[UserExtractDataConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Extract data from a JDBC database into CSV or Parquet files. Supports incremental extraction, parallel partitioning, and selective table filtering for efficient large-scale data exports. See [Extract Tutorial](/starflow/guides/extract/tutorial).
          |
          |Extract data from any database defined in mapping file.
          |
          |Extraction is done in parallel by default and use all the available processors. It can be changed using `parallelism` CLI config.
          |Extraction of a table can be divided in smaller chunk and fetched in parallel by defining partitionColumn and its numPartitions.
          |
          |Examples
          |========
          |
          |Objective: Extract data
          |
          |  starlake.sh extract-data --config my-config --outputDir $PWD/output
          |
          |Objective: Plan to fetch all data but with different scheduling (once a day for all and twice a day for some) with failure recovery like behavior.
          |  starlake.sh extract-data --config my-config --outputDir $PWD/output --includeSchemas aSchema
          |         --includeTables table1RefreshedTwiceADay,table2RefreshedTwiceADay --ifExtractedBefore "2023-04-21 12:00:00"
          |         --clean
          |
          |""".stripMargin
      ),
      builder
        .opt[String]("config")
        .action((x, c) => c.copy(extractConfig = x))
        .required()
        .text("Database tables & connection info"),
      builder
        .opt[Int]("limit")
        .action((x, c) => c.copy(limit = x))
        .optional()
        .text("Limit number of records"),
      builder
        .opt[Int]("numPartitions")
        .action((x, c) => c.copy(numPartitions = x))
        .optional()
        .text("parallelism level regarding partitionned tables"),
      builder
        .opt[Int]("parallelism")
        .action((x, c) => c.copy(parallelism = Some(x)))
        .optional()
        .text(
          s"parallelism level of the extraction process. By default equals to the available cores: ${Runtime.getRuntime.availableProcessors()}"
        ),
      builder
        .opt[Unit]("ignoreExtractionFailure")
        .action((_, c) => c.copy(ignoreExtractionFailure = true))
        .optional()
        .text("Don't fail extraction job when any extraction fails."),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(cleanOnExtract = true))
        .optional()
        .text("Clean all files of table only when it is extracted."),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputDir = Some(x)))
        .required()
        .text("Where to output csv files"),
      builder
        .opt[Unit]("incremental")
        .action((_, c) => c.copy(fullExport = Some(false)))
        .optional()
        .text("Export only new data since last extraction."),
      builder
        .opt[String]("ifExtractedBefore")
        .action((x, c) => c.copy(ifExtractedBefore = Some(DateTime.parse(x).getMillis)))
        .optional()
        .text(
          "DateTime to compare with the last beginning extraction dateTime. If it is before that date, extraction is done else skipped."
        ),
      builder
        .opt[Seq[String]]("includeSchemas")
        .action((x, c) => c.copy(includeSchemas = x.map(_.trim)))
        .valueName("schema1,schema2")
        .optional()
        .text("Domains to include during extraction."),
      builder
        .opt[Seq[String]]("excludeSchemas")
        .valueName("schema1,schema2...")
        .optional()
        .action((x, c) => c.copy(excludeSchemas = x.map(_.trim)))
        .text(
          "Domains to exclude during extraction. if `include-domains` is defined, this config is ignored."
        ),
      builder
        .opt[Seq[String]]("includeTables")
        .valueName("table1,table2,table3...")
        .optional()
        .action((x, c) => c.copy(includeTables = x.map(_.trim)))
        .text("Schemas to include during extraction."),
      builder
        .opt[Seq[String]]("excludeTables")
        .valueName("table1,table2,table3...")
        .optional()
        .action((x, c) => c.copy(excludeTables = x.map(_.trim)))
        .text(
          "Schemas to exclude during extraction. if `include-schemas` is defined, this config is ignored."
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x)),
      builder.checkConfig { c =>
        val domainChecks = (c.excludeSchemas, c.includeSchemas) match {
          case (Nil, Nil) | (_, Nil) | (Nil, _) => Nil
          case _ => List("You can't specify includeSchemas and excludeSchemas at the same time.")
        }
        val schemaChecks = (c.excludeTables, c.includeTables) match {
          case (Nil, Nil) | (_, Nil) | (Nil, _) => Nil
          case _ => List("You can't specify includeTables and excludeTables at the same time.")
        }
        val allErrors = domainChecks ++ schemaChecks
        if (allErrors.isEmpty) {
          builder.success
        } else {
          builder.failure(allErrors.mkString("\n"))
        }
      }
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class JDBC2YmlConfig.
    */
  def parse(args: Seq[String]): Option[UserExtractDataConfig] = {
    OParser.parse(parser, args, UserExtractDataConfig(), setup)
  }

  override def run(config: UserExtractDataConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try(new ExtractDataJob(schemaHandler).run(config)).map(_ => JobResult.empty)
}

object ExtractDataCmd extends ExtractDataCmd
