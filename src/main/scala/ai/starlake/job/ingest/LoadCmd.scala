package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, JsonSerializer, SparkJobResult, Utils}
import scopt.OParser

import scala.util.Try

/** Command to load data into the data warehouse.
  *
  * Usage: starlake load [options]
  */
trait LoadCmd extends Cmd[LoadConfig] {

  def command = "load"

  override def pageDescription: String =
    "Ingest raw files into your data warehouse by watching specified domains and tables with schema validation and options."
  override def pageKeywords: Seq[String] =
    Seq("starlake load", "data ingestion", "file loading", "schema validation", "ETL")

  val parser: OParser[Unit, LoadConfig] = {
    val builder = OParser.builder[LoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Load data files from the pending directory into the data warehouse using schema definitions. See [Load Tutorial](/starflow/guides/load/tutorial)."""
      ),
      builder
        .opt[Seq[String]]("domains")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Domains to watch"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to watch"),
      builder
        .opt[Seq[String]]("include")
        .action((x, c) => c.copy(domains = x))
        .valueName("domain1,domain2...")
        .optional()
        .text("Deprecated: Domains to watch"),
      builder
        .opt[Seq[String]]("schemas")
        .valueName("schema1,schema2,schema3...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Deprecated: Schemas to watch"),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Watch arguments to be used as substitutions"),
      builder
        .opt[Unit]("test")
        .optional()
        .action((x, c) => c.copy(test = true))
        .text("Should we run this load as a test ? Default value is false"),
      builder
        .opt[Seq[String]]("files")
        .optional()
        .action((x, c) => c.copy(files = Some(x.toList)))
        .text("load this file only"),
      builder
        .opt[Seq[String]]("primaryKeys")
        .optional()
        .action((x, c) => c.copy(primaryKey = x.toList))
        .text("primary keys to set on the table schema"),
      builder
        .opt[String]("scheduledDate")
        .optional()
        .action((x, c) => c.copy(scheduledDate = Some(x.replace('T', ' ').replaceAll("'", ""))))
        .text("Scheduled date for the job, in format yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
      builder
        .opt[Unit]("inPlace")
        .optional()
        .action((_, c) => c.copy(inPlace = true))
        .text("Ingest files from their current location without moving them. Requires --files"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[LoadConfig] =
    OParser.parse(
      parser,
      args,
      LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
    )

  override def run(config: LoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val result = workflow(schemaHandler).load(config)
    result match {
      case scala.util.Success(sparkResult: SparkJobResult) =>
        sparkResult.counters.foreach(c => Utils.printOut(c.toString()))
        if (sys.env.contains("SL_API"))
          System.out.println(
            "IngestionCounters:" + JsonSerializer.mapper.writeValueAsString(sparkResult.counters)
          )
      case _ =>
    }
    result
  }
}

object LoadCmd extends LoadCmd
