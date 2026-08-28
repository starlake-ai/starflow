package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to compute metrics.
  *
  * Usage: starlake metrics [options]
  */
object MetricsCmd extends Cmd[MetricsConfig] {

  val command = "metrics"

  override def pageDescription: String =
    "Compute and publish data quality metrics for a given domain and schema, with optional Google Cloud authentication."
  override def pageKeywords: Seq[String] =
    Seq("starlake metrics", "data quality", "schema metrics", "data profiling")

  val parser: OParser[Unit, MetricsConfig] = {
    val builder = OParser.builder[MetricsConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Compute data quality metrics (continuous, discrete, and frequency metrics) on loaded tables. See [Metrics Guide](/starflow/guides/load/metrics)."""
      ),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .required()
        .text("Domain Name"),
      builder
        .opt[String]("schema")
        .action((x, c) => c.copy(schema = x))
        .required()
        .text("Schema Name"),
      builder
        .opt[Map[String, String]]("authInfo")
        .action((x, c) => c.copy(authInfo = x))
        .optional()
        .text("Auth Info.  Google Cloud use: gcpProjectId and gcpSAJsonKey"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  /** Function to parse command line arguments (domain and schema).
    *
    * @param args
    *   : Command line parameters
    * @return
    *   : an Option of MetricConfing with the parsed domain and schema names.
    */
  def parse(args: Seq[String]): Option[MetricsConfig] = {
    OParser.parse(parser, args, MetricsConfig(), setup)
  }

  override def run(config: MetricsConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).metric(config)
}
