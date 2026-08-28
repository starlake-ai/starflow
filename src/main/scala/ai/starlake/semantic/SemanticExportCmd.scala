package ai.starlake.semantic

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import scopt.OParser

import scala.util.Try

/** Command to export semantic models to an interchange format.
  *
  * Usage: starlake semantic-export [options]
  */
object SemanticExportCmd extends Cmd[SemanticExportConfig] {

  val command = "semantic-export"

  override def pageDescription: String =
    "Export semantic models from metadata/semantic to Apache Ossie, a LookML project or a Power BI TMDL folder."
  override def pageKeywords: Seq[String] =
    Seq(
      "starlake semantic-export",
      "semantic model",
      "semantic layer",
      "apache ossie",
      "lookml",
      "looker",
      "tmdl",
      "power bi",
      "open semantic interchange",
      "BI",
      "AI agents"
    )

  val parser: OParser[Unit, SemanticExportConfig] = {
    val builder = OParser.builder[SemanticExportConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Export the semantic models stored in metadata/semantic/ to another semantic
          |format. Supported formats: ossie (Apache Ossie, incubating), lookml (a Looker
          |project: one view file per table plus a model file with explores) and tmdl
          |(a Power BI TMDL folder: database.tmdl, model.tmdl, relationships.tmdl and
          |one tables/<table>.tmdl per table).
          |
          |For ossie, Starflow-specific attributes with no Ossie counterpart are
          |preserved in custom_extensions blocks under the STARLAKE vendor name.
          |
          |For lookml, --connection sets the Looker connection name in the model file.
          |
          |For tmdl, --connection names the Starflow connection used to derive each
          |table's Power Query source; simple aggregate metrics are translated to DAX
          |and anything else becomes a BLANK() measure carrying the original SQL in a
          |TODO comment.
          |
          |example: starlake semantic-export
          |         --format tmdl
          |         --model ecommerce_analytics
          |         --connection snowflake_prod
          |         --output /tmp/tmdl-models""".stripMargin
      ),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (Set("ossie", "lookml", "tmdl").contains(x)) builder.success
          else
            builder.failure(s"Unsupported format '$x'. Supported formats: ossie, lookml, tmdl")
        )
        .text("Target format: ossie (default), lookml or tmdl")
        .optional(),
      builder
        .opt[String]("model")
        .action((x, c) => c.copy(model = Some(x)))
        .text(
          "Name of a single semantic model to export (model 'name' field or file basename). All models by default"
        )
        .optional(),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .text(
          "Output directory. Defaults to metadata/semantic/export/ with one subfolder per format"
        )
        .optional(),
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connection = Some(x)))
        .text(
          "lookml: Looker connection name written to the model file; tmdl: Starflow connection used to derive the Power Query source. Defaults to the project's connectionRef"
        )
        .optional(),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[SemanticExportConfig] =
    OParser.parse(parser, args, SemanticExportConfig(), setup)

  override def run(config: SemanticExportConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    new SemanticExportJob(config).run()
}
