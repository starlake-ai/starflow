package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

/** Command to generate the project documentation website.
  *
  * Usage: starlake site [options]
  */
object SiteCmd extends Cmd[SiteConfig] {

  val command = "site"

  override def pageDescription: String =
    "Generate a documentation site from your Starflow project."
  override def pageKeywords: Seq[String] =
    Seq("starlake site", "documentation generation", "docusaurus", "data catalog", "project docs")

  val parser: OParser[Unit, SiteConfig] = {
    val builder = OParser.builder[SiteConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Generate a documentation portal from your Starflow project metadata (schemas, tasks, lineage). See [Site Builder Guide](/starflow/guides/documentation/starlake-site-builder).
          |
          |Generate site
          |""".stripMargin
      ),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = File(x)))
        .text("Output Directory")
        .optional(),
      builder
        .opt[String]("template")
        .action((x, c) => c.copy(templateName = Some(x)))
        .text("Template name or path to custom templates (default: standalone)")
        .optional(),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = Some(x)))
        .text("json / html (default: html)")
        .optional(),
      builder
        .opt[Unit]("json")
        .action((x, c) => c.copy(format = Some("json")))
        .text("output result as json")
        .optional(),
      builder
        .opt[Unit]("clean")
        .action((x, c) => c.copy(clean = Some(true)))
        .text("Whether to clean the output directory before generating the site")
        .optional()
    )
  }

  def parse(args: Seq[String]): Option[SiteConfig] =
    OParser.parse(parser, args, SiteConfig(), setup)

  override def run(config: SiteConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    new SiteHandler(config, schemaHandler).run().map(_ => JobResult.empty)
}
