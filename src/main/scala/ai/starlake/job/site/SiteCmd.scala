package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import better.files.File
import scopt.OParser

import scala.util.Try

/** Generates the project documentation site.
  *
  * Usage: starlake site [options]
  */
object SiteCmd extends Cmd[SiteConfig] {

  val command = "site"

  override def pageDescription: String =
    "Generate a documentation site from your Starlake project."
  override def pageKeywords: Seq[String] =
    Seq("starlake site", "documentation generation", "data catalog", "lineage", "project docs")

  val parser: OParser[Unit, SiteConfig] = {
    val builder = OParser.builder[SiteConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """
          |Generate a documentation site from your Starlake project: tables, tasks, columns, doc blocks and project-wide lineage. The output folder holds sl-model.json and a static index.html that also opens from the file system. See [Documentation site guide](/guides/documentation/starlake-site-builder).
          |""".stripMargin
      ),
      builder
        .opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = File(x)))
        .text("Output directory (default: ./site)")
        .optional(),
      builder
        .opt[Unit]("clean")
        .action((_, c) => c.copy(clean = Some(true)))
        .text("Delete the output directory before generating the site")
        .optional(),
      builder
        .opt[Unit]("stats")
        .action((_, c) => c.copy(stats = true))
        .text(
          "Query each connection's metadata (row count, size, last modified, actual column types) and write sl-stats.json. Never reads table data."
        )
        .optional(),
      builder
        .opt[Int]("statsTimeout")
        .action((x, c) => c.copy(statsTimeoutSeconds = x))
        .validate(x =>
          if (x > 0) builder.success
          else builder.failure("statsTimeout must be a positive number of seconds")
        )
        .text("Per-connection timeout in seconds for --stats (default: 120)")
        .optional(),
      builder
        .opt[Unit]("serve")
        .action((_, c) => c.copy(serve = true))
        .text("After generating, serve the site on 127.0.0.1, on the --port port, until Ctrl-C")
        .optional(),
      builder
        .opt[Int]("port")
        .action((x, c) => c.copy(port = x))
        .validate(x =>
          if (x >= 1 && x <= 65535) builder.success
          else builder.failure("port must be between 1 and 65535")
        )
        .text("Port used by --serve (default: 8080)")
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
