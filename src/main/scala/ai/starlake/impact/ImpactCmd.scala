package ai.starlake.impact

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{ExitCodeJobResult, JobResult}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import scopt.{DefaultOEffectSetup, OEffectSetup, OParser}

import scala.util.{Failure, Success, Try}

/** Result of `starlake impact`. `report` is empty and `error` set when the analysis could not run
  * (exit code 2).
  */
case class ImpactJobResult(
  report: Option[ImpactReport],
  exitCode: Int,
  error: Option[String] = None
) extends ExitCodeJobResult

object ImpactCmd extends Cmd[ImpactConfig] with LazyLogging {
  val command = "impact"

  override def pageDescription: String =
    "Report which load tables and transforms a change touches, how (breaking, non-breaking, metadata), and which downstream tasks it impacts, using column-level lineage. Runs offline."
  override def pageKeywords: Seq[String] =
    Seq("starlake impact", "change impact", "breaking change", "column lineage", "CI")

  /** `impact` only reads the project: Main must not create folders in it, and its stdout must hold
    * the report only.
    */
  override def readOnly: Boolean = true

  private val formats = Set("text", "json", "markdown", "html")
  private val failOns = Set("breaking", "non-breaking")

  val parser: OParser[Unit, ImpactConfig] = {
    val builder = OParser.builder[ImpactConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Compare two versions of the project and report direct changes and their downstream impact.
          |Default: merge-base(--base, HEAD) against the current project directory, uncommitted edits included.
          |Nothing is executed and no database connection is opened.
          |Exit codes: 0 report written, 1 --fail-on matched, 2 usage, git or project loading error.""".stripMargin
      ),
      builder
        .opt[String]("base")
        .action((x, c) => c.copy(base = Some(x)))
        .optional()
        .text("git ref of the old side; the merge-base with --head (or HEAD) is used"),
      builder
        .opt[String]("head")
        .action((x, c) => c.copy(head = Some(x)))
        .optional()
        .text("git ref of the new side (default: the current project directory as is)"),
      builder
        .opt[String]("path1")
        .action((x, c) => c.copy(path1 = Some(x)))
        .optional()
        .text("old project directory, instead of --base"),
      builder
        .opt[String]("path2")
        .action((x, c) => c.copy(path2 = Some(x)))
        .optional()
        .text("new project directory, instead of --head"),
      builder
        .opt[String]("env")
        .action((x, c) => c.copy(env = Some(x)))
        .optional()
        .text("env used to render SQL variables"),
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x.toLowerCase))
        .optional()
        .text("text (default), json, markdown or html"),
      builder
        .opt[String]("output")
        .action((x, c) => c.copy(output = Some(x)))
        .optional()
        .text(
          "write the report to this local file instead of stdout; recommended in CI, where the launcher may print to stdout too"
        ),
      builder
        .opt[String]("template")
        .action((x, c) => c.copy(template = Some(x)))
        .optional()
        .text(
          "Jinja template for the html format; the values it receives are already HTML-escaped"
        ),
      builder
        .opt[String]("fail-on")
        .action((x, c) => c.copy(failOn = Some(x.toLowerCase)))
        .optional()
        .text(
          "exit 1 when the report has a breaking change (breaking) or anything but metadata (non-breaking)"
        ),
      builder.checkConfig { c =>
        if (c.base.isDefined && (c.path1.isDefined || c.path2.isDefined))
          builder.failure("--base cannot be combined with --path1 / --path2")
        else if (c.head.isDefined && (c.path1.isDefined || c.path2.isDefined))
          builder.failure("--head cannot be combined with --path1 / --path2")
        else if (c.base.isEmpty && c.path1.isEmpty)
          builder.failure("one of --base or --path1 is required")
        else if (!formats.contains(c.format))
          builder.failure(s"--format must be one of ${formats.toList.sorted.mkString(", ")}")
        else if (c.failOn.exists(f => !failOns.contains(f)))
          builder.failure("--fail-on must be breaking or non-breaking")
        else builder.success
      }
    )
  }

  /** Main runs commands inside Console.withErr and shows what was captured only when the command
    * fails. A usage error of `impact` is a successful result with exit code 2, so scopt's error
    * messages go straight to the process stderr.
    */
  private val effectSetup: OEffectSetup = new DefaultOEffectSetup {
    override def displayToErr(msg: String): Unit = System.err.println(msg)
  }

  override def parse(args: Seq[String]): Option[ImpactConfig] =
    OParser.parse(parser, args, ImpactConfig(), setup, effectSetup)

  private def exitCode(report: ImpactReport, failOn: Option[String]): Int = failOn match {
    case Some("breaking") if report.summary.hasBreaking        => 1
    case Some("non-breaking") if report.summary.hasNonMetadata => 1
    case _                                                     => 0
  }

  private def failed(e: Throwable): ImpactJobResult = {
    val message = Option(e.getMessage).filter(_.nonEmpty).getOrElse(e.getClass.getName)
    System.err.println(s"impact: $message")
    logger.debug("impact failed", e)
    ImpactJobResult(None, 2, Some(message))
  }

  override def run(config: ImpactConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = Success {
    val outcome = ImpactSides.withSides(config) { (oldSide, newSide) =>
      for {
        report <- ImpactAnalysis.analyze(
          oldSide.dir,
          newSide.dir,
          oldSide.label,
          newSide.label,
          config.env
        )
        rendered <- Try(ImpactRenderer.render(report, config.format, config.template))
        _ <- Try {
          config.output match {
            case Some(out) =>
              // a local file, whatever the file system of the project (it may be remote)
              val file = File(out)
              file.parent.createDirectories()
              file.writeText(rendered)
            case None => println(rendered) // scalastyle:ignore
          }
        }
      } yield report
    }
    outcome match {
      case Failure(e)      => failed(e)
      case Success(report) => ImpactJobResult(Some(report), exitCode(report, config.failOn))
    }
  }

  /** Usage errors exit with 2, like git and loading errors, so CI can tell "impact found" (1) from
    * "the tool could not run" (2). scopt has already printed the error itself.
    */
  override def onParseFailure(): Try[JobResult] = {
    System.err.println(usage())
    Success(ImpactJobResult(None, 2, Some("invalid arguments")))
  }
}
