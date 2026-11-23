package ai.starlake.impact

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{ExitCodeJobResult, GitSnapshot, JobResult, ProjectCheckout}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import scopt.{DefaultOEffectSetup, OEffectSetup, OParser}

import java.time.LocalDateTime
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
        .text("write the report to this file instead of stdout"),
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

  def analyze(oldDir: File, newDir: File, oldLabel: String, newLabel: String, env: Option[String])(
    implicit settings: Settings
  ): Try[ImpactReport] = {
    // one instant for both sides: SQL using {{sl_datetime}} must render the same on each
    val dateVars = SchemaHandler.slDateVars(LocalDateTime.now)
    for {
      oldView <- ProjectSnapshot.load(oldDir, oldLabel, env, dateVars)
      newView <- ProjectSnapshot.load(newDir, newLabel, env, dateVars)
    } yield {
      val direct = ChangeDetector.detect(oldView, newView)
      val (impacted, edges) = ImpactPropagator.propagate(direct, newView)
      val unresolved = (edges ++ ImpactPropagator.collisions(oldView, newView)).distinct
        .sortBy(e => (e.from.name, e.to.name))
      ImpactReport(
        oldLabel,
        newLabel,
        direct,
        impacted,
        unresolved,
        ChangeDetector.otherChanges(oldView, newView, direct)
      )
    }
  }

  private final case class Side(dir: File, label: String, checkout: Option[ProjectCheckout]) {
    def cleanup(): Unit = checkout.foreach(_.cleanup())
  }

  private def repoOf(projectDir: File): Try[File] =
    GitSnapshot.topLevel(projectDir).recoverWith { case _ =>
      Failure(
        new IllegalArgumentException(
          s"$projectDir is not inside a git repository, which --base and --head need; use --path1 / --path2 to compare directories"
        )
      )
    }

  /** `git rev-parse --verify --quiet` fails without a message on an unknown ref. */
  private def commitOf(repo: File, ref: String): Try[String] =
    GitSnapshot.resolveCommit(repo, ref).recoverWith { case _ =>
      Failure(new IllegalArgumentException(s"unknown git ref '$ref'"))
    }

  private def isProject(dir: File): Boolean = (dir / "metadata").isDirectory

  /** A side given as a directory must be a project: a wrong path would otherwise load as an empty
    * project, and every node would show as added (or nothing would change).
    */
  private def requireProject(dir: File, shown: String): Try[File] =
    if (isProject(dir)) Success(dir)
    else {
      val hint =
        if (dir.name == "metadata") ", pass the directory that contains the metadata folder"
        else ""
      Failure(
        new IllegalArgumentException(
          s"$shown is not a project directory: it has no metadata folder$hint"
        )
      )
    }

  /** Resolves the old side first, then the new one. A checkout already extracted is deleted when
    * the other side fails. Only the old side of a git comparison may lack a project, when the
    * merge-base predates it: everything then shows as added, and its label says so.
    */
  private def resolveSides(config: ImpactConfig)(implicit settings: Settings): Try[(Side, Side)] = {
    val projectDir = File(settings.appConfig.root)
    val workingTree = config.path2.isEmpty && config.head.isEmpty

    def oldSide: Try[Side] = (config.path1, config.base) match {
      case (Some(p), _) => requireProject(File(p), p).map(Side(_, p, None))
      case (None, Some(b)) =>
        for {
          repo <- repoOf(projectDir)
          base <- commitOf(repo, b)
          head <- commitOf(repo, config.head.getOrElse("HEAD"))
          mb   <- GitSnapshot.mergeBase(repo, base, head)
          co   <- GitSnapshot.projectAt(projectDir, mb)
        } yield {
          val absent = if (isProject(co.projectDir)) "" else ", no project at this commit"
          Side(co.projectDir, s"merge-base of $b (${mb.take(7)}$absent)", Some(co))
        }
      case (None, None) =>
        Failure(new IllegalArgumentException("one of --base or --path1 is required"))
    }

    def newSide: Try[Side] = (config.path2, config.head) match {
      case (Some(p), _) => requireProject(File(p), p).map(Side(_, p, None))
      case (None, Some(h)) =>
        for {
          repo   <- repoOf(projectDir)
          commit <- commitOf(repo, h)
          co     <- GitSnapshot.projectAt(projectDir, commit)
          side <- requireProject(co.projectDir, s"$projectDir at $h (${commit.take(7)})")
            .map(dir => Side(dir, s"$h (${commit.take(7)})", Some(co)))
            .recoverWith { case e => co.cleanup(); Failure(e) }
        } yield side
      case (None, None) =>
        requireProject(projectDir, projectDir.pathAsString).map(Side(_, "working tree", None))
    }

    for {
      // checked first, so that no checkout is extracted for nothing
      _ <- if (workingTree) requireProject(projectDir, projectDir.pathAsString) else Success(())
      o <- oldSide
      n <- newSide.recoverWith { case e => o.cleanup(); Failure(e) }
    } yield (o, n)
  }

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
    resolveSides(config) match {
      case Failure(e) => failed(e)
      case Success((oldSide, newSide)) =>
        try {
          val outcome = for {
            report   <- analyze(oldSide.dir, newSide.dir, oldSide.label, newSide.label, config.env)
            rendered <- Try(ImpactRenderer.render(report, config.format, config.template))
            _ <- Try {
              config.output match {
                case Some(out) => settings.storageHandler().write(rendered, new Path(out))
                case None      => println(rendered) // scalastyle:ignore
              }
            }
          } yield report
          outcome match {
            case Failure(e)      => failed(e)
            case Success(report) => ImpactJobResult(Some(report), exitCode(report, config.failOn))
          }
        } finally {
          oldSide.cleanup()
          newSide.cleanup()
        }
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
