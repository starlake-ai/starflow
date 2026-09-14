package ai.starlake.job.run

import ai.starlake.job.ReportFormatConfig

/** Which recorded run `--resume` refers to. */
sealed trait ResumeTarget

object ResumeTarget {

  /** The most recently started run in the store. */
  case object Latest extends ResumeTarget

  final case class Id(runId: String) extends ResumeTarget
}

/** @param select
  *   selector expressions, unioned. Empty selects the whole project.
  * @param exclude
  *   selector expressions subtracted after `select`. Subtraction wins.
  * @param dryRun
  *   resolve and print the plan, execute nothing
  * @param resumeLatest
  *   `--resume`: continue the most recent run
  * @param resumeId
  *   `--resume-id`: continue that run. Two flags rather than an optional-valued `--resume`, because
  *   scopt 4 has no optional-value option and faking one needs an argv pre-pass.
  * @param force
  *   resume even though the project changed since the recorded run
  * @param noRunLog
  *   execute without writing a run log
  */
case class RunConfig(
  parallelism: Option[Int] = None,
  failFast: Boolean = false,
  options: Map[String, String] = Map.empty,
  select: Seq[String] = Nil,
  exclude: Seq[String] = Nil,
  dryRun: Boolean = false,
  reportFormat: Option[String] = None,
  resumeLatest: Boolean = false,
  resumeId: Option[String] = None,
  force: Boolean = false,
  noRunLog: Boolean = false
) extends ReportFormatConfig

object RunConfig {

  /** Set to `false`, `0`, `no` or `off` to disable the run log for every `starlake run` in this
    * environment, without touching the command line. For a container image or an api-spawned CLI
    * that is the only practical place to say it.
    */
  val RunLogEnv = "SL_RUN_LOG"

  /** Whether this invocation writes a run log.
    *
    * `--no-run-log` and `SL_RUN_LOG=false` both disable it, and nothing re-enables it: a run that
    * was told twice not to log is not an argument to resolve. Disabling only stops *writing*. A
    * resume still reads the recorded run, because refusing to read would turn a logging preference
    * into a silent re-execution of work that already succeeded.
    */
  def runLogEnabled(config: RunConfig, env: Map[String, String] = sys.env): Boolean = {
    val disabledByEnv = env
      .get(RunLogEnv)
      .map(_.trim.toLowerCase)
      .exists(Set("false", "0", "no", "off").contains)
    !config.noRunLog && !disabledByEnv
  }

  /** Resolves the resume flags, rejecting combinations that cannot mean anything.
    *
    * Validated here rather than in scopt because a scopt failure surfaces as
    * `Failure(IllegalArgumentException)`, which Main maps to exit 1, and a usage error must exit 2.
    */
  def resumeRequest(config: RunConfig): Either[String, Option[ResumeTarget]] =
    (config.resumeLatest, config.resumeId) match {
      case (true, Some(_)) =>
        Left(
          "Use either --resume, which continues the most recent run, or --resume-id <run-id>," +
          " which names one. Not both."
        )
      case (false, None) => Right(None)
      case (_, id) =>
        val target = id.map(ResumeTarget.Id.apply).getOrElse(ResumeTarget.Latest)
        if (config.select.nonEmpty || config.exclude.nonEmpty || config.options.nonEmpty)
          Left(
            "--select, --exclude and --options cannot be combined with a resume: the selection and" +
            " the options are replayed from the recorded run. Start a new run to change them."
          )
        else Right(Some(target))
    }
}
