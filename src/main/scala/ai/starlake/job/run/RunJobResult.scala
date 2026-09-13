package ai.starlake.job.run

import ai.starlake.utils.JobResult

/** Result of a `starlake run` invocation. exitCode: 0 all succeeded, 1 at least one task
  * failed or was skipped, 2 configuration or graph error (cycle).
  */
final case class RunJobResult(
  exitCode: Int,
  summary: Option[RunSummary],
  errorMessage: Option[String]
) extends JobResult
