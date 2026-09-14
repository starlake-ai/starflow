package ai.starlake.job.run

import ai.starlake.job.ReportFormatConfig

/** @param select
  *   selector expressions, unioned. Empty selects the whole project.
  * @param exclude
  *   selector expressions subtracted after `select`. Subtraction wins.
  * @param dryRun
  *   resolve and print the plan, execute nothing
  */
case class RunConfig(
  parallelism: Option[Int] = None,
  failFast: Boolean = false,
  options: Map[String, String] = Map.empty,
  select: Seq[String] = Nil,
  exclude: Seq[String] = Nil,
  dryRun: Boolean = false,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
