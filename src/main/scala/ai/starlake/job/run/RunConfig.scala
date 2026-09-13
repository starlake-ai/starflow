package ai.starlake.job.run

import ai.starlake.job.ReportFormatConfig

case class RunConfig(
  parallelism: Option[Int] = None,
  failFast: Boolean = false,
  options: Map[String, String] = Map.empty,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
