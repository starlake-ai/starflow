package ai.starlake.impact

import ai.starlake.job.ReportFormatConfig

case class ImpactConfig(
  base: Option[String] = None,
  head: Option[String] = None,
  path1: Option[String] = None,
  path2: Option[String] = None,
  env: Option[String] = None,
  format: String = "text",
  output: Option[String] = None,
  template: Option[String] = None,
  failOn: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
