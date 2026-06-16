package ai.starlake.job.quack

import ai.starlake.job.ReportFormatConfig

case class QuackConfig(
  action: String = "",
  connectionName: Option[String] = None,
  bind: Option[String] = None,
  port: Option[Int] = None,
  token: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
