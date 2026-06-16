package ai.starlake.job.gizmo

import ai.starlake.job.ReportFormatConfig

case class GizmoConfig(
  action: String = "",
  connectionName: Option[String] = None,
  port: Option[Int] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
