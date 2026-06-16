package ai.starlake.job.ingest

import ai.starlake.job.ReportFormatConfig

case class PreLoadConfig(
  domain: String,
  tables: Seq[String] = Seq.empty,
  strategy: Option[PreLoadStrategy] = None,
  globalAckFilePath: Option[String] = None,
  options: Map[String, String] = Map.empty,
  reportFormat: Option[String] = None,
  notReadySentinel: Option[String] = None
) extends ReportFormatConfig
