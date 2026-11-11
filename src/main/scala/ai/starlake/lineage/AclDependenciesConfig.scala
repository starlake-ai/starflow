package ai.starlake.lineage

import ai.starlake.job.ReportFormatConfig
import better.files.File

case class AclDependenciesConfig(
  grantees: List[String] = Nil,
  tables: List[String] = Nil,
  outputFile: Option[File] = None,
  reload: Boolean = false,
  json: Boolean = false,
  all: Boolean = false,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
