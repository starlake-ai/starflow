package ai.starlake.lineage

import ai.starlake.job.ReportFormatConfig
import better.files.File

case class AutoTaskDependenciesConfig(
  outputFile: Option[File] = None,
  tasks: Option[Seq[String]] = None,
  reload: Boolean = false,
  objects: Seq[String] = Seq("task", "table"),
  viz: Boolean = false,
  print: Boolean = false,
  all: Boolean = false,
  verbose: Boolean = false,
  json: Boolean = false,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
