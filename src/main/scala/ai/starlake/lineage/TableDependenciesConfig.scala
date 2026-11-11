package ai.starlake.lineage

import ai.starlake.job.ReportFormatConfig
import better.files.File

/** @param all
  *   Include all tables in the dot file ? All by default
  * @param includeAllAttributes
  *   Include all attributes in the output
  * @param json
  *   JSON output
  * @param outputFile
  *   Where to save the generated dot file ? Output to the console by default
  * @param related
  *   Include only entities with relations
  * @param reload
  *   Reload domains first
  * @param tables
  *   Which tables should we include in the dot file ?
  */
case class TableDependenciesConfig(
  all: Boolean = true,
  includeAllAttributes: Boolean = true,
  json: Boolean = false,
  outputFile: Option[File] = None,
  related: Boolean = false,
  reload: Boolean = false,
  tables: Option[Seq[String]] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
