package ai.starlake.extract

import ai.starlake.job.ReportFormatConfig
import ai.starlake.schema.model.WriteMode

case class TablesExtractConfig(
  writeMode: Option[WriteMode] = None,
  connectionRef: Option[String] = None,
  tables: Map[String, List[String]] = Map.empty,
  database: Option[String] = None,
  persist: Boolean = false,
  external: Boolean = false,
  accessToken: Option[String] = None,
  reportFormat: Option[String] = None,
  tableTypes: List[String] = Nil
) extends ReportFormatConfig
