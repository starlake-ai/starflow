package ai.starlake.dbt.mapping

import ai.starlake.dbt.report.ImportReport
import org.apache.hadoop.fs.Path

/** A file the import writes into the Starlake project */
sealed trait DbtOutput { def path: Path }

final case class FileOutput(path: Path, content: String) extends DbtOutput

/** A seed CSV copied into the incoming area of its domain */
final case class CopyOutput(path: Path, source: java.nio.file.Path) extends DbtOutput

/** The DuckDB connection of the dbt target, added to application.sl.yml */
final case class ConnectionEdit(name: String, url: String)

final case class ImportPlan(
  outputs: List[DbtOutput],
  connection: Option[ConnectionEdit],
  report: ImportReport
)
