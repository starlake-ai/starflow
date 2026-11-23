package ai.starlake.job.site.stats

import java.util.Locale
import scala.collection.immutable.TreeMap

case class ColumnStat(name: String, `type`: String, nullable: Boolean)

case class ColumnDiff(
  name: String,
  declared: Option[String],
  actual: Option[String],
  status: String // match | mismatch | missingInWarehouse | extraInWarehouse
)

case class TableStats(
  connectionRef: String,
  exists: Boolean,
  tableType: Option[String], // TABLE | VIEW | EXTERNAL
  format: Option[String], // delta | iceberg | parquet | ... (Spark only)
  rowCount: Option[Long],
  rowCountEstimated: Boolean,
  bytes: Option[Long],
  lastModified: Option[String], // ISO-8601 instant
  columns: List[ColumnStat],
  columnDiff: List[ColumnDiff]
)

case class ConnectionStats(
  engine: String,
  status: String, // ok | error | unsupported
  error: Option[String],
  durationMs: Long
)

case class SiteStats(
  schemaVersion: Int,
  generatedAt: String,
  connections: TreeMap[String, ConnectionStats],
  tables: TreeMap[String, TableStats]
)

/** What a collector returns for one physical table, before fan-out to node ids. */
case class RawTableStats(
  exists: Boolean,
  tableType: Option[String],
  format: Option[String],
  rowCount: Option[Long],
  rowCountEstimated: Boolean,
  bytes: Option[Long],
  lastModified: Option[String],
  columns: List[ColumnStat]
)

object RawTableStats {
  val Missing: RawTableStats = RawTableStats(false, None, None, None, false, None, None, Nil)
}

/** A table as it exists in the warehouse. Values keep their declared case (BigQuery dataset names
  * are case sensitive); `key` is used for deduplication.
  */
case class PhysicalTable(database: Option[String], schema: String, table: String) {
  def key: (Option[String], String, String) =
    (
      database.map(_.toLowerCase(Locale.ROOT)),
      schema.toLowerCase(Locale.ROOT),
      table.toLowerCase(Locale.ROOT)
    )
}
