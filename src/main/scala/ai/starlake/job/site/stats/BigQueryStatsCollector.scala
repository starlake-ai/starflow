package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import Rows._
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object BigQuerySql {

  /** Project and dataset names are interpolated inside backquotes: reject anything that could close
    * the quoting or break the statement.
    */
  private def safe(kind: String, value: String): String = {
    require(
      value != null && value.nonEmpty && !value.exists(c => c == '`' || c.isControl),
      s"Unsafe or empty BigQuery $kind name: '$value'"
    )
    value
  }

  private def ref(project: String, dataset: String, obj: String) =
    s"`${safe("project", project)}.${safe("dataset", dataset)}.$obj`"

  def tablesSql(project: String, dataset: String): String =
    "SELECT table_id, type, row_count, size_bytes, last_modified_time FROM " +
    ref(project, dataset, "__TABLES__")

  def columnsSql(project: String, dataset: String): String =
    "SELECT table_name, column_name, data_type, is_nullable FROM " +
    ref(project, dataset, "INFORMATION_SCHEMA.COLUMNS") +
    " ORDER BY table_name, ordinal_position"

  def tableType(code: Option[Long]): String = code match {
    case Some(2L) => "VIEW"
    case Some(3L) => "EXTERNAL"
    case _        => "TABLE"
  }

  def merge(tableRows: List[Row], columnRows: List[Row]): Map[String, RawTableStats] = {
    val cols = InfoSchemaMapping.columns(columnRows)
    tableRows.flatMap { r =>
      str(r, "table_id").map { name =>
        val tpe = tableType(long(r, "type"))
        val isTable = tpe == "TABLE"
        name.toLowerCase(Locale.ROOT) -> RawTableStats(
          exists = true,
          tableType = Some(tpe),
          format = None,
          rowCount = if (isTable) long(r, "row_count") else None,
          rowCountEstimated = false,
          bytes = if (isTable) long(r, "size_bytes") else None,
          lastModified = long(r, "last_modified_time").map(ms => Instant.ofEpochMilli(ms).toString),
          columns = cols.getOrElse(name.toLowerCase(Locale.ROOT), Nil)
        )
      }
    }.toMap
  }
}

object BigQueryRows {
  def query(bq: BigQuery, sql: String): List[Row] = {
    val result = bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
    val fields = result.getSchema.getFields.asScala.map(_.getName.toLowerCase(Locale.ROOT)).toList
    result
      .iterateAll()
      .asScala
      .map { row =>
        fields.zipWithIndex.map { case (f, i) =>
          val v = row.get(i)
          f -> (if (v.isNull) null else v.getValue)
        }.toMap
      }
      .toList
  }
}

class BigQueryStatsCollector extends StatsCollector with LazyLogging {
  val engine = "bigquery"

  private def apiStats(
    bq: BigQuery,
    project: String,
    dataset: String,
    table: String
  ): RawTableStats =
    Option(bq.getTable(TableId.of(project, dataset, table))) match {
      case None => RawTableStats.Missing
      case Some(t) =>
        val defn: TableDefinition = t.getDefinition[TableDefinition]()
        val tpe = defn.getType match {
          case TableDefinition.Type.VIEW | TableDefinition.Type.MATERIALIZED_VIEW => "VIEW"
          case TableDefinition.Type.EXTERNAL                                      => "EXTERNAL"
          case _                                                                  => "TABLE"
        }
        val columns = Option(defn.getSchema).toList.flatMap(
          _.getFields.asScala.map(f =>
            ColumnStat(
              f.getName,
              f.getType.getStandardType.name(),
              nullable = !Option(f.getMode).contains(Field.Mode.REQUIRED)
            )
          )
        )
        RawTableStats(
          exists = true,
          tableType = Some(tpe),
          format = None,
          rowCount = Option(t.getNumRows).map(_.longValue()),
          rowCountEstimated = false,
          bytes = Option(t.getNumBytes).map(_.longValue()),
          lastModified = Option(t.getLastModifiedTime).map(ms => Instant.ofEpochMilli(ms).toString),
          columns = columns
        )
    }

  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): CollectResult = {
    val bq =
      BigQueryJobBase.bigquery(Some(connectionRef), None, tables.flatMap(_.database).headOption)
    // BigQuery dataset names are case sensitive
    StatsCollector.isolate(StatsCollector.bySchema(tables, caseSensitive = true)) { g =>
      val project =
        g.database.orElse(connection.options.get("projectId")).getOrElse(bq.getOptions.getProjectId)
      BigQueryStatsCollector.collectDataset(
        project,
        g,
        BigQueryRows.query(bq, _),
        apiStats(bq, project, g.schema, _)
      )
    }
  }
}

object BigQueryStatsCollector extends LazyLogging {

  /** `__TABLES__` and INFORMATION_SCHEMA.COLUMNS, or the API per table when that SQL fails. The
    * fallback adds the non-fatal note `<label> used the API: __TABLES__ failed: <message>`; a table
    * the API fails on is left out and adds `<label>.<table> failed: <message>`, the other tables
    * are kept. The per-table failures come before the note, so the first failure is a real error.
    */
  private[stats] def collectDataset(
    project: String,
    group: SchemaGroup,
    query: String => List[Row],
    api: String => RawTableStats
  ): GroupResult = {
    val dataset = group.schema
    // built outside the Try: an unsafe name must fail the group with its message, not fall back
    val tablesSql = BigQuerySql.tablesSql(project, dataset)
    val columnsSql = BigQuerySql.columnsSql(project, dataset)
    Try(BigQuerySql.merge(query(tablesSql), query(columnsSql))) match {
      case Success(merged) =>
        GroupResult(
          group.tables
            .map(t =>
              t -> merged.getOrElse(t.table.toLowerCase(Locale.ROOT), RawTableStats.Missing)
            )
            .toMap
        )
      case Failure(e) =>
        val reason = StatsCollector.message(e)
        logger.warn(s"__TABLES__ query failed on $project.$dataset, using the API: $reason", e)
        val note = s"${group.label} used the API: __TABLES__ failed: $reason"
        val results = group.tables.map { t =>
          Try(api(t.table)) match {
            case Success(stats) => Right(t -> stats)
            case Failure(apiError) =>
              val label = s"${group.label}.${t.table}"
              logger.warn(s"BigQuery API stats for $label failed", apiError)
              Left(s"$label failed: ${StatsCollector.message(apiError)}")
          }
        }
        // per-table errors first: the published message is the first failure, the note is not one
        GroupResult(
          results.collect { case Right(r) => r }.toMap,
          results.collect { case Left(f) => f } :+ note
        )
    }
  }
}
