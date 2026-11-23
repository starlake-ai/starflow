package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import Rows._
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
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
        val isTable = tpe != "VIEW"
        name.toLowerCase -> RawTableStats(
          exists = true,
          tableType = Some(tpe),
          format = None,
          rowCount = if (isTable) long(r, "row_count") else None,
          rowCountEstimated = false,
          bytes = if (isTable) long(r, "size_bytes") else None,
          lastModified = long(r, "last_modified_time").map(ms => Instant.ofEpochMilli(ms).toString),
          columns = cols.getOrElse(name.toLowerCase, Nil)
        )
      }
    }.toMap
  }
}

object BigQueryRows {
  def query(bq: BigQuery, sql: String): List[Row] = {
    val result = bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
    val fields = result.getSchema.getFields.asScala.map(_.getName.toLowerCase).toList
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
  ): Map[PhysicalTable, RawTableStats] = {
    val bq =
      BigQueryJobBase.bigquery(Some(connectionRef), None, tables.flatMap(_.database).headOption)
    tables
      .groupBy(t => (t.database, t.schema))
      .toList
      .flatMap { case ((db, dataset), ts) =>
        val project =
          db.orElse(connection.options.get("projectId")).getOrElse(bq.getOptions.getProjectId)
        Try(
          BigQuerySql.merge(
            BigQueryRows.query(bq, BigQuerySql.tablesSql(project, dataset)),
            BigQueryRows.query(bq, BigQuerySql.columnsSql(project, dataset))
          )
        ) match {
          case Success(merged) =>
            ts.map(t => t -> merged.getOrElse(t.table.toLowerCase, RawTableStats.Missing))
          case Failure(e) =>
            logger.warn(
              s"__TABLES__ query failed on $project.$dataset, using the API: ${e.getMessage}"
            )
            ts.map(t => t -> apiStats(bq, project, dataset, t.table))
        }
      }
      .toMap
  }
}
