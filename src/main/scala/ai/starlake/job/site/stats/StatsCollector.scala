package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.extract.JdbcDbUtils
import Rows._
import com.typesafe.scalalogging.LazyLogging

import java.sql.Connection
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** What a collector returns for one connection: stats for the tables of every schema group that
  * succeeded (RawTableStats.Missing when the table does not exist), and one message per failure.
  * Tables of a failed group are absent from `tables`.
  */
case class CollectResult(tables: Map[PhysicalTable, RawTableStats], failures: List[String]) {

  /** Every group failed: a successful group always reports each of its tables. */
  def allFailed: Boolean = tables.isEmpty && failures.nonEmpty
}

/** The outcome of one schema group: its tables, plus non-fatal failures (stats-only queries). */
case class GroupResult(tables: Map[PhysicalTable, RawTableStats], failures: List[String] = Nil)

/** Tables of one (database, schema) or BigQuery (project, dataset). */
case class SchemaGroup(database: Option[String], schema: String, tables: List[PhysicalTable]) {
  def label: String = database.fold(schema)(db => s"$db.$schema")
}

object ConnectionStatus {

  /** Connection-level status from a collector result: `error` when every group failed, otherwise
    * `successStatus` with the failures (if any) as the error message.
    */
  def from(
    engine: String,
    result: CollectResult,
    durationMs: Long,
    successStatus: String = "ok"
  ): ConnectionStats = {
    val error = if (result.failures.isEmpty) None else Some(result.failures.mkString("; "))
    val status = if (result.allFailed) "error" else successStatus
    ConnectionStats(engine, status, error, durationMs)
  }
}

trait StatsCollector {
  def engine: String

  /** Connection status reported when collection succeeds. */
  def connectionStatus: String = "ok"

  /** Returns one entry per requested table of every schema group that succeeded
    * (RawTableStats.Missing when the table does not exist) and the group failures. Throws only on
    * connection-level failures (cannot connect, cannot build the client).
    */
  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): CollectResult
}

object StatsCollector extends LazyLogging {

  /** Opens (and always closes) a connection the way the rest of the product does, so data branches,
    * preActions and postActions apply.
    */
  def withConnection[T](connection: ConnectionInfo)(f: Connection => T)(implicit
    settings: Settings
  ): T = {
    val branch = Try(settings.schemaHandler().dataBranch(connection.options)) match {
      case Success(b) => b
      case Failure(e) =>
        logger.warn(s"Could not resolve the data branch, using the main branch: ${message(e)}", e)
        None
    }
    JdbcDbUtils.withJDBCConnection(branch, connection.options)(f)
  }

  def message(e: Throwable): String =
    Option(e.getMessage)
      .map(_.replaceAll("\\s+", " ").trim)
      .filter(_.nonEmpty)
      .getOrElse(
        e.getClass.getName
      )

  private[stats] def bySchema(
    tables: List[PhysicalTable],
    caseSensitive: Boolean = false
  ): List[SchemaGroup] = {
    def norm(s: String) = if (caseSensitive) s else s.toLowerCase
    tables
      .groupBy(t => (t.database.map(norm), norm(t.schema)))
      .values
      .toList
      .map(ts => SchemaGroup(ts.head.database, ts.head.schema, ts))
      .sortBy(_.label.toLowerCase)
  }

  /** Runs each group in its own Try: a failing group loses its tables and adds `<label>: <message>`
    * to the failures, the other groups are kept.
    */
  def isolate(groups: List[SchemaGroup])(work: SchemaGroup => GroupResult): CollectResult = {
    val results = groups.map { g =>
      Try(work(g)) match {
        case Success(r) => r
        case Failure(e) =>
          logger.warn(s"Stats for schema ${g.label} failed: ${message(e)}", e)
          GroupResult(Map.empty, List(s"${g.label}: ${message(e)}"))
      }
    }
    CollectResult(results.flatMap(_.tables).toMap, results.flatMap(_.failures))
  }

  /** A stats-only query (row counts, bytes): on failure, no rows and `<label> stats: <message>`. */
  def statsOnly(label: String)(f: => List[Row]): (List[Row], List[String]) =
    Try(f) match {
      case Success(rows) => (rows, Nil)
      case Failure(e) =>
        logger.warn(s"Stats query for schema $label failed, no row counts: ${message(e)}", e)
        (Nil, List(s"$label stats: ${message(e)}"))
    }
}

/** PostgreSQL, Redshift, DuckDB and Snowflake: catalog views only, never count(*). */
class JdbcStatsCollector(dialect: JdbcDialectStats) extends StatsCollector {
  val engine: String = dialect.engine

  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): CollectResult =
    StatsCollector.withConnection(connection) { conn =>
      val query = JdbcStatsCollector.recovering(conn) _
      StatsCollector.isolate(StatsCollector.bySchema(tables)) { g =>
        val db = g.database.orElse(dialect.defaultDatabase(connection))
        JdbcStatsCollector.collectGroup(dialect, query, g, db)
      }
    }
}

object JdbcStatsCollector {

  /** Runs a query; on failure outside autocommit, rolls back so the next query still runs. */
  private[stats] def recovering(conn: Connection)(sql: String): List[Row] =
    try JdbcRows.query(conn, sql)
    catch {
      case NonFatal(e) =>
        if (!Try(conn.getAutoCommit).getOrElse(true)) Try(conn.rollback())
        throw e
    }

  /** Tables and columns failing fail the group; the stats query failing only drops the counts. The
    * stats query runs last so it cannot disturb the catalog queries.
    */
  private[stats] def collectGroup(
    dialect: JdbcDialectStats,
    query: String => List[Row],
    group: SchemaGroup,
    db: Option[String]
  ): GroupResult = {
    val tableRows = query(dialect.tablesSql(db, group.schema))
    val columnRows = query(dialect.columnsSql(db, group.schema))
    val (statsRows, failures) = dialect
      .statsSql(db, group.schema)
      .map(sql => StatsCollector.statsOnly(group.label)(query(sql)))
      .getOrElse((Nil, Nil))
    val merged =
      InfoSchemaMapping.merge(tableRows, statsRows, columnRows, dialect.rowCountEstimated)
    GroupResult(
      group.tables
        .map(t => t -> merged.getOrElse(t.table.toLowerCase, RawTableStats.Missing))
        .toMap,
      failures
    )
  }
}

object GenericMetadataMapping {

  /** DatabaseMetaData.getColumns rows to the information_schema row contract. */
  def toInfoSchemaColumns(rows: List[Row]): List[Row] = rows.map { r =>
    def boxed(k: String): java.lang.Long = long(r, k).map(java.lang.Long.valueOf).orNull
    Map[String, Any](
      "table_name"               -> str(r, "table_name").orNull,
      "column_name"              -> str(r, "column_name").orNull,
      "data_type"                -> str(r, "type_name").getOrElse(""),
      "is_nullable"              -> (if (long(r, "nullable").contains(0L)) "NO" else "YES"),
      "character_maximum_length" -> boxed("column_size"),
      "numeric_precision"        -> boxed("column_size"),
      "numeric_scale"            -> boxed("decimal_digits")
    )
  }

  /** Escapes the LIKE wildcards of a DatabaseMetaData pattern argument. */
  def escapePattern(name: String, escape: String): String =
    if (escape == null || escape.isEmpty) name
    else
      name
        .replace(escape, escape + escape)
        .replace("_", escape + "_")
        .replace("%", escape + "%")
}

/** Any other JDBC engine: actual columns only, no row count or size. */
class GenericJdbcStatsCollector(val engine: String) extends StatsCollector {
  override val connectionStatus: String = "unsupported"

  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): CollectResult =
    StatsCollector.withConnection(connection) { conn =>
      val md = conn.getMetaData
      val escape = Try(md.getSearchStringEscape).toOption.orNull
      StatsCollector.isolate(StatsCollector.bySchema(tables)) { g =>
        val db = g.database.orNull
        val schema = g.schema
        val found = List(schema, schema.toUpperCase, schema.toLowerCase).distinct.iterator
          .map(GenericMetadataMapping.escapePattern(_, escape))
          .map(s => s -> JdbcRows.readAndClose(md.getTables(db, s, "%", null)))
          .find(_._2.nonEmpty)
        found match {
          case None => GroupResult(g.tables.map(_ -> RawTableStats.Missing).toMap)
          case Some((s, tableRows)) =>
            val columnRows = GenericMetadataMapping.toInfoSchemaColumns(
              JdbcRows.readAndClose(md.getColumns(db, s, "%", "%"))
            )
            val merged = InfoSchemaMapping.merge(tableRows, Nil, columnRows, estimated = false)
            GroupResult(
              g.tables
                .map(t => t -> merged.getOrElse(t.table.toLowerCase, RawTableStats.Missing))
                .toMap
            )
        }
      }
    }
}
