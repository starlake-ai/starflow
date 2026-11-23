package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.extract.JdbcDbUtils
import Rows._

import java.sql.Connection
import scala.util.Try

trait StatsCollector {
  def engine: String

  /** Connection status reported when collection succeeds. */
  def connectionStatus: String = "ok"

  /** Returns one entry per requested table; RawTableStats.Missing when the table does not exist.
    */
  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): Map[PhysicalTable, RawTableStats]
}

object StatsCollector {

  /** Opens (and always closes) a connection the way the rest of the product does, so data branches,
    * preActions and postActions apply.
    */
  def withConnection[T](connection: ConnectionInfo)(f: Connection => T)(implicit
    settings: Settings
  ): T = {
    val branch = Try(settings.schemaHandler().dataBranch(connection.options)).toOption.flatten
    JdbcDbUtils.withJDBCConnection(branch, connection.options)(f)
  }

  private[stats] def bySchema(
    tables: List[PhysicalTable]
  ): List[List[PhysicalTable]] =
    tables.groupBy(t => (t.database.map(_.toLowerCase), t.schema.toLowerCase)).values.toList
}

/** PostgreSQL, Redshift, DuckDB and Snowflake: catalog views only, never count(*). */
class JdbcStatsCollector(dialect: JdbcDialectStats) extends StatsCollector {
  val engine: String = dialect.engine

  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): Map[PhysicalTable, RawTableStats] =
    StatsCollector.withConnection(connection) { conn =>
      StatsCollector
        .bySchema(tables)
        .flatMap { ts =>
          val db = ts.head.database.orElse(dialect.defaultDatabase(connection))
          val schema = ts.head.schema
          val merged = InfoSchemaMapping.merge(
            JdbcRows.query(conn, dialect.tablesSql(db, schema)),
            dialect.statsSql(db, schema).map(JdbcRows.query(conn, _)).getOrElse(Nil),
            JdbcRows.query(conn, dialect.columnsSql(db, schema)),
            dialect.rowCountEstimated
          )
          ts.map(t => t -> merged.getOrElse(t.table.toLowerCase, RawTableStats.Missing))
        }
        .toMap
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
  ): Map[PhysicalTable, RawTableStats] =
    StatsCollector.withConnection(connection) { conn =>
      val md = conn.getMetaData
      val escape = Try(md.getSearchStringEscape).toOption.orNull
      StatsCollector
        .bySchema(tables)
        .flatMap { ts =>
          val db = ts.head.database.orNull
          val schema = ts.head.schema
          val found = List(schema, schema.toUpperCase, schema.toLowerCase).distinct.iterator
            .map(GenericMetadataMapping.escapePattern(_, escape))
            .map(s => s -> JdbcRows.readAndClose(md.getTables(db, s, "%", null)))
            .find(_._2.nonEmpty)
          found match {
            case None => ts.map(_ -> RawTableStats.Missing)
            case Some((s, tableRows)) =>
              val columnRows = GenericMetadataMapping.toInfoSchemaColumns(
                JdbcRows.readAndClose(md.getColumns(db, s, "%", "%"))
              )
              val merged = InfoSchemaMapping.merge(tableRows, Nil, columnRows, estimated = false)
              ts.map(t => t -> merged.getOrElse(t.table.toLowerCase, RawTableStats.Missing))
          }
        }
        .toMap
    }
}
