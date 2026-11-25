package ai.starlake.dbt.warehouse

import ai.starlake.extract.JdbcDbUtils

import java.sql.{Connection, DatabaseMetaData}
import java.util
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Using}

/** A JDBC connection of the project: queries through the driver, columns and tables from its
  * DatabaseMetaData. Names are tried as written, then upper case, then lower case, since engines
  * store them differently (Snowflake upper, DuckDB and PostgreSQL lower).
  */
final class JdbcWarehouse(connectionName: String, options: Map[String, String])
    extends DbtWarehouse {
  def name: String = connectionName

  private def withConnection[T](f: Connection => T): Try[T] =
    Try(JdbcDbUtils.withJDBCConnection(None, options)(f))

  def query(sql: String, maxRows: Int): Try[DbtTable] = withConnection { conn =>
    Using.resource(conn.createStatement()) { st =>
      st.setMaxRows(maxRows)
      Using.resource(st.executeQuery(sql)) { rs =>
        val md = rs.getMetaData
        val names = (1 to md.getColumnCount).map(md.getColumnLabel).toList
        val rows = ListBuffer.empty[util.List[Object]]
        while (rows.size < maxRows && rs.next()) {
          val row = new util.ArrayList[Object]()
          names.indices.foreach(i => row.add(DbtResults.value(rs.getObject(i + 1))))
          rows += row
        }
        new DbtTable(names, rows.toList)
      }
    }
  }

  private def spellings(s: String): List[String] = List(s, s.toUpperCase, s.toLowerCase).distinct

  /** getColumns and getTables take LIKE patterns: `_` and `%` in a name must be escaped */
  private def literal(md: DatabaseMetaData, s: String): String =
    Option(md.getSearchStringEscape).filter(_.nonEmpty) match {
      case Some(esc) =>
        s.flatMap(c => if (c == '_' || c == '%' || s"$c" == esc) s"$esc$c" else s"$c")
      case None => s
    }

  def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] = withConnection { conn =>
    val md = conn.getMetaData
    val quote = Option(md.getIdentifierQuoteString).map(_.trim).filter(_.nonEmpty).getOrElse("\"")
    val found = for {
      schema <- spellings(r.schema).iterator
      table  <- spellings(r.identifier).iterator
      cols = Using.resource(
        md.getColumns(r.database.orNull, literal(md, schema), literal(md, table), null)
      ) { rs =>
        val buf = ListBuffer.empty[(Int, DbtWarehouseColumn)]
        while (rs.next())
          if (
            rs.getString("TABLE_SCHEM").equalsIgnoreCase(r.schema) &&
            rs.getString("TABLE_NAME").equalsIgnoreCase(r.identifier)
          )
            buf += rs.getInt("ORDINAL_POSITION") -> new DbtWarehouseColumn(
              rs.getString("COLUMN_NAME"),
              rs.getString("TYPE_NAME"),
              quote
            )
        buf.sortBy(_._1).map(_._2).toList
      }
      if cols.nonEmpty
    } yield cols
    found.nextOption().getOrElse(Nil)
  }

  def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] = withConnection { conn =>
    val md = conn.getMetaData
    val found = for {
      schema <- spellings(r.schema).iterator
      table  <- spellings(r.identifier).iterator
      if Using.resource(
        md.getTables(r.database.orNull, literal(md, schema), literal(md, table), null)
      ) { rs =>
        var hit = false
        while (!hit && rs.next())
          hit = rs.getString("TABLE_SCHEM").equalsIgnoreCase(r.schema) &&
            rs.getString("TABLE_NAME").equalsIgnoreCase(r.identifier)
        hit
      }
    } yield r
    found.nextOption()
  }
}
