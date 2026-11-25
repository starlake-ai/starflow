package ai.starlake.dbt.warehouse

import java.time.format.DateTimeFormatter
import java.util
import scala.jdk.CollectionConverters._

/** run_query()'s answer, as dbt's agate table: rows by index, `columns`, `column_names` */
final class DbtTable(columnNames: List[String], rowList: List[util.List[Object]])
    extends util.AbstractList[util.List[Object]] {
  override def get(index: Int): util.List[Object] = rowList(index)
  override def size(): Int = rowList.size
  def getRows: util.List[util.List[Object]] = new util.ArrayList(rowList.asJava)
  def getColumn_names: util.List[String] = new util.ArrayList(columnNames.asJava)
  def getColumns: DbtTableColumns = new DbtTableColumns(columnNames.zipWithIndex.map {
    case (n, i) => new DbtTableColumn(n, new util.ArrayList(rowList.map(_.get(i)).asJava))
  })
  def print_table(): String = ""
}

final class DbtTableColumn(val name: String, vals: util.List[Object]) {
  def getName: String = name
  def values(): util.List[Object] = vals
}

/** `table.columns`: by index (`columns[0]`) or by name (`columns['status']`) */
final class DbtTableColumns(cols: List[DbtTableColumn]) extends util.AbstractList[DbtTableColumn] {
  override def get(index: Int): DbtTableColumn = cols(index)
  override def size(): Int = cols.size
  def byName(name: String): DbtTableColumn =
    cols.find(_.name == name).orElse(cols.find(_.name.equalsIgnoreCase(name))).orNull
}

/** adapter.get_columns_in_relation()'s column, as dbt's Column */
final class DbtWarehouseColumn(name: String, dataType: String, quote: String) {
  private val t = dataType.toLowerCase
  def getName: String = name
  def getColumn: String = name
  def getData_type: String = dataType
  def getDtype: String = dataType
  def getQuoted: String = s"$quote$name$quote"
  def is_string(): Boolean = Set("char", "text", "string", "varchar").exists(t.contains)
  def is_integer(): Boolean =
    Set("int", "integer", "bigint", "smallint", "tinyint", "hugeint").exists(t.contains)
  def is_float(): Boolean = Set("float", "double", "real").exists(t.contains)
  def is_numeric(): Boolean = Set("numeric", "decimal", "number").exists(t.contains)
  def is_number(): Boolean = is_integer() || is_float() || is_numeric()
  override def toString: String = name
  override def equals(o: Any): Boolean = o match {
    case c: DbtWarehouseColumn => c.getName == name && c.getData_type == dataType
    case _                     => false
  }
  override def hashCode(): Int = (name, dataType).hashCode()
}

object DbtResults {
  private val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** A JDBC value as Jinja renders it as Python does: dates and timestamps as ISO text */
  def value(o: Object): Object = o match {
    case null                   => null
    case d: java.sql.Date       => d.toLocalDate.toString
    case t: java.sql.Timestamp  => value(t.toLocalDateTime)
    case d: java.time.LocalDate => d.toString
    case t: java.time.LocalDateTime =>
      if (t.getNano == 0) t.format(timestamp) else t.toString.replace('T', ' ')
    case t: java.time.OffsetDateTime => value(t.toLocalDateTime)
    case i: java.lang.Integer        => java.lang.Long.valueOf(i.longValue)
    case s: java.lang.Short          => java.lang.Long.valueOf(s.longValue)
    case b: java.math.BigInteger     => java.lang.Long.valueOf(b.longValue)
    case f: java.lang.Float          => java.lang.Double.valueOf(f.doubleValue)
    case other                       => other
  }
}
