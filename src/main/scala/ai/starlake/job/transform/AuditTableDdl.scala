package ai.starlake.job.transform

import java.util.concurrent.ConcurrentHashMap

/** Brings an existing audit table (audit, expectations, rejected, extract and the metrics tables)
  * up to the columns of its createSql in reference-engines.conf. That DDL only runs when the table
  * does not exist, so a table created by an older Starflow keeps its old columns and every write
  * that selects a newer one (SCHEDULED_DATE, added in 2025-08) fails.
  */
object AuditTableDdl {

  private val ColumnList = """(?s)\{\{\s*table\s*\}\}\s*\((.*)\)""".r.unanchored
  private val Nullability = """(?i)\s+(NOT\s+NULL|NULL)\s*$""".r

  /** The columns of a CREATE TABLE {{table}} (...) statement, in order: name -> type, without NOT
    * NULL, since a column added to a table that has rows cannot require a value.
    */
  def columns(createSql: String): List[(String, String)] =
    createSql match {
      case ColumnList(body) =>
        splitTopLevel(body).map(_.trim.replaceAll("\\s+", " ")).filter(_.nonEmpty).flatMap {
          definition =>
            definition.split(" ", 2) match {
              case Array(name, typ) => Some(name -> Nullability.replaceFirstIn(typ, "").trim)
              case _                => None
            }
        }
      case _ => Nil
    }

  /** Splits on the commas outside parentheses, so DECIMAL(65,30) stays one type */
  private def splitTopLevel(body: String): List[String] = {
    val parts = List.newBuilder[String]
    val current = new StringBuilder
    var depth = 0
    body.foreach {
      case '(' => depth += 1; current += '('
      case ')' => depth -= 1; current += ')'
      case ',' if depth == 0 =>
        parts += current.toString
        current.clear()
      case c => current += c
    }
    parts += current.toString
    parts.result()
  }

  /** The DDL columns the table lacks. With no known table column, nothing: the columns could not be
    * read, and adding every column would fail on the first that exists.
    */
  def missing(
    ddlColumns: List[(String, String)],
    tableColumns: List[String]
  ): List[(String, String)] =
    if (tableColumns.isEmpty) Nil
    else {
      val existing = tableColumns.map(_.toLowerCase).toSet
      ddlColumns.filterNot { case (name, _) => existing.contains(name.toLowerCase) }
    }

  def addColumnSql(engine: String, table: String, column: String, typ: String): String =
    engine.toLowerCase match {
      case "spark"     => s"ALTER TABLE $table ADD COLUMNS ($column $typ)"
      case "sqlserver" => s"ALTER TABLE $table ADD $column $typ"
      case _           => s"ALTER TABLE $table ADD COLUMN $column $typ"
    }

  private val checked = ConcurrentHashMap.newKeySet[String]()

  /** Adds the missing columns of an existing audit table, once per connection and table in this
    * JVM, whatever the outcome: a failure (a user without the ALTER privilege) is not retried on
    * every audit write. It is thrown for the caller to log, and the write that follows then fails
    * with the missing column error, which names the column.
    *
    * @param tableColumns
    *   the columns of the existing table, read only when the table was not checked yet
    * @param run
    *   runs one ALTER TABLE statement
    */
  def evolve(
    key: String,
    engine: String,
    table: String,
    createSql: String,
    tableColumns: => List[String]
  )(run: String => Unit): List[String] =
    if (checked.contains(key)) Nil
    else
      // Parallel loads write their audit rows at once: the others wait for the columns
      synchronized {
        if (!checked.add(key)) Nil
        else
          missing(columns(createSql), tableColumns).map { case (column, typ) =>
            run(addColumnSql(engine, table, column, typ))
            column
          }
      }

  /** Tests that drop and recreate audit tables in the same JVM */
  private[starlake] def forgetChecked(): Unit = checked.clear()
}
