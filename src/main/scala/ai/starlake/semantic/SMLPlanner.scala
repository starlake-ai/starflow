package ai.starlake.semantic

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Plans the SML export of one Snowflake-style semantic model: maps fields to dataset columns,
  * classifies metrics, assigns fact and dimension roles, builds dimensions and routes
  * relationships. The result is an immutable [[SMLPlan.Model]] that [[SMLConverter]] renders.
  */
private[semantic] object SMLPlanner extends LazyLogging {

  import SMLPlan._
  import SemanticModelOps._

  /** A dimension ("dimension"), time_dimension ("time") or fact ("fact") of a table. */
  private case class Field(
    name: String,
    kind: String,
    expr: Option[String],
    dataType: String,
    node: JsonNode
  )

  /** Working state of one table: its fields, the dataset columns built so far and the
    * field-to-column map every later reference goes through.
    */
  private final class TableState(val name: String, val node: JsonNode, val fields: List[Field]) {
    val columns: ArrayBuffer[Column] = ArrayBuffer()
    private val columnByField = mutable.Map[String, String]()
    private val refByField = mutable.Map[String, String]()
    private val generated = mutable.Map[String, String]()
    var hierarchies: List[SMLHierarchies.Hierarchy] = Nil

    fields.foreach { f =>
      val physical = f.expr.map(_.trim) match {
        case None                                    => Some(f.name)
        case Some(e) if IdentifierPattern.matches(e) => Some(e)
        case Some(_)                                 => None
      }
      physical.filterNot(hasColumn) match {
        case Some(p) =>
          columns += Column(p, f.dataType, None)
          columnByField(f.name.toLowerCase) = p
          refByField(f.name.toLowerCase) = p
        case None =>
          val sql = f.expr.getOrElse(f.name).trim
          val column = addColumn(f.name, sql, f.dataType)
          columnByField(f.name.toLowerCase) = column
          refByField(f.name.toLowerCase) = s"($sql)"
      }
    }

    def hasColumn(column: String): Boolean = columns.exists(_.name.equalsIgnoreCase(column))

    def field(fieldName: String): Option[Field] =
      fields.find(_.name.equalsIgnoreCase(fieldName.trim))

    /** Dataset column holding the field. */
    def column(fieldName: String): String = columnByField(fieldName.toLowerCase)

    /** SQL reference to the field: its physical column, or its expression in parentheses. */
    def ref(fieldName: String): String = refByField(fieldName.toLowerCase)

    /** Append a calculated column, suffixing `_sl` until the name is free. */
    def addColumn(base: String, sql: String, dataType: String): String = {
      var columnName = base
      while (hasColumn(columnName)) columnName = s"${columnName}_sl"
      columns += Column(columnName, dataType, Some(sql))
      columnName
    }

    /** Calculated column shared by every caller using the same key. */
    def generatedColumn(key: String, base: String, sql: String, dataType: String): String =
      generated.getOrElseUpdate(key.toLowerCase, addColumn(base, sql, dataType))

    lazy val primaryKey: List[Field] = {
      val names = elems(node.path("primary_key"), "columns").map(_.asText())
      val resolved = names.flatMap(field)
      if (resolved.size == names.size) resolved
      else {
        logger.warn(s"Table '$name': primary_key references unknown fields, ignored")
        Nil
      }
    }
  }

  def plan(modelName: String, model: JsonNode): Model = {
    val (tables, skippedHierarchies) = buildTables(modelName, model)
    Model(
      name = modelName,
      description = combinedDescription(model),
      connections = tables.map(connectionOf(modelName, _)).distinct,
      datasets = datasets(modelName, tables),
      dimensions = Nil,
      metrics = Nil,
      calculations = Nil,
      relationships = Nil,
      degenerateDimensions = Nil,
      usesTimeHierarchies = false,
      stats = Stats(skippedHierarchies, 0, 0, 0)
    )
  }

  private val TypeWithParams = """^([A-Z_][A-Z0-9_]*)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$""".r

  /** SML data type of a Starflow `data_type`; unknown or absent types map to string. */
  def smlDataType(raw: Option[String]): String =
    raw.map(_.trim.toUpperCase).filter(_.nonEmpty) match {
      case None => "string"
      case Some(t @ TypeWithParams(base, precision, scale)) =>
        base match {
          case "NUMBER" | "DECIMAL" | "NUMERIC" =>
            (Option(precision), Option(scale)) match {
              case (Some(p), Some(s)) => s"decimal($p,$s)"
              case (Some(p), None)    => s"decimal($p,0)"
              case _                  => "decimal"
            }
          case "INT" | "INTEGER" | "SMALLINT"         => "int"
          case "BIGINT"                               => "long"
          case "TINYINT"                              => "tinyint"
          case "FLOAT" | "REAL"                       => "float"
          case "DOUBLE"                               => "double"
          case "TEXT" | "STRING" | "VARCHAR" | "CHAR" => "string"
          case "BOOLEAN" | "BOOL"                     => "boolean"
          case "DATE"                                 => "date"
          case "DATETIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" =>
            "datetime"
          case _ =>
            logger.debug(s"Data type '$t' has no SML equivalent, exported as string")
            "string"
        }
      case Some(other) =>
        logger.debug(s"Data type '$other' has no SML equivalent, exported as string")
        "string"
    }

  private def fieldsOf(table: JsonNode): List[Field] = {
    def of(key: String, kind: String): List[Field] =
      elems(table, key).flatMap { n =>
        text(n, "name").map(name =>
          Field(name, kind, text(n, "expr"), smlDataType(text(n, "data_type")), n)
        )
      }
    of("dimensions", "dimension") ++ of("time_dimensions", "time") ++ of("facts", "fact")
  }

  private def isHidden(node: JsonNode): Boolean =
    text(node, "access_modifier").exists(_.equalsIgnoreCase("private_access"))

  /** Tables with their fields and validated hierarchies, plus the number of skipped hierarchies. */
  private def buildTables(modelName: String, model: JsonNode): (List[TableState], Int) = {
    val tables = elems(model, "tables").flatMap { t =>
      text(t, "name") match {
        case Some(n) => Some(new TableState(n, t, fieldsOf(t)))
        case None =>
          logger.warn(s"Model '$modelName': a table without name is skipped")
          None
      }
    }
    val skipped = tables.map { t =>
      val (hierarchies, skippedCount) = SMLHierarchies.parse(
        t.node,
        t.fields.map(_.name),
        t.fields.filter(_.kind == "time").map(_.name)
      )
      t.hierarchies = hierarchies
      if (t.node.has("filters"))
        logger.info(s"Table '${t.name}': filters have no SML equivalent, skipped")
      skippedCount
    }.sum
    if (model.has("verified_queries"))
      logger.info(s"Model '$modelName': verified_queries have no SML equivalent, skipped")
    (tables, skipped)
  }

  private def connectionOf(modelName: String, t: TableState): Connection = {
    val base = t.node.path("base_table")
    val database = text(base, "database")
    val schema = text(base, "schema")
    val suffix = (database, schema) match {
      case (Some(d), Some(s)) => s"$d.$s"
      case (Some(d), None)    => d
      case (None, Some(s))    => s
      case (None, None)       => "default"
    }
    Connection(s"$modelName - $suffix", database, schema)
  }

  private def datasets(modelName: String, tables: List[TableState]): List[Dataset] =
    tables.map { t =>
      Dataset(
        t.name,
        combinedDescription(t.node),
        connectionOf(modelName, t).name,
        text(t.node.path("base_table"), "table").getOrElse(t.name),
        t.columns.toList
      )
    }
}
