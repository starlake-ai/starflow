package ai.starlake.semantic

import ai.starlake.semantic.SMLMetricParser.{AggToken, AggregateCall}
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
    val (metrics, calculations, fallbacks) = planMetrics(model, tables)
    logUnusedFacts(model, tables)
    Model(
      name = modelName,
      description = combinedDescription(model),
      connections = tables.map(connectionOf(modelName, _)).distinct,
      datasets = datasets(modelName, tables),
      dimensions = Nil,
      metrics = metrics,
      calculations = calculations,
      relationships = Nil,
      degenerateDimensions = Nil,
      usesTimeHierarchies = false,
      stats = Stats(skippedHierarchies, 0, 0, fallbacks)
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

  private sealed trait Arg
  private case class FieldArg(column: String) extends Arg
  private case object StarArg extends Arg
  private case class ExprArg(sql: String) extends Arg

  private case class MetricInput(
    name: String,
    label: String,
    description: Option[String],
    hidden: Boolean,
    expr: String,
    owner: Option[TableState]
  )

  private val QualifierRef = """([A-Za-z_][A-Za-z0-9_]*)\.[A-Za-z_]""".r

  /** Table and argument kind of an aggregate call. The target is the table named by the argument's
    * qualifiers, else the owner. None when qualifiers name several tables, when no table is known,
    * or when the call targets another table and that is not allowed.
    */
  private def resolveCall(
    call: AggregateCall,
    owner: Option[TableState],
    tables: List[TableState],
    allowOtherTable: Boolean
  ): Option[(TableState, Arg)] = {
    val arg = call.arg.trim
    val qualified = QualifierRef
      .findAllMatchIn(arg)
      .map(_.group(1))
      .flatMap(q => tables.find(_.name.equalsIgnoreCase(q)))
      .toList
      .distinct
    val target = qualified match {
      case Nil                                                 => owner
      case t :: Nil if allowOtherTable || owner.exists(_ eq t) => Some(t)
      case _                                                   => None
    }
    target.map { t =>
      val kind =
        if (arg == "*") StarArg
        else {
          val bare = arg match {
            case QualifiedPattern(q, f) if q.equalsIgnoreCase(t.name) => Some(f)
            case IdentifierPattern()                                  => Some(arg)
            case _                                                    => None
          }
          bare.flatMap(t.field) match {
            case Some(f) => FieldArg(t.column(f.name))
            case None    => ExprArg(SMLMetricParser.substitute(arg, fieldRef(t, _)))
          }
        }
      (t, kind)
    }
  }

  /** SQL reference of a bare or table-qualified field token of `t`. */
  private def fieldRef(t: TableState, token: String): Option[String] = {
    val fieldName = token match {
      case QualifiedPattern(q, f) if q.equalsIgnoreCase(t.name) => Some(f)
      case QualifiedPattern(_, _)                               => None
      case other                                                => Some(other)
    }
    fieldName.flatMap(t.field).map(f => t.ref(f.name))
  }

  /** Column and calculation method of a resolved call; may add generated columns. */
  private def materialize(
    call: AggregateCall,
    target: TableState,
    arg: Arg,
    columnBase: String
  ): (String, String) =
    arg match {
      case FieldArg(column) => (column, call.method)
      case StarArg =>
        target.primaryKey match {
          case pk :: Nil => (target.column(pk.name), "count non-null")
          case _ =>
            (target.generatedColumn("_sl_row_count", "_sl_row_count", "1", "int"), "sum")
        }
      case ExprArg(sql) => (target.addColumn(columnBase, sql, "double"), call.method)
    }

  /** Metrics, calculations and the number of metrics exported as NULL calculations. */
  private def planMetrics(
    model: JsonNode,
    tables: List[TableState]
  ): (List[Metric], List[Calculation], Int) = {
    val tableLevel = tables.flatMap(t => elems(t.node, "metrics").map(m => (m, t)))
    val owned = assignModelMetrics(model, tables.map(_.name), _.toLowerCase).toList.flatMap {
      case (key, entries) =>
        entries.collect { case (m, true) => (m, tables.find(_.name.toLowerCase == key).get) }
    }
    val modelLevel = elems(model, "metrics").map(m => (m, owned.find(_._1 eq m).map(_._2)))
    val allNames =
      (tableLevel.map(_._1) ++ modelLevel.map(_._1)).map(text(_, "name").getOrElse("").toLowerCase)

    def input(m: JsonNode, owner: Option[TableState], prefix: Option[String]): Option[MetricInput] =
      (text(m, "name"), text(m, "expr")) match {
        case (Some(name), Some(expr)) =>
          val unique = prefix match {
            case Some(p) if allNames.count(_ == name.toLowerCase) > 1 => s"${p}_$name"
            case _                                                    => name
          }
          Some(MetricInput(unique, name, combinedDescription(m), isHidden(m), expr, owner))
        case _ =>
          logger.warn(s"Metric without name or expr skipped: $m")
          None
      }
    val inputs =
      tableLevel.flatMap { case (m, t) => input(m, Some(t), Some(t.name)) } ++
      modelLevel.flatMap { case (m, owner) => input(m, owner, None) }

    val metrics = ArrayBuffer[Metric]()
    val calculations = ArrayBuffer[Calculation]()
    var fallbacks = 0
    inputs.foreach { in =>
      val native = for {
        owner         <- in.owner
        call          <- SMLMetricParser.parseCall(in.expr)
        (target, arg) <- resolveCall(call, Some(owner), tables, allowOtherTable = false)
      } yield {
        val (column, method) = materialize(call, target, arg, s"_sl_${in.name}")
        Metric(in.name, in.label, in.description, target.name, column, method, in.hidden)
      }
      native match {
        case Some(metric) => metrics += metric
        case None =>
          val arithmetic = SMLMetricParser.decompose(in.expr).flatMap { tokens =>
            val resolved = tokens.collect { case AggToken(c) => c }.map { c =>
              resolveCall(c, in.owner, tables, allowOtherTable = true).map(r => (c, r))
            }
            if (resolved.forall(_.isDefined)) Some((tokens, resolved.flatten)) else None
          }
          arithmetic match {
            case Some((tokens, resolved)) =>
              val names = resolved.zipWithIndex.map { case ((call, (target, arg)), i) =>
                val baseName = s"_sl_${in.name}_${i + 1}"
                val (column, method) = materialize(call, target, arg, baseName)
                metrics += Metric(
                  baseName,
                  baseName,
                  None,
                  target.name,
                  column,
                  method,
                  hidden = true
                )
                baseName
              }
              calculations += Calculation(
                in.name,
                in.label,
                in.description,
                SMLMetricParser.render(tokens, names),
                in.hidden
              )
            case None =>
              fallbacks += 1
              val todo = s"TODO Starflow: translate original SQL to MDX: ${in.expr}"
              calculations += Calculation(
                in.name,
                in.label,
                Some(in.description.fold(todo)(d => s"$d. $todo")),
                "NULL",
                in.hidden
              )
          }
      }
    }
    (metrics.toList, calculations.toList, fallbacks)
  }

  /** Facts that no metric expression mentions produce no SML metric; say so once per fact. */
  private def logUnusedFacts(model: JsonNode, tables: List[TableState]): Unit = {
    val exprs =
      (tables.flatMap(t => elems(t.node, "metrics")) ++ elems(model, "metrics"))
        .flatMap(text(_, "expr"))
    tables.foreach { t =>
      t.fields.filter(_.kind == "fact").foreach { f =>
        val word = ("(?i)\\b" + java.util.regex.Pattern.quote(f.name) + "\\b").r
        if (!exprs.exists(e => word.findFirstIn(e).isDefined))
          logger.info(
            s"Table '${t.name}': fact '${f.name}' is not used by any metric, no SML metric generated"
          )
      }
    }
  }
}
