package ai.starlake.semantic

import ai.starlake.semantic.SMLHierarchies.{GeneralHierarchy, TimeHierarchy}
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
    val (relationships, skippedRelationships) = parseRelationships(model, tables)
    val (metrics, calculations, fallbacks, assigned) = planMetrics(model, tables)
    logUnusedFacts(model, tables)

    // A metric assigned to a table gives it the fact role even when it is exported as a NULL
    // calculation; hidden metrics of calculations give it to the tables they aggregate.
    val factTables = tables.filter { t =>
      t.fields.exists(_.kind == "fact") ||
      assigned.contains(t.name) ||
      metrics.exists(_.dataset == t.name)
    }.toSet
    val targets = relationships.map(_.right).toSet
    val dimensionTables = tables.filter { t =>
      targets.contains(t) ||
      (!factTables.contains(t) && t.hierarchies.exists(_.isInstanceOf[GeneralHierarchy]))
    }.toSet
    tables
      .filterNot(t => factTables.contains(t) || dimensionTables.contains(t))
      .foreach(t =>
        logger.warn(s"Table '${t.name}' has no fact or dimension role, exported as a dataset only")
      )

    val dims = planDimensions(tables, relationships, factTables, dimensionTables)
    val routing = routeRelationships(tables, relationships, factTables, dims.regular)
    val dimensions = tables.flatMap { t =>
      dims.regular
        .get(t.name)
        .map { case (d, _) => d.copy(relationships = routing.embedded.getOrElse(t.name, Nil)) }
        .toList ++ dims.degenerate.getOrElse(t.name, Nil)
    }
    val stats = Stats(
      skippedHierarchies + dims.skippedHierarchies,
      dims.skippedDimensions,
      skippedRelationships + routing.skipped,
      fallbacks
    )
    logger.info(
      s"Model '$modelName': SML export skipped ${stats.skippedHierarchies} hierarchies, " +
      s"${stats.skippedDimensions} dimensions, ${stats.skippedRelationships} relationships; " +
      s"${stats.fallbackCalculations} metrics exported as NULL calculations"
    )
    SMLNames.uniquify(
      Model(
        name = modelName,
        description = combinedDescription(model),
        connections = tables.map(connectionOf(modelName, _)).distinct,
        datasets = datasets(modelName, tables),
        dimensions = dimensions,
        metrics = metrics,
        calculations = calculations,
        relationships = routing.model,
        degenerateDimensions = dimensions.filter(_.isDegenerate).map(_.name),
        usesTimeHierarchies = dimensions.exists(_.isTime),
        stats = stats
      )
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
        text(n, "name")
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(name => Field(name, kind, text(n, "expr"), smlDataType(text(n, "data_type")), n))
      }
    of("dimensions", "dimension") ++ of("time_dimensions", "time") ++ of("facts", "fact")
  }

  private def isHidden(node: JsonNode): Boolean =
    text(node, "access_modifier").exists(_.equalsIgnoreCase("private_access"))

  /** Tables with their fields and validated hierarchies, plus the number of skipped hierarchies. */
  private def buildTables(modelName: String, model: JsonNode): (List[TableState], Int) = {
    val named = elems(model, "tables").flatMap { t =>
      text(t, "name").map(_.trim).filter(_.nonEmpty) match {
        case Some(n) => Some((n, t))
        case None =>
          logger.warn(s"Model '$modelName': a table without name is skipped")
          None
      }
    }
    val tables = named.zip(SMLNames.unique("dataset", named.map(_._1))).map { case ((_, t), name) =>
      new TableState(name, t, fieldsOf(t))
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
        text(t.node.path("base_table"), "table")
          .orElse(text(t.node, "name").map(_.trim))
          .getOrElse(t.name),
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

  /** Metrics, calculations, the number of metrics exported as NULL calculations and the names of
    * the tables owning at least one metric definition.
    */
  private def planMetrics(
    model: JsonNode,
    tables: List[TableState]
  ): (List[Metric], List[Calculation], Int, Set[String]) = {
    val tableLevel = tables.flatMap(t => elems(t.node, "metrics").map(m => (m, t)))
    val owned = assignModelMetrics(model, tables.map(_.name), _.toLowerCase).toList.flatMap {
      case (key, entries) =>
        entries.collect { case (m, true) => m }.flatMap { m =>
          tables.find(_.name.toLowerCase == key).map(t => (m, t))
        }
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
    val measures = new SMLNames.Namespace("measure")
    var fallbacks = 0
    inputs.foreach { in =>
      val native = for {
        owner         <- in.owner
        call          <- SMLMetricParser.parseCall(in.expr)
        (target, arg) <- resolveCall(call, Some(owner), tables, allowOtherTable = false)
      } yield {
        val (column, method) = materialize(call, target, arg, s"_sl_${in.name}")
        val name = measures.claim(in.name)
        Metric(name, in.label, in.description, target.name, column, method, in.hidden)
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
                val name = measures.claim(baseName)
                metrics += Metric(
                  name,
                  name,
                  None,
                  target.name,
                  column,
                  method,
                  hidden = true
                )
                name
              }
              calculations += Calculation(
                measures.claim(in.name),
                in.label,
                in.description,
                SMLMetricParser.render(tokens, names),
                in.hidden
              )
            case None =>
              fallbacks += 1
              val todo = s"TODO Starflow: translate original SQL to MDX: ${in.expr}"
              calculations += Calculation(
                measures.claim(in.name),
                in.label,
                Some(in.description.fold(todo) { d =>
                  if (d.endsWith(".")) s"$d $todo" else s"$d. $todo"
                }),
                "NULL",
                in.hidden
              )
          }
      }
    }
    (metrics.toList, calculations.toList, fallbacks, inputs.flatMap(_.owner.map(_.name)).toSet)
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

  private case class Rel(
    name: String,
    left: TableState,
    right: TableState,
    pairs: List[(Field, Field)]
  )

  /** Valid relationships and the number of skipped ones. */
  private def parseRelationships(model: JsonNode, tables: List[TableState]): (List[Rel], Int) = {
    var skipped = 0
    val rels = elems(model, "relationships").flatMap { rel =>
      val left = text(rel, "left_table").flatMap(l => tables.find(_.name.equalsIgnoreCase(l.trim)))
      val right =
        text(rel, "right_table").flatMap(r => tables.find(_.name.equalsIgnoreCase(r.trim)))
      val name = text(rel, "name").map(_.trim).filter(_.nonEmpty).getOrElse {
        s"${text(rel, "left_table").fold("")(_.trim)}_to_${text(rel, "right_table").fold("")(_.trim)}"
      }
      def skip(reason: String): Option[Rel] = {
        logger.warn(s"Relationship '$name' skipped, $reason")
        skipped += 1
        None
      }
      text(rel, "relationship_type")
        .filterNot(t => t.equalsIgnoreCase("many_to_one") || t.equalsIgnoreCase("one_to_one"))
        .foreach { t =>
          logger.warn(
            s"Relationship '$name': relationship_type '$t' has no SML mapping, handled as many_to_one"
          )
        }
      val columns = elems(rel, "relationship_columns").map { rc =>
        (text(rc, "left_column").getOrElse(""), text(rc, "right_column").getOrElse(""))
      }
      (left, right) match {
        case (Some(l), Some(r)) =>
          if (columns.isEmpty) skip("it has no relationship_columns")
          else {
            val pairs = columns.map { case (lc, rc) => (l.field(lc), r.field(rc)) }
            if (pairs.exists { case (a, b) => a.isEmpty || b.isEmpty })
              skip("a relationship column is not a field of its table")
            else Some(Rel(name, l, r, pairs.map { case (a, b) => (a.get, b.get) }))
          }
        case _ => skip("left_table or right_table is not a table of the model")
      }
    }
    (rels, skipped)
  }

  private def fieldLevel(t: TableState, fieldName: String, key: List[String]): LevelAttribute = {
    val f = t.field(fieldName).get
    LevelAttribute(
      s"${t.name} ${f.name}",
      f.name,
      combinedDescription(f.node),
      t.name,
      key.map(t.column),
      t.column(f.name),
      t.column(f.name),
      isUniqueKey = false,
      timeUnit = None,
      hidden = isHidden(f.node)
    )
  }

  /** The dimension of a dimension-role table, its leaf key fields and the number of general
    * hierarchies skipped because their leaf key field is not their last level; None without a key.
    */
  private def regularDimension(
    t: TableState,
    relationships: List[Rel]
  ): Option[(Dimension, List[Field], Int)] = {
    val keyFields =
      if (t.primaryKey.nonEmpty) t.primaryKey
      else {
        val fromRelationships = relationships.filter(_.right eq t).map(_.pairs.map(_._2))
        if (fromRelationships.map(_.map(_.name.toLowerCase)).distinct.size > 1)
          logger.warn(
            s"Table '${t.name}': relationships target different key columns, using those of the first relationship"
          )
        fromRelationships.headOption.getOrElse(Nil)
      }
    if (keyFields.isEmpty) {
      logger.warn(s"Table '${t.name}': no primary key nor relationship key, dimension skipped")
      None
    } else {
      val leafField = keyFields.last
      val leaf = LevelAttribute(
        s"${t.name} ${leafField.name}",
        leafField.name,
        combinedDescription(leafField.node),
        t.name,
        keyFields.map(f => t.column(f.name)),
        t.column(leafField.name),
        t.column(leafField.name),
        isUniqueKey = true,
        timeUnit = None,
        hidden = isHidden(leafField.node)
      )
      val attributes = mutable.LinkedHashMap[String, LevelAttribute]()
      val general = t.hierarchies.collect { case g: GeneralHierarchy => g }
      var skippedGeneral = 0
      val validGeneral = general.filter { g =>
        val leafIndex = g.levels.indexWhere(_.field.equalsIgnoreCase(leafField.name))
        val ok = leafIndex < 0 || leafIndex == g.levels.size - 1
        if (!ok) {
          logger.warn(
            s"Table '${t.name}': general hierarchy '${g.name}' skipped, leaf key field " +
            s"'${leafField.name}' is not its last level"
          )
          skippedGeneral += 1
        }
        ok
      }
      val hierarchies =
        if (validGeneral.isEmpty)
          List(Hierarchy(s"${t.name} Hierarchy", t.name, None, List(leaf.name)))
        else
          validGeneral.map { g =>
            val levelNames = g.levels.map { l =>
              val attribute =
                if (l.field.equalsIgnoreCase(leafField.name)) leaf
                else fieldLevel(t, l.field, l.key)
              attributes.getOrElseUpdate(attribute.name, attribute).name
            }
            val withLeaf =
              if (levelNames.last == leaf.name) levelNames else levelNames :+ leaf.name
            Hierarchy(s"${t.name} ${g.name} Hierarchy", g.name, g.description, withLeaf)
          }
      attributes.getOrElseUpdate(leaf.name, leaf)
      val levelFields =
        validGeneral.flatMap(_.levels.map(_.field.toLowerCase)).toSet + leafField.name.toLowerCase
      val secondary = t.fields
        .filter(f => f.kind != "fact" && !levelFields.contains(f.name.toLowerCase))
        .map { f =>
          SecondaryAttribute(
            s"${t.name} ${f.name}",
            f.name,
            combinedDescription(f.node),
            t.name,
            t.column(f.name),
            isHidden(f.node)
          )
        }
      val dimension = Dimension(
        s"${t.name} Dimension",
        t.name,
        combinedDescription(t.node),
        isTime = false,
        isDegenerate = false,
        attributes.values.toList,
        hierarchies,
        leaf.name,
        secondary,
        Nil
      )
      Some((dimension, keyFields, skippedGeneral))
    }
  }

  private def degenerateHierarchy(t: TableState, g: GeneralHierarchy): Dimension = {
    val levels = g.levels.map(l => fieldLevel(t, l.field, l.key))
    Dimension(
      s"${t.name} ${g.name} Dimension",
      g.name,
      g.description,
      isTime = false,
      isDegenerate = true,
      levels,
      List(Hierarchy(s"${t.name} ${g.name} Hierarchy", g.name, g.description, levels.map(_.name))),
      levels.last.name,
      Nil,
      Nil
    )
  }

  /** Degenerate time dimension; generates the EXTRACT and CAST columns its levels need. */
  private def timeDimension(t: TableState, h: TimeHierarchy): Dimension = {
    val f = t.field(h.time).get
    val c = t.ref(f.name)
    def gen(unit: String, sql: String, dataType: String): String =
      t.generatedColumn(s"${f.name}:$unit", s"${f.name}_$unit", sql, dataType)
    val year =
      if (h.units.exists(_ != "day")) Some(gen("year", s"EXTRACT(YEAR FROM $c)", "int")) else None
    val levels = h.units.map { unit =>
      val (keys, column) = unit match {
        case "year" => (List(year.get), year.get)
        case "quarter" =>
          val q = gen("quarter", s"EXTRACT(QUARTER FROM $c)", "int")
          (List(year.get, q), q)
        case "month" =>
          val m = gen("month", s"EXTRACT(MONTH FROM $c)", "int")
          (List(year.get, m), m)
        case _ =>
          val d =
            if (f.dataType == "date") t.column(f.name)
            else gen("day", s"CAST($c AS DATE)", "date")
          (List(d), d)
      }
      LevelAttribute(
        s"${t.name} ${h.name} $unit",
        s"${f.name} $unit",
        None,
        t.name,
        keys,
        column,
        column,
        isUniqueKey = false,
        timeUnit = Some(unit),
        hidden = false
      )
    }
    Dimension(
      s"${t.name} ${h.name} Dimension",
      h.name,
      h.description,
      isTime = true,
      isDegenerate = true,
      levels,
      List(Hierarchy(s"${t.name} ${h.name} Hierarchy", h.name, h.description, levels.map(_.name))),
      levels.last.name,
      Nil,
      Nil
    )
  }

  /** One single-level degenerate dimension per plain field of a fact-only table. */
  private def plainFieldDimensions(
    t: TableState,
    general: List[GeneralHierarchy],
    time: List[TimeHierarchy],
    relationships: List[Rel]
  ): List[Dimension] = {
    val used = general.flatMap(_.levels.map(_.field.toLowerCase)).toSet ++
      time.map(_.time.toLowerCase) ++
      relationships.filter(_.left eq t).flatMap(_.pairs.map(_._1.name.toLowerCase))
    t.fields.filter(f => f.kind != "fact" && !used.contains(f.name.toLowerCase)).map { f =>
      val level = fieldLevel(t, f.name, List(f.name))
      Dimension(
        s"${t.name} ${f.name} Dimension",
        f.name,
        combinedDescription(f.node),
        isTime = false,
        isDegenerate = true,
        List(level),
        List(Hierarchy(s"${t.name} ${f.name} Hierarchy", f.name, None, List(level.name))),
        level.name,
        Nil,
        Nil
      )
    }
  }

  private case class DimensionPlan(
    regular: Map[String, (Dimension, List[Field])],
    degenerate: Map[String, List[Dimension]],
    skippedHierarchies: Int,
    skippedDimensions: Int
  )

  private def planDimensions(
    tables: List[TableState],
    relationships: List[Rel],
    factTables: Set[TableState],
    dimensionTables: Set[TableState]
  ): DimensionPlan = {
    var skippedHierarchies = 0
    var skippedDimensions = 0
    val regular = mutable.Map[String, (Dimension, List[Field])]()
    val degenerate = mutable.Map[String, List[Dimension]]()
    tables.foreach { t =>
      val isFact = factTables.contains(t)
      val isDimension = dimensionTables.contains(t)
      if (isDimension)
        regularDimension(t, relationships) match {
          case Some((d, keyFields, skippedGeneral)) =>
            regular(t.name) = (d, keyFields)
            skippedHierarchies += skippedGeneral
          case None => skippedDimensions += 1
        }
      val general = t.hierarchies.collect { case g: GeneralHierarchy => g }
      val time = t.hierarchies.collect { case h: TimeHierarchy => h }
      val built = ArrayBuffer[Dimension]()
      if (isFact && !isDimension) general.foreach(g => built += degenerateHierarchy(t, g))
      time.foreach { h =>
        if (isFact) built += timeDimension(t, h)
        else {
          logger.warn(
            s"Table '${t.name}': time hierarchy '${h.name}' skipped, time hierarchies are only supported on fact tables"
          )
          skippedHierarchies += 1
        }
      }
      if (isFact && !isDimension) built ++= plainFieldDimensions(t, general, time, relationships)
      degenerate(t.name) = built.toList
    }
    DimensionPlan(regular.toMap, degenerate.toMap, skippedHierarchies, skippedDimensions)
  }

  private case class Routing(
    model: List[ModelRelationship],
    embedded: Map[String, List[EmbeddedRelationship]],
    skipped: Int
  )

  private def routeRelationships(
    tables: List[TableState],
    relationships: List[Rel],
    factTables: Set[TableState],
    regular: Map[String, (Dimension, List[Field])]
  ): Routing = {
    var skipped = 0
    val modelRelationships = ArrayBuffer[ModelRelationship]()
    val embedded = mutable.Map[String, List[EmbeddedRelationship]]()
    val selfReferencing = mutable.Set[String]()
    relationships.foreach { r =>
      regular.get(r.right.name) match {
        case None =>
          logger.warn(s"Relationship '${r.name}' skipped, table '${r.right.name}' has no dimension")
          skipped += 1
        case Some((rightDim, rightKey)) =>
          joinColumns(r, rightDim, rightKey) match {
            case None => skipped += 1
            case Some(columns) =>
              val leftDim = regular.get(r.left.name).map(_._1)
              val isSelfReferencing = r.left eq r.right
              val isFact = factTables.contains(r.left)
              // A table with both roles reaches its embedded relationships through its self link;
              // a direct model relationship would give AtScale two paths to the same dimension.
              if (isFact && (isSelfReferencing || leftDim.isEmpty))
                modelRelationships += ModelRelationship(
                  r.name,
                  r.left.name,
                  columns,
                  rightDim.name,
                  rightDim.leafLevel,
                  None
                )
              if (isSelfReferencing) {
                // A dimension must not embed itself: the self-referencing relationship is exported
                // as a role-played model relationship only, and only when the table has a fact role.
                if (isFact) {
                  selfReferencing += r.name
                  logger.warn(
                    s"Relationship '${r.name}' is self-referencing, exported as a role-played model relationship only"
                  )
                } else {
                  logger.warn(
                    s"Relationship '${r.name}' skipped, self-referencing relationships require table '${r.left.name}' to have a fact role"
                  )
                  skipped += 1
                }
              } else {
                leftDim.foreach { d =>
                  embedded(r.left.name) =
                    embedded.getOrElse(r.left.name, Nil) :+ EmbeddedRelationship(
                      r.name,
                      r.left.name,
                      columns,
                      d.hierarchies.headOption.map(_.name).getOrElse(""),
                      d.leafLevel,
                      rightDim.name,
                      rightDim.leafLevel,
                      None
                    )
                }
                if (!isFact && leftDim.isEmpty) {
                  logger.warn(
                    s"Relationship '${r.name}' skipped, table '${r.left.name}' has neither metrics nor a dimension"
                  )
                  skipped += 1
                }
              }
          }
      }
    }
    val userRelationships = rolePlayModel(modelRelationships.toList).map { r =>
      if (selfReferencing.contains(r.name)) r.copy(rolePlay = Some(s"${r.name} {0}")) else r
    }
    // Generated self links never take part in role-play grouping and are appended last, in
    // table order, after role play has been resolved for user relationships.
    val selfLinks = tables.filter(factTables.contains).flatMap { t =>
      regular.get(t.name).map { case (d, key) =>
        ModelRelationship(
          s"${t.name}_self",
          t.name,
          key.map(f => t.column(f.name)),
          d.name,
          d.leafLevel,
          None
        )
      }
    }
    Routing(
      userRelationships ++ selfLinks,
      embedded.view.mapValues(rolePlayEmbedded).toMap,
      skipped
    )
  }

  /** Left columns of `r` in the order of the right dimension's leaf key: reordered when the right
    * columns are a permutation of the key (AtScale pairs join columns with key columns by
    * position), kept with a warning when they are other columns, None (logged) when their count
    * differs.
    */
  private def joinColumns(
    r: Rel,
    rightDim: Dimension,
    rightKey: List[Field]
  ): Option[List[String]] = {
    val rightNames = r.pairs.map(_._2.name.toLowerCase)
    val keyNames = rightKey.map(_.name.toLowerCase)
    val leftColumns = r.pairs.map { case (l, _) => r.left.column(l.name) }
    if (rightNames.size != keyNames.size) {
      logger.warn(
        s"Relationship '${r.name}' skipped, it has ${rightNames.size} columns but the leaf key of " +
        s"'${rightDim.name}' has ${keyNames.size}"
      )
      None
    } else if (rightNames == keyNames) Some(leftColumns)
    else if (rightNames.sorted == keyNames.sorted && rightNames.distinct.size == rightNames.size)
      Some(keyNames.map(k => leftColumns(rightNames.indexOf(k))))
    else {
      logger.warn(
        s"Relationship '${r.name}': right columns are not the leaf key of '${rightDim.name}'"
      )
      Some(leftColumns)
    }
  }

  private def rolePlayModel(relationships: List[ModelRelationship]): List[ModelRelationship] = {
    val counts =
      relationships.groupBy(r => (r.dataset, r.toDimension)).view.mapValues(_.size).toMap
    relationships.map { r =>
      if (counts((r.dataset, r.toDimension)) > 1) r.copy(rolePlay = Some(s"${r.name} {0}")) else r
    }
  }

  private def rolePlayEmbedded(
    relationships: List[EmbeddedRelationship]
  ): List[EmbeddedRelationship] = {
    val counts = relationships.groupBy(_.toDimension).view.mapValues(_.size).toMap
    relationships.map { r =>
      if (counts(r.toDimension) > 1) r.copy(rolePlay = Some(s"${r.name} {0}")) else r
    }
  }
}
