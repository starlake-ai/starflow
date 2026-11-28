package ai.starlake.semantic

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}

import scala.collection.mutable

/** Converts Snowflake-style semantic models to an AtScale SML repository (spec 1.8): catalog,
  * connections, datasets, dimensions, metrics, calculations and one model file. Planning lives in
  * [[SMLPlanner]]; this object only renders the plan as YAML.
  */
object SMLConverter {

  import SMLPlan._

  private val SmlVersion = new java.math.BigDecimal("1.8")

  /** No document start marker, minimal quotes, numeric-looking strings kept as strings. */
  val yaml: ObjectMapper = new ObjectMapper(
    YAMLFactory
      .builder()
      .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
      .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
      .enable(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS)
      .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR)
      .build()
  )

  /** Relative file paths and YAML contents of the SML repository of one model. */
  def convert(modelName: String, model: JsonNode, asConnection: String): Seq[(String, String)] =
    render(SMLPlanner.plan(modelName, model), asConnection)

  def render(plan: Model, asConnection: String): Seq[(String, String)] = {
    def files(folder: String, nodes: List[(String, ObjectNode)]): List[(String, String)] =
      fileNames(nodes.map(_._1)).zip(nodes).map { case (file, (_, node)) =>
        s"$folder/$file" -> yaml.writeValueAsString(node)
      }
    List("catalog.yml" -> yaml.writeValueAsString(catalog(plan))) ++
    files("connections", plan.connections.map(c => c.name -> connection(c, asConnection))) ++
    files("datasets", plan.datasets.map(d => d.name -> dataset(d))) ++
    files("dimensions", plan.dimensions.map(d => d.name -> dimension(d))) ++
    files("metrics", plan.metrics.map(m => m.name -> metric(m))) ++
    files("calculations", plan.calculations.map(c => c.name -> calculation(c))) ++
    files("models", List(plan.name -> modelNode(plan)))
  }

  private val Unsafe = """[^A-Za-z0-9 _.-]""".r

  /** One file name per unique name: unsafe characters replaced by '_', case-insensitive collisions
    * suffixed with _2, _3, ...
    */
  def fileNames(names: List[String]): List[String] = {
    val used = mutable.Set[String]()
    names.map { name =>
      val base = Unsafe.replaceAllIn(name, "_")
      val candidate = Iterator
        .from(1)
        .map(i => if (i == 1) base else s"${base}_$i")
        .find(c => !used.contains(c.toLowerCase))
        .get
      used += candidate.toLowerCase
      s"$candidate.yml"
    }
  }

  private def node(
    uniqueName: String,
    objectType: String,
    label: String,
    description: Option[String]
  ): ObjectNode = {
    val n = yaml.createObjectNode()
    n.put("unique_name", uniqueName)
    n.put("object_type", objectType)
    n.put("label", label)
    description.foreach(n.put("description", _))
    n
  }

  private def strings(n: ObjectNode, key: String, values: List[String]): Unit = {
    val array = n.putArray(key)
    values.foreach(array.add)
  }

  private def flag(n: ObjectNode, key: String, value: Boolean): Unit =
    if (value) n.put(key, true)

  private def catalog(plan: Model): ObjectNode = {
    val n = node(plan.name, "catalog", plan.name, plan.description)
    n.put("version", SmlVersion)
    n.put("aggressive_agg_promotion", false)
    n.put("build_speculative_aggs", false)
    n
  }

  private def connection(c: Connection, asConnection: String): ObjectNode = {
    val n = node(c.name, "connection", c.name, None)
    n.put("as_connection", asConnection)
    c.database.foreach(n.put("database", _))
    c.schema.foreach(n.put("schema", _))
    n
  }

  private def dataset(d: Dataset): ObjectNode = {
    val n = node(d.name, "dataset", d.name, d.description)
    n.put("connection_id", d.connection)
    n.put("table", d.table)
    val columns = n.putArray("columns")
    d.columns.foreach { c =>
      val cn = columns.addObject()
      cn.put("name", c.name)
      cn.put("data_type", c.dataType)
      c.sql.foreach(cn.put("sql", _))
    }
    n
  }

  private def dimension(d: Dimension): ObjectNode = {
    val n = node(d.name, "dimension", d.label, d.description)
    n.put("type", if (d.isTime) "time" else "standard")
    flag(n, "is_degenerate", d.isDegenerate)
    val hierarchies = n.putArray("hierarchies")
    d.hierarchies.zipWithIndex.foreach { case (h, i) =>
      val hn = hierarchies.addObject()
      hn.put("unique_name", h.name)
      hn.put("label", h.label)
      h.description.foreach(hn.put("description", _))
      val levels = hn.putArray("levels")
      h.levels.foreach { level =>
        val ln = levels.addObject()
        ln.put("unique_name", level)
        if (i == 0 && level == d.leafLevel && d.secondaryAttributes.nonEmpty) {
          val attributes = ln.putArray("secondary_attributes")
          d.secondaryAttributes.foreach { s =>
            val sn = attributes.addObject()
            sn.put("unique_name", s.name)
            sn.put("label", s.label)
            s.description.foreach(sn.put("description", _))
            sn.put("dataset", s.dataset)
            strings(sn, "key_columns", List(s.column))
            sn.put("name_column", s.column)
            sn.put("sort_column", s.column)
            flag(sn, "is_hidden", s.hidden)
          }
        }
      }
    }
    val attributes = n.putArray("level_attributes")
    d.levelAttributes.foreach { a =>
      val an = attributes.addObject()
      an.put("unique_name", a.name)
      an.put("label", a.label)
      a.description.foreach(an.put("description", _))
      an.put("dataset", a.dataset)
      strings(an, "key_columns", a.keyColumns)
      an.put("name_column", a.nameColumn)
      an.put("sort_column", a.sortColumn)
      flag(an, "is_unique_key", a.isUniqueKey)
      a.timeUnit.foreach(an.put("time_unit", _))
      flag(an, "is_hidden", a.hidden)
    }
    if (d.relationships.nonEmpty) {
      val relationships = n.putArray("relationships")
      d.relationships.foreach { r =>
        val rn = relationships.addObject()
        rn.put("unique_name", r.name)
        val from = rn.putObject("from")
        from.put("dataset", r.dataset)
        strings(from, "join_columns", r.joinColumns)
        from.put("hierarchy", r.hierarchy)
        from.put("level", r.level)
        val to = rn.putObject("to")
        to.put("dimension", r.toDimension)
        to.put("level", r.toLevel)
        rn.put("type", "embedded")
        r.rolePlay.foreach(rn.put("role_play", _))
      }
    }
    n
  }

  private def metric(m: Metric): ObjectNode = {
    val n = node(m.name, "metric", m.label, m.description)
    n.put("calculation_method", m.method)
    n.put("dataset", m.dataset)
    n.put("column", m.column)
    flag(n, "is_hidden", m.hidden)
    n
  }

  private def calculation(c: Calculation): ObjectNode = {
    val n = node(c.name, "metric_calc", c.label, c.description)
    n.put("expression", c.expression)
    flag(n, "is_hidden", c.hidden)
    n
  }

  private def modelNode(plan: Model): ObjectNode = {
    val n = node(plan.name, "model", plan.name, plan.description)
    val relationships = n.putArray("relationships")
    plan.relationships.foreach { r =>
      val rn = relationships.addObject()
      rn.put("unique_name", r.name)
      val from = rn.putObject("from")
      from.put("dataset", r.dataset)
      strings(from, "join_columns", r.joinColumns)
      val to = rn.putObject("to")
      to.put("dimension", r.toDimension)
      to.put("level", r.toLevel)
      r.rolePlay.foreach(rn.put("role_play", _))
    }
    val metrics = n.putArray("metrics")
    (plan.metrics.map(_.name) ++ plan.calculations.map(_.name)).foreach { name =>
      metrics.addObject().put("unique_name", name)
    }
    if (plan.degenerateDimensions.nonEmpty) strings(n, "dimensions", plan.degenerateDimensions)
    n
  }
}
