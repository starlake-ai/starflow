package ai.starlake.semantic

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.utils.{JobResult, Utils, YamlSerde}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._
import scala.util.Try

/** Exports semantic models stored in metadata/semantic/ to the Apache Ossie (incubating)
  * interchange format, a LookML project, a Power BI TMDL folder or an AtScale SML repository.
  *
  * Input models follow the Snowflake-style semantic model layout (tables with dimensions /
  * time_dimensions / facts / metrics / filters, relationships, model-level metrics,
  * verified_queries). Attributes with no Ossie equivalent are preserved verbatim in
  * `custom_extensions` blocks under the STARLAKE vendor name.
  */
class SemanticExportJob(config: SemanticExportConfig)(implicit settings: Settings)
    extends LazyLogging {

  private val storage = settings.storageHandler()

  def run(): Try[JobResult] = Try {
    val semanticPath = DatasetArea.semantic
    val modelFiles =
      (storage.list(semanticPath, ".yaml", recursive = false) ++
        storage.list(semanticPath, ".yml", recursive = false)).map(_.path).distinct

    val models = modelFiles.map { path =>
      val node = YamlSerde.mapper.readTree(storage.read(path))
      val basename = path.getName.replaceAll("\\.ya?ml$", "")
      val name = Option(node.path("name").asText(null)).filter(_.nonEmpty).getOrElse(basename)
      (path, basename, name, node)
    }

    val selected = config.model match {
      case Some(m) =>
        val hits = models.filter { case (_, basename, name, _) => m == name || m == basename }
        if (hits.isEmpty)
          throw new IllegalArgumentException(
            s"Semantic model '$m' not found in $semanticPath. " +
            s"Available models: ${models.map(_._3).mkString(", ")}"
          )
        hits
      case None => models
    }

    if (selected.isEmpty)
      logger.warn(s"No semantic model found in $semanticPath, nothing to export")

    val outputDir = config.output
      .map(new Path(_))
      .getOrElse(new Path(semanticPath, s"export/${config.format}"))
    storage.mkdirs(outputDir)

    selected.foreach { case (path, _, name, node) =>
      def writeAll(files: Seq[(String, String)]): Unit = {
        val modelDir = new Path(outputDir, name)
        storage.mkdirs(modelDir)
        files.foreach { case (relativePath, content) =>
          val target = new Path(modelDir, relativePath)
          storage.write(content, target)
          logger.info(s"Exported semantic model '$name' ($path) to $target")
        }
      }
      config.format match {
        case "lookml" =>
          val connection = config.connection.getOrElse(settings.appConfig.connectionRef)
          writeAll(LookMLConverter.convert(name, node, connection))
        case "tmdl" =>
          val connectionName = config.connection.getOrElse(settings.appConfig.connectionRef)
          val connectionInfo = settings.appConfig.connections.get(connectionName)
          if (connectionInfo.isEmpty)
            logger.warn(
              s"Connection '$connectionName' not found, TMDL partitions will use a generic source"
            )
          writeAll(TMDLConverter.convert(name, node, connectionInfo))
        case "sml" =>
          val asConnection = config.connection.getOrElse(settings.appConfig.connectionRef)
          if (asConnection.trim.isEmpty)
            logger.warn(
              s"Semantic model '$name': as_connection is empty, set it with --connection " +
              "to the AtScale data warehouse connection"
            )
          val plan = SMLPlanner.plan(name, node)
          if (plan.usesTimeHierarchies)
            settings.appConfig.connections
              .get(settings.appConfig.connectionRef)
              .flatMap(info => Try(info.getJdbcEngineName().toString.toLowerCase).toOption)
              .filter(engine => engine == "sqlserver" || engine == "synapse")
              .foreach { engine =>
                logger.warn(
                  s"Semantic model '$name': time hierarchy columns use EXTRACT, which $engine does not support"
                )
              }
          writeAll(SMLConverter.render(plan, asConnection))
        case _ =>
          val ossie = OssieConverter.convert(name, node)
          val target = new Path(outputDir, s"$name.ossie.yaml")
          storage.write(YamlSerde.mapper.writeValueAsString(ossie), target)
          logger.info(s"Exported semantic model '$name' ($path) to $target")
      }
    }
    JobResult.empty
  }
}

/** Pure JsonNode-to-JsonNode conversion from the Snowflake-style semantic model format to Apache
  * Ossie (spec version 0.2.0.dev0).
  */
object OssieConverter {

  /** Ossie core-spec version this converter targets. */
  val SpecVersion = "0.2.0.dev0"
  private val Vendor = "STARLAKE"

  private def yamlMapper = YamlSerde.mapper
  private val jsonMapper = Utils.newJsonMapper()

  def convert(modelName: String, model: JsonNode): ObjectNode = {
    val root = yamlMapper.createObjectNode()
    root.put("version", SpecVersion)
    val sm = yamlMapper.createObjectNode()
    root.set[ObjectNode]("semantic_model", yamlMapper.createArrayNode().add(sm))

    sm.put("name", modelName)
    copyText(model, "description", sm)

    // Model-level ai_context: verified query questions become AI examples.
    val questions = elems(model, "verified_queries").flatMap(q => text(q, "question"))
    if (questions.nonEmpty) {
      val ai = sm.putObject("ai_context")
      val ex = ai.putArray("examples")
      questions.foreach(ex.add)
    }

    val datasets = sm.putArray("datasets")
    elems(model, "tables").foreach(table => datasets.add(dataset(table)))

    val relationships = elems(model, "relationships").map(relationship)
    if (relationships.nonEmpty) {
      val arr = sm.putArray("relationships")
      relationships.foreach(arr.add)
    }

    val tableMetrics = elems(model, "tables").flatMap { table =>
      elems(table, "metrics").map(m => metric(m, sourceDataset = text(table, "name")))
    }
    val modelMetrics = elems(model, "metrics").map(m => metric(m, sourceDataset = None))
    if ((tableMetrics ++ modelMetrics).nonEmpty) {
      val arr = sm.putArray("metrics")
      (tableMetrics ++ modelMetrics).foreach(arr.add)
    }

    // Preserve verified queries (with SQL) losslessly.
    if (model.has("verified_queries"))
      starlakeExtension(sm, "verified_queries", model.get("verified_queries"))

    root
  }

  private def dataset(table: JsonNode): ObjectNode = {
    val ds = yamlMapper.createObjectNode()
    ds.put("name", table.path("name").asText())
    ds.put("source", source(table))
    copyText(table, "description", ds)

    val pkColumns = elems(table.path("primary_key"), "columns").map(_.asText())
    if (pkColumns.nonEmpty) {
      val pk = ds.putArray("primary_key")
      pkColumns.foreach(pk.add)
    }

    val uniqueDims =
      elems(table, "dimensions").filter(_.path("unique").asBoolean(false)).flatMap(text(_, "name"))
    if (uniqueDims.nonEmpty) {
      val uk = ds.putArray("unique_keys")
      uniqueDims.foreach(c => uk.add(yamlMapper.createArrayNode().add(c)))
    }

    val fields = ds.putArray("fields")
    elems(table, "dimensions").foreach(d => fields.add(field(d, isTime = Some(false))))
    elems(table, "time_dimensions").foreach(d => fields.add(field(d, isTime = Some(true))))
    elems(table, "facts").foreach(f => fields.add(field(f, isTime = None)))

    if (table.has("filters"))
      starlakeExtension(ds, "filters", table.get("filters"))
    if (table.has("hierarchies"))
      starlakeExtension(ds, "hierarchies", table.get("hierarchies"))
    ds
  }

  private def field(col: JsonNode, isTime: Option[Boolean]): ObjectNode = {
    val f = yamlMapper.createObjectNode()
    val name = col.path("name").asText()
    f.put("name", name)
    expression(f, text(col, "expr").getOrElse(name))
    copyText(col, "description", f)
    text(col, "data_type").flatMap(datatype).foreach(f.put("datatype", _))
    isTime.foreach(t => f.putObject("dimension").put("is_time", t))
    synonyms(col, f)

    val kept =
      List("unique", "is_enum", "sample_values", "access_modifier", "cortex_search_service")
        .filter(col.has)
    if (kept.nonEmpty) {
      val extra = jsonMapper.createObjectNode()
      kept.foreach(k => extra.set[JsonNode](k, col.get(k)))
      starlakeExtensionNode(f, extra)
    }
    f
  }

  private def relationship(rel: JsonNode): ObjectNode = {
    val r = yamlMapper.createObjectNode()
    r.put("name", rel.path("name").asText())
    r.put("from", rel.path("left_table").asText())
    r.put("to", rel.path("right_table").asText())
    val from = r.putArray("from_columns")
    val to = r.putArray("to_columns")
    elems(rel, "relationship_columns").foreach { rc =>
      text(rc, "left_column").foreach(from.add)
      text(rc, "right_column").foreach(to.add)
    }
    val kept = List("join_type", "relationship_type").filter(rel.has)
    if (kept.nonEmpty) {
      val extra = jsonMapper.createObjectNode()
      kept.foreach(k => extra.set[JsonNode](k, rel.get(k)))
      starlakeExtensionNode(r, extra)
    }
    r
  }

  private def metric(m: JsonNode, sourceDataset: Option[String]): ObjectNode = {
    val out = yamlMapper.createObjectNode()
    out.put("name", m.path("name").asText())
    expression(out, m.path("expr").asText())
    copyText(m, "description", out)
    synonyms(m, out)
    sourceDataset.foreach { ds =>
      val extra = jsonMapper.createObjectNode()
      extra.put("source_dataset", ds)
      starlakeExtensionNode(out, extra)
    }
    out
  }

  // ── helpers ──────────────────────────────────────────────────────────

  private def source(table: JsonNode): String = {
    val base = table.path("base_table")
    val parts =
      List("database", "schema", "table").flatMap(k => text(base, k)).filter(_.nonEmpty)
    if (parts.nonEmpty) parts.mkString(".") else table.path("name").asText()
  }

  private def expression(target: ObjectNode, expr: String): Unit = {
    val dialect = target.putObject("expression").putArray("dialects").addObject()
    dialect.put("dialect", "ANSI_SQL")
    dialect.put("expression", expr)
  }

  private def synonyms(from: JsonNode, target: ObjectNode): Unit = {
    val syns = elems(from, "synonyms").map(_.asText()).filter(_.nonEmpty)
    if (syns.nonEmpty) {
      val arr = target.putObject("ai_context").putArray("synonyms")
      syns.foreach(arr.add)
    }
  }

  /** Map Snowflake-style datatypes to the Ossie datatype enum. Unknown types map to Opaque; absent
    * types are omitted.
    */
  private def datatype(raw: String): Option[String] = {
    val t = raw.trim.toUpperCase
    if (t.isEmpty) None
    else
      Some(t match {
        case "TEXT" | "STRING" | "VARCHAR" | "CHAR"                       => "String"
        case "INT" | "INTEGER" | "BIGINT" | "SMALLINT"                    => "Integer"
        case "NUMBER" | "NUMERIC" | "DECIMAL"                             => "Decimal"
        case "FLOAT" | "DOUBLE" | "REAL"                                  => "Float"
        case "BOOLEAN" | "BOOL"                                           => "Boolean"
        case "DATE"                                                       => "Date"
        case "TIME"                                                       => "Time"
        case "DATETIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => "DateTime"
        case "TIMESTAMP_TZ"                                               => "DateTimeTz"
        case _                                                            => "Opaque"
      })
  }

  private def starlakeExtension(target: ObjectNode, key: String, value: JsonNode): Unit = {
    val extra = jsonMapper.createObjectNode()
    extra.set[JsonNode](key, value)
    starlakeExtensionNode(target, extra)
  }

  private def starlakeExtensionNode(target: ObjectNode, data: ObjectNode): Unit = {
    val extensions =
      if (target.has("custom_extensions")) target.get("custom_extensions").asInstanceOf[ArrayNode]
      else target.putArray("custom_extensions")
    val ext = extensions.addObject()
    ext.put("vendor_name", Vendor)
    ext.put("data", jsonMapper.writeValueAsString(data))
  }

  private def elems(node: JsonNode, key: String): List[JsonNode] =
    if (node.has(key) && node.get(key).isArray) node.get(key).elements().asScala.toList
    else Nil

  private def text(node: JsonNode, key: String): Option[String] =
    Option(node.path(key).asText(null)).filter(_.nonEmpty)

  private def copyText(from: JsonNode, key: String, target: ObjectNode): Unit =
    text(from, key).foreach(target.put(key, _))
}
