package ai.starlake.dbt.manifest

import ai.starlake.dbt.DbtImportException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.nio.file.{Files, Path}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Reads dbt's target/manifest.json (schema v10 to v12) into [[DbtManifest]] */
object ManifestReader {
  val SupportedVersions: Range = 10 to 12

  private val VersionRe = """.*/manifest/v(\d+)\.json""".r
  private val mapper = new ObjectMapper()

  def read(path: Path): Try[DbtManifest] = Try {
    if (!Files.isRegularFile(path))
      throw new DbtImportException(
        s"dbt manifest not found: $path. Run `dbt deps && dbt parse` in the dbt project first."
      )
    parse(mapper.readTree(path.toFile), path)
  }

  def parse(root: JsonNode, path: Path): DbtManifest = {
    val meta = root.path("metadata")
    val schemaUrl = meta.path("dbt_schema_version").asText("")
    val version = schemaUrl match {
      case VersionRe(v) => v.toInt
      case _ =>
        throw new DbtImportException(s"$path is not a dbt manifest: no metadata.dbt_schema_version")
    }
    if (!SupportedVersions.contains(version))
      throw new DbtImportException(
        s"Unsupported dbt manifest schema v$version ($schemaUrl): starlake dbt import reads manifest v10 to v12, written by dbt 1.6 and later"
      )
    val metadata = DbtManifestMetadata(
      schemaVersion = version,
      dbtVersion = meta.path("dbt_version").asText(""),
      adapterType = meta.path("adapter_type").asText(""),
      projectName = meta.path("project_name").asText("")
    )
    val disabled = values(root.path("disabled")).flatMap(_.elements().asScala).toList
    DbtManifest(
      metadata = metadata,
      nodes = values(root.path("nodes")).map(node).toList,
      disabled = disabled
        .filter(d =>
          Set("model", "seed", "snapshot", "test").contains(d.path("resource_type").asText())
        )
        .map(node),
      sources = values(root.path("sources")).map(source).toList,
      macros = values(root.path("macros")).map { m =>
        DbtMacro(
          uniqueId = text(m, "unique_id"),
          packageName = text(m, "package_name"),
          name = text(m, "name"),
          macroSql = text(m, "macro_sql")
        )
      }.toList,
      docs = values(root.path("docs")).map { d =>
        DbtDoc(
          uniqueId = text(d, "unique_id"),
          packageName = text(d, "package_name"),
          name = text(d, "name"),
          blockContents = text(d, "block_contents")
        )
      }.toList,
      reportedOnly =
        List("exposures", "metrics", "semantic_models", "saved_queries", "unit_tests").flatMap {
          kind => root.path(kind).fieldNames().asScala.map(kind -> _)
        },
      manifestPath = path,
      graph = List("nodes", "sources", "macros", "exposures", "metrics").map { section =>
        section -> (DbtJson.toScala(root.path(section)) match {
          case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
          case _            => Map.empty[String, Any]
        })
      }.toMap
    )
  }

  private def values(node: JsonNode): Iterator[JsonNode] = node.elements().asScala

  private def text(node: JsonNode, field: String): String =
    if (node.path(field).isNull || node.path(field).isMissingNode) "" else node.path(field).asText()

  private def optText(node: JsonNode, field: String): Option[String] =
    Option(text(node, field)).filter(_.nonEmpty)

  private def strings(node: JsonNode): List[String] = node.elements().asScala.map(_.asText()).toList

  private def columns(node: JsonNode): List[DbtColumn] =
    values(node.path("columns")).map { c =>
      DbtColumn(
        name = text(c, "name"),
        description = text(c, "description"),
        dataType = optText(c, "data_type"),
        docBlocks = strings(c.path("doc_blocks"))
      )
    }.toList

  private def node(n: JsonNode): DbtNode = {
    val config = DbtJson.toScala(n.path("config")) match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
      case _            => Map.empty[String, Any]
    }
    val testMetadata = n.path("test_metadata") match {
      case t if t.isObject =>
        Some(
          DbtTestMetadata(
            name = text(t, "name"),
            namespace = optText(t, "namespace"),
            kwargs = DbtJson.toScala(t.path("kwargs")).asInstanceOf[Map[String, Any]]
          )
        )
      case _ => None
    }
    DbtNode(
      uniqueId = text(n, "unique_id"),
      resourceType = text(n, "resource_type"),
      packageName = text(n, "package_name"),
      name = text(n, "name"),
      database = optText(n, "database"),
      schema = text(n, "schema"),
      alias = optText(n, "alias").getOrElse(text(n, "name")),
      version = optText(n, "version"),
      latestVersion = optText(n, "latest_version"),
      language = optText(n, "language").getOrElse("sql"),
      rawCode = text(n, "raw_code"),
      originalFilePath = text(n, "original_file_path"),
      rootPath = optText(n, "root_path"),
      description = text(n, "description"),
      docBlocks = strings(n.path("doc_blocks")),
      columns = columns(n),
      config = config,
      tags = strings(n.path("tags")),
      dependsOnNodes = strings(n.path("depends_on").path("nodes")),
      dependsOnMacros = strings(n.path("depends_on").path("macros")),
      testMetadata = testMetadata,
      attachedNode = optText(n, "attached_node"),
      columnName = optText(n, "column_name")
    )
  }

  private def period(node: JsonNode): Option[DbtPeriod] =
    if (node.path("count").isNumber && node.path("period").isTextual)
      Some(DbtPeriod(node.path("count").asInt(), node.path("period").asText()))
    else None

  private def source(s: JsonNode): DbtSource = {
    val freshnessNode =
      if (s.path("freshness").isObject) s.path("freshness")
      else s.path("config").path("freshness")
    val freshness = Some(
      DbtFreshness(
        period(freshnessNode.path("warn_after")),
        period(freshnessNode.path("error_after"))
      )
    ).filter(f => f.warnAfter.isDefined || f.errorAfter.isDefined)
    DbtSource(
      uniqueId = text(s, "unique_id"),
      packageName = text(s, "package_name"),
      sourceName = text(s, "source_name"),
      name = text(s, "name"),
      database = optText(s, "database"),
      schema = text(s, "schema"),
      identifier = optText(s, "identifier").getOrElse(text(s, "name")),
      description = optText(s, "description").getOrElse(text(s, "source_description")),
      docBlocks = strings(s.path("doc_blocks")),
      columns = columns(s),
      freshness = freshness,
      loadedAtField = optText(s, "loaded_at_field"),
      tags = strings(s.path("tags")),
      enabled =
        !s.path("config").path("enabled").isBoolean || s.path("config").path("enabled").asBoolean()
    )
  }
}

/** JSON values as Scala values: objects keep their key order, null is None */
object DbtJson {
  def toScala(node: JsonNode): Any =
    if (node == null || node.isNull || node.isMissingNode) None
    else if (node.isObject)
      ListMap(node.properties().asScala.toSeq.map(e => e.getKey -> toScala(e.getValue)): _*)
    else if (node.isArray) node.elements().asScala.map(toScala).toList
    else if (node.isBoolean) node.asBoolean()
    else if (node.isInt) node.asInt()
    else if (node.isIntegralNumber) node.asLong()
    else if (node.isNumber) node.asDouble()
    else node.asText()
}
