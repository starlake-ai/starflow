package ai.starlake.extract.impl.restapi

import ai.starlake.config.Settings
import ai.starlake.extract.spi.SchemaExtractor
import ai.starlake.schema.model.{DomainInfo, SchemaInfo, TableAttribute}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.typesafe.scalalogging.LazyLogging

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

/** Extracts Starlake schemas from REST API endpoints by fetching a sample page and inferring the
  * schema from the JSON response structure.
  */
class RestAPISchemaExtractor(
  config: RestAPIExtractSchema
) extends SchemaExtractor
    with LazyLogging {

  override def extract()(implicit settings: Settings): Iterable[DomainInfo] = {
    val client = new RestAPIClient(config)
    val endpoints = config.resolvedEndpoints

    // Group endpoints by domain
    val endpointsByDomain = endpoints.groupBy(_.domain)

    endpointsByDomain.map { case (domainName, domainEndpoints) =>
      val tables = domainEndpoints.flatMap { endpoint =>
        val schemas = extractSchemasForEndpointTree(client, endpoint)
        // Schema evolution detection: compare with previously extracted schemas
        schemas.foreach(detectSchemaEvolution(domainName, _))
        schemas
      }
      DomainInfo(
        name = domainName,
        tables = tables
      )
    }
  }

  /** Log warnings when inferred schema differs from existing YAML definitions */
  private def detectSchemaEvolution(domainName: String, table: SchemaInfo)(implicit
    settings: Settings
  ): Unit = {
    try {
      val storageHandler = settings.storageHandler()
      val loadDir = ai.starlake.config.DatasetArea.load
      val tablePath =
        new org.apache.hadoop.fs.Path(
          new org.apache.hadoop.fs.Path(loadDir, domainName),
          s"${table.name}.sl.yml"
        )
      if (storageHandler.exists(tablePath)) {
        val content = storageHandler.read(tablePath)
        val existingTables = ai.starlake.utils.YamlSerde
          .deserializeYamlTables(content, tablePath.toString)
          .map(_.table)
        existingTables.find(_.name == table.name).foreach { existing =>
          val existingFields = existing.attributes.map(_.name).toSet
          val newFields = table.attributes.map(_.name).toSet
          val added = newFields -- existingFields
          val removed = existingFields -- newFields
          if (added.nonEmpty)
            logger.warn(
              s"Schema evolution: ${domainName}.${table.name} has ${added.size} new field(s): ${added
                  .mkString(", ")}"
            )
          if (removed.nonEmpty)
            logger.warn(
              s"Schema evolution: ${domainName}.${table.name} has ${removed.size} removed field(s): ${removed
                  .mkString(", ")}"
            )
        }
      }
    } catch {
      case _: Exception => // Silently skip evolution detection on error
    }
  }

  /** Extract schemas for an endpoint and all its children recursively */
  private def extractSchemasForEndpointTree(
    client: RestAPIClient,
    endpoint: RestAPIEndpoint
  ): List[SchemaInfo] = {
    val parentSchema = extractSchemaForEndpoint(client, endpoint)
    val childSchemas = if (endpoint.children.nonEmpty) {
      // Fetch a sample parent record to resolve {parent.id} placeholders in children paths
      try {
        val response = client.execute(
          endpoint.path,
          endpoint.method,
          endpoint.queryParams ++ limitParams(endpoint),
          endpoint.headers,
          endpoint.requestBody
        )
        val dataNode = JsonPathUtil.extractDataArray(response.body, endpoint.responsePath)
        val sampleParent = findSampleRecord(dataNode)
        sampleParent
          .map { parent =>
            endpoint.children.flatMap { child =>
              val resolvedPath = resolveParentPlaceholders(child.path, parent)
              val resolvedChild = child.copy(path = resolvedPath)
              extractSchemasForEndpointTree(client, resolvedChild)
            }
          }
          .getOrElse(Nil)
      } catch {
        case e: Exception =>
          logger.warn(
            s"Could not extract children schemas for ${endpoint.path}: ${e.getMessage}"
          )
          Nil
      }
    } else Nil

    parentSchema.toList ++ childSchemas
  }

  private def extractSchemaForEndpoint(
    client: RestAPIClient,
    endpoint: RestAPIEndpoint
  ): Option[SchemaInfo] = {
    try {
      logger.info(s"Extracting schema from ${endpoint.path}")
      val response = client.execute(
        endpoint.path,
        endpoint.method,
        endpoint.queryParams ++ limitParams(endpoint),
        endpoint.headers,
        endpoint.requestBody
      )
      val dataNode = JsonPathUtil.extractDataArray(response.body, endpoint.responsePath)
      val sampleRecord = findSampleRecord(dataNode)
      sampleRecord.map { record =>
        val attributes = inferAttributes(record, endpoint.excludeFields)
        SchemaInfo(
          name = endpoint.tableName,
          pattern = Pattern.compile(s"${endpoint.tableName}.*"),
          attributes = attributes
        )
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to extract schema from ${endpoint.path}: ${e.getMessage}")
        None
    }
  }

  /** Request only a single record for schema inference */
  private def limitParams(endpoint: RestAPIEndpoint): Map[String, String] = {
    endpoint.pagination match {
      case Some(OffsetPagination(limitParam, offsetParam, _)) =>
        Map(limitParam -> "1", offsetParam -> "0")
      case Some(CursorPagination(_, _, _, Some(limitParam))) =>
        Map(limitParam -> "1")
      case Some(PageNumberPagination(pageParam, _, Some(limitParam))) =>
        Map(limitParam -> "1", pageParam -> "1")
      case Some(LinkHeaderPagination(_, Some(limitParam))) =>
        Map(limitParam -> "1")
      case _ =>
        Map.empty
    }
  }

  private def findSampleRecord(dataNode: JsonNode): Option[JsonNode] = {
    if (dataNode.isArray && dataNode.size() > 0) Some(dataNode.get(0))
    else if (dataNode.isObject) Some(dataNode)
    else None
  }

  /** Infer Starlake TableAttributes from a JSON record */
  private def inferAttributes(
    record: JsonNode,
    excludeFields: List[Pattern]
  ): List[TableAttribute] = {
    record.fieldNames().asScala.toList.flatMap { fieldName =>
      val excluded = excludeFields.exists(_.matcher(fieldName).matches())
      if (excluded) None
      else {
        val fieldNode = record.get(fieldName)
        Some(inferAttribute(fieldName, fieldNode))
      }
    }
  }

  private def inferAttribute(name: String, node: JsonNode): TableAttribute = {
    if (node == null || node.isNull || node.isMissingNode) {
      TableAttribute(name = name, `type` = "string")
    } else {
      node.getNodeType match {
        case JsonNodeType.STRING =>
          val text = node.asText()
          val inferredType = inferStringType(text)
          TableAttribute(name = name, `type` = inferredType)

        case JsonNodeType.NUMBER =>
          if (node.isIntegralNumber) TableAttribute(name = name, `type` = "long")
          else TableAttribute(name = name, `type` = "decimal")

        case JsonNodeType.BOOLEAN =>
          TableAttribute(name = name, `type` = "boolean")

        case JsonNodeType.ARRAY =>
          val itemType = if (node.size() > 0) {
            val firstItem = node.get(0)
            if (firstItem.isObject) {
              // Array of objects — infer nested attributes
              val nestedAttrs = inferAttributes(firstItem, Nil)
              return TableAttribute(
                name = name,
                `type` = "struct",
                array = Some(true),
                attributes = nestedAttrs
              )
            } else {
              inferScalarType(firstItem)
            }
          } else {
            "string"
          }
          TableAttribute(name = name, `type` = itemType, array = Some(true))

        case JsonNodeType.OBJECT =>
          val nestedAttrs = inferAttributes(node, Nil)
          TableAttribute(name = name, `type` = "struct", attributes = nestedAttrs)

        case _ =>
          TableAttribute(name = name, `type` = "string")
      }
    }
  }

  private def inferScalarType(node: JsonNode): String = {
    if (node == null || node.isNull) "string"
    else
      node.getNodeType match {
        case JsonNodeType.STRING  => inferStringType(node.asText())
        case JsonNodeType.NUMBER  => if (node.isIntegralNumber) "long" else "decimal"
        case JsonNodeType.BOOLEAN => "boolean"
        case _                    => "string"
      }
  }

  /** Try to infer a more specific type from a string value */
  private def inferStringType(text: String): String = {
    if (text.isEmpty) return "string"

    // ISO date: 2024-01-15
    if (text.matches("""\d{4}-\d{2}-\d{2}""")) return "date"
    // ISO datetime: 2024-01-15T10:30:00Z or 2024-01-15T10:30:00.000Z or with offset
    if (text.matches("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*""")) return "timestamp"

    "string"
  }

  /** Replace {parent.fieldName} placeholders in the path with values from the parent record */
  private def resolveParentPlaceholders(path: String, parentRecord: JsonNode): String = {
    val pattern = """\{parent\.([^}]+)\}""".r
    pattern.replaceAllIn(
      path,
      m => {
        val field = m.group(1)
        val value = parentRecord.get(field)
        if (value == null || value.isNull) field
        else java.net.URLEncoder.encode(value.asText(), "UTF-8")
      }
    )
  }
}
