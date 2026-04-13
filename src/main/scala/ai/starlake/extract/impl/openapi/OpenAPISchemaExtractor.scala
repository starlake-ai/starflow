package ai.starlake.extract.impl.openapi

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.core.utils.NamingUtils
import ai.starlake.extract.spi.SchemaExtractor
import ai.starlake.extract.{ExtractPathHelper, OnExtract, SanitizeStrategy}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.*
import com.typesafe.scalalogging.LazyLogging
import io.swagger.v3.oas.models.media.{
  ArraySchema,
  BinarySchema,
  BooleanSchema,
  ByteArraySchema,
  DateSchema,
  DateTimeSchema,
  EmailSchema,
  FileSchema,
  IntegerSchema,
  JsonSchema,
  MapSchema,
  NumberSchema,
  ObjectSchema,
  PasswordSchema,
  Schema as OpenAPISwaggerSchema,
  StringSchema,
  UUIDSchema
}
import io.swagger.v3.oas.models.responses.ApiResponse
import io.swagger.v3.oas.models.{OpenAPI, Operation, PathItem}
import io.swagger.v3.parser.OpenAPIV3Parser
import io.swagger.v3.parser.core.models.ParseOptions

import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** `OpenAPISchemaExtractor` is responsible for extracting OpenAPI schemas into Starlake-compatible
  * domain and schema structures. This class facilitates the transformation and organization of
  * OpenAPI specifications for downstream processing and integration within the Starlake pipeline.
  *
  * The class utilizes several helper methods to handle specific tasks such as extracting API
  * information, grouping schemas by table, processing routes, mapping OpenAPI schemas to Starlake
  * formats, and resolving schema collisions.
  *
  * Fields:
  *   - `extractConfig`: Configuration settings used during the schema extraction process.
  *   - `connectionSettings`: Connection-related settings or configurations for accessing resources.
  *
  * This class extends:
  *   - `SchemaExtractor`: Provides base functionality for schema extraction processes.
  *   - `ExtractPathHelper`: Adds utilities for handling extraction paths.
  *   - `LazyLogging`: Enables logging capabilities for debugging and monitoring.
  */
class OpenAPISchemaExtractor(
  private val extractConfig: OpenAPIExtractSchema,
  private val connectionSettings: ConnectionInfo,
  attributeSanitizeStrategy: SanitizeStrategy,
  applySnakeCaseOnName: Boolean
) extends SchemaExtractor
    with ExtractPathHelper
    with LazyLogging {
  private type SchemaName = String
  private type SchemaDescription = String

  /** Extracts domain schemas and associated tables from an OpenAPI specification provided in the
    * connection settings, processes them according to the extractor configuration, and returns a
    * collection of domains.
    *
    * @param settings
    *   An implicit parameter providing access to application and storage configuration, required
    *   for reading OpenAPI files and performing schema handling operations.
    * @return
    *   An Iterable collection of Domain objects, each containing metadata and schemas derived from
    *   the OpenAPI configuration and extractor settings.
    */
  override def extract()(implicit settings: Settings): Iterable[DomainInfo] = {
    require(
      connectionSettings.`type` == ConnectionType.FS,
      "Connection should be FS with 'path' option defined"
    )
    implicit val storageHandler: StorageHandler = settings.storageHandler()
    val openAPIFileContent = storageHandler.read(
      openAPIPath(connectionSettings.getFSPath()),
      connectionSettings.getFSEncoding()
    )
    val apiEssentialSchemas = extractApiEssentialInformation(openAPIFileContent)
    for {
      domain <- extractConfig.domains
    } yield {
      val schemas = processDomain(apiEssentialSchemas, domain)
      DomainInfo(
        name = domain.name,
        metadata = Some(
          Metadata(
            format = Some(Format.JSON),
            encoding = Some("UTF-8"),
            fillWithDefaultValue = false
          )
        ),
        tables = schemas
      )
    }
  }

  /** Processes a given OpenAPIDomain object to extract and transform schema information in
    * conjunction with the provided list of ApiEssentialInformation. This involves analyzing routes,
    * grouping schemas by related tables, and generating output schemas.
    *
    * @param apiEssentialSchemas
    *   A list of essential API information, including schema details, API paths, tags, and other
    *   descriptive metadata relevant for processing.
    * @param domain
    *   The OpenAPIDomain object representing a specific domain within the OpenAPI specification,
    *   including its routes and base path.
    */
  private def processDomain(
    apiEssentialSchemas: List[ApiEssentialInformation],
    domain: OpenAPIDomain
  ) = {
    val schemasByTableName = (for {
      route <- domain.routes
    } yield {
      processRoute(apiEssentialSchemas, domain, route)
        .map(apiInfoWithTable(domain, route, _))
    }).flatten

    groupSchemasByTable(schemasByTableName).flatMap { case (schema, connexInfos) =>
      generateStarlakeSchemas(schema, connexInfos, extractConfig.formatTypeMapping)
    }
  }

  /** Groups schemas by their associated table and then further groups them by their schema
    * structure. This method consolidates schema information linked to table names while ensuring
    * there are no schema collisions. If a collision is detected, appropriate action is taken.
    *
    * @param schemasByTableName
    *   A list of schema descriptions and their corresponding API essential information. Each tuple
    *   links a schema to a table name and provides metadata about the API endpoint associated with
    *   it.
    */
  private def groupSchemasByTable(
    schemasByTableName: List[(SchemaDescription, ApiEssentialInformation)]
  ) = {
    schemasByTableName
      .groupBy { case (tableName, _) => tableName }
      .view
      .mapValues { infos =>
        checkSchemaCollision(infos.map(_._2))
        infos
          .map(_._2)
          .headOption
          .getOrElse(
            throw new RuntimeException("Should never happen since this comes from a group by")
          )
      }
      .groupBy { case (_, i) => i.schema }
      .view
      .mapValues(_.toList) // just transform to list in order to be compatible with 2.12
      .toList
  }

  /** Constructs a mapping between a normalized table name and updated API essential information by
    * processing domain details, route-specific configurations, and transformation rules for schema
    * exclusion and field renaming.
    *
    * @param domain
    *   The OpenAPIDomain object representing a domain in the API specification. Includes the base
    *   path and associated route details used for table name generation.
    * @param route
    *   The OpenAPIRoute object containing route configuration such as paths, exclusions, field
    *   patterns, and explosion rules to apply during schema processing.
    * @param aei
    *   The ApiEssentialInformation object providing API-specific details like schema, operation
    *   metadata, and description. Also contains methods to build table names and handle schema
    *   transformations.
    */
  private def apiInfoWithTable(
    domain: OpenAPIDomain,
    route: OpenAPIRoute,
    aei: ApiEssentialInformation
  ) = {
    aei.buildTableName(
      domain.basePath.orElse(extractConfig.basePath),
      route.as,
      route.explode.map(_.rename).getOrElse(Map.empty)
    ) -> aei.copy(schema =
      aei.schema.copy(schema = excludeFieldsFromSchema(aei.schema.schema, route.excludeFields))
    )
  }

  /** Processes a specific route within the given OpenAPI domain by filtering and expanding relevant
    * API schemas based on route configuration, operations, and exclusion rules.
    *
    * @param apiEssentialSchemas
    *   A list of `ApiEssentialInformation` containing essential metadata and schema representations
    *   for different API operations.
    * @param domain
    *   The `OpenAPIDomain` containing metadata, schemas, and associated routes for a given OpenAPI
    *   specification.
    * @param route
    *   The `OpenAPIRoute` object defining the paths, operations, and exclusion patterns for
    *   processing the specific route.
    */
  private def processRoute(
    apiEssentialSchemas: List[ApiEssentialInformation],
    domain: OpenAPIDomain,
    route: OpenAPIRoute
  ) = {
    apiEssentialSchemas
      .filter { info =>
        // filter ignored schema before handling table collision names
        val filterResult =
          domain.schemas.forall(s => info.schema.matchWith(s.include)) && domain.schemas.forall(s =>
            info.schema.notMatchWith(
              s.exclude
            )
          ) && // filter ignored path
          info.matchWith(route.paths) && info.notMatchWith(
            route.exclude
          ) && // filter method
          route.operations.contains(info.apiMethod)
        if (filterResult) {
          logger.info(s"Route kept ${info.apiMethod} ${info.apiPath}")
        } else {
          logger.info(s"Route dropped ${info.apiMethod} ${info.apiPath}")
        }
        filterResult
      }
      .flatMap(expandRouteSchema(route, _))
  }

  /** Expands the route schema by applying explosion rules if defined in the route configuration.
    * When the `explode` configuration is present, it processes the associated schema, generating a
    * list of transformed `ApiEssentialInformation` instances for each exploded attribute. If no
    * explosion rules are defined, the original `ApiEssentialInformation` is returned as is.
    *
    * @param route
    *   The `OpenAPIRoute` object containing details about the API route, including optional
    *   explosion rules for schema processing.
    * @param aei
    *   The `ApiEssentialInformation` instance that holds metadata and schema information about the
    *   API, to be processed according to the route configuration.
    */
  private def expandRouteSchema(route: OpenAPIRoute, aei: ApiEssentialInformation) = {
    route.explode match {
      case Some(explodeConfig) =>
        logger.info(s"Exploding route ${aei.apiMethod} ${aei.apiPath} on ${explodeConfig.on}")
        val output =
          explodeStructureSchema(aei.schema.schema, explodeConfig.on, explodeConfig.exclude).map {
            case (attributePath, (schema, attributeDescription)) =>
              aei.copy(
                attributePath = Some(attributePath),
                schema = RichOpenAPISchema(schema, Nil),
                attributeDescription = attributeDescription
              )
          }
        logger.info(
          s"Exploded route ${aei.apiMethod} ${aei.apiPath} into ${output.size} schemas ${output
              .flatMap(_.attributePath)
              .mkString("belonging to paths ", ", ", "")}"
        )
        output
      case None =>
        List(aei)
    }
  }

  /** Generates Starlake schemas based on the provided OpenAPI schema, associated connection
    * information, and format type mappings. For each OpenAPI schema, this method creates Starlake
    * tables grouped by API routes while considering schema structures and metadata.
    *
    * @param schema
    *   The enriched OpenAPI schema, containing the schema object and associated schema names.
    * @param connexInfos
    *   A collection of schema descriptions and corresponding API essential information, linking the
    *   schema to API routes and providing metadata like paths and descriptions.
    * @param formatTypeMapping
    *   A mapping between OpenAPI format types and corresponding Starlake data types, used to
    *   convert schema fields appropriately.
    */
  private def generateStarlakeSchemas(
    schema: RichOpenAPISchema,
    connexInfos: List[(SchemaDescription, ApiEssentialInformation)],
    formatTypeMapping: Map[String, String]
  ) = {
    // Sharing the same schema doesn't mean they should be in the table in the output.
    // We may easily think that retrieving a list /clients and a unique item /clients/{id} share the same schema
    // and should be in the same table, and that would probably be true.
    // However, we may encounter a shared structure such as returning a list of id along with some metadata
    // and this may be returned by /clients and /clients/{id}/wallets but in this case,
    // the data should be separated into two tables.
    // This is why, we generate as many tables as API found and let the user group/filter out routes.
    // Filtering out route may be done with the schema reference name or the route itself, allowing a user to ignore shared structure without specifying all routes himself.
    // grouping by schema speed up starlake schema generation since it's done once for all routes.
    // keep description of the shortest path

    val routesStr =
      connexInfos
        .map { case (_, aei) =>
          aei.apiPath + aei.attributePath.map(" and property " + _).getOrElse("")
        }
        .mkString(", ")
    val slSchemaGenerator = openAPISchemaToStarlakeSchema(schema.schema, formatTypeMapping)
    logger.info(
      s"Generating starlake schema of OpenAPI Schema ${schema.schemaNamesStr} for routes: $routesStr"
    )
    for (connexInfo <- connexInfos) yield {
      slSchemaGenerator(connexInfo._1)
        .copy(
          comment = connexInfo._2.description,
          tags = connexInfo._2.tags.toSet
        )
    }
  }

  /** Checks for schema collisions within a list of API essential information. If multiple entries
    * are found with different schemas but identical table names, an exception is thrown. If schemas
    * are identical, a log message is generated, and only the first schema is retained.
    *
    * @param infos
    *   A list of `ApiEssentialInformation` instances, each containing details about API methods,
    *   paths, and their corresponding schemas, used to identify potential schema collisions.
    * @return
    *   This method does not return any value but may throw a `RuntimeException` if schema
    *   collisions are detected.
    */
  private def checkSchemaCollision(infos: List[ApiEssentialInformation]): Unit = {
    if (infos.size > 1) {
      val collisionPaths = infos
        .map(i => i.apiMethod.toString + " " + i.apiPath + " (" + i.schema.schemaNamesStr + ")")
        .mkString(", ")
      if (infos.map(_.schema).distinct.size > 1) {
        throw new RuntimeException(
          s"The following routes generate the same table name but don't share the same schema: $collisionPaths. Please exclude routes, schema or force table names"
        )
      } else {
        logger.info(
          s"The following routes generate the same table name, keeping only the first one: $collisionPaths"
        )
      }
    }
  }

  /** Extracts a list of `PathInfo` objects from the provided OpenAPI specification. Each `PathInfo`
    * contains metadata about an API path, including the path itself, its description, summary, and
    * associated `PathItem`.
    *
    * @param openAPI
    *   The `OpenAPI` object representing the OpenAPI specification from which paths and their
    *   metadata will be extracted.
    * @return
    *   A list of `PathInfo` instances, each containing information about a specific API path
    *   defined in the OpenAPI paths map. Returns an empty list if no paths are defined.
    */
  private def extractPathInfos(openAPI: OpenAPI): List[PathInfo] = {
    Option(openAPI.getPaths)
      .map(_.asScala)
      .getOrElse(Nil)
      .map { case (apiPath, pathItem) =>
        val pathDescription = Option(pathItem.getDescription)
        val pathSummary = Option(pathItem.getSummary)
        PathInfo(apiPath, pathDescription, pathSummary, pathItem)
      }
      .toList
  }

  /** Extracts operations from a given PathItem and maps them to a list of PathOperation objects,
    * which include metadata such as tags, descriptions, and summaries.
    *
    * @param pathItem
    *   The PathItem instance from the OpenAPI specification, representing a set of operations
    *   (e.g., GET, POST) for a specific path.
    * @return
    *   A list of PathOperation objects, each representing an operation (e.g., GET, POST) from the
    *   provided PathItem, along with its associated metadata.
    */
  private def getOperations(pathItem: PathItem): List[PathOperation] = {
    (Option(pathItem.getGet).map(Get -> _).toList ++ Option(pathItem.getPost)
      .map(Post -> _)
      .toList).map { case (method, operation) =>
      PathOperation(
        Option(operation.getTags).map(_.asScala.toList).getOrElse(Nil),
        method,
        Option(operation.getDescription),
        Option(operation.getSummary),
        operation
      )
    }
  }

  /** Extracts a list of successful HTTP response definitions (2XX range) from the provided OpenAPI
    * operation object. Each response includes metadata such as status code, operation description,
    * summary, and the response body.
    *
    * @param operation
    *   The OpenAPI `Operation` object, which defines the API operation and its associated
    *   responses.
    * @return
    *   A list of `PathResponse` objects, representing the responses within the 200-299 HTTP status
    *   code range. Returns an empty list if no 2XX responses are found.
    */
  private def get2XXResponses(operation: Operation): List[PathResponse] = {
    Option(operation.getResponses).toList.flatMap { responses =>
      responses.asScala.flatMap { case (httpStatus, resp) =>
        val httpStatusInt = Try(httpStatus.toInt).getOrElse(0)
        if (httpStatusInt >= 200 && httpStatusInt < 300)
          Some(
            PathResponse(
              httpStatusInt,
              Option(operation.getDescription),
              Option(operation.getSummary),
              resp
            )
          )
        else
          None
      }
    }
  }

  private def isMapSchema(schema: OpenAPISwaggerSchema[?]): Boolean = {
    Option(schema).exists {
      case _: MapSchema => true
      case _ =>
        "object".equalsIgnoreCase(schema.getType) && Option(
          schema.getAdditionalProperties
        ).isDefined
    }
  }

  private def isObjectSchemaLike(schema: OpenAPISwaggerSchema[?]): Boolean = {
    Option(schema).exists {
      case _: ObjectSchema => true
      case _: JsonSchema
          if Option(schema.getProperties).isDefined && Option(schema.getItems).isEmpty =>
        true
      case _ =>
        "object".equalsIgnoreCase(schema.getType) && Option(
          schema.getAdditionalProperties
        ).isEmpty
    }
  }

  private def isAttributeFormatBasedSchema(schema: OpenAPISwaggerSchema[?]): Boolean = {
    Option(schema).filter(s => Option(s.getFormat).isDefined).exists {
      case _: StringSchema | _: JsonSchema => true
      case _                               => false
    }
  }

  private def isAttributeTypedBasedSchema(schema: OpenAPISwaggerSchema[?]): Boolean = {
    Option(schema)
      .filter(s => Option(s.getType).isDefined || Option(s.getTypes).exists(_.asScala.nonEmpty))
      .exists {
        case _: StringSchema | _: JsonSchema => true
        case _                               => false
      }
  }

  private def isArraySchemaLike(schema: OpenAPISwaggerSchema[?]): Boolean = {
    Option(schema).exists {
      case _: ArraySchema => true
      case _: JsonSchema
          if Option(schema.getProperties).isEmpty && Option(schema.getItems).isDefined =>
        true
      case _ => false
    }
  }

  private def isArrayOfStructSchemaLike(schema: OpenAPISwaggerSchema[?]): Boolean = {
    isArraySchemaLike(schema) && Option(schema.getItems).exists(isObjectSchemaLike)
  }

  /** Extracts a list of ResponseSchema objects from the provided API response, analyzing the
    * OpenAPI schema definitions.
    *
    * @param apiResponse
    *   The API response from which the schema is extracted.
    * @param openApiSchemas
    *   A map linking OpenAPI schemas to their descriptions as a list of strings.
    * @return
    *   A list of ResponseSchema objects derived from the API response's schema information.
    */
  private def extractStructureSchema(
    apiResponse: ApiResponse,
    openApiSchemas: Map[OpenAPISwaggerSchema[?], List[String]]
  ): List[ResponseSchema] = {
    Option(apiResponse.getContent).toList.flatMap(_.asScala.flatMap {
      case (mediaType, mediaTypeObject) =>
        val schemaDescription =
          Option(mediaTypeObject.getSchema).flatMap(s => Option(s.getDescription))

        def handleObjectSchema = {
          Some(
            ResponseSchema(
              mediaType,
              schemaDescription,
              mediaTypeObject.getSchema,
              openApiSchemas.getOrElse(mediaTypeObject.getSchema, Nil)
            )
          )
        }

        mediaTypeObject.getSchema match {
          case s if isObjectSchemaLike(s) =>
            handleObjectSchema
          case s if isArrayOfStructSchemaLike(s) =>
            Some(
              ResponseSchema(
                mediaType,
                Option(s.getItems.getDescription).orElse(schemaDescription),
                s.getItems,
                openApiSchemas.getOrElse(
                  s.getItems,
                  Nil
                )
              )
            )
          case _ => None
        }
    })
  }

  /** Excludes fields from the given OpenAPI schema that match the specified patterns.
    *
    * @param responseSchema
    *   the root OpenAPI schema from which fields will be excluded
    * @param fieldPatterns
    *   a list of regex patterns used to identify field names to be excluded
    * @return
    *   the modified OpenAPI schema with matching fields excluded
    */
  private def excludeFieldsFromSchema(
    responseSchema: OpenAPISwaggerSchema[?],
    fieldPatterns: List[Pattern]
  ): OpenAPISwaggerSchema[?] = {

    def processExclusion(
      currentSchema: OpenAPISwaggerSchema[?],
      currentPath: Option[String] = None
    ): OpenAPISwaggerSchema[?] = {
      def handleObjectSchema(os: OpenAPISwaggerSchema[?]): OpenAPISwaggerSchema[?] = {
        val newProperties = os.getProperties.asScala
          .filter { case (name, _) =>
            !fieldPatterns.exists(
              _.matcher(currentPath.map(_ + "_" + name).getOrElse(name)).matches()
            )
          }
          .map { case (name, pSchema) =>
            name -> processExclusion(pSchema, currentPath.map(_ + "_" + name))
          }
          .toMap[String, OpenAPISwaggerSchema[?]]
          .asJava
        os.setProperties(newProperties)
        os
      }

      currentSchema match {
        case s if isObjectSchemaLike(s) =>
          handleObjectSchema(s)
        case s if isArrayOfStructSchemaLike(s) =>
          s.setItems(processExclusion(s.getItems, currentPath))
          s
        case _ => currentSchema
      }
    }
    if (fieldPatterns.isEmpty) {
      responseSchema
    } else {
      processExclusion(responseSchema)
    }
  }

  /** Explodes a given structure schema into a flat map of property paths and their corresponding
    * schemas. The method traverses the schema recursively based on the given explosion strategy,
    * and processes object and array schemas to produce a linearized representation.
    *
    * @param responseSchema
    *   The root OpenAPI schema to be exploded. It must be an object schema.
    * @param explosionStrategy
    *   The strategy used to determine the level and type of schema explosion (e.g., All, Object,
    *   Array).
    * @return
    *   A map where keys are the exploded property paths and values are tuples containing the
    *   corresponding OpenAPI schema and an optional description.
    */
  private def explodeStructureSchema(
    responseSchema: OpenAPISwaggerSchema[?],
    explosionStrategy: RouteExplosionStrategy,
    excludeProperties: List[Pattern]
  ): Map[String, (OpenAPISwaggerSchema[?], Option[String])] = {
    def internalExplodeProperties(
      os: OpenAPISwaggerSchema[?],
      currentPath: Option[String] = None
    ): Map[SchemaDescription, (OpenAPISwaggerSchema[?], Option[SchemaDescription])] = {
      os.getProperties.asScala
        .filter { case (_, s) =>
          isObjectSchemaLike(s) || isArrayOfStructSchemaLike(s)
        }
        .map { case (property, schema) =>
          internalExplode(currentPath.map(_ + "_").getOrElse("") + property, schema)
        }
        .flatten
        .toMap
    }

    def internalExplode(
      currentPath: String,
      currentSchema: OpenAPISwaggerSchema[?]
    ): Map[String, (OpenAPISwaggerSchema[?], Option[String])] = {
      def handleObjectSchema(os: OpenAPISwaggerSchema[?]) = {
        if (explosionStrategy == All || explosionStrategy == `Object`) {
          Map(currentPath -> (os -> Option(os.getDescription)))
        } else {
          internalExplodeProperties(os, Some(currentPath))
        }
      }

      if (excludeProperties.exists(_.matcher(currentPath).matches())) {
        logger.info("Excluding property " + currentPath)
        Map.empty
      } else {
        currentSchema match {
          case s if isObjectSchemaLike(s) =>
            handleObjectSchema(s)
          case s if isArrayOfStructSchemaLike(s) =>
            if (explosionStrategy == All || explosionStrategy == Array) {
              Map(
                currentPath -> (s.getItems -> Option(s.getItems.getDescription)
                  .orElse(Option(s.getDescription)))
              )
            } else {
              s.getItems.getProperties.asScala
                .filter { case (_, s) =>
                  isObjectSchemaLike(s) || isArrayOfStructSchemaLike(s)
                }
                .map { case (property, schema) =>
                  internalExplode(currentPath + "_" + property, schema)
                }
                .flatten
                .toMap
            }
        }
      }
    }
    require(
      isObjectSchemaLike(responseSchema),
      "Expected an object schema but found " + responseSchema.getClass.getName
    )
    internalExplodeProperties(responseSchema)
  }

  /** Extracts a list of essential API information from the provided OpenAPI file path. This method
    * parses the OpenAPI file, extracts relevant schema and operation details, and organizes them
    * into a collection of `ApiEssentialInformation`.
    *
    * @param openApiFilePath
    *   the file path to the OpenAPI definition, which is used to read and parse the API
    *   specification.
    * @return
    *   a list of essential API information, where each entry provides details such as tags, schema,
    *   method, path, descriptions, and related metadata.
    */
  private def extractApiEssentialInformation(
    openApiFilePath: String
  ): List[ApiEssentialInformation] = {
    val parseOptions = new ParseOptions()
    parseOptions.setResolve(true)
    parseOptions.setResolveFully(true)
    val openAPI: OpenAPI =
      new OpenAPIV3Parser()
        .readContents(
          openApiFilePath,
          null,
          parseOptions
        )
        .getOpenAPI
    val schemaNames = openAPI.getComponents.getSchemas.asScala.toList
      .groupBy { case (_, schema) => schema }
      .view
      .mapValues(_.map { case (schemaName, _) => schemaName })
      .toMap
    (for {
      pathInfo      <- extractPathInfos(openAPI)
      operation     <- getOperations(pathInfo.pathItem)
      validResponse <- get2XXResponses(operation.operation)
      structureSchemas = extractStructureSchema(validResponse.apiResponse, schemaNames)
    } yield {
      if (structureSchemas.nonEmpty) {
        val jsonMediaType = """.*json.*""".r
        val xmlMediaType = """.*xml.*""".r
        structureSchemas
          .sortBy { r =>
            Option(r.mediaType).getOrElse("").toLowerCase match {
              case jsonMediaType() => 1
              case xmlMediaType()  => 2
              case _               => 3
            }
          }
          .headOption
          .flatMap { schema =>
            Some(
              ApiEssentialInformation(
                tags = operation.tags,
                schema = RichOpenAPISchema(schema.schema, schema.schemaNames),
                apiMethod = operation.method,
                apiPath = pathInfo.apiPath,
                attributePath = None,
                attributeDescription = None,
                schemaDescription = schema.schemaDescription,
                responseDescription = validResponse.description.orElse(validResponse.summary),
                operationDescription = operation.description.orElse(operation.summary),
                pathDescription = pathInfo.description.orElse(pathInfo.summary)
              )
            )
          }
      } else {
        None
      }
    }).flatten
  }

  /** Converts an OpenAPI schema definition into a Starlake schema representation.
    *
    * @param openAPIschema
    *   The OpenAPI schema to be converted. It is expected to adhere to the OpenAPI specification
    *   and include the relevant properties, definitions, and formats to describe the structure.
    * @param formatTypeMapping
    *   A mapping between OpenAPI formats (e.g., date-time, binary) and their corresponding Starlake
    *   type equivalents (e.g., timestamp, string).
    * @return
    *   A function that accepts a schema name (as a string) and generates a corresponding Starlake
    *   `Schema` instance. This `Schema` contains the converted attributes and metadata from the
    *   OpenAPI schema.
    */
  private def openAPISchemaToStarlakeSchema(
    openAPIschema: OpenAPISwaggerSchema[?],
    formatTypeMapping: Map[String, String]
  ): SchemaName => SchemaInfo = {
    val typeIgnored = scala.collection.mutable.ListBuffer[String]()
    def buildAttribute(
      fieldName: String,
      attributeSchema: OpenAPISwaggerSchema[?]
    ): TableAttribute = {
      def asAttributeOf(
        attributeType: String,
        childAttributes: List[TableAttribute] = Nil
      ): TableAttribute = {
        val comment: Option[String] = Option(attributeSchema.getEnum)
          .map("One of: " + _.asScala.toList.map(_.toString).mkString(", "))
          .getOrElse("") match {
          case "" => Option(attributeSchema.getDescription)
          case enums =>
            Option(attributeSchema.getDescription)
              .map { value =>
                if (value.endsWith(".")) {
                  value + " " + enums
                } else {
                  value + ". " + enums
                }
              }
              .orElse(Some(enums))
        }
        val sanitizedFieldName =
          NamingUtils.normalizeAttributeName(fieldName, toSnakeCase = applySnakeCaseOnName)

        new TableAttribute(
          name = if (attributeSanitizeStrategy == OnExtract) sanitizedFieldName else fieldName,
          rename =
            if (attributeSanitizeStrategy == OnExtract) None
            else Some(sanitizedFieldName).filter(_ != fieldName),
          `type` = attributeType,
          required = Option(attributeSchema.getNullable).map(!_),
          comment = comment,
          attributes = childAttributes,
          sample = Option(attributeSchema.getExample).map(_.toString)
        )
      }

      def resolveType(attributeTypes: List[String]): String = {
        val resolvedType =
          if (attributeTypes.contains("object") || attributeTypes.contains("array")) {
            PrimitiveType.variant.toString
          } else if (attributeTypes.contains("integer")) {
            PrimitiveType.int.toString
          } else if (attributeTypes.contains("string")) {
            PrimitiveType.string.toString
          } else if (attributeTypes.contains("number")) {
            PrimitiveType.decimal.toString
          } else if (attributeTypes.contains("boolean")) {
            PrimitiveType.boolean.toString
          } else {
            attributeTypes match {
              case t :: _ =>
                t
              case _ =>
                PrimitiveType.variant.toString
            }
          }
        if (attributeTypes.size > 1) {
          logger.warn(
            "Using attribute type " + resolvedType + " out of " + attributeTypes
              .mkString(", ")
          )
        }
        resolvedType
      }

      def formatAttribute(
        typeToFormat: SchemaDescription,
        detectWithPrimitives: Boolean = false
      ) = {
        val attributeType = formatTypeMapping.getOrElse(
          typeToFormat,
          typeToFormat match {
            //            case "date-time" => PrimitiveType.timestamp.toString
            //            case "date"      => PrimitiveType.date.toString
            case "time"     => PrimitiveType.string.toString
            case "duration" => PrimitiveType.string.toString
            case ""         => PrimitiveType.string.toString
            case undefinedType =>
              val primitiveDetection = if (detectWithPrimitives) {
                Try {
                  new PrimitiveTypeDeserializer().simpleTypeFromString(undefinedType)
                }.toOption
              } else None
              primitiveDetection match {
                case Some(p) => p.value
                case None =>
                  if (!typeIgnored.contains(undefinedType)) {
                    logger.info(s"Attribute format $undefinedType ignored and formatted as String")
                    typeIgnored.append(undefinedType)
                  }
                  attributeSchema match {
                    case _: JsonSchema => PrimitiveType.variant.toString
                    case _             => PrimitiveType.string.toString
                  }
              }
          }
        )
        asAttributeOf(attributeType)
      }

      attributeSchema match {
        case _: ArraySchema =>
          buildAttribute(fieldName, attributeSchema.getItems).copy(
            array = Some(true),
            required = Option(attributeSchema.getNullable).map(!_),
            comment = Option(attributeSchema.getDescription)
          )
        case _ if isArrayOfStructSchemaLike(attributeSchema) =>
          buildAttribute(fieldName, attributeSchema.getItems).copy(
            array = Some(true),
            required = Option(attributeSchema.getNullable).map(!_),
            comment = Option(attributeSchema.getDescription)
          )
        case s if isObjectSchemaLike(s) =>
          val attributes = buildAttributes(attributeSchema)
          if (attributes.isEmpty) {
            asAttributeOf(PrimitiveType.variant.toString)
          } else {
            asAttributeOf(PrimitiveType.struct.toString, attributes)
          }
        case _: UUIDSchema | _: EmailSchema | _: FileSchema | _: PasswordSchema | _: BinarySchema |
            _: ByteArraySchema =>
          asAttributeOf(PrimitiveType.string.toString)
        case _: DateSchema     => asAttributeOf(PrimitiveType.date.toString)
        case _: DateTimeSchema => asAttributeOf(PrimitiveType.timestamp.toString)
        case _: IntegerSchema =>
          asAttributeOf(PrimitiveType.long.toString)
        case _: NumberSchema =>
          asAttributeOf(PrimitiveType.decimal.toString)
        case _: BooleanSchema =>
          asAttributeOf(PrimitiveType.boolean.toString)
        case os if isMapSchema(os) =>
          asAttributeOf(PrimitiveType.variant.toString)
        case _ if isAttributeFormatBasedSchema(attributeSchema) =>
          val format = Option(attributeSchema.getFormat).getOrElse("")
          formatAttribute(format)
        case _ if isAttributeTypedBasedSchema(attributeSchema) =>
          val attributeTypes =
            Option(attributeSchema.getType)
              .map(List(_))
              .getOrElse(attributeSchema.getTypes.asScala.toList)
          val typeToFormat: String = resolveType(attributeTypes)
          formatAttribute(typeToFormat, detectWithPrimitives = true)
        case _ =>
          throw new RuntimeException(
            s"OpenAPI type ${attributeSchema.getClass.getSimpleName} is not handled during starlake schema conversion."
          )
      }
    }
    def buildAttributes(schemaWithAttributes: OpenAPISwaggerSchema[?]): List[TableAttribute] = {
      def handleObjectSchema = {
        Option(schemaWithAttributes.getProperties)
          .map(_.asScala)
          .getOrElse(Nil)
          .map { case (propertyName, propertySchema) =>
            buildAttribute(propertyName, propertySchema)
          }
          .toList
      }

      schemaWithAttributes match {
        case s if isObjectSchemaLike(s) =>
          handleObjectSchema
        case _ =>
          throw new RuntimeException(
            s"OpenAPI type ${schemaWithAttributes.getClass.getSimpleName} is not expected to have properties. If it's the root object, Starlake doesn't support it yet."
          )
      }
    }
    val attributes = buildAttributes(openAPIschema)
    (schemaName: String) =>
      SchemaInfo(
        name = schemaName,
        pattern = Pattern.compile(Pattern.quote(schemaName) + ".*"),
        attributes = attributes
      )
  }
}
