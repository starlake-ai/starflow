package ai.starlake.utils

import ai.starlake.config.Settings.{latestSchemaVersion, AppConfig}
import ai.starlake.config.{ApplicationDesc, CometColumns, Settings}
import ai.starlake.exceptions.SchemaValidationException
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.*
import ai.starlake.utils.ImplicitRichPath.*
import ai.starlake.utils.YamlMigrator.V1.TableForExtractConfig
import com.fasterxml.jackson.databind.node.{ArrayNode, BooleanNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema._
import com.networknt.schema.path.PathType
import com.networknt.schema.walk.{ApplyDefaultsStrategy, WalkConfig}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object YamlSerde extends LazyLogging with YamlUtils {
  val mapper: ObjectMapper = Utils.newYamlMapper()

  def serialize[T](entity: T): String = mapper.writeValueAsString(entity)

  /** wrap entity to a container if possible.
    */
  private def wrapEntityToDesc[T](entity: T) = {
    entity match {
      case e: AutoJobInfo        => TransformDesc(latestSchemaVersion, e)
      case e: AutoTaskInfo       => TaskDesc(latestSchemaVersion, e)
      case e: DomainInfo         => DomainDesc(latestSchemaVersion, e)
      case e: SchemaInfo         => TableDesc(latestSchemaVersion, e)
      case e: ExtractSchemasInfo => ExtractDesc(latestSchemaVersion, e)
      case e: DagInfo            => DagDesc(latestSchemaVersion, e)
      case e: AppConfig          => ApplicationDesc(latestSchemaVersion, e)
      case _                     => entity
    }
  }

  def serializeToPath[T](targetPath: Path, entity: T)(implicit storage: StorageHandler): Unit = {
    storage.write(serialize(wrapEntityToDesc(entity)), targetPath)
  }

  def deserializeIamPolicyTags(content: String): IamPolicyTags = {
    val rootNode = mapper.readTree(content)
    mapper.treeToValue(rootNode, classOf[IamPolicyTags])
  }

  def toMap(job: AutoJobInfo)(implicit settings: Settings): Map[String, Any] = {
    val jobWriter = mapper
      .writer()
      .withAttribute(classOf[Settings], settings)
    val jsonContent = jobWriter.writeValueAsString(job)
    // val jobReader = mapper.reader().withAttribute(classOf[Settings], settings)
    mapper.readValue(jsonContent, classOf[Map[String, Any]])
  }

  /** Built once: this means parsing a 110 KB file, and a project validates many config files. */
  private[starlake] lazy val starlakeSchema: Schema = forceLocaleIn(Locale.ROOT) {
    // javaSemantics no longer exists in 2.0.4: losslessNarrowing(true) reproduces the same
    // effect, since TypeFactory's only javaSemantics check was `isJavaSemantics() || isLosslessNarrowing()`.
    val schemaRegistryConfig = SchemaRegistryConfig
      .builder()
      .pathType(PathType.JSON_PATH)
      .formatAssertionsEnabled(true)
      .losslessNarrowing(true)
      .build()

    val registry = SchemaRegistry.withDefaultDialect(
      SpecificationVersion.DRAFT_2019_09,
      (builder: SchemaRegistry.Builder) => {
        builder.schemaRegistryConfig(schemaRegistryConfig); ()
      }
    )

    // The file is plain JSON, which the JSON parser reads far faster than the YAML one.
    registry.getSchema(
      adaptSchemaV7ToStrictV201909(
        new ObjectMapper().readTree(getClass.getResourceAsStream("/starlake.json"))
      )
    )
  }

  private def forceLocaleIn[T](locale: Locale)(func: => T) = {
    val previousDefault = Locale.getDefault
    Locale.setDefault(locale)
    try {
      func
    } finally {
      Locale.setDefault(previousDefault)
    }
  }

  private def adaptSchemaV7ToStrictV201909(node: JsonNode): JsonNode = {
    def adaptIt(currentNodeName: String, node: JsonNode): (String, JsonNode) = {
      node match {
        case on: ObjectNode =>
          val newObjectNode = mapper.createObjectNode()
          val objectType = Option(on.get("type"))
            .flatMap {
              case t: TextNode => Some(t.asText())
              case _           => None
            }
            .getOrElse("")
          if (
            "object".equalsIgnoreCase(objectType) && !on.has(
              "additionalProperties"
            ) && !currentNodeName.endsWith("Base")
          ) {
            newObjectNode.set[JsonNode]("unevaluatedProperties", BooleanNode.FALSE)
          }
          node.fields().asScala.foreach { kv =>
            val (newKey, newValue) = adaptIt(kv.getKey, kv.getValue)
            newObjectNode.set[JsonNode](newKey, newValue)
          }
          val newNodeName = if (currentNodeName == "definitions") "$defs" else currentNodeName
          newNodeName -> newObjectNode
        case an: ArrayNode =>
          val newArrayNode = mapper
            .createArrayNode()
          newArrayNode
            .addAll(
              an.asScala
                .map { currentNode =>
                  val (_, newValue) = adaptIt(currentNodeName, currentNode)
                  newValue
                }
                .toList
                .asJava
            )
          currentNodeName -> newArrayNode
        case _: TextNode if currentNodeName == "$schema" =>
          currentNodeName -> new TextNode(SpecificationVersion.DRAFT_2019_09.getDialectId)
        case tn: TextNode if currentNodeName == "$ref" =>
          currentNodeName -> new TextNode(tn.asText().replaceFirst("^#/definitions/", "#/\\$defs/"))
        case _ => currentNodeName -> node
      }
    }
    val (_, newSchema) = adaptIt("", node)
    newSchema
  }

  /** Validate and enrich given config with default values defined in schema.
    * @throws SchemaValidationException
    *   If not valid
    */
  @throws[SchemaValidationException]
  def validateConfigFile(
    subPath: String,
    content: String,
    inputFilename: String,
    migrationList: List[YamlMigratorInterface],
    postProcess: Option[YamlMigratorInterface] = None
  ): JsonNode = {
    val rawRootNode: JsonNode = mapper.readTree(content)
    validateConfigFileFromNode(subPath, rawRootNode, inputFilename, migrationList, postProcess)
  }

  /** Validate and enrich given config with default values defined in schema.
    * @throws SchemaValidationException
    *   If not valid
    */
  @throws[SchemaValidationException]
  def validateConfigFileFromNode(
    subPath: String,
    rawRootNode: JsonNode,
    inputFilename: String,
    migrationList: List[YamlMigratorInterface],
    postProcess: Option[YamlMigratorInterface] = None
  ): JsonNode = {
    val effectiveRootNode = if (migrationList.exists(_.canMigrate(rawRootNode))) {
      logger.warn(s"Migrating config of $inputFilename on-the-fly")
      migrationList.foldLeft(rawRootNode) { case (node, migrator) =>
        migrator.migrate(node)
      }
    } else {
      rawRootNode
    }
    if (!effectiveRootNode.hasNonNull(subPath)) {
      throw new RuntimeException(
        s"No '$subPath' attribute found in $inputFilename. Please check your config and define it under '$subPath' attribute."
      )
    }
    val validationResult: Result =
      forceLocaleIn(Locale.ROOT) { // Use root instead of ENGLISH otherwise it fallbacks to local language if it exists. ROOT messages are in ENGLISH.
        val schema = starlakeSchema
        val executionContext = schema.createExecutionContext()
        executionContext.setWalkConfig(
          WalkConfig
            .builder()
            .applyDefaultsStrategy(new ApplyDefaultsStrategy(true, true, true))
            .build()
        )
        schema.walk(
          executionContext,
          effectiveRootNode,
          true
        )
      }
    // getErrors() is a plain List in 2.0.4 (was a value-equality Set in 1.4.0's getValidationMessages()),
    // so oneOf/anyOf branches that raise structurally-identical errors are no longer deduped upstream.
    // Error overrides equals/hashCode by value (keyword, instanceLocation, evaluationPath, details, ...),
    // so .distinct here reproduces the old Set semantics exactly.
    val validationMessages = validationResult.getErrors.asScala.toList.distinct

    if (validationMessages.nonEmpty) {
      val formattedErrors = validationMessages
        .map(error => error.getInstanceLocation.toString -> error.toString)
        .groupBy { case (location, _) =>
          location
        }
        .view
        .mapValues(_.map { case (_, errorMessage) =>
          errorMessage
        }.mkString("\n     - ", "\n     - ", ""))
        .values
        .toList
        .sorted
        .mkString("")
      throw new SchemaValidationException(
        s"Invalid content for $inputFilename:$formattedErrors"
      )
    }
    postProcess
      .map { f =>
        if (f.canMigrate(effectiveRootNode)) {
          f.migrate(effectiveRootNode)
        } else {
          throw new RuntimeException(
            s"Post process hasn't been applied for $inputFilename but was expected to."
          )
        }
      }
      .getOrElse(effectiveRootNode)
  }

  def deserializeYamlExtractConfig(
    content: String,
    inputFilename: String,
    propageDefault: Boolean = true
  ): ExtractSchemasInfo = {
    val extractSubPath = "extract"
    val extractNode =
      validateConfigFile(
        extractSubPath,
        content,
        inputFilename,
        List(YamlMigrator.V1.ExtractConfig),
        Some(YamlMigrator.ScalaClass.ExtractConfig)
      ).path(extractSubPath)
    val jdbcSchemas = mapper.treeToValue(extractNode, classOf[ExtractSchemasInfo])
    if (propageDefault) {
      jdbcSchemas.propagateGlobalJdbcSchemas()
    } else {
      jdbcSchemas
    }
  }
  def deserializeYamlRefs(content: String, path: String): RefDesc = {
    val refsSubPath = "refs"
    val refsNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.RefsConfig))
    mapper.treeToValue(refsNode, classOf[RefDesc])
  }

  def deserializeYamlApplicationNode(content: String, path: String): JsonNode = {
    val refsSubPath = "application"
    validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.ApplicationConfig))
  }

  def deserializeYamlTables(content: String, path: String): List[TableDesc] = {
    val withNewCometFilename = content.replaceAll(
      CometColumns.oldCometInputFileNameColumn,
      CometColumns.cometInputFileNameColumn
    )
    deserializeYamlTables(mapper.readTree(withNewCometFilename), path)
  }

  private def deserializeYamlTables(jsonRootNode: JsonNode, path: String): List[TableDesc] = {
    Try {
      val rootNode = {
        val node = if (YamlMigrator.PreV1.TableConfig.canMigrate(jsonRootNode)) {
          YamlMigrator.PreV1.TableConfig.migrate(jsonRootNode)
        } else {
          jsonRootNode
        }
        node match {
          case on: ObjectNode => on
          case _ =>
            throw new RuntimeException(
              s"Expected an ObjectNode in $path but found ${node.getClass.getSimpleName}"
            )
        }
      }
      val tableListSubPath = "tables"
      val tablesNode = if (rootNode.has(tableListSubPath)) {
        rootNode.path(tableListSubPath) match {
          case tableList: ArrayNode =>
            tableList.asScala.map(wrapToContainer("table"))
          case n =>
            throw new RuntimeException(
              s"Expecting array for tables but found ${n.getNodeType.name()}"
            )
        }
      } else {
        List(rootNode)
      }
      tablesNode.map { rawTableNode =>
        // fallback to table since this is how we should define tables in starlake
        val tableNode =
          validateConfigFileFromNode(
            "table",
            rawTableNode,
            path,
            List(YamlMigrator.V1.TableConfig)
          )
        val metadata = tableNode.path("metadata")
        val isJsonArray = if (!metadata.isMissingNode) {
          metadata.path("format").asText().toLowerCase() == "array_json"
        } else
          false
        val ref = mapper.treeToValue(tableNode, classOf[TableDesc])
        if (isJsonArray)
          ref.copy(table =
            ref.table
              .copy(metadata = ref.table.metadata.map(m => m.copy(array = Some(true))))
          )
        else
          ref
      }.toList
    } match {
      case Success(value) => value
      case Failure(exception) =>
        exception.printStackTrace()
        throw new Exception(s"Invalid Schema file: $path(${exception.getMessage})", exception)
    }
  }

  def deserializeYamlLoadConfig(
    content: String,
    path: String,
    isForExtract: Boolean
  ): Try[DomainInfo] = {
    Try {
      val loadSubPath = "load"
      val filePath = new Path(path)
      val rawRootNode: JsonNode = mapper.readTree(content)
      val attachedTables: List[SchemaInfo] =
        if (
          !rawRootNode.has("version") && (rawRootNode.path("load").has("tables")) || rawRootNode
            .has("tables")
        ) {
          val tablesNode =
            if (rawRootNode.has("tables")) rawRootNode.path("tables")
            else rawRootNode.path("load").path("tables")
          val tables = if (isForExtract) {
            new TableForExtractConfig(filePath.fileNameWithoutSlExt)
              .migrate(wrapToContainer("tables")(tablesNode))
          } else {
            tablesNode
          }
          deserializeYamlTables(wrapToContainer("tables")(tables), path).map(_.table)
        } else {
          Nil
        }
      val domainNode =
        validateConfigFileFromNode(
          loadSubPath,
          rawRootNode,
          path,
          List(
            YamlMigrator.V1.LoadConfig
          )
        )
      val loadDesc = mapper.treeToValue(domainNode, classOf[DomainDesc])
      loadDesc.load.copy(tables = attachedTables)
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        logger.error(s"Invalid domain file: $path(${exception.getMessage})", exception)
        Failure(exception)
    }
  }

  def deserializeYamlTypes(content: String, path: String): List[Type] = {
    val refsSubPath = "types"
    val refsNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.TypesConfig))
    mapper.treeToValue(refsNode, classOf[TypesInfo]).types
  }

  def deserializeYamlDagConfig(content: String, path: String): Try[DagInfo] = {
    Try {
      val dagSubPath = "dag"
      val dagNode = validateConfigFile(dagSubPath, content, path, List(YamlMigrator.V1.DagConfig))
      mapper.treeToValue(dagNode, classOf[DagDesc])
    } match {
      case Success(value) => Success(value.dag)
      case Failure(exception) =>
        logger.error(s"Invalid dag file: $path(${exception.getMessage})")
        Failure(exception)
    }
  }

  def deserializeYamlEnvConfig(content: String, path: String): EnvDesc = {
    val envSubPath = "env"
    if (content.trim.isEmpty) {
      EnvDesc(latestSchemaVersion, Map.empty)
    } else {
      val dagNode = validateConfigFile(envSubPath, content, path, List(YamlMigrator.V1.EnvConfig))
      mapper.treeToValue(dagNode, classOf[EnvDesc])
    }
  }

  // Used by starlake-api
  def deserializeYamlTask(content: String, path: String): AutoTaskInfo = {
    val refsSubPath = "task"
    val taskNode = validateConfigFile(refsSubPath, content, path, List(YamlMigrator.V1.TaskConfig))
      .path("task") match {
      case oNode: ObjectNode => oNode
      case _ =>
        throw new RuntimeException("Should never happen since it has been validated")
    }
    mapper.treeToValue(taskNode, classOf[AutoTaskInfo])
  }

  def deserializeYamlTransform(content: String, path: String): Try[AutoJobInfo] = {
    Try {
      val transformSubPath = "transform"
      val transformNode =
        validateConfigFile(transformSubPath, content, path, List(YamlMigrator.V1.TransformConfig))
      mapper.treeToValue(transformNode, classOf[TransformDesc]).transform
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        if (content.trim == "transform:") {
          logger.warn(s"Empty transform file: $path")
          Success(AutoJobInfo("", Nil))
        } else {
          logger.error(s"Invalid transform file: $path(${exception.getMessage})")
          Failure(exception)
        }
    }
  }
}
