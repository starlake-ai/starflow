package ai.starlake.extract

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.StringUtils
import ai.starlake.extract.spi.SchemaExtractorWorkflow
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.utils.StarlakeNotFoundException
import ai.starlake.utils.YamlSerde
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class ExtractSchema(schemaHandler: SchemaHandler) extends ExtractPathHelper with LazyLogging {

  implicit val schemaHandlerImplicit: SchemaHandler = schemaHandler

  @nowarn
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] = {
    ExtractSchemaCmd.run(args, schemaHandler).map(_ => ())
  }
  def run(config: ExtractSchemaConfig)(implicit settings: Settings): Unit = {
    ExtractUtils.timeIt("Schema extraction") {
      extract(config)
    }
  }

  def extractTable(
    domain: String,
    table: String,
    connectionName: Option[String],
    accessToken: Option[String]
  )(implicit
    settings: Settings
  ): Try[DomainInfo] =
    extractTable(s"$domain.$table", connectionName, accessToken)

  def extractTable(
    domainAndTableName: String,
    connectionName: Option[String],
    accessToken: Option[String]
  )(implicit
    settings: Settings
  ): Try[DomainInfo] = Try {
    val userConfig = ExtractSchemaConfig(
      connectionRef = connectionName.orElse(Some(settings.appConfig.connectionRef)),
      tables = List(domainAndTableName),
      external = true,
      accessToken = accessToken
    )
    val extractedDomains = extract(userConfig)
    extractedDomains match {
      case Some(domains) if domains.nonEmpty =>
        domains.headOption.getOrElse(throw new StarlakeNotFoundException("No domain extracted"))
      case _ =>
        throw new Exception(s"Could not extract table $domainAndTableName")
    }
  }

  /** Generate YML file from JDBC Schema stored in a YML file
    *
    * @param jdbcMapFile
    *   : Yaml File containing the JDBC Schema to extract
    * @param ymlOutputDir
    *   : Where to output the YML file. The generated filename will be in the for
    *   TABLE_SCHEMA_NAME.yml
    * @param settings
    *   : Application configuration file
    */
  def extract(userConfig: ExtractSchemaConfig)(implicit
    settings: Settings
  ): Option[List[DomainInfo]] = {
    val jdbcSchemas = fromConfig(userConfig)
    val connectionSettings: ConnectionInfo = jdbcSchemas.connectionRef match {
      case Some(connectionRef) =>
        settings.appConfig.getConnection(connectionRef).withAccessToken(userConfig.accessToken)
      case None =>
        userConfig.connectionRef
          .map(settings.appConfig.getConnection)
          .getOrElse(settings.appConfig.getDefaultConnection())
          .withAccessToken(userConfig.accessToken)
    }
    jdbcSchemas.openAPI match {
      case Some(_) =>
        // TODO: temporarily doing switch case but plans to remove it once all others extractions implement Schema Extractor
        // TODO: implement templates as in jdbcSchema
        SchemaExtractorWorkflow.run(userConfig, jdbcSchemas)
        None
      case None =>
        val extractArea = if (userConfig.external) DatasetArea.external else DatasetArea.extract
        if (connectionSettings.isBigQuery()) {
          val extractedDomains =
            jdbcSchemas.jdbcSchemas.getOrElse(Nil).flatMap { jdbcSchema =>
              val bigQueryConfig =
                ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(userConfig, jdbcSchema)
              ExtractBigQuerySchemaCmd.extract(bigQueryConfig, schemaHandler)
            }
          Some(extractedDomains)
        } else { // JDBC
          val extractedDomains =
            ParUtils.withExecutor(userConfig.parallelism) { implicit extractEC =>
              implicit val extractExecutionContext: ExtractExecutionContext =
                new ExtractExecutionContext(extractEC)
              ParUtils.runInParallel(
                jdbcSchemas.jdbcSchemas.getOrElse(Nil),
                userConfig.parallelism
              ) { jdbcSchema =>
                val domainTemplate = jdbcSchema.template
                  .orElse {
                    val defaultDomainFile = "_domain_" + userConfig.extractConfig
                    if (
                      settings
                        .storageHandler()
                        .exists(mappingPath(extractArea, defaultDomainFile))
                    ) {
                      Some(defaultDomainFile)
                    } else {
                      None
                    }
                  }
                  .map { ymlTemplate =>
                    val domainTemplatePath = mappingPath(extractArea, ymlTemplate)
                    logger.info(s"Loading domain template from $domainTemplatePath")
                    val content = settings
                      .storageHandler()
                      .read(domainTemplatePath)
                    YamlSerde
                      .deserializeYamlLoadConfig(
                        content,
                        ymlTemplate,
                        isForExtract = true
                      ) match {
                      case Success(domain) =>
                        val tableTemplatePath = mappingPath(
                          extractArea,
                          jdbcSchema.template
                            .map(t => "_table_" + t)
                            .getOrElse("_table_" + userConfig.extractConfig)
                        )
                        if (settings.storageHandler().exists(tableTemplatePath)) {
                          logger.info(s"Loading table template from $tableTemplatePath")
                          val tableContent = settings
                            .storageHandler()
                            .read(tableTemplatePath)
                          val tableDesc: List[TableDesc] =
                            YamlSerde
                              .deserializeYamlTables(tableContent, tableTemplatePath.toString)
                          domain.copy(tables = tableDesc.map(_.table))
                        } else {
                          domain
                        }
                      case Failure(e) => throw e
                    }
                  }
                val currentDomain = schemaHandler.getDomain(jdbcSchema.schema, raw = true)
                ExtractUtils.timeIt(s"Schema extraction of ${jdbcSchema.schema}") {
                  extractSchema(
                    jdbcSchema,
                    connectionSettings,
                    schemaOutputDir(userConfig),
                    domainTemplate,
                    currentDomain,
                    userConfig.external
                  )
                }
              }
            }
          Some(extractedDomains.toList)
        }
    }
  }

  private def fromConfig(
    config: ExtractSchemaConfig
  )(implicit settings: Settings): ExtractSchemasInfo = {
    val extractArea = if (config.external) DatasetArea.external else DatasetArea.extract
    val extractConfigPath = mappingPath(extractArea, config.extractConfig)
    val ymlExtractConfig =
      if (settings.storageHandler().exists(extractConfigPath)) {
        val content = settings
          .storageHandler()
          .read(extractConfigPath)
          .richFormat(schemaHandler.activeEnvVars(), Map.empty)
        val extractSchemasConfig =
          YamlSerde.deserializeYamlExtractConfig(content, config.extractConfig)
        Some(extractSchemasConfig)
      } else {
        None
      }
    if (config.all) {
      val connectionRef =
        ymlExtractConfig
          .flatMap(_.connectionRef)
          .orElse(config.connectionRef)
          .getOrElse(settings.appConfig.connectionRef)
      val schemaNames = ParUtils.withExecutor() { ec =>
        implicit val extractEC: ExtractExecutionContext = new ExtractExecutionContext(ec)
        ExtractSchema.extractSchemaNames(Some(connectionRef), config.accessToken)
      }
      val result =
        schemaNames match {
          case Failure(e) =>
            throw e
          case Success(schemaNames) =>
            val jdbcSchemas = schemaNames.map { case (schema, tables) =>
              val jdbcSchema = JDBCSchema(
                schema = schema,
                tables = tables.map { t => new JDBCTable().copy(name = t) },
                pattern = None,
                template = None
              )
              jdbcSchema
            }
            ExtractSchemasInfo(jdbcSchemas = Some(jdbcSchemas), connectionRef = Some(connectionRef))
        }
      result
    } else {
      if (config.tables.isEmpty && config.extractConfig.isEmpty) {
        throw new Exception("Either tables or extractConfig must be defined")
      }
      if (config.tables.nonEmpty) {
        val jdbcTablesDesc = config.tables
          .map { table =>
            val parts = table.split("\\.")
            if (parts.length != 2) {
              throw new Exception(s"Invalid table format: $table")
            }
            val schema = parts(0)
            val t = parts(1)
            (schema, t)
          }
          .groupBy(_._1)
          .toList

        val jdbcSchemas =
          jdbcTablesDesc.map { case (schema, tables) =>
            val jdbcSchema = JDBCSchema(
              schema = schema,
              tables = tables.map { t => new JDBCTable().copy(name = t._2) }.toList,
              pattern = None,
              template = None
            )
            jdbcSchema
          }
        ExtractSchemasInfo(jdbcSchemas = Some(jdbcSchemas), connectionRef = config.connectionRef)
      } else {
        ymlExtractConfig match {
          case Some(extractSchemasConfig) =>
            extractSchemasConfig
          case None =>
            throw new FileNotFoundException(
              s"Could not found extract config ${config.extractConfig}. Please check its existence."
            )
        }
      }
    }
  }

  def extractSchema(
    jdbcSchema: JDBCSchema,
    connectionSettings: ConnectionInfo,
    baseOutputDir: Path,
    domainTemplate: Option[DomainInfo],
    currentDomain: Option[DomainInfo],
    external: Boolean
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): DomainInfo = {
    implicit val storageHandler: StorageHandler = settings.storageHandler()
    val domainName = jdbcSchema.sanitizeName match {
      case Some(true) => StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
      case _          => jdbcSchema.schema
    }
    storageHandler.mkdirs(new Path(baseOutputDir, domainName))

    // real extraction happens here
    val extractedDomain = extractDomain(jdbcSchema, connectionSettings, domainTemplate)

    val domain = extractedDomain.copy(
      comment = extractedDomain.comment.orElse(currentDomain.flatMap(_.comment)),
      tags =
        if (extractedDomain.tags.nonEmpty) extractedDomain.tags
        else currentDomain.map(_.tags).getOrElse(Set.empty),
      rename = extractedDomain.rename.orElse(currentDomain.flatMap(_.rename)),
      database = extractedDomain.database.orElse(currentDomain.flatMap(_.database)),
      metadata = Metadata
        .mergeAll(Nil ++ currentDomain.flatMap(_.metadata) ++ extractedDomain.metadata)
        .copy(fillWithDefaultValue = false)
        .asOption()
    )
    val tables =
      domain.tables.map { table =>
        val restoredTable =
          currentDomain.flatMap(_.tables.find(_.name == table.name)) match {
            case Some(currentTable) =>
              val mergedTable = table.mergeWith(
                currentTable,
                domain.metadata,
                AttributeMergeStrategy(
                  failOnContainerMismatch = false,
                  failOnAttributesEmptinessMismatch = false,
                  keepSourceDiffAttributesStrategy = KeepOnlyScriptDiff,
                  attributePropertiesMergeStrategy = RefFirst
                )
              )
              mergedTable.copy(metadata =
                mergedTable.metadata.map(_.copy(fillWithDefaultValue = false))
              )
            case None =>
              table.copy(metadata =
                Metadata
                  .mergeAll(Nil ++ domain.metadata ++ table.metadata)
                  .`keepIfDifferent`(
                    domain.metadata.getOrElse(Metadata())
                  )
                  .copy(fillWithDefaultValue = false)
                  .asOption()
              )
          }

        val tableWithPatternAndWrite = jdbcSchema.pattern match {
          case None => restoredTable
          case Some(pattern) =>
            val interpolatePattern = formatExtractPattern(jdbcSchema, table.name, pattern)
            val pat = Pattern.compile(interpolatePattern)
            restoredTable.copy(pattern = pat)
        }
        tableWithPatternAndWrite
      }

    val finalDomain = domain.copy(tables = tables)

    if (external) {
      schemaHandler.saveToExternals(List(finalDomain))
    } else {
      schemaHandler.saveTo(List(finalDomain), baseOutputDir)
    }
    finalDomain
  }

  /** Generate YML file from the JDBCSchema
    *
    * @param jdbcSchema
    *   : the JDBC Schema to extract
    * @param settings
    *   : Application configuration file
    */
  private def extractDomain(
    jdbcSchema: JDBCSchema,
    connectionSettings: ConnectionInfo,
    domainTemplate: Option[DomainInfo]
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): DomainInfo = {
    val selectedTablesAndColumns =
      JdbcDbUtils.extractJDBCTables(
        jdbcSchema,
        connectionSettings,
        skipRemarks = false,
        keepOriginalName = false,
        includeColumns = true
      )
    JdbcDbUtils.extractDomain(jdbcSchema, domainTemplate, selectedTablesAndColumns)
  }

  private def formatExtractPattern(
    jdbcSchema: JDBCSchema,
    table: String,
    pattern: String
  )(implicit settings: Settings): String = {
    pattern.richFormat(
      Map(
        "catalog" -> jdbcSchema.catalog.map(Regex.quote).getOrElse(""),
        "schema"  -> Regex.quote(jdbcSchema.schema),
        "table"   -> Regex.quote(table)
      ),
      Map.empty
    )
  }

}

object ExtractSchema {

  def extractSchemaNames(
    connectionNameOpt: Option[String],
    accessToken: Option[String],
    tables: Map[String, List[String]] = Map.empty
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(String, List[String])]] = {
    val connectionName = connectionNameOpt
      .getOrElse(settings.appConfig.connectionRef)
    val connection = settings.appConfig.connections(connectionName).withAccessToken(accessToken)
    val connType = connection.`type`
    connType match {
      case ConnectionType.JDBC =>
        val result = JdbcDbUtils.extractSchemasAndTableNames(connection)
        result
      case ConnectionType.BQ =>
        val extractor = new ExtractBigQuerySchema(
          TablesExtractConfig(
            connectionRef = Some(connectionName),
            accessToken = accessToken,
            tables = tables
          )
        )
        val result = extractor.extractSchemasAndTableNames()
        result

      case ConnectionType.FS =>
        val job = new SparkExtractorJob()
        job.schemasAndTableNames()
      case _ =>
        Try {
          throw new IllegalArgumentException(s"Unsupported connection type: $connType")
        }

    }
  }
}
