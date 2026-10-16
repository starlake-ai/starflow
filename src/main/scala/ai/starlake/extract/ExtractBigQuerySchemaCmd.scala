package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{DomainInfo, JDBCSchema}
import ai.starlake.utils.JobResult
import scopt.{OParser, OParserBuilder}

import scala.util.Try

/** Command to extract schema from BigQuery.
  *
  * Usage: starlake extract-bq-schema [options]
  */
object ExtractBigQuerySchemaCmd extends TablesExtractCmd {
  override def command: String = "extract-bq-schema"

  override def pageDescription: String =
    "Extract BigQuery table schemas and dataset metadata with options to filter tables, set connections, and persist results."
  override def pageKeywords: Seq[String] =
    Seq("starlake extract-bq-schema", "BigQuery schema", "schema extraction", "metadata")

  override protected def extraOptions(
    builder: OParserBuilder[TablesExtractConfig]
  ): List[OParser[_, TablesExtractConfig]] = List(
    builder
      .opt[Seq[String]]("table-types")
      .action((x, c) => c.copy(tableTypes = x.toList))
      .optional()
      .text(
        s"Types of the objects to extract, among ${ExtractBigQuerySchema.supportedTableTypeNames}. Defaults to TABLE. The types of a standard SQL VIEW are read with a dry run query, which needs bigquery.jobs.create, or from its stored schema, which may be stale"
      )
  )

  /** @param extractConnectionRef
    *   connection of the extract config, which takes precedence over the one of the command
    */
  def fromExtractSchemaConfig(
    config: ExtractSchemaConfig,
    jdbcSchema: JDBCSchema,
    extractConnectionRef: Option[String] = None
  ): TablesExtractConfig = {
    val tablesRenamed = jdbcSchema.tables.map { table =>
      if (table.name == "_" || table.name == "") "*" else table.name
    }
    val tables =
      if (jdbcSchema.schema.isEmpty)
        Map.empty[String, List[String]]
      else
        Map(jdbcSchema.schema -> tablesRenamed)
    // fillWithDefaultValues() replaces an empty tableTypes with the JDBC default table types, which
    // include VIEW: consider them as not set, to keep extracting BigQuery tables only by default.
    val tableTypes =
      if (jdbcSchema.tableTypes.toSet == JDBCSchema.defaultTableTypes.toSet) Nil
      else jdbcSchema.tableTypes
    TablesExtractConfig(
      connectionRef = extractConnectionRef.orElse(config.connectionRef),
      tables = tables,
      database = jdbcSchema.catalog,
      external = config.external,
      accessToken = config.accessToken,
      tableTypes = tableTypes
    )
  }

  override def run(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    Try {
      extract(config, schemaHandler)
    }.map(_ => JobResult.empty)

  def extract(config: TablesExtractConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): List[DomainInfo] = {
    if (config.external) {
      val extractor = new ExtractBigQuerySchema(config)
      val externalDomains = extractor.extractSchemasAndTables(schemaHandler, config.tables)
      schemaHandler.saveToExternals(externalDomains)
      externalDomains
    } else {
      ExtractBigQuerySchema.extractAndSaveToExternal(config, schemaHandler)
    }
  }
}
