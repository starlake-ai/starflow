package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{
  BigQuery,
  Dataset,
  Field,
  FieldList,
  JobId,
  JobInfo,
  JobStatistics,
  QueryJobConfiguration,
  Schema,
  TableDefinition,
  TableId,
  TableInfo,
  ViewDefinition
}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.{Locale, UUID}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ExtractBigQuerySchema(config: TablesExtractConfig)(implicit settings: Settings)
    extends LazyLogging {
  val implicitSettings: Settings = settings
  val bqJob: BigQueryJobBase = new BigQueryJobBase {
    val settings = implicitSettings
    override def cliConfig: BigQueryLoadConfig = BigQueryLoadConfig(
      connectionRef = config.connectionRef,
      outputDatabase = config.database,
      accessToken = config.accessToken
    )
  }

  val bigquery: BigQuery = bqJob.bigquery(accessToken = config.accessToken)

  private val tableTypes: Set[TableDefinition.Type] =
    ExtractBigQuerySchema.parseTableTypes(config.tableTypes)

  /** Extracts all schemas and table names from BigQuery. This method lists all datasets and their
    * tables, filtering out temporary tables that start with "zztmp_".
    * @return
    *   A Try containing a list of tuples, each with a dataset name and a list of table names.
    */
  def extractSchemasAndTableNames(): Try[List[(String, List[String])]] = {
    Try {
      val datasets = bigquery
        .listDatasets(DatasetListOption.pageSize(10000))
        .iterateAll()
        .asScala

      datasets.map { dataset =>
        val bqTables = bigquery
          .listTables(dataset.getDatasetId, TableListOption.pageSize(10000))
          .iterateAll()
          .asScala
          .filterNot(_.getTableId.getTable().startsWith("zztmp_"))
          .map(_.getTableId.getTable())
          .toList
          .sorted
        val datasetName = dataset.getDatasetId.getDataset()
        datasetName -> bqTables
      }.toList
    }
  }

  def extractSchemasAndTables(
    schemaHandler: SchemaHandler,
    tablesToExtract: Map[String, List[String]]
  ): List[DomainInfo] = {
    // case insensitive extraction
    val datasetNames = tablesToExtract.keys.toList
    val lowercaseDatasetNames = tablesToExtract.keys.map(_.toLowerCase()).toList
    val filteredDatasets =
      if (datasetNames.size == 1) {
        // We optimize extraction for a single dataset
        val datasetName = datasetNames.head
        val dataset = bigquery.getDataset(datasetName)
        List(dataset)
      } else {
        val datasets = bigquery.listDatasets(DatasetListOption.pageSize(10000))
        datasets
          .iterateAll()
          .asScala
          .filter(ds =>
            datasetNames.isEmpty || lowercaseDatasetNames.contains(
              ds.getDatasetId.getDataset().toLowerCase()
            )
          )
      }
    filteredDatasets.map { dataset =>
      extractDataset(schemaHandler, dataset)
    }.toList
  }

  private def extractDataset(schemaHandler: SchemaHandler, dataset: Dataset): DomainInfo = {
    val datasetId = dataset.getDatasetId()
    val bqTables = bigquery.listTables(datasetId, TableListOption.pageSize(10000))
    val allDatawareTables =
      bqTables.iterateAll.asScala.filter(!_.getTableId.getTable().startsWith("zztmp_"))
    val datasetName = dataset.getDatasetId.getDataset()
    val allTables =
      config.tables.get(datasetName) match {
        case None =>
          allDatawareTables
        case Some(tables) if tables.contains("*") =>
          allDatawareTables
        case Some(tables) =>
          allDatawareTables
            .filter(t => tables.exists(_.equalsIgnoreCase(t.getTableId.getTable())))
      }
    val tables = {
      val tablesToExclude = schemaHandler
        .domains()
        .filter(_.finalName.equalsIgnoreCase(datasetName))
        .flatMap(_.tables.map(_.finalName.toLowerCase()))
        .toSet
      if (tablesToExclude.isEmpty) allTables
      else allTables.filterNot(t => tablesToExclude.contains(t.getTableId.getTable().toLowerCase()))
    }
    val schemas = ExtractBigQuerySchema.extractTables(
      tables,
      tableTypes,
      // We get the Table again because Tables are returned with a null schema by listTables above.
      tableId => bigquery.getTable(tableId),
      dryRunSchema(Option(dataset.getLocation))
    )
    DomainInfo(
      name = dataset.getDatasetId().getDataset(),
      tables = schemas,
      comment = Option(dataset.getDescription),
      metadata = Some(
        Metadata(sink = Some(BigQuerySink(connectionRef = config.connectionRef).toAllSinks()))
      ),
      database = Option(dataset.getDatasetId().getProject())
    )
  }

  /** The project the dry runs of views are billed to: the connection one rather than the extracted
    * one, which may belong to someone else
    */
  private lazy val dryRunProject: String =
    Try(BigQueryJobBase.projectId(bqJob.connectionOptions.get("projectId"), None))
      .getOrElse(bigquery.getOptions.getProjectId)

  /** @return
    *   the schema of a free dry run of `SELECT *` on the view, run in the dataset location
    */
  private def dryRunSchema(location: Option[String])(view: TableInfo): Schema =
    bigquery
      .create(ExtractBigQuerySchema.dryRunJob(view, dryRunProject, location))
      .getStatistics[JobStatistics.QueryStatistics]()
      .getSchema
}

object ExtractBigQuerySchema extends LazyLogging {

  /** BigQuery object types extracted when no table type is configured. */
  val defaultTableTypes: Set[TableDefinition.Type] = Set(TableDefinition.Type.TABLE)

  /** Table types accepted by the `tableTypes` attribute and the `--table-types` option. */
  val supportedTableTypes: Map[String, TableDefinition.Type] = List(
    TableDefinition.Type.TABLE,
    TableDefinition.Type.VIEW,
    TableDefinition.Type.MATERIALIZED_VIEW,
    TableDefinition.Type.EXTERNAL,
    TableDefinition.Type.SNAPSHOT
  ).map(tableType => tableType.name() -> tableType).toMap

  val supportedTableTypeNames: String =
    supportedTableTypes.keys.toList.sorted.mkString(", ")

  /** Maps configured table types to BigQuery object types. Names are case insensitive and spaces
    * may be used instead of underscores ("MATERIALIZED VIEW"). Unknown names, such as JDBC only
    * types, are ignored with a warning.
    *
    * @param tableTypes
    *   configured table types, Nil to extract tables only
    * @return
    *   the BigQuery object types to extract
    */
  def parseTableTypes(tableTypes: List[String]): Set[TableDefinition.Type] = {
    if (tableTypes.isEmpty) {
      defaultTableTypes
    } else {
      val parsed = tableTypes.map { tableType =>
        tableType -> supportedTableTypes.get(
          tableType.trim.toUpperCase(Locale.ROOT).replaceAll("\\s+", "_")
        )
      }
      val unknown = parsed.collect { case (tableType, None) => tableType }
      val known = parsed.flatMap(_._2).toSet
      if (unknown.nonEmpty)
        logger.warn(
          s"Ignoring table types not supported by BigQuery: ${unknown.mkString(", ")}. Supported table types are $supportedTableTypeNames"
        )
      if (known.isEmpty)
        throw new IllegalArgumentException(
          s"No supported BigQuery table type in ${tableTypes.mkString(", ")}. Supported table types are $supportedTableTypeNames"
        )
      known
    }
  }

  /** @return
    *   true if the object type is one of the table types to extract, or is not known yet. Logs a
    *   warning when the object is skipped.
    */
  def isExtractable(table: TableInfo, tableTypes: Set[TableDefinition.Type]): Boolean = {
    Option(table.getDefinition[TableDefinition]()).flatMap(d => Option(d.getType)) match {
      case Some(tableType) if !tableTypes.contains(tableType) =>
        logger.warn(
          s"Skipping ${tableType.name()} ${table.getTableId.getDataset()}.${table.getTableId
              .getTable()}: only ${tableTypes.map(_.name()).toList.sorted.mkString(", ")} objects are extracted. Add ${tableType.name()} to the BigQuery table types to extract it (tableTypes attribute of the extract config, --table-types option of extract-bq-schema)"
        )
        false
      case _ =>
        true
    }
  }

  /** Extracts the schemas of the listed objects whose type is one of the table types to extract. An
    * object that cannot be fetched or whose schema cannot be converted is skipped with an error.
    *
    * @param listedTables
    *   objects returned by listTables: their definition holds their type but not their schema
    * @param tableTypes
    *   the BigQuery object types to extract
    * @param getTable
    *   fetches an object with its schema, null if it does not exist anymore
    * @param getViewSchema
    *   reads the effective schema of a view
    */
  def extractTables(
    listedTables: Iterable[TableInfo],
    tableTypes: Set[TableDefinition.Type],
    getTable: TableId => TableInfo,
    getViewSchema: TableInfo => Schema
  ): List[SchemaInfo] = {
    // Filter on the listed type first to avoid fetching objects that would be skipped anyway, and
    // again once fetched in case the listing did not return it.
    val extracted =
      listedTables.filter(isExtractable(_, tableTypes)).toList.flatMap { listedTable =>
        val tableId = listedTable.getTableId
        val tableName = s"${tableId.getDataset()}.${tableId.getTable()}"
        Try(Option(getTable(tableId))) match {
          case Success(Some(table)) if isExtractable(table, tableTypes) =>
            logger.info(s"Extracting table $tableName")
            Try(extractTableOrView(table, getViewSchema)) match {
              case Success(Some(schema)) =>
                Some(Right(schema))
              case Success(None) =>
                Some(Left(tableName))
              case Failure(e) =>
                logger.error(s"Failed to extract the schema of $tableName", e)
                Some(Left(tableName))
            }
          case Success(Some(_)) =>
            None
          case Success(None) =>
            logger.warn(s"Skipping $tableName: it does not exist anymore")
            None
          case Failure(e) =>
            logger.error(s"Failed to get table $tableName", e)
            Some(Left(tableName))
        }
      }
    val notExtracted = extracted.collect { case Left(tableName) => tableName }
    if (notExtracted.nonEmpty)
      logger.error(
        s"${notExtracted.size} selected object(s) could not be extracted, see the logs above: ${notExtracted
            .mkString(", ")}"
      )
    extracted.collect { case Right(schema) => schema }
  }

  /** @return
    *   a job dry running `SELECT *` on the view in standard SQL
    */
  def dryRunJob(view: TableInfo, project: String, location: Option[String]): JobInfo = {
    val tableId = view.getTableId
    val viewProject = Option(tableId.getProject()).map(_ + ".").getOrElse("")
    val query = s"SELECT * FROM `$viewProject${tableId.getDataset()}.${tableId.getTable()}`"
    val jobId = JobId.newBuilder().setJob(UUID.randomUUID().toString).setProject(project)
    location.foreach(jobId.setLocation)
    JobInfo.of(
      jobId.build(),
      QueryJobConfiguration.newBuilder(query).setDryRun(true).setUseLegacySql(false).build()
    )
  }

  /** BigQuery does not recompute the stored schema of a view when its underlying tables change, so
    * its types can be stale: the schema of a view is read from its effective schema, with the
    * descriptions of the stored one. Falls back to the stored schema only when the effective one
    * cannot be read.
    */
  private def extractTableOrView(
    table: TableInfo,
    getViewSchema: TableInfo => Schema
  ): Option[SchemaInfo] =
    table.getDefinition[TableDefinition]() match {
      // Legacy SQL is the dialect of the views that do not set it. The dry run of a legacy SQL
      // query returns no schema, and standard SQL cannot query a legacy SQL view.
      case view: ViewDefinition if !java.lang.Boolean.FALSE.equals(view.useLegacySql()) =>
        logger.warn(
          s"View ${table.getTableId.getDataset()}.${table.getTableId.getTable()} uses legacy SQL, whose effective schema cannot be read with a dry run: using its stored schema, which may be stale"
        )
        extractTable(table)
      case view: ViewDefinition =>
        val viewName = s"${table.getTableId.getDataset()}.${table.getTableId.getTable()}"
        Try(
          Option(getViewSchema(table)).filter(schema => fieldsOf(schema.getFields).nonEmpty)
        ) match {
          // A conversion failure of the effective schema is the error of the view: falling back to
          // the stored schema could extract stale types
          case Success(Some(effectiveSchema)) =>
            val schema = mergeViewSchema(viewName, Option(view.getSchema), effectiveSchema)
            extractTable(
              table.toBuilder.setDefinition(view.toBuilder.setSchema(schema).build()).build()
            )
          case Success(None) =>
            logger.warn(
              s"The dry run of view $viewName returned no schema: using its stored schema, which may be stale"
            )
            extractTable(table)
          case Failure(e) =>
            logger.warn(
              s"Could not dry run view $viewName ($e): using its stored schema, which may be stale"
            )
            extractTable(table)
        }
      case _ =>
        extractTable(table)
    }

  /** @return
    *   the effective schema of a view, with the descriptions of its stored schema, matched by name
    *   at every nesting level. Logs a warning listing the differences when the stored schema is
    *   stale.
    */
  def mergeViewSchema(
    viewName: String,
    storedSchema: Option[Schema],
    effectiveSchema: Schema
  ): Schema = {
    val storedFields = storedSchema.map(schema => fieldsOf(schema.getFields)).getOrElse(Nil)
    val effectiveFields = fieldsOf(effectiveSchema.getFields)
    val differences = schemaDifferences(storedFields, effectiveFields)
    if (storedSchema.isDefined && differences.nonEmpty) {
      val shown = differences.take(10).mkString(", ")
      val more = if (differences.size > 10) ", ..." else ""
      logger.warn(
        s"The stored schema of view $viewName is stale, using its effective schema instead. ${differences.size} difference(s): $shown$more"
      )
    }
    Schema.of(mergeFields(storedFields, effectiveFields).asJava)
  }

  private def fieldsOf(fields: FieldList): List[Field] =
    Option(fields).map(_.asScala.toList).getOrElse(Nil)

  /** BigQuery column names are case insensitive */
  private def byName(fields: List[Field]): Map[String, Field] =
    fields.map(field => field.getName.toLowerCase(Locale.ROOT) -> field).toMap

  private def findField(fieldsByName: Map[String, Field], field: Field): Option[Field] =
    fieldsByName.get(field.getName.toLowerCase(Locale.ROOT))

  private def mergeFields(storedFields: List[Field], effectiveFields: List[Field]): List[Field] = {
    val storedByName = byName(storedFields)
    effectiveFields.map { field =>
      findField(storedByName, field) match {
        case None =>
          field
        case Some(storedField) =>
          val builder = field.toBuilder
          // The description of the view column wins over the one of the underlying column
          Option(storedField.getDescription).foreach(builder.setDescription)
          if (field.getSubFields != null) {
            val subFields =
              mergeFields(fieldsOf(storedField.getSubFields), fieldsOf(field.getSubFields))
            builder.setType(field.getType, FieldList.of(subFields.asJava))
          }
          builder.build()
      }
    }
  }

  /** @return
    *   the type, parameter and mode changes, new and missing columns from the stored to the
    *   effective fields, at every nesting level
    */
  private[extract] def schemaDifferences(
    storedFields: List[Field],
    effectiveFields: List[Field],
    prefix: String = ""
  ): List[String] = {
    def describe(field: Field): String = {
      val parameters =
        List(field.getMaxLength, field.getPrecision, field.getScale).flatMap(Option(_))
      val mode = Option(field.getMode).filter(_ != Field.Mode.NULLABLE).map(" " + _)
      field.getType.name() +
      (if (parameters.isEmpty) "" else parameters.mkString("(", ",", ")")) +
      mode.getOrElse("")
    }
    val storedByName = byName(storedFields)
    val effectiveByName = byName(effectiveFields)
    val changed = effectiveFields.flatMap { field =>
      val name = prefix + field.getName
      findField(storedByName, field) match {
        case None =>
          List(s"new column $name")
        case Some(storedField) =>
          val typeChange =
            if (describe(storedField) == describe(field)) Nil
            else List(s"$name ${describe(storedField)} -> ${describe(field)}")
          typeChange ++ schemaDifferences(
            fieldsOf(storedField.getSubFields),
            fieldsOf(field.getSubFields),
            s"$name."
          )
      }
    }
    val missing = storedFields
      .filter(findField(effectiveByName, _).isEmpty)
      .map(storedField => s"missing column $prefix${storedField.getName}")
    changed ++ missing
  }

  /** @return
    *   the table schema read from the table definition, whatever the BigQuery object type, or None
    *   if the definition has no schema.
    */
  def extractTable(table: TableInfo): Option[SchemaInfo] = {
    val tableName = table.getTableId().getTable()
    Option(table.getDefinition[TableDefinition]()).flatMap(d => Option(d.getSchema)) match {
      case Some(bqSchema) =>
        val sparkSchema: StructType = BigQuerySchemaConverters.toSpark(bqSchema)
        val schema = SchemaInfo.fromSparkSchema(tableName, StructField("ignore", sparkSchema))
        Some(schema.copy(comment = Option(table.getDescription())))
      case None =>
        logger.warn(
          s"Skipping ${table.getTableId.getDataset()}.$tableName: its definition has no schema"
        )
        None
    }
  }

  def run(
    args: Array[String]
  )(implicit settings: Settings, schemaHandler: SchemaHandler): Try[Unit] = {
    ExtractBigQuerySchemaCmd.run(args.toIndexedSeq, schemaHandler).map(_ => ())
  }

  def extractAndSaveToExternal(
    config: TablesExtractConfig,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): List[DomainInfo] = {
    val domains =
      new ExtractBigQuerySchema(config).extractSchemasAndTables(schemaHandler, config.tables)
    schemaHandler.saveToExternals(domains)
    domains
  }
}
