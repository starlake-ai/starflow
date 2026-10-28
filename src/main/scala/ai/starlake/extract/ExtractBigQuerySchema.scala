package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
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
  StandardSQLTypeName,
  TableDefinition,
  TableId,
  TableInfo,
  ViewDefinition
}
import com.typesafe.scalalogging.LazyLogging

import java.util.{Locale, UUID}
import scala.collection.mutable.ListBuffer
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

  /** @param withParameterizedTypes
    *   true to give the parameterized columns types of their own, added to the types of the project
    *   (#1853), as the extraction commands do. Otherwise they get the built-in types, without their
    *   parameters, so that reading a schema writes no type.
    */
  def extractSchemasAndTables(
    schemaHandler: SchemaHandler,
    tablesToExtract: Map[String, List[String]],
    withParameterizedTypes: Boolean = false
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
    val parameterized =
      if (withParameterizedTypes) Some(parameterizedTypes(schemaHandler)) else None
    val domains = filteredDatasets.map { dataset =>
      extractDataset(schemaHandler, dataset, parameterized)
    }.toList
    // The types of the parameterized columns, which the extracted domains use
    parameterized.map(_.takeNewTypes()).filter(_.nonEmpty).foreach { newTypes =>
      BigQueryParameterizedTypes.addToProject(newTypes)
      schemaHandler.types(reload = true)
    }
    domains
  }

  private def extractDataset(
    schemaHandler: SchemaHandler,
    dataset: Dataset,
    parameterizedTypes: Option[BigQueryParameterizedTypes]
  ): DomainInfo = {
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
      dryRunSchema(Option(dataset.getLocation)),
      typeNames(schemaHandler),
      parameterizedTypes
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

  // The types of the parameterized columns of the extraction, generated from the project types
  // that the built-in types name
  private var extractedTypes: Option[BigQueryParameterizedTypes] = None
  private def parameterizedTypes(schemaHandler: SchemaHandler): BigQueryParameterizedTypes =
    synchronized {
      extractedTypes.getOrElse {
        val typeName = typeNames(schemaHandler)
        val types = new BigQueryParameterizedTypes(
          schemaHandler.types(),
          builtInTypeName => {
            val builtIn = BigQueryParameterizedTypes.builtIn(builtInTypeName)
            schemaHandler
              .types()
              .find(_.name == typeName(builtInTypeName))
              .filter(_.primitiveType == builtIn.primitiveType)
              .getOrElse(builtIn)
          }
        )
        extractedTypes = Some(types)
        types
      }
    }

  // The project type names, resolved once per extraction
  private var projectTypeNames: Option[String => String] = None
  private def typeNames(schemaHandler: SchemaHandler): String => String = synchronized {
    projectTypeNames.getOrElse {
      val typeNames = ExtractBigQuerySchema.projectTypeNames(schemaHandler)
      projectTypeNames = Some(typeNames)
      typeNames
    }
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
    * object that cannot be fetched is skipped with an error.
    *
    * @param listedTables
    *   objects returned by listTables: their definition holds their type but not their schema
    * @param tableTypes
    *   the BigQuery object types to extract
    * @param getTable
    *   fetches an object with its schema, null if it does not exist anymore
    * @param getViewSchema
    *   reads the effective schema of a view
    * @param typeName
    *   the name of the project type of a built-in type
    * @param parameterizedTypes
    *   the types of the parameterized columns, if they get types of their own: the caller adds them
    *   to the project
    */
  def extractTables(
    listedTables: Iterable[TableInfo],
    tableTypes: Set[TableDefinition.Type],
    getTable: TableId => TableInfo,
    getViewSchema: TableInfo => Schema,
    typeName: String => String = identity,
    parameterizedTypes: Option[BigQueryParameterizedTypes] = None
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
            Try(extractTableOrView(table, getViewSchema, typeName, parameterizedTypes)) match {
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
    getViewSchema: TableInfo => Schema,
    typeName: String => String,
    parameterizedTypes: Option[BigQueryParameterizedTypes]
  ): Option[SchemaInfo] =
    table.getDefinition[TableDefinition]() match {
      // Legacy SQL is the dialect of the views that do not set it. The dry run of a legacy SQL
      // query returns no schema, and standard SQL cannot query a legacy SQL view.
      case view: ViewDefinition if !java.lang.Boolean.FALSE.equals(view.useLegacySql()) =>
        logger.warn(
          s"View ${table.getTableId.getDataset()}.${table.getTableId.getTable()} uses legacy SQL, whose effective schema cannot be read with a dry run: using its stored schema, which may be stale"
        )
        extractTable(table, typeName, parameterizedTypes)
      case view: ViewDefinition =>
        val viewName = s"${table.getTableId.getDataset()}.${table.getTableId.getTable()}"
        Try(
          Option(getViewSchema(table)).filter(schema => fieldsOf(schema.getFields).nonEmpty)
        ) match {
          case Success(Some(effectiveSchema)) =>
            val schema = mergeViewSchema(viewName, Option(view.getSchema), effectiveSchema)
            extractTable(
              table.toBuilder.setDefinition(view.toBuilder.setSchema(schema).build()).build(),
              typeName,
              parameterizedTypes
            )
          case Success(None) =>
            logger.warn(
              s"The dry run of view $viewName returned no schema: using its stored schema, which may be stale"
            )
            extractTable(table, typeName, parameterizedTypes)
          case Failure(e) =>
            logger.warn(
              s"Could not dry run view $viewName ($e): using its stored schema, which may be stale"
            )
            extractTable(table, typeName, parameterizedTypes)
        }
      case _ =>
        extractTable(table, typeName, parameterizedTypes)
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

  /** @return
    *   the attribute of a BigQuery column, named after the built-in type of its BigQuery type, or
    *   after the type of its parameters for a `NUMERIC(P, S)`, `BIGNUMERIC(P, S)`, `STRING(L)` or
    *   `BYTES(L)` column, when the parameterized columns get types of their own. Never fails: an
    *   unknown type is a string. Notes the parameters and types it does not keep.
    */
  private def attributeOf(
    field: Field,
    prefix: String,
    typeName: String => String,
    parameterizedTypes: Option[BigQueryParameterizedTypes],
    notes: ListBuffer[String]
  ): TableAttribute = {
    val path = prefix + field.getName
    val mode = Option(field.getMode).getOrElse(Field.Mode.NULLABLE)
    val parameterizedTypeName = parameterizedTypes.flatMap(_.typeNameOf(field))
    def withParameters(bigQueryType: String): Option[String] =
      (Option(field.getPrecision), Option(field.getScale), Option(field.getMaxLength)) match {
        case (None, None, None)                   => None
        case _ if parameterizedTypeName.isDefined => None
        case _ if bigQueryType == "NUMERIC" && field.getPrecision == 38L && field.getScale == 9L =>
          None
        case (precision, scale, maxLength) =>
          val parameters = (precision.toList ++ scale.toList ++ maxLength.toList).mkString(", ")
          Some(s"$path is a $bigQueryType($parameters), whose parameters its type does not keep")
      }
    // A type unknown to this BigQuery client has no standard type
    val (builtInTypeName, note) =
      Option(field.getType).flatMap(t => Option(t.getStandardType)) match {
        case Some(StandardSQLTypeName.STRUCT)  => "struct" -> None
        case Some(StandardSQLTypeName.STRING)  => "string" -> withParameters("STRING")
        case Some(StandardSQLTypeName.BYTES)   => "binary" -> withParameters("BYTES")
        case Some(StandardSQLTypeName.INT64)   => "long"   -> None
        case Some(StandardSQLTypeName.FLOAT64) => "double" -> None
        // The type numeric creates an unparameterized NUMERIC, and decimal a NUMERIC(38, 9)
        case Some(StandardSQLTypeName.NUMERIC) =>
          (if (field.getPrecision == null) "numeric" else "decimal") -> withParameters("NUMERIC")
        case Some(StandardSQLTypeName.BIGNUMERIC) =>
          "bignumeric" -> withParameters("BIGNUMERIC")
        case Some(StandardSQLTypeName.BOOL)      => "boolean"   -> None
        case Some(StandardSQLTypeName.TIMESTAMP) => "timestamp" -> None
        case Some(StandardSQLTypeName.DATE)      => "date"      -> None
        case Some(StandardSQLTypeName.TIME)      => "time"      -> None
        case Some(StandardSQLTypeName.DATETIME)  => "datetime"  -> None
        case Some(StandardSQLTypeName.GEOGRAPHY) => "geography" -> None
        case Some(StandardSQLTypeName.JSON)      => "variant"   -> None
        case Some(StandardSQLTypeName.INTERVAL)  => "interval"  -> None
        case Some(StandardSQLTypeName.RANGE) =>
          Option(field.getRangeElementType)
            .flatMap(t => Option(t.getType))
            .map(_.toUpperCase(Locale.ROOT)) match {
            case Some("DATE")      => "range_date"      -> None
            case Some("DATETIME")  => "range_datetime"  -> None
            case Some("TIMESTAMP") => "range_timestamp" -> None
            case other =>
              "string" -> Some(s"$path is a RANGE<${other.getOrElse("")}>, extracted as a string")
          }
        case _ =>
          val bigQueryType = Option(field.getType).map(_.name()).getOrElse("column of no type")
          "string" -> Some(s"$path is a $bigQueryType, extracted as a string")
      }
    note.foreach(notes += _)
    val isStruct = builtInTypeName == "struct"
    TableAttribute(
      name = field.getName,
      `type` =
        if (isStruct) builtInTypeName
        else parameterizedTypeName.getOrElse(typeName(builtInTypeName)),
      array = if (mode == Field.Mode.REPEATED) Some(true) else None,
      required = Some(mode == Field.Mode.REQUIRED),
      comment = Option(field.getDescription),
      attributes =
        if (isStruct)
          fieldsOf(field.getSubFields).map(
            attributeOf(_, path + ".", typeName, parameterizedTypes, notes)
          )
        else Nil
    )
  }

  /** @return
    *   the name of the project type of a built-in type: the built-in type itself, unless a project
    *   type of the same name has another primitive type. Then the first type of the project, not a
    *   built-in one, of the same primitive type and BigQuery mapping, or of the same primitive
    *   type, or the name, when the project has none. Each name is resolved once, with a warning
    *   when it is replaced.
    */
  def projectTypeNames(schemaHandler: SchemaHandler): String => String = {
    lazy val projectTypes = schemaHandler.types()
    val names = scala.collection.mutable.Map.empty[String, String]
    def resolve(builtInTypeName: String): String =
      SchemaHandler.builtInTypes.find(_.name == builtInTypeName) match {
        // The project's own numeric would not create the unparameterized NUMERIC of the column
        case Some(builtInType)
            if builtInType.name == "numeric" && projectTypes.exists(tpe =>
              tpe.name == "numeric" && !BigQueryParameterizedTypes.sameType(tpe, builtInType)
            ) =>
          val decimal = resolve("decimal")
          logger.warn(
            s"The type numeric of the project is not the built-in numeric, which creates an unparameterized BigQuery NUMERIC: the NUMERIC columns are extracted with the type $decimal"
          )
          decimal
        case Some(builtInType)
            if !projectTypes.exists(tpe =>
              tpe.name == builtInType.name && tpe.primitiveType == builtInType.primitiveType
            ) =>
          // A type of the parameters of a column does not replace a built-in type
          val ownTypes = projectTypes.filter(tpe =>
            tpe.primitiveType == builtInType.primitiveType && !SchemaHandler.builtInTypes
              .contains(tpe) && !BigQueryParameterizedTypes.isParameterized(tpe)
          )
          val bigQueryMapping = builtInType.getDdlMappingForDB("bigquery")
          ownTypes
            .find(tpe =>
              bigQueryMapping.nonEmpty && tpe.getDdlMappingForDB("bigquery") == bigQueryMapping
            )
            .orElse(ownTypes.headOption) match {
            case Some(projectType) =>
              logger.warn(
                s"The project replaces the built-in type ${builtInType.name} with a type of another primitive type: its columns are extracted with the type ${projectType.name}"
              )
              projectType.name
            case None =>
              logger.warn(
                s"The project replaces the built-in type ${builtInType.name} with a type of another primitive type, and has no other type of primitive type ${builtInType.primitiveType}: its columns are extracted with the type ${builtInType.name} of the project"
              )
              builtInTypeName
          }
        case _ =>
          builtInTypeName
      }
    builtInTypeName =>
      names.synchronized(names.getOrElseUpdate(builtInTypeName, resolve(builtInTypeName)))
  }

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
  def extractTable(
    table: TableInfo,
    typeName: String => String = identity,
    parameterizedTypes: Option[BigQueryParameterizedTypes] = None
  ): Option[SchemaInfo] = {
    val tableName = table.getTableId().getTable()
    Option(table.getDefinition[TableDefinition]()).flatMap(d => Option(d.getSchema)) match {
      case Some(bqSchema) =>
        val notes = ListBuffer.empty[String]
        val attributes =
          fieldsOf(bqSchema.getFields).map(attributeOf(_, "", typeName, parameterizedTypes, notes))
        if (notes.nonEmpty)
          logger.warn(
            s"${table.getTableId.getDataset()}.$tableName: ${notes.mkString(", ")}"
          )
        Some(
          SchemaInfo(
            tableName,
            java.util.regex.Pattern.compile("ignore"),
            attributes,
            comment = Option(table.getDescription())
          )
        )
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
    schemaHandler: SchemaHandler,
    withParameterizedTypes: Boolean = false
  )(implicit settings: Settings): List[DomainInfo] = {
    val domains =
      new ExtractBigQuerySchema(config).extractSchemasAndTables(
        schemaHandler,
        config.tables,
        withParameterizedTypes
      )
    schemaHandler.saveToExternals(domains)
    domains
  }
}
