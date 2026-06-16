package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.BigQuery.{DatasetListOption, TableListOption}
import com.google.cloud.bigquery.{BigQuery, Dataset, StandardTableDefinition, Table}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters._
import scala.util.Try

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
    val schemas = tables.flatMap { bqTable =>
      logger.info(s"Extracting table $datasetName.${bqTable.getTableId.getTable()}")
      // We get the Table again below because Tables are returned with a null definition by listTables above.
      Try(bigquery.getTable(bqTable.getTableId())) match {
        case scala.util.Success(tableWithDefinition) =>
          if (tableWithDefinition.getDefinition().isInstanceOf[StandardTableDefinition])
            Some(extractTable(tableWithDefinition))
          else
            None
        case scala.util.Failure(e) =>
          logger.error(s"Failed to get table ${bqTable.getTableId()}", e)
          None
      }
    }
    DomainInfo(
      name = dataset.getDatasetId().getDataset(),
      tables = schemas.toList,
      comment = Option(dataset.getDescription),
      metadata = Some(
        Metadata(sink = Some(BigQuerySink(connectionRef = config.connectionRef).toAllSinks()))
      ),
      database = Option(dataset.getDatasetId().getProject())
    )
  }

  private def extractTable(table: Table): SchemaInfo = {
    val bqSchema =
      table.getDefinition[StandardTableDefinition].getSchema
    val sparkSchema: StructType = BigQuerySchemaConverters.toSpark(bqSchema)
    val schema =
      SchemaInfo.fromSparkSchema(table.getTableId().getTable(), StructField("ignore", sparkSchema))
    schema.copy(comment = Option(table.getDescription()))
  }
}

object ExtractBigQuerySchema {
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
