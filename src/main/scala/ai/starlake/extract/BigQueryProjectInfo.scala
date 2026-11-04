package ai.starlake.extract

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import com.google.cloud.bigquery.{BigQuery, BigQueryException, Dataset, DatasetId, Table}

import scala.jdk.CollectionConverters._

object BigQueryInfo {
  def extractInfo(
    config: TablesExtractConfig
  )(implicit settings: Settings): List[(Dataset, List[Table])] = {
    val implicitSettings = settings
    val bqJob = new BigQueryJobBase {
      val settings: Settings = implicitSettings
      override def cliConfig: BigQueryLoadConfig = new BigQueryLoadConfig(
        connectionRef = config.connectionRef,
        outputDatabase = None,
        accessToken = config.accessToken
      )
    }
    extractInfo(bqJob.bigquery(accessToken = config.accessToken))
  }

  private[extract] def extractInfo(bigquery: BigQuery): List[(Dataset, List[Table])] =
    bigquery
      .listDatasets()
      .iterateAll()
      .asScala
      .flatMap(dataset => extractDatasetInfo(bigquery, dataset.getDatasetId))
      .toList

  /** What gets deleted during the scan is skipped: it no longer exists. For that, getDataset and
    * getTable return null, and listing the tables of a dataset throws a 404.
    */
  private def extractDatasetInfo(
    bigquery: BigQuery,
    datasetId: DatasetId
  ): Option[(Dataset, List[Table])] =
    try {
      Option(bigquery.getDataset(datasetId)).map { dataset =>
        val tables = bigquery
          .listTables(datasetId)
          .iterateAll()
          .asScala
          .flatMap(table => Option(bigquery.getTable(table.getTableId)))
          .toList
        (dataset, tables)
      }
    } catch {
      case e: BigQueryException if e.getCode == 404 => None
    }
}
