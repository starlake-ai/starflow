package ai.starlake.job.sink.bigquery

import ai.starlake.schema.model.{
  AccessControlEntry,
  Engine,
  Materialization,
  RowLevelSecurity,
  SchemaInfo
}
import org.apache.spark.sql.DataFrame

case class BigQueryLoadCliConfig(
  connectionRef: Option[String] = None,
  source: Either[String, DataFrame] = Left(""),
  outputDatabase: Option[String] = None,
  outputDataset: Option[String] = None,
  outputTable: Option[String] = None,
  outputPartition: Option[String] = None,
  outputClustering: Seq[String] = Nil,
  sourceFormat: String = "",
  createDisposition: String = "",
  writeDisposition: String = "",
  location: Option[String] = None,
  days: Option[Int] = None,
  rls: List[RowLevelSecurity] = Nil,
  requirePartitionFilter: Boolean = false,
  engine: Engine = Engine.SPARK,
  partitionsToUpdate: List[String] = Nil,
  acl: List[AccessControlEntry] = Nil,
  starlakeSchema: Option[SchemaInfo] = None,
  domainTags: Set[String] = Set.empty,
  domainDescription: Option[String] = None,
  materialization: Materialization = Materialization.TABLE,
  accessToken: Option[String]
) {
  /*
  def asBigqueryLoadConfig()(implicit settings: Settings) = BigQueryLoadConfig(
    connectionRef = connectionRef,
    source = source,
    outputTableId = Some(
      BigQueryJobBase.extractProjectDatasetAndTable(
        outputDatabase,
        outputDataset.getOrElse(throw new StarlakeConfigException("outputDataset must be defined")),
        outputTable.get
      )
    ),
    outputPartition = outputPartition,
    outputClustering = outputClustering,
    sourceFormat = sourceFormat,
    createDisposition = createDisposition,
    writeDisposition = writeDisposition,
    days = days,
    rls = rls,
    requirePartitionFilter = requirePartitionFilter,
    engine = engine,
    partitionsToUpdate = partitionsToUpdate,
    acl = acl,
    starlakeSchema = starlakeSchema,
    domainTags = domainTags,
    domainDescription = domainDescription,
    materialization = materialization,
    outputDatabase = outputDatabase,
    accessToken = accessToken
  )

   */
}
