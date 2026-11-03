package ai.starlake.job.sink.bigquery

import ai.starlake.utils.RedactedToString
import ai.starlake.schema.model._
import com.google.cloud.bigquery.{JobInfo, TableId}
import org.apache.spark.sql.DataFrame

case class BigQueryLoadConfig(
  connectionRef: Option[String],
  source: Either[String, DataFrame] = Left(""),
  outputTableId: Option[TableId] = None,
  outputPartition: Option[String] = None,
  outputClustering: Seq[String] = Nil,
  sourceFormat: String = "",
  createDisposition: String = JobInfo.CreateDisposition.CREATE_IF_NEEDED.name(),
  writeDisposition: String = JobInfo.WriteDisposition.WRITE_APPEND.name(),
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
  outputTableDesc: Option[String] = None,
  attributesDesc: List[TableAttribute] = Nil,
  outputDatabase: Option[String] = None,
  enableRefresh: Option[Boolean] = None,
  refreshIntervalMs: Option[Long] = None,
  accessToken: Option[String] = None,
  forceIndirect: Boolean = false
) extends RedactedToString
