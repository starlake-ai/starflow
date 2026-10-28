package ai.starlake.integration.transform

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File
import com.google.cloud.bigquery._

import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A BigQuery project of its own, whose transform tasks write tables of their own in one dataset.
  * The tables are dropped after the spec. Its tests run with SL_REMOTE_TEST only.
  */
abstract class BigQueryTransformIntegrationBase extends IntegrationTestBase {
  protected val dataset = "starlake_it_transform"

  protected val remote: Boolean = sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean

  private val tables = ListBuffer.empty[String]

  /** A table name of its own for each run, dropped after the spec */
  protected def newTable(prefix: String): String =
    dropAfter(s"${prefix}_${UUID.randomUUID().toString.replace("-", "").take(8)}")

  /** A table of the dataset to drop after the spec */
  protected def dropAfter(table: String): String = {
    tables += table
    table
  }

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-transform").toURI)

  protected lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-transform-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  protected lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  protected def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  protected def values(sql: String): List[List[Option[String]]] =
    run(sql)
      .iterateAll()
      .asScala
      .toList
      .map(_.asScala.toList.map(v => Option(v.getValue).map(_.toString)))

  protected def createDataset(): Unit =
    // Another run may create the dataset at the same time
    if (bigquery.getDataset(dataset) == null)
      Try(bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build()))

  /** Writes the task of a table and runs it */
  protected def transform(table: String, task: String, sql: String): Boolean = {
    val folder = (project / "metadata" / "transform" / dataset).createDirectories()
    (folder / s"$table.sl.yml").overwrite(task)
    (folder / s"$table.sql").overwrite(sql)
    new Main().run(Array("transform", "--name", s"$dataset.$table"))
  }

  override protected def afterAll(): Unit = {
    if (remote)
      tables.foreach(table => Try(bigquery.delete(TableId.of(dataset, table))))
    project.delete(swallowIOExceptions = true)
    super.afterAll()
  }
}
