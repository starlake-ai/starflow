package ai.starlake.integration.transform

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File
import com.google.cloud.bigquery._

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

/** OVERWRITE_BY_PARTITION transforms on BigQuery (#1875): the SELECT of a task may read the
  * partitions it replaces, and the NULL partition is replaced like the others.
  */
class OverwriteByPartitionBQIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_overwrite_by_partition"
  // A table of its own for each run
  private val table = s"parts_${UUID.randomUUID().toString.replace("-", "").take(8)}"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-overwrite-by-partition").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-overwrite-by-partition-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  /** Three rows in the partition 2026-10-01, one in 2026-10-02, one in the NULL partition */
  private def setup(): Unit = {
    // Another run may create the dataset at the same time
    if (bigquery.getDataset(dataset) == null)
      Try(bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build()))
    run(s"""CREATE OR REPLACE TABLE `$dataset.$table` (p DATE, k INT64, v INT64, note STRING)
           |PARTITION BY p""".stripMargin)
    run(s"""INSERT INTO `$dataset.$table` VALUES
           |(DATE '2026-10-01', 1, 100, 'a'), (DATE '2026-10-01', 2, 200, 'b'),
           |(DATE '2026-10-01', 3, 300, 'c'), (DATE '2026-10-02', 4, 400, 'd'), (NULL, 9, 900, 'e')
           |""".stripMargin)
  }

  private def rows(): List[(Option[String], Long, Long, Option[String])] =
    run(s"SELECT CAST(p AS STRING), k, v, note FROM `$dataset.$table` ORDER BY k, v")
      .iterateAll()
      .asScala
      .map(row =>
        (
          Option(row.get(0).getValue).map(_.toString),
          row.get(1).getLongValue,
          row.get(2).getLongValue,
          Option(row.get(3).getValue).map(_.toString)
        )
      )
      .toList

  /** Runs the task of this SELECT, which replaces the partitions of the table */
  private def transform(sql: String): Boolean = {
    val folder = (project / "metadata" / "transform" / dataset).createDirectories()
    (folder / s"$table.sl.yml").overwrite(
      s"""version: 1
         |task:
         |  name: $table
         |  writeStrategy:
         |    type: OVERWRITE_BY_PARTITION
         |  sink:
         |    partition:
         |      - p
         |""".stripMargin
    )
    (folder / s"$table.sql").overwrite(sql)
    new Main().run(Array("transform", "--name", s"$dataset.$table"))
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "OVERWRITE_BY_PARTITION on BigQuery" should "replace the partitions that the SELECT reads" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        // An untyped NULL is inserted into the STRING column as before: a table created from the
        // SELECT would make it an INT64
        assert(
          transform(
            s"SELECT p, k, v + 1 AS v, NULL AS note FROM $dataset.$table WHERE p = DATE '2026-10-01'"
          )
        )
        rows() shouldBe List(
          (Some("2026-10-01"), 1L, 101L, None),
          (Some("2026-10-01"), 2L, 201L, None),
          (Some("2026-10-01"), 3L, 301L, None),
          (Some("2026-10-02"), 4L, 400L, Some("d")),
          (None, 9L, 900L, Some("e"))
        )
      }
    }

    it should "replace the NULL partition" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        assert(transform("SELECT CAST(NULL AS DATE) AS p, 9 AS k, 901 AS v, 'f' AS note"))
        rows() shouldBe List(
          (Some("2026-10-01"), 1L, 100L, Some("a")),
          (Some("2026-10-01"), 2L, 200L, Some("b")),
          (Some("2026-10-01"), 3L, 300L, Some("c")),
          (Some("2026-10-02"), 4L, 400L, Some("d")),
          (None, 9L, 901L, Some("f"))
        )
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean)
      bigquery.delete(TableId.of(dataset, table))
    project.delete(swallowIOExceptions = true)
  }
}
