package ai.starlake.integration.transform

import ai.starlake.job.transform.BigQueryAutoTask
import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._

/** OVERWRITE_BY_PARTITION on a BigQuery table that requires a partition filter (#1880): the filter
  * is turned on again after the run, failed or not, whether the sink declares it or not.
  */
class OverwriteByPartitionFilterBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val table = newTable("filter")

  private val start = System.currentTimeMillis()

  /** Two rows in the partition 2026-10-01, one in 2026-10-02
    * @param filter
    *   whether the table requires a partition filter
    * @param timePartitioning
    *   whether the filter is set on the time partitioning of the table, as older clients did
    */
  private def setup(filter: Boolean = true, timePartitioning: Boolean = false): Unit = {
    createDataset()
    if (timePartitioning) {
      bigquery.delete(TableId.of(dataset, table))
      bigquery.create(
        TableInfo.of(
          TableId.of(dataset, table),
          StandardTableDefinition
            .newBuilder()
            .setSchema(
              Schema.of(
                Field.of("p", StandardSQLTypeName.DATE),
                Field.of("k", StandardSQLTypeName.INT64),
                Field.of("v", StandardSQLTypeName.INT64)
              )
            )
            .setTimePartitioning(
              TimePartitioning
                .newBuilder(TimePartitioning.Type.DAY)
                .setField("p")
                .setRequirePartitionFilter(filter)
                .build()
            )
            .build()
        )
      )
    } else
      run(s"""CREATE OR REPLACE TABLE `$dataset.$table` (p DATE, k INT64, v INT64)
             |PARTITION BY p OPTIONS (require_partition_filter = $filter)""".stripMargin)
    run(s"""INSERT INTO `$dataset.$table` VALUES
           |(DATE '2026-10-01', 1, 100), (DATE '2026-10-01', 2, 200), (DATE '2026-10-02', 3, 300)
           |""".stripMargin)
  }

  private def requiresPartitionFilter(): Boolean =
    BigQueryAutoTask.requiresPartitionFilter(bigquery.getTable(TableId.of(dataset, table)))

  /** The rows, read without a query, which the partition filter would refuse */
  private def rows(): List[(String, Long, Long)] =
    bigquery
      .listTableData(TableId.of(dataset, table))
      .iterateAll()
      .asScala
      .map(row => (row.get(0).getStringValue, row.get(1).getLongValue, row.get(2).getLongValue))
      .toList
      .sortBy(_._2)

  private def transform(sql: String, requirePartitionFilter: Option[Boolean]): Boolean =
    transform(
      table,
      s"""version: 1
         |task:
         |  name: $table
         |  writeStrategy:
         |    type: OVERWRITE_BY_PARTITION
         |  sink:
         |    partition:
         |      - p
         |${requirePartitionFilter.map(f => s"    requirePartitionFilter: $f").getOrElse("")}
         |""".stripMargin,
      sql
    )

  private val replaceFirstPartition =
    s"SELECT p, k, v + 1 AS v FROM `$dataset.$table` WHERE p = DATE '2026-10-01'"

  private val replaced =
    List(("2026-10-01", 1L, 101L), ("2026-10-01", 2L, 201L), ("2026-10-02", 3L, 300L))

  private val initial =
    List(("2026-10-01", 1L, 100L), ("2026-10-01", 2L, 200L), ("2026-10-02", 3L, 300L))

  if (remote) {
    "OVERWRITE_BY_PARTITION on BigQuery" should "keep the partition filter of a table whose sink does not declare it" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        assert(transform(replaceFirstPartition, None))
        requiresPartitionFilter() shouldBe true
        rows() shouldBe replaced
      }
    }

    it should "turn the partition filter on when the sink declares it" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        assert(transform(replaceFirstPartition, Some(true)))
        requiresPartitionFilter() shouldBe true
        rows() shouldBe replaced
      }
    }

    it should "turn the partition filter on again when the run fails, with the error of the run" in {
      withEnvs(
        "SL_ROOT"                        -> project.pathAsString,
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        setup()
        val failing =
          s"""SELECT p, k, CASE WHEN k = 2 THEN ERROR('sl it failure') ELSE v END AS v
             |FROM `$dataset.$table` WHERE p = DATE '2026-10-01'""".stripMargin
        assert(!transform(failing, Some(true)))
        requiresPartitionFilter() shouldBe true
        rows() shouldBe initial
        // The audit log keeps the second of its timestamp only
        val messages = values(
          s"""SELECT message FROM `audit.audit`
             |WHERE domain = '$dataset' AND schema = '$table' AND step = 'TRANSFORM'
             |AND NOT success AND timestamp >= TIMESTAMP_SECONDS(${start / 1000})""".stripMargin
        ).flatten.flatten
        messages.exists(_.contains("sl it failure")) shouldBe true
      }
    }

    it should "turn the partition filter off when the sink declares it off" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        assert(transform(replaceFirstPartition, Some(false)))
        requiresPartitionFilter() shouldBe false
        rows() shouldBe replaced
      }
    }

    it should "leave a table that does not require a partition filter without one" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup(filter = false)
        assert(transform(replaceFirstPartition, None))
        requiresPartitionFilter() shouldBe false
        rows() shouldBe replaced
      }
    }

    it should "keep the partition filter set on the time partitioning of the table" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup(timePartitioning = true)
        assert(transform(replaceFirstPartition, None))
        requiresPartitionFilter() shouldBe true
        rows() shouldBe replaced
      }
    }
  }
}
