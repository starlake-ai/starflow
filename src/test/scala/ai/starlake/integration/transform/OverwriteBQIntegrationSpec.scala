package ai.starlake.integration.transform

import ai.starlake.job.Main
import ai.starlake.job.transform.BigQueryAutoTask
import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** OVERWRITE on an existing BigQuery table (#1938): the rows are replaced in a transaction, after
  * the SELECT is evaluated, so that a failure leaves the table as it was and the SELECT may read
  * the table it replaces.
  */
class OverwriteBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val table = newTable("overwrite")

  private val loaded = newTable("overwrite_load")

  /** One row in each of the partitions 2026-10-01 and 2026-10-02
    * @param filter
    *   whether the table requires a partition filter
    */
  private def setup(filter: Boolean = false): Unit = {
    createDataset()
    run(s"""CREATE OR REPLACE TABLE `$dataset.$table` (p DATE, k INT64, v INT64)
           |PARTITION BY p OPTIONS (require_partition_filter = $filter)""".stripMargin)
    run(s"""INSERT INTO `$dataset.$table` VALUES
           |(DATE '2026-10-01', 1, 100), (DATE '2026-10-02', 2, 200)""".stripMargin)
  }

  private def requiresPartitionFilter(): Boolean =
    BigQueryAutoTask.requiresPartitionFilter(bigquery.getTable(TableId.of(dataset, table)))

  /** The rows of a table, read without a query, which a partition filter would refuse */
  private def rows(table: String): List[List[String]] =
    bigquery
      .listTableData(TableId.of(dataset, table))
      .iterateAll()
      .asScala
      .map(_.asScala.toList.map(_.getStringValue))
      .toList
      .sortBy(_.mkString(","))

  private def overwrite(sql: String): Boolean =
    transform(
      table,
      s"""version: 1
         |task:
         |  name: $table
         |  writeStrategy:
         |    type: OVERWRITE
         |  sink:
         |    partition:
         |      - p
         |""".stripMargin,
      sql
    )

  /** Runs a task that fails on the row whose key is 2, at run time: a dry run does not see it */
  private def failingOverwrite(where: String): Unit =
    Try(
      overwrite(
        s"""SELECT p, k, CASE WHEN k = 2 THEN ERROR('sl it failure') ELSE v END AS v
           |FROM `$dataset.$table` $where""".stripMargin
      )
    ) match {
      case Success(succeeded) => succeeded shouldBe false
      // The command may throw the error of the run
      case Failure(exception) => exception.getMessage should include("sl it failure")
    }

  /** The messages of the failed audit rows of a table, of any step, since a time in milliseconds.
    * The audit log keeps the second of its timestamps only
    */
  private def failures(table: String, since: Long): List[String] =
    values(
      s"""SELECT message FROM `audit.audit`
         |WHERE domain = '$dataset' AND schema = '$table'
         |AND NOT success AND timestamp >= TIMESTAMP_SECONDS(${since / 1000})""".stripMargin
    ).flatten.flatten

  private val initial = List(List("2026-10-01", "1", "100"), List("2026-10-02", "2", "200"))

  if (remote) {
    "OVERWRITE on BigQuery" should "replace the rows with those of a SELECT that reads the table" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup()
        assert(overwrite(s"SELECT p, k, v + 10 AS v FROM `$dataset.$table`"))
        rows(table) shouldBe List(List("2026-10-01", "1", "110"), List("2026-10-02", "2", "210"))
      }
    }

    it should "keep the rows of the table when the SELECT fails, and fail with the error of the run" in {
      withEnvs(
        "SL_ROOT"                        -> project.pathAsString,
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        setup()
        val since = System.currentTimeMillis()
        failingOverwrite("")
        rows(table) shouldBe initial
        failures(table, since).exists(_.contains("sl it failure")) shouldBe true
      }
    }

    it should "keep the partition filter of a table that requires one" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        setup(filter = true)
        assert(
          overwrite(s"SELECT p, k, v + 10 AS v FROM `$dataset.$table` WHERE p >= DATE '2026-01-01'")
        )
        requiresPartitionFilter() shouldBe true
        rows(table) shouldBe List(List("2026-10-01", "1", "110"), List("2026-10-02", "2", "210"))
      }
    }

    it should "turn the partition filter of a table on again before the SELECT of the task, which fails" in {
      withEnvs(
        "SL_ROOT"                        -> project.pathAsString,
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        setup(filter = true)
        val since = System.currentTimeMillis()
        failingOverwrite("WHERE p >= DATE '2026-01-01'")
        requiresPartitionFilter() shouldBe true
        rows(table) shouldBe initial
        // Not a SELECT refused for its lack of a partition filter
        failures(table, since).exists(_.contains("sl it failure")) shouldBe true
      }
    }

    "A two-step OVERWRITE load on BigQuery" should "keep the rows of the table when its second step fails" in {
      withEnvs(
        "SL_ROOT"                        -> project.pathAsString,
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        createDataset()
        run(s"CREATE OR REPLACE TABLE `$dataset.$loaded` (id STRING, marker STRING)")
        run(s"INSERT INTO `$dataset.$loaded` VALUES ('0', 'before')")
        val domain = (project / "metadata" / "load" / dataset).createDirectories()
        (domain / "_config.sl.yml").overwrite(
          s"""version: 1
             |load:
             |  name: $dataset
             |  metadata:
             |    directory: "{{root_path}}/incoming/$dataset"
             |""".stripMargin
        )
        // The script makes the load take two steps. It converts no value: the first step does not
        // check it, and it fails in the second step only
        (domain / s"$loaded.sl.yml").overwrite(
          s"""version: 1
             |table:
             |  name: $loaded
             |  pattern: "$loaded.*.csv"
             |  metadata:
             |    format: DSV
             |    withHeader: true
             |    separator: ";"
             |    writeStrategy:
             |      type: OVERWRITE
             |  attributes:
             |    - name: id
             |      type: string
             |    - name: marker
             |      type: string
             |      script: "IF(id = '2', ERROR('sl it failure'), 'after')"
             |""".stripMargin
        )
        val file = (project / "incoming" / dataset).createDirectories() / s"$loaded-1.csv"
        file.overwrite("id\n1\n2\n")
        val since = System.currentTimeMillis()
        Try(
          new Main().run(
            Array(
              "load",
              "--domains",
              dataset,
              "--tables",
              loaded,
              "--files",
              file.uri.toString,
              "--inPlace"
            )
          )
        ).getOrElse(false) shouldBe false
        rows(loaded) shouldBe List(List("0", "before"))
        // Its second step failed, not its first
        failures(loaded, since).exists(_.contains("sl it failure")) shouldBe true
      }
    }
  }
}
