package ai.starlake.integration.transform

import com.google.cloud.bigquery.{StandardTableDefinition, TableId}

import scala.jdk.CollectionConverters._

/** The validity columns of an SCD2 task on BigQuery: the first run creates the table even when the
  * SELECT returns one of them (#1884), and they take the type that the task declares or names
  * (#1885)
  */
class SCD2ValidityColumnsBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val strategy =
    """  writeStrategy:
      |    type: SCD2
      |    key: [id]
      |    timestamp: updated_at
      |    startTs: valid_from
      |    endTs: valid_to""".stripMargin

  private def select(label: String, day: String, extra: String = ""): String =
    s"SELECT 1 AS id, '$label' AS label, TIMESTAMP '2026-10-$day 00:00:00' AS updated_at$extra"

  /** Runs the task on a table that does not exist yet, then again with a newer version of the row
    */
  private def runTwice(table: String, task: String, extra: String = ""): Unit = {
    createDataset()
    bigquery.delete(TableId.of(dataset, table))
    val yaml = s"version: 1\ntask:\n  name: $table\n$task\n"
    assert(transform(table, yaml, select("a", "01", extra)))
    assert(transform(table, yaml, select("b", "02", extra)))
  }

  /** The columns of the table, with their types */
  private def columns(table: String): List[(String, String)] =
    bigquery
      .getTable(TableId.of(dataset, table))
      .getDefinition[StandardTableDefinition]
      .getSchema
      .getFields
      .asScala
      .map(field => field.getName -> field.getType.toString)
      .toList

  /** The versions of the row: label, and whether the version is open */
  private def versions(table: String): List[List[Option[String]]] =
    values(s"SELECT label, valid_to IS NULL FROM `$dataset.$table` ORDER BY updated_at")

  private val history = List(List(Some("a"), Some("false")), List(Some("b"), Some("true")))

  if (remote) {
    "The first run of an SCD2 task on BigQuery" should "create its table when the SELECT returns the start column" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val table = newTable("scd2_start")
        runTwice(table, strategy, ", TIMESTAMP '2026-10-01 00:00:00' AS valid_from")
        columns(table)
          .map(_._1) shouldBe List("id", "label", "updated_at", "valid_from", "valid_to")
        versions(table) shouldBe history
      }
    }

    "The validity columns of an SCD2 task on BigQuery" should "take the type that its attributes declare" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val table = newTable("scd2_declared")
        // The typed attributes also make the second run synchronize the schema of the table
        runTwice(
          table,
          s"""$strategy
             |  attributes:
             |    - name: id
             |      type: long
             |    - name: label
             |      type: string
             |    - name: updated_at
             |      type: timestamp
             |    - name: valid_from
             |      type: date
             |    - name: valid_to
             |      type: date""".stripMargin
        )
        columns(table).filter(_._1.startsWith("valid")) shouldBe List(
          "valid_from" -> "DATE",
          "valid_to"   -> "DATE"
        )
        versions(table) shouldBe history
      }
    }

    it should "take the type that its strategy names" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val table = newTable("scd2_named")
        runTwice(
          table,
          s"""$strategy
             |    startTsType: date
             |    endTsType: date""".stripMargin
        )
        columns(table).filter(_._1.startsWith("valid")) shouldBe List(
          "valid_from" -> "DATE",
          "valid_to"   -> "DATE"
        )
        versions(table) shouldBe history
      }
    }

    it should "be timestamps when the task neither declares nor names their type" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val table = newTable("scd2_timestamp")
        runTwice(table, strategy)
        columns(table).filter(_._1.startsWith("valid")) shouldBe List(
          "valid_from" -> "TIMESTAMP",
          "valid_to"   -> "TIMESTAMP"
        )
        versions(table) shouldBe history
      }
    }
  }
}
