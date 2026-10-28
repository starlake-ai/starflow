package ai.starlake.integration.transform

import com.google.cloud.bigquery.{StandardTableDefinition, TableId}

import scala.jdk.CollectionConverters._

/** The schema sync of a BigQuery SCD2 task uses the validity columns of its strategy (#1878) */
class SCD2SchemaSyncBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val table = newTable("scd2")

  private def columns(): List[String] =
    bigquery
      .getTable(TableId.of(dataset, table))
      .getDefinition[StandardTableDefinition]
      .getSchema
      .getFields
      .asScala
      .map(_.getName)
      .toList

  /** Creates the table with these columns, then runs the task, whose typed attributes make the
    * schema of the table synchronized before the run
    */
  private def runTask(tableColumns: String, attributes: List[(String, String)]): Boolean = {
    createDataset()
    run(s"CREATE OR REPLACE TABLE `$dataset.$table` ($tableColumns)")
    transform(
      table,
      s"""version: 1
         |task:
         |  name: $table
         |  writeStrategy:
         |    type: SCD2
         |    key: [id]
         |    timestamp: updated_at
         |    startTs: valid_from
         |    endTs: valid_to
         |  attributes:
         |${attributes
          .map { case (name, tpe) => s"    - name: $name\n      type: $tpe" }
          .mkString("\n")}
         |""".stripMargin,
      "SELECT 'a' AS id, TIMESTAMP '2026-10-01 00:00:00' AS updated_at"
    )
  }

  private val keyAttributes = List("id" -> "string", "updated_at" -> "timestamp")

  private val validityAttributes = List("valid_from" -> "timestamp", "valid_to" -> "timestamp")

  /** The row of the run, with its validity */
  private def written(): List[List[Option[String]]] =
    values(s"SELECT id, valid_from IS NOT NULL, valid_to IS NULL FROM `$dataset.$table`")

  if (remote) {
    "An SCD2 task on BigQuery" should "keep the validity columns that its strategy names" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        assert(
          runTask(
            "id STRING, updated_at TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP",
            keyAttributes ++ validityAttributes
          )
        )
        columns() shouldBe List("id", "updated_at", "valid_from", "valid_to")
        written() shouldBe List(List(Some("a"), Some("true"), Some("true")))
      }
    }

    it should "add the validity columns that its strategy names" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        assert(runTask("id STRING, updated_at TIMESTAMP", keyAttributes))
        columns() shouldBe List("id", "updated_at", "valid_from", "valid_to")
        written() shouldBe List(List(Some("a"), Some("true"), Some("true")))
      }
    }

    it should "keep the global validity columns that earlier versions added" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        assert(
          runTask(
            """id STRING, updated_at TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP,
              |sl_start_ts TIMESTAMP, sl_end_ts TIMESTAMP""".stripMargin,
            keyAttributes ++ validityAttributes
          )
        )
        columns() shouldBe List(
          "id",
          "updated_at",
          "valid_from",
          "valid_to",
          "sl_start_ts",
          "sl_end_ts"
        )
        written() shouldBe List(List(Some("a"), Some("true"), Some("true")))
      }
    }
  }
}
