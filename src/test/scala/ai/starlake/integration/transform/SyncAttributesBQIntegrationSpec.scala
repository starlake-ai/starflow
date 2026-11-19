package ai.starlake.integration.transform

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.transform.{TransformCmd, TransformConfig}
import ai.starlake.schema.model.{AttributeStatus, TableAttribute}
import better.files.File

/** On BigQuery, `transform --sync-preview` and `--sync-apply` read the columns of the SELECT of a
  * task from a dry run on the connection of the task, with the temporary tables of its presql as
  * common table expressions, and write the types of extract-bq-schema (#1950).
  */
class SyncAttributesBQIntegrationSpec extends IntegrationTestBase {

  // The project of the test is written in a directory of its own
  override protected def cleanup(): Unit = {}

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "transform --sync-apply on BigQuery" should "write the types of a dry run on the connection of the task" in {
      val project = File.newTemporaryDirectory("starlake-sync-bq-")
      try {
        val metadata = project / "metadata"
        // The default connection is not the one of the task, and could not run its SELECT
        (metadata / "application.sl.yml")
          .createIfNotExists(createParents = true)
          .write(
            s"""version: 1
             |application:
             |  connectionRef: duckdb
             |  connections:
             |    duckdb:
             |      type: "jdbc"
             |      options:
             |        url: "jdbc:duckdb:${project.pathAsString}/datasets/duckdb.db"
             |        driver: "org.duckdb.DuckDBDriver"
             |    bigquery:
             |      type: "bigquery"
             |      options:
             |        location: europe-west1
             |        authType: "APPLICATION_DEFAULT"
             |        authScopes: "https://www.googleapis.com/auth/cloud-platform"
             |        writeMethod: "direct"
             |""".stripMargin
          )
        // A project without load domains still has their directory, which the SQL rewriting lists
        (metadata / "load").createDirectories()
        val folder = metadata / "transform" / "sync_it"
        (folder / "sync_types.sl.yml")
          .createIfNotExists(createParents = true)
          .write(
            """version: 1
            |task:
            |  connectionRef: bigquery
            |  attributes:
            |    - name: id
            |      type: integer
            |    - name: amount
            |      type: decimal
            |      comment: written by hand, to keep
            |    - name: updated_at
            |      type: timestamp
            |""".stripMargin
          )
        // Reads no table: the dry run needs no dataset
        val sql =
          """-- SL_PRESQL
            |CREATE TEMP TABLE previous_state AS SELECT 1 AS id, NUMERIC '1.5' AS amount
            |-- SL_SQL
            |SELECT p.id, p.amount, CURRENT_DATETIME('Europe/Paris') AS updated_at,
            |  CAST(1 AS BIGNUMERIC) AS big_amount, CURRENT_TIMESTAMP() AS loaded_at,
            |  [1, 2] AS ids, STRUCT('Paris' AS city) AS address
            |FROM previous_state p
            |""".stripMargin
        (folder / "sync_types.sql").write(sql)

        withEnvs("SL_ROOT" -> project.pathAsString) {
          implicit val settings: Settings =
            Settings(Settings.referenceConfig, None, None, None, None)
          settings
            .schemaHandler()
            .syncPreviewSqlWithDb("sync_it.sync_types", None, None)
            .map { case (attribute, status) =>
              (attribute.name, attribute.`type`, attribute.array, status)
            } shouldBe List(
            // INT64 and NUMERIC, as declared
            ("id", "integer", None, AttributeStatus.UNCHANGED),
            ("amount", "decimal", None, AttributeStatus.UNCHANGED),
            // A DATETIME is not a timestamp
            ("updated_at", "datetime", None, AttributeStatus.MODIFIED),
            ("big_amount", "bignumeric", None, AttributeStatus.ADDED),
            ("loaded_at", "timestamp", None, AttributeStatus.ADDED),
            ("ids", "long", Some(true), AttributeStatus.ADDED),
            ("address", "struct", None, AttributeStatus.ADDED)
          )

          TransformCmd
            .run(
              TransformConfig(name = "sync_it.sync_types", syncApply = true),
              settings.schemaHandler()
            )
            .isSuccess shouldBe true
          val attributes =
            settings
              .schemaHandler(reload = true)
              .taskByFullName("sync_it.sync_types")
              .get
              .attributes
          attributes shouldBe List(
            TableAttribute("id", "integer"),
            TableAttribute("amount", "decimal", comment = Some("written by hand, to keep")),
            TableAttribute("updated_at", "datetime"),
            TableAttribute("big_amount", "bignumeric"),
            TableAttribute("loaded_at", "timestamp"),
            TableAttribute("ids", "long", array = Some(true)),
            TableAttribute("address", "struct", attributes = List(TableAttribute("city", "string")))
          )
          // The SQL file of the task is kept
          (folder / "sync_types.sql").contentAsString shouldBe sql
        }
      } finally project.delete(swallowIOExceptions = true)
    }
  }
}
