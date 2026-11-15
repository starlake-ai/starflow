package ai.starlake.integration.transform

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.metrics.BootstrapExpectationMacrosSpec._
import ai.starlake.job.metrics.ExpectationAssertionHandler
import com.google.cloud.bigquery._

import java.util.UUID
import scala.util.Try

/** The expectation macros that `bootstrap` ships run on BigQuery as on DuckDB: each one returns 0
  * on the table where its rule holds, and a positive number on the table where it is broken (#1899,
  * #1967).
  */
class BootstrapExpectationMacrosBQIntegrationSpec extends IntegrationTestBase {
  // One dataset per run: the CI runs share the project
  private val dataset = "starlake_it_bootstrap_" + UUID.randomUUID().toString.replace("-", "")

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  /** A statement or a call of the DuckDB rules, written for BigQuery: its dataset, its types and
    * its string literals
    */
  private def onBigQuery(sql: String): String =
    sql
      .replace("dq.", s"$dataset.")
      .replaceAll("\\bINT\\b", "INT64")
      .replace("VARCHAR", "STRING")
      .replace("DOUBLE", "FLOAT64")
      .replace("'INTEGER'", "'INT64'")
      .replace("'BLOB'", "'BYTES'")
      .replace("'O''K'", "'O\\'K'")

  private def query(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).build())

  /** Runs a macro call as `ExpectationJob` does on BigQuery: one row, one integer */
  private def run(call: String, table: String): Int = {
    val row = query(render(onBigQuery(call), table, "bigquery")).iterateAll().iterator().next()
    // As BigQueryExpectationAssertionHandler reads it
    Try(ExpectationAssertionHandler.result(new java.math.BigDecimal(row.get(0).getStringValue)))
      .getOrElse(Integer.MIN_VALUE)
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "The expectation macros of bootstrap" should
    "return 0 when their rule holds, a positive number when it is broken, on BigQuery" in {
      // Where the other BigQuery integration tests write
      bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
      try {
        duckDbTables.filterNot(_.startsWith("CREATE SCHEMA")).map(onBigQuery).foreach(query)
        rulesNotKept(s"$dataset.ok", s"$dataset.ko")(run) shouldBe Nil
      } finally
        bigquery.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents())
    }
  }
}
