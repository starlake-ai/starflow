package ai.starlake.integration.transform

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main
import com.google.cloud.bigquery.{DatasetInfo, QueryJobConfiguration, TableId}

class TransformIntegrationBQSpec extends BigQueryIntegrationSpecBase {

  /** The transform's source tables, provisioned directly so the transform test does not depend on
    * the load test having populated them: CI showed sales.customers can be missing when the
    * transform starts, which fails it with "Table not found". Tables are only created when absent,
    * so a real load's output is left untouched, and a later full run starts from the base class's
    * clean slate anyway.
    */
  /** Everything in this test lives in europe-west1 (application.sl.yml); the materialization
    * dataset has to be there too, because the connector runs the transform's query in the
    * materialization dataset's location and cannot see sales from anywhere else.
    */
  private val materializationDataset = "SL_BQ_TEST_DS"

  private def ensureDataset(name: String): Unit =
    if (bigquery.getDataset(name) == null)
      bigquery.create(DatasetInfo.newBuilder(name).setLocation("europe-west1").build())

  private def provisionTransformSources(): Unit = {
    ensureDataset(materializationDataset)
    ensureDataset("sales")
    def ensure(table: String, createSql: String, seedSql: String): Unit =
      if (bigquery.getTable(TableId.of("sales", table)) == null) {
        bigquery.query(QueryJobConfiguration.newBuilder(createSql).build())
        bigquery.query(QueryJobConfiguration.newBuilder(seedSql).build())
      }
    ensure(
      "customers",
      "CREATE TABLE sales.customers(id STRING, signup TIMESTAMP, contact STRING, " +
      "birthdate DATE, name1 STRING, name2 STRING, id1 STRING)",
      "INSERT INTO sales.customers(id, name1) VALUES ('A009701', 'fixture'), ('B308629', 'fixture')"
    )
    // amount is NUMERIC, not FLOAT64: the loaded schema types it as decimal, and the transform's
    // sum(amount) has to append into the existing byseller_kpi target whose sum column is NUMERIC
    ensure(
      "orders",
      "CREATE TABLE sales.orders(id STRING, customer_id STRING, amount NUMERIC, " +
      "seller_id STRING, ts TIMESTAMP)",
      "INSERT INTO sales.orders(id, customer_id, amount) " +
      "VALUES ('O1', 'A009701', 10.5), ('O2', 'B308629', 20.0)"
    )
  }

  "Native Bigquery Load" should "succeed" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        // set on the load test too: the Spark session is built once per JVM, during this test,
        // and the materialization dataset is baked into its conf, so setting it only on the
        // transform test would come too late
        "SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET" -> materializationDataset,
        "SL_ROOT"                                   -> theSampleFolder.pathAsString
      ) {
        cleanup()
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("stage")
          )
        )
        assert(
          new Main().run(
            Array("load")
          )
        )
      }
    }
  }
  "Native Bigquery Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      provisionTransformSources()
      withEnvs(
        "SL_ENV"                                        -> "BQ",
        "SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE" -> "DYNAMIC",
        "SL_SPARK_BIGQUERY_MATERIALIZATION_DATASET"     -> materializationDataset,
        "SL_ROOT"                                       -> theSampleFolder.pathAsString
      ) {
        assert(
          new Main().run(
            Array(
              "transform",
              "--name",
              "sales_kpi.byseller_kpi0"
            )
          )
        )
      }
    }
  }
}
