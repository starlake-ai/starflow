package ai.starlake.job.site.stats

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.site.{ModelBuilder, SiteFixture}

import java.sql.DriverManager
import scala.concurrent.duration._
import scala.util.Using

class StatsRunnerDuckDbSpec extends IntegrationTestBase with SiteFixture {
  "StatsRunner" should "collect the fixture DuckDB tables end to end" in withFixture {
    implicit settings =>
      val datasets = fixtureRoot / "datasets"
      datasets.createDirectories()
      val url = s"jdbc:duckdb:${(datasets / "duckdb.db").pathAsString}"
      Using.resource(DriverManager.getConnection(url)) { c =>
        Using.resource(c.createStatement()) { st =>
          st.execute("CREATE SCHEMA IF NOT EXISTS sales")
          st.execute(
            "CREATE OR REPLACE TABLE sales.orders " +
            "(id BIGINT, customer_id BIGINT, amount DECIMAL(38,9), note VARCHAR)"
          )
        }
      }
      val model = ModelBuilder(settings.schemaHandler()).build()
      val stats = StatsRunner(30.seconds).run(model.nodes)

      stats.connections("duckdb").status shouldBe "ok"
      stats.connections("duckdb").engine shouldBe "duckdb"
      val orders = stats.tables("load:sales.orders")
      orders.exists shouldBe true
      orders.connectionRef shouldBe "duckdb"
      orders.columnDiff.map(d => d.name -> d.status) shouldBe List(
        "id"          -> "match",
        "customer_id" -> "match",
        "amount"      -> "match",
        "note"        -> "extraInWarehouse"
      )
      stats.tables("load:sales.customers").exists shouldBe false
      stats.tables("load:sales.customers").columnDiff shouldBe Nil
      stats.tables.keySet.exists(_.startsWith("external:")) shouldBe false
  }
}
