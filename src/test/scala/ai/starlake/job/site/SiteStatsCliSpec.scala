package ai.starlake.job.site

import ai.starlake.job.Main
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper

import java.sql.DriverManager
import scala.util.Using

class SiteStatsCliSpec extends SiteFixture {

  "SiteCmd" should "parse --stats and --statsTimeout" in {
    val c = SiteCmd.parse(Seq("--stats", "--statsTimeout", "30")).get
    c.stats shouldBe true
    c.statsTimeoutSeconds shouldBe 30
    SiteCmd.parse(Seq()).get.stats shouldBe false
    SiteCmd.parse(Seq("--statsTimeout", "0")) shouldBe None
  }

  "site without --stats" should "not write sl-stats.json" in withFixture { _ =>
    val out = fixtureRoot / "site-nostats-out"
    new Main().run(Array("site", "--outputDir", out.pathAsString, "--clean")) shouldBe true
    (out / "sl-stats.json").exists shouldBe false
    (out / "index.html").contentAsString should not include """id="sl-stats""""
  }

  "site --stats" should "write sl-stats.json from the fixture DuckDB database" in withFixture { _ =>
    val db = fixtureRoot / "datasets" / "duckdb.db"
    db.parent.createDirectoryIfNotExists(createParents = true)
    Using.resource(DriverManager.getConnection(s"jdbc:duckdb:${db.pathAsString}")) { c =>
      Using.resource(c.createStatement()) { st =>
        st.execute("CREATE SCHEMA IF NOT EXISTS sales")
        st.execute(
          "CREATE OR REPLACE TABLE sales.customers (id BIGINT, email VARCHAR, extra VARCHAR)"
        )
      }
    }
    val out = fixtureRoot / "site-stats-out"
    new Main().run(
      Array("site", "--outputDir", out.pathAsString, "--clean", "--stats")
    ) shouldBe true

    val stats = new ObjectMapper().readTree((out / "sl-stats.json").contentAsString)
    stats.get("schemaVersion").asInt() shouldBe 1
    stats.get("connections").get("duckdb").get("status").asText() shouldBe "ok"
    stats.get("connections").get("duckdb").get("engine").asText() shouldBe "duckdb"

    val customers = stats.get("tables").get("load:sales.customers")
    customers.get("exists").asBoolean() shouldBe true
    customers.has("rowCount") shouldBe true
    val diff = customers.get("columnDiff")
    (0 until diff.size()).map(i =>
      diff.get(i).get("name").asText() -> diff.get(i).get("status").asText()
    ) should contain("extra" -> "extraInWarehouse")

    stats.get("tables").get("load:sales.orders").get("exists").asBoolean() shouldBe false
    (out / "index.html").contentAsString should include("""id="sl-stats"""")
  }

  "site --stats on the starbake sample" should "report absent tables instead of failing" in {
    val root = File.newTemporaryDirectory("sl-starbake-")
    File("samples/starbake").copyTo(root)(File.CopyOptions(overwrite = true))
    val db = root / "datasets" / "duckdb.db"
    db.delete(swallowIOExceptions = true)
    val out = root / "site-out"
    withEnvs("SL_ROOT" -> root.pathAsString, "SL_ENV" -> "DUCKDB") {
      new Main().run(
        Array("site", "--outputDir", out.pathAsString, "--clean", "--stats")
      ) shouldBe true
    }
    val stats = new ObjectMapper().readTree((out / "sl-stats.json").contentAsString)
    val tables = stats.get("tables")
    tables.size() should be > 0
    val it = tables.elements()
    while (it.hasNext) it.next().get("exists").asBoolean() shouldBe false
  }
}
