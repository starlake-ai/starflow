package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AllSinks, DomainInfo, Format, Metadata, SchemaInfo}
import com.typesafe.config.ConfigFactory

import java.util.regex.Pattern

class LoadEngineSpec extends TestHelper {

  private val configWithDuckDb = ConfigFactory
    .parseString("""
      |connections.duckdb_test {
      |  type = "jdbc"
      |  loader = "native"
      |  options {
      |    url: "jdbc:duckdb:/tmp/load_engine_spec.duckdb"
      |    driver: "org.duckdb.DuckDBDriver"
      |  }
      |}
      |connections.duckdb_noloader {
      |  type = "jdbc"
      |  options {
      |    url: "jdbc:duckdb:/tmp/load_engine_spec_noloader.duckdb"
      |    driver: "org.duckdb.DuckDBDriver"
      |  }
      |}
      |connections.redshift_test {
      |  type = "jdbc"
      |  options {
      |    url: "jdbc:redshift://localhost:5439/dev"
      |    driver: "com.amazon.redshift.jdbc42.Driver"
      |  }
      |}
      |""".stripMargin)
    .withFallback(testConfiguration)

  new WithSettings(configWithDuckDb) {
    val bqSink = Some(AllSinks(connectionRef = Some("BQ")))

    "a native DSV load to BigQuery" should "use the bigquery loader" in {
      val metadata =
        Metadata(format = Some(Format.DSV), loader = Some("native"), sink = bqSink)
      IngestionJob.selectLoader(metadata) shouldBe "bigquery"
    }

    "a spark-loader load to BigQuery" should "use the spark loader" in {
      val metadata =
        Metadata(format = Some(Format.DSV), loader = Some("spark"), sink = bqSink)
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a native JSON array load to BigQuery" should "fall back to spark" in {
      val metadata = Metadata(
        format = Some(Format.JSON),
        array = Some(true),
        loader = Some("native"),
        sink = bqSink
      )
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a native PARQUET load to BigQuery" should "fall back to spark" in {
      val metadata =
        Metadata(format = Some(Format.PARQUET), loader = Some("native"), sink = bqSink)
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a DSV load to DuckDB with loader native on the connection" should "use the duckdb loader" in {
      val metadata = Metadata(
        format = Some(Format.DSV),
        sink = Some(AllSinks(connectionRef = Some("duckdb_test")))
      )
      IngestionJob.selectLoader(metadata) shouldBe "duckdb"
    }

    "a native DSV load to the filesystem connection" should "use the spark loader" in {
      val metadata = Metadata(
        format = Some(Format.DSV),
        loader = Some("native"),
        sink = Some(AllSinks(connectionRef = Some("spark")))
      )
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a native DSV load to Redshift" should "fall back to spark (no native loader implemented)" in {
      val metadata = Metadata(
        format = Some(Format.DSV),
        loader = Some("native"),
        sink = Some(AllSinks(connectionRef = Some("redshift_test")))
      )
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a DSV load with no loader set anywhere" should "fall back to appConfig.loader (spark)" in {
      settings.appConfig.loader shouldBe "spark"
      val metadata = Metadata(
        format = Some(Format.DSV),
        sink = Some(AllSinks(connectionRef = Some("duckdb_noloader")))
      )
      IngestionJob.selectLoader(metadata) shouldBe "spark"
    }

    "a DSV load under duckDBMode settings" should "use the duckdb loader" in {
      // duckDBMode rewrites every connection to DuckDB with loader native,
      // so a table whose original sink resolves to spark no longer needs a Spark session.
      val duckDbSettings = ai.starlake.config.Settings.duckDBMode(settings)
      val metadata = Metadata(
        format = Some(Format.DSV),
        sink = Some(AllSinks(connectionRef = Some("duckdb_noloader")))
      )
      IngestionJob.selectLoader(metadata)(duckDbSettings) shouldBe "duckdb"
    }

    "loadRequiresSpark" should "merge domain and table metadata before deciding" in {
      val domain = DomainInfo(
        name = "sales",
        metadata = Some(Metadata(loader = Some("native"), sink = bqSink))
      )
      val table = SchemaInfo(
        name = "orders",
        pattern = Pattern.compile("orders.*\\.csv"),
        attributes = Nil,
        metadata = Some(Metadata(format = Some(Format.DSV)))
      )
      IngestionJob.loadRequiresSpark(domain, table) shouldBe false

      val sparkTable = table.copy(metadata = Some(Metadata(format = Some(Format.PARQUET))))
      IngestionJob.loadRequiresSpark(domain, sparkTable) shouldBe true
    }
  }
}
