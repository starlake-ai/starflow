package ai.starlake.extract

import ai.starlake.TestHelper
import com.typesafe.config.{Config, ConfigFactory}

class ExtractDataJobDuckDbSpec extends TestHelper {

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/extract_audit.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbConfiguration) {
    "initExportAuditTable on DuckDB" should "create the audit table and find it back" in {
      val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      ParUtils.withExecutor() { implicit ec =>
        implicit val extractExecutionContext = new ExtractExecutionContext(ec)
        val columns =
          extractDataJob.initExportAuditTable(settings.appConfig.connections("test-duckdb"))
        columns.map(_.name.toLowerCase()) should contain theSameElementsAs List(
          "domain",
          "schema",
          "last_ts",
          "last_date",
          "last_long",
          "last_decimal",
          "start_ts",
          "end_ts",
          "duration",
          "mode",
          "count",
          "success",
          "message",
          "step"
        )
      }
    }
  }
}
