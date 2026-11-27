package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ListBuffer

/** A load in SCD2 with `checkColumns: ["*"]` opens a version only when a column of the file changes
  */
class DuckDbSCD2CheckColumnsLoadSpec extends TestHelper {

  lazy val duckDbConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/scd2_check_load.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(duckDbConfiguration) {

    /** The versions of the key: label, note, and whether the version is current */
    private def versions(): List[(String, String, Boolean)] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery("SELECT label, note, valid_to FROM scd2check.customer")
        val buf = ListBuffer.empty[(String, String, Boolean)]
        while (rs.next()) buf += ((rs.getString(1), rs.getString(2), rs.getObject(3) == null))
        buf.toList.sortBy { case (label, note, current) =>
          (current, String.valueOf(label), String.valueOf(note))
        }
      }

    private def load(file: String, first: Boolean): Unit =
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/scd2check/scd2check.sl.yml",
        datasetDomainName = "scd2check",
        sourceDatasetPathName = s"/sample/scd2check/$file"
      ) {
        if (first) cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "scd2check",
          "/sample/scd2check/customer.sl.yml",
          Some("customer.sl.yml")
        )
        loadPending.isSuccess shouldBe true
      }

    "A DuckDB load in SCD2 with checkColumns '*'" should "open a version only when a column of the file changes" in {
      load("SCD2CHECK-1.csv", first = true)
      versions() shouldBe List(("a", null, true))
      load("SCD2CHECK-2.csv", first = false)
      versions() shouldBe List(("a", null, true))
      load("SCD2CHECK-3.csv", first = false)
      versions() shouldBe List(("a", null, false), ("a", "x", true))
    }
  }
}
