package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.Charset
import scala.io.Codec

class DuckDbNativePositionLoadSpec extends TestHelper {

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_position_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbConfiguration) {

    private def queryDuckDb[T](sql: String)(f: java.sql.ResultSet => T): T = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn.createStatement().executeQuery(sql)
        f(rs)
      }
    }

    "Native DuckDB load of a POSITION file" should "slice lines with SUBSTR and reject cells that cannot be cast" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionduck/positionduck.sl.yml",
        datasetDomainName = "positionduck",
        sourceDatasetPathName = "/sample/positionduck/XPOSDUCKTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionduck",
          "/sample/positionduck/account_positionduck.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val rows = queryDuckDb(
          "SELECT name, amount FROM positionduck.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(String, Option[Long])]()
          while (rs.next()) {
            val name = rs.getString("name")
            val amount = rs.getLong("amount")
            val amountOpt = if (rs.wasNull()) None else Some(amount)
            buf += ((name, amountOpt))
          }
          buf.toList
        }

        rows.size shouldBe 2
        // fixed-width slices keep their trailing spaces, as on the BigQuery native path
        rows.map(_._1) shouldBe List("Jane      ", "John      ")
        // the BadRow line holds abcde where a number is declared, so it is rejected
        // rather than loaded with a NULL amount
        rows.find(_._1.trim == "BadRow") shouldBe None
        rows.find(_._1.trim == "John").flatMap(_._2) shouldBe Some(12345L)
        rows.find(_._1.trim == "Jane").flatMap(_._2) shouldBe Some(67890L)
        result.get.counters.get.rejectedCount shouldBe 1
      }
    }

    "Native DuckDB load of a POSITION file with header" should "skip the header line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionduckhdr/positionduckhdr.sl.yml",
        datasetDomainName = "positionduckhdr",
        sourceDatasetPathName = "/sample/positionduckhdr/HPOSDUCKTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionduckhdr",
          "/sample/positionduckhdr/account_positionduckhdr.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb(
          "SELECT name FROM positionduckhdr.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) {
            buf += rs.getString("name")
          }
          buf.toList
        }

        names.map(_.trim) shouldBe List("Jane", "John")
      }
    }

    "Native DuckDB load of an ISO-8859-1 POSITION file" should "honor the metadata encoding" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionduckenc/positionduckenc.sl.yml",
        datasetDomainName = "positionduckenc",
        sourceDatasetPathName = "/sample/positionduckenc/EPOSDUCKTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionduckenc",
          "/sample/positionduckenc/account_positionduckenc.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending(new Codec(Charset.forName("ISO-8859-1")))
        result.isSuccess shouldBe true

        val rows = queryDuckDb(
          "SELECT name, amount FROM positionduckenc.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(String, Long)]()
          while (rs.next()) {
            buf += ((rs.getString("name").trim, rs.getLong("amount")))
          }
          buf.toList
        }

        rows shouldBe List(("Hervé", 12345L), ("Jane", 67890L))
      }
    }

    "Native DuckDB load of a POSITION file with a truncated line" should
    "reject the short line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionduckshort/positionduckshort.sl.yml",
        datasetDomainName = "positionduckshort",
        sourceDatasetPathName = "/sample/positionduckshort/XPOSSHORTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionduckshort",
          "/sample/positionduckshort/account_positionduckshort.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb(
          "SELECT name FROM positionduckshort.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }

        names shouldBe List("Jane      ", "John      ")
        result.get.counters.get.rejectedCount shouldBe 1
      }
    }
  }
}
