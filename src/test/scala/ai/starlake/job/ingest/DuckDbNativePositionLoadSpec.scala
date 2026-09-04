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

    // The audit sink is configured to a separate, shared connection (see
    // application-test.conf, audit.sink.connectionRef), not to the domain's own DuckDB file.
    // That connection is reused, unfiltered, by every test in this suite, so callers must
    // filter by jobid to see only the rows written by their own load.
    private def queryAudit[T](sql: String)(f: java.sql.ResultSet => T): T = {
      val options = settings.appConfig.audit.getConnection().options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn.createStatement().executeQuery(sql)
        f(rs)
      }
    }

    // IngestionWorkflow.load aggregates the jobid of every table it loads as a comma-joined
    // string (starting with an empty accumulator), so a single-table load's own jobid comes
    // back prefixed with a leading comma. Strip it to get the jobid this load actually wrote
    // to the audit rejected table. Each error is stored as "<input file>: <message>".
    private def rejectedErrors(domainName: String, jobid: String): List[String] =
      queryAudit(
        s"SELECT error FROM audit.rejected " +
        s"WHERE domain = '$domainName' AND jobid = '${jobid.stripPrefix(",")}' ORDER BY error"
      ) { rs =>
        val buf = scala.collection.mutable.ListBuffer[String]()
        while (rs.next()) buf += rs.getString("error")
        buf.toList
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

        // the audit row carries the message built by capturePositionRejects, so the clause
        // that fired and the attribute and DDL type it names are pinned here
        val errors = rejectedErrors("positionduck", result.get.counters.get.jobid)
        errors.size shouldBe 1
        errors.head should endWith("amount: cannot cast to BIGINT")
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

        val rows = queryDuckDb(
          "SELECT name, amount FROM positionduckshort.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(String, Option[Long])]()
          while (rs.next()) {
            val name = rs.getString("name")
            val amount = rs.getLong("amount")
            buf += ((name, if (rs.wasNull()) None else Some(amount)))
          }
          buf.toList
        }

        rows.map(_._1) shouldBe List("Blank     ", "Jane      ", "John      ")
        // The Blank line is 15 characters long, so it clears the length clause, and its
        // amount slice is all spaces. The TRIM guard on the cast clause is what keeps it out
        // of the rejects: without it, every fixed width line with an empty optional numeric
        // column would be rejected. It loads with a NULL amount instead.
        rows.find(_._1.trim == "Blank").map(_._2) shouldBe Some(None)
        rows.find(_._1.trim == "John").flatMap(_._2) shouldBe Some(12345L)
        // only the truncated line is rejected
        result.get.counters.get.rejectedCount shouldBe 1

        // the length clause fired, not the cast clause, and it names the length it requires
        val errors = rejectedErrors("positionduckshort", result.get.counters.get.jobid)
        errors.size shouldBe 1
        errors.head should endWith(
          "line is shorter than the 15 characters required by the declared positions"
        )
      }
    }
  }
}
