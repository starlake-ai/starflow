package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}

class DuckDbNativeRejectSpec extends TestHelper {

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_reject_native.db"
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

    "Native DuckDB load of a DSV file with malformed lines" should
    "load the good lines and count the rejected ones" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb(
          "SELECT name FROM dsvduckreject.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }
        names shouldBe List("alice", "carol", "eve")

        val counters = result.get.counters
        counters.isDefined shouldBe true
        counters.get.acceptedCount shouldBe 3
        counters.get.rejectedCount shouldBe 2
        counters.get.inputCount shouldBe 5
      }
    }

    "Native DuckDB load without sinkReplayToFile" should "write no replay file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe true

        storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path) shouldBe Nil
      }
    }
  }

  lazy val duckDbReplayConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_replay_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbReplayConfiguration) {

    "Native DuckDB load with sinkReplayToFile" should
    "write the rejected lines verbatim under the replay area, header first" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe true

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe
        "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
      }
    }
  }

  lazy val duckDbThresholdConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |rejectMaxRecords: 1
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_threshold_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbThresholdConfiguration) {

    private def tableExists(domainName: String, tableName: String): Boolean = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery(
            s"SELECT count(*) AS cnt FROM duckdb_tables() " +
            s"WHERE schema_name = '$domainName' AND table_name = '$tableName'"
          )
        rs.next()
        rs.getInt("cnt") > 0
      }
    }

    "Native DuckDB load breaching rejectMaxRecords" should
    "fail, leave the target untouched and still write the replay file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe false

        tableExists("dsvduckreject", "account") shouldBe false

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe
        "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
      }
    }
  }

  lazy val duckDbRejectAllConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |rejectAllOnError: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_rejectall_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbRejectAllConfiguration) {

    private def tableExists(domainName: String, tableName: String): Boolean = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery(
            s"SELECT count(*) AS cnt FROM duckdb_tables() " +
            s"WHERE schema_name = '$domainName' AND table_name = '$tableName'"
          )
        rs.next()
        rs.getInt("cnt") > 0
      }
    }

    "Native DuckDB load with rejectAllOnError" should "fail on the first rejected line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe false

        tableExists("dsvduckreject", "account") shouldBe false
      }
    }
  }
}
