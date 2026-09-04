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

    "Native DuckDB load with malformed lines" should
    "record one audit rejected row per rejected line" in {
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
        // IngestionWorkflow.load aggregates the jobid of every table it loads as a
        // comma-joined string (starting with an empty accumulator), so a single-table load's
        // own jobid comes back prefixed with a leading comma. Strip it to get the jobid this
        // load actually wrote to the audit rejected table.
        val jobid = result.get.counters.get.jobid.stripPrefix(",")

        val errors = queryAudit(
          s"SELECT error FROM audit.rejected WHERE domain = 'dsvduckreject' AND jobid = '$jobid' " +
          "ORDER BY error"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("error")
          buf.toList
        }

        errors.size shouldBe 2
        errors.count(_.contains("NOTANUM")) shouldBe 1
        errors.count(_.contains("MISSING COLUMNS")) shouldBe 1
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
