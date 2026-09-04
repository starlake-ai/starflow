package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import scala.util.Using

class DuckDbNativeRejectSpec extends TestHelper {

  // Every WithSettings block below points test-duckdb at its own database file, so these
  // helpers take the block's settings implicitly instead of closing over a single one.
  private def queryDuckDb[T](sql: String)(f: java.sql.ResultSet => T)(implicit
    settings: Settings
  ): T = {
    val options = settings.appConfig.connections("test-duckdb").options
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
      Using.resource(conn.createStatement()) { statement =>
        Using.resource(statement.executeQuery(sql))(f)
      }
    }
  }

  // The audit sink is configured to a separate, shared connection (see
  // application-test.conf, audit.sink.connectionRef), not to the domain's own DuckDB file.
  // That connection is reused, unfiltered, by every test in this suite, so callers must
  // filter by jobid to see only the rows written by their own load.
  private def queryAudit[T](sql: String)(f: java.sql.ResultSet => T)(implicit
    settings: Settings
  ): T = {
    val options = settings.appConfig.audit.getConnection().options
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
      Using.resource(conn.createStatement()) { statement =>
        Using.resource(statement.executeQuery(sql))(f)
      }
    }
  }

  private def tableExists(domainName: String, tableName: String)(implicit
    settings: Settings
  ): Boolean =
    queryDuckDb(
      s"SELECT count(*) AS cnt FROM duckdb_tables() " +
      s"WHERE schema_name = '$domainName' AND table_name = '$tableName'"
    ) { rs =>
      rs.next()
      rs.getInt("cnt") > 0
    }

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

        // DatasetArea.replay("dsvduckreject") is the same directory for every WithSettings
        // block in this spec (starlakeTestRoot is shared, and cleanMetadata does not touch
        // it). This block never enables sinkReplayToFile, so nothing here writes a replay
        // file, but clearing it anyway keeps this assertion self-contained rather than
        // relying on running before the tests that do write one.
        storageHandler.delete(DatasetArea.replay("dsvduckreject"))

        loadPending.isSuccess shouldBe true

        storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path) shouldBe Nil
      }
    }
  }

  lazy val duckDbJinjaConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_jinja_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbJinjaConfiguration) {

    // DuckDB embeds the offending value verbatim in its CSV error messages, so input data
    // reaches the Jinja pass that AutoTask runs on the audit SELECT (parseSQL = true). One
    // input line below carries {{ANUM}}, which that pass used to resolve as an unknown
    // variable and replace with the empty string, silently rewriting the recorded error.
    // The next one carries {%ANUM%}: an unknown tag is a FATAL error whatever
    // failOnUnknownTokens says, and Jinjava.render throws on it, so an unescaped {% used to
    // abort the whole load instead of rejecting the single line that carried it.
    // The last one carries {}}%ANUM%#}}, which holds no delimiter at all. It is here because
    // stripping the six delimiters as pairs is not a fixpoint: the passes are non overlapping
    // and left to right, so deleting }} and #} joined what was around them and the escaping
    // handed the Jinja pass a {%ANUM%} it had synthesized itself, aborting the load over a
    // line that never carried a tag. escapeLiteral now drops every brace instead.
    // The last line carries a backslash, which the audit sink dialects that treat it as an
    // escape character inside string literals (Snowflake, BigQuery, MySQL) would read as
    // escaping whatever follows it, up to the literal's closing quote. Postgres, the audit sink
    // here, is standard conforming and would have stored it as is, so this pins that the
    // replacement happens rather than proving anything about those dialects.
    "Native DuckDB load of a DSV line containing Jinja delimiters" should
    "record it in the audit rejected table without failing the load" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckjinja/dsvduckjinja.sl.yml",
        datasetDomainName = "dsvduckjinja",
        sourceDatasetPathName = "/sample/dsvduckjinja/XDSVJINJATBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckjinja",
          "/sample/dsvduckjinja/account_dsvduckjinja.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb("SELECT name FROM dsvduckjinja.account ORDER BY name") { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }
        names shouldBe List("alice", "carol")
        result.get.counters.get.rejectedCount shouldBe 4

        val jobid = result.get.counters.get.jobid.stripPrefix(",")
        val errors = queryAudit(
          s"SELECT error FROM audit.rejected WHERE domain = 'dsvduckjinja' AND jobid = '$jobid'"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("error")
          buf.toList
        }

        errors.size shouldBe 4
        // the escaping drops every brace and turns the single quote into a dash, so the value
        // the engine reported survives in a form the Jinja pass cannot act on. Without it the
        // Jinja pass eats {{ANUM}} and the row reads NOTX-Y.
        errors.count(_.contains("NOTANUMX-Y")) shouldBe 1
        // and without it {%ANUM%} throws, so there would be no row here at all: the load
        // would have failed on the assertion above. Only the braces go, which is why the
        // percent signs are still here and the value no longer reads NOTANUMZ.
        errors.count(_.contains("NOT%ANUM%Z")) shouldBe 1
        // the line that carried no delimiter of its own. Pairwise stripping built {%ANUM%}
        // out of it and threw, so this row only exists because the braces now go wholesale.
        errors.count(_.contains("NOT%ANUM%#W")) shouldBe 1
        // the backslash is neutralized the same way the single quote is, so the recorded row
        // reads NOT-ANUM. Doubling it instead would be wrong on the standard conforming
        // dialects, which would then store the doubled backslash.
        errors.count(_.contains("NOT-ANUM")) shouldBe 1
        errors.foreach { error =>
          error should not include "{"
          error should not include "}"
          error should not include "\\"
        }
      }
    }
  }

  lazy val duckDbOptionsConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_options_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbOptionsConfiguration) {

    // The table below carries ignore_errors = false in its metadata options, which used to be
    // interpolated verbatim next to the store_rejects the loader injects. DuckDB answers that
    // combination with "Binder Error: STORE_REJECTS option is only supported when IGNORE_ERRORS
    // is not manually set to false", failing a load that worked before reject capture existed.
    "Native DuckDB load of a table whose options fight the reject capture" should
    "drop those options and load as if they were not there" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckopts/dsvduckopts.sl.yml",
        datasetDomainName = "dsvduckopts",
        sourceDatasetPathName = "/sample/dsvduckopts/XDSVOPTSTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckopts",
          "/sample/dsvduckopts/account_dsvduckopts.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb("SELECT name FROM dsvduckopts.account ORDER BY name") { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }
        names shouldBe List("alice", "carol", "eve")
        result.get.counters.get.acceptedCount shouldBe 3
        result.get.counters.get.rejectedCount shouldBe 2
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

        // DatasetArea.replay("dsvduckreject") is the same directory for every WithSettings
        // block in this spec (starlakeTestRoot is shared, and cleanMetadata does not touch
        // it), so start from a clean slate to keep this test independent of what other tests
        // in this file left behind.
        storageHandler.delete(DatasetArea.replay("dsvduckreject"))

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

        // DatasetArea.replay("dsvduckreject") is the same directory for every WithSettings
        // block in this spec (starlakeTestRoot is shared, and cleanMetadata does not touch
        // it), so start from a clean slate to keep this test independent of what other tests
        // in this file left behind.
        storageHandler.delete(DatasetArea.replay("dsvduckreject"))

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

  lazy val duckDbOverwriteConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_overwrite_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbOverwriteConfiguration) {

    "Native DuckDB load with OVERWRITE strategy" should
    "report the accepted count of the new rows rather than a delta against the replaced table" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckoverwrite/dsvduckoverwrite.sl.yml",
        datasetDomainName = "dsvduckoverwrite",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckoverwrite",
          "/sample/dsvduckoverwrite/account_dsvduckoverwrite.sl.yml",
          Some("account.sl.yml")
        )

        // Not the point of this test, but a regression on the fresh-table path should
        // still be visible here.
        val firstResult = loadPending
        firstResult.isSuccess shouldBe true
        firstResult.get.counters.isDefined shouldBe true
        firstResult.get.counters.get.acceptedCount shouldBe 3
        firstResult.get.counters.get.rejectedCount shouldBe 2
        firstResult.get.counters.get.inputCount shouldBe 5

        // Loading the same file again replaces the 3 previously accepted rows with 3 new
        // ones. Before the fix, acceptedCount is computed as a delta against the table
        // that OVERWRITE just emptied and refilled, so it comes back as 0.
        val secondResult = loadPending
        secondResult.isSuccess shouldBe true
        secondResult.get.counters.isDefined shouldBe true
        secondResult.get.counters.get.acceptedCount shouldBe 3
        secondResult.get.counters.get.rejectedCount shouldBe 2
        secondResult.get.counters.get.inputCount shouldBe 5

        val names = queryDuckDb(
          "SELECT name FROM dsvduckoverwrite.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }
        names shouldBe List("alice", "carol", "eve")
      }
    }
  }

  lazy val duckDbGroupedConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |grouped: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_grouped_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbGroupedConfiguration) {

    "Native DuckDB load of two files in one ingestion" should
    "accumulate the rejected lines of every file into one replay file" in {
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

        // DatasetArea.replay("dsvduckreject") is the same directory for every WithSettings
        // block in this spec (starlakeTestRoot is shared, and cleanMetadata does not touch
        // it), so start from a clean slate to keep this test independent of what other tests
        // in this file left behind.
        storageHandler.delete(DatasetArea.replay("dsvduckreject"))

        // delivered before loadPending, which delivers the first file and then loads
        // every staged file matching the table pattern in one ingestion
        withSettings.deliverTestFile(
          "/sample/dsvduckreject/XDSVREJECTTBL2",
          new Path(DatasetArea.stage("dsvduckreject"), "XDSVREJECTTBL2")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        result.get.counters.get.rejectedCount shouldBe 3

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        val content = storageHandler.read(replayFiles.head)
        content.linesIterator.toList.size shouldBe 4
        content.contains("2;bob;NOTANUM") shouldBe true
        content.contains("badline;dave") shouldBe true
        content.contains("7;grace;NOTANUM") shouldBe true
      }
    }
  }

  lazy val duckDbPreExistingConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |rejectMaxRecords: 1
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_preexisting_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbPreExistingConfiguration) {

    private def accountNames(): List[String] =
      queryDuckDb("SELECT name FROM dsvduckpreexist.account ORDER BY name") { rs =>
        val buf = scala.collection.mutable.ListBuffer[String]()
        while (rs.next()) buf += rs.getString("name")
        buf.toList
      }

    "Native DuckDB load breaching rejectMaxRecords over an already loaded table" should
    "roll back and leave the rows of the previous load intact" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckpreexist/dsvduckpreexist.sl.yml",
        datasetDomainName = "dsvduckpreexist",
        sourceDatasetPathName = "/sample/dsvduckpreexist/XDSVPREEXISTTBL1"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckpreexist",
          "/sample/dsvduckpreexist/account_dsvduckpreexist.sl.yml",
          Some("account.sl.yml")
        )

        // a clean file first, so the target table exists and holds rows before the aborted
        // load below
        loadPending.isSuccess shouldBe true
        accountNames() shouldBe List("alice", "carol")
      }

      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckpreexist/dsvduckpreexist.sl.yml",
        datasetDomainName = "dsvduckpreexist",
        sourceDatasetPathName = "/sample/dsvduckpreexist/XDSVPREEXISTTBL2"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckpreexist",
          "/sample/dsvduckpreexist/account_dsvduckpreexist.sl.yml",
          Some("account.sl.yml")
        )

        // 2 rejected lines against rejectMaxRecords = 1, so the load is aborted
        loadPending.isSuccess shouldBe false

        // the strictly stronger guarantee than "the target was never created": the rows of
        // the first load are still there, and the single good line of the aborted APPEND
        // (eve) never landed
        tableExists("dsvduckpreexist", "account") shouldBe true
        accountNames() shouldBe List("alice", "carol")
      }
    }
  }

  lazy val duckDbRejectSinkFailureConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |audit.sink.connectionRef: "broken-audit"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_sinkfailure_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |connections.broken-audit {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/no/such/directory/audit.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbRejectSinkFailureConfiguration) {

    // The Spark path saves the rejects before the accepted rows
    // (SparkIngestionPipeline.ingest), so a reject sink that fails means nothing was written.
    // The native path used to report the rejects after the target rows had been committed,
    // which left the load reported as failed while the data was in the table, and a retry
    // under APPEND would then append the good rows a second time. The audit connection below
    // points at a directory that does not exist, so NativeRejectedSink throws on the first
    // rejected line of an otherwise ordinary load.
    "Native DuckDB load whose reject sink fails" should
    "fail without committing the accepted rows" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreplayfail/dsvduckreplayfail.sl.yml",
        datasetDomainName = "dsvduckreplayfail",
        sourceDatasetPathName = "/sample/dsvduckreplayfail/XDSVREPLAYFAILTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreplayfail",
          "/sample/dsvduckreplayfail/account_dsvduckreplayfail.sl.yml",
          Some("account.sl.yml")
        )

        // sinkReplayToFile is on in this block, so both halves of reportRejects run and the
        // replay file has to be written before the sink is even reached. Start from a clean
        // slate so the assertion below cannot pass on a file another test left behind.
        storageHandler.delete(DatasetArea.replay("dsvduckreplayfail"))

        loadPending.isSuccess shouldBe false

        // nothing was committed, so once the audit connection is fixed the same file can be
        // loaded again without appending the good rows twice
        tableExists("dsvduckreplayfail", "account") shouldBe false

        // the replay file is written first, so the user still gets the lines to fix even
        // though the audit sink took the load down right after
        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreplayfail"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe
        "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
      }
    }
  }

  lazy val duckDbRenameConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_rename_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbRenameConfiguration) {

    // The reject trail has to be named the way the rest of the audit trail is named.
    // IngestionAudit.buildAuditLog writes domain.name and schema.name into audit.audit, and the
    // Spark reject path (IngestionJob.saveRejected) writes the same declared names into
    // audit.rejected and into the replay file name. The native loader used to write the final
    // names, so a table carrying a rename left audit.rejected rows that no longer joined
    // audit.audit on (jobid, domain, schema), and a replay file the Spark loader would not have
    // written there. Every other fixture in this file is free of rename, where name and finalName
    // are the same string and the bug is invisible.
    "Native DuckDB load of a table declaring a rename" should
    "name the audit rejected rows and the replay file after the declared table" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckrename/dsvduckrename.sl.yml",
        datasetDomainName = "dsvduckrename",
        sourceDatasetPathName = "/sample/dsvduckrename/XDSVRENAMETBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckrename",
          "/sample/dsvduckrename/account_dsvduckrename.sl.yml",
          Some("account.sl.yml")
        )

        // DatasetArea.replay("dsvduckrename") survives cleanMetadata, so start from a clean
        // slate, as the other replay blocks in this file do.
        storageHandler.delete(DatasetArea.replay("dsvduckrename"))

        val result = loadPending
        result.isSuccess shouldBe true

        // the rename is honored where it belongs: the rows land in the renamed target table
        tableExists("dsvduckrename", "account_renamed") shouldBe true
        tableExists("dsvduckrename", "account") shouldBe false

        val jobid = result.get.counters.get.jobid.stripPrefix(",")

        val rejectedNames = queryAudit(
          s"SELECT domain, schema FROM audit.rejected WHERE jobid = '$jobid' ORDER BY error"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(String, String)]()
          while (rs.next()) buf += ((rs.getString("domain"), rs.getString("schema")))
          buf.toList
        }
        rejectedNames shouldBe List(
          ("dsvduckrename", "account"),
          ("dsvduckrename", "account")
        )

        // and the audit.audit rows of the very same load. The LOAD row is the one the rejected
        // rows have to join, and it carries the declared name. The two step path also runs its
        // second step through an AutoTask, which logs its own TRANSFORM row against the table it
        // actually writes into, so that one carries the renamed name and is expected to.
        val auditNames = queryAudit(
          s"SELECT domain, schema, step FROM audit.audit WHERE jobid = '$jobid' ORDER BY step"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(String, String, String)]()
          while (rs.next())
            buf += ((rs.getString("domain"), rs.getString("schema"), rs.getString("step")))
          buf.toList
        }
        auditNames shouldBe List(
          ("dsvduckrename", "account", "LOAD"),
          ("dsvduckrename", "account_renamed", "TRANSFORM")
        )
        rejectedNames.distinct shouldBe
        auditNames.filter(_._3 == "LOAD").map { case (d, s, _) => (d, s) }

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckrename"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        replayFiles.head.getName should startWith("dsvduckrename.account.")
      }
    }
  }

  lazy val duckDbSecondStepFailureConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_secondstep_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbSecondStepFailureConfiguration) {

    // The two step path used to drop the Try returned by the second step task, so a task that
    // failed after the rejects had been reported was still reported as a successful load: the
    // counters read "success, 0 accepted, 1 rejected" for a load whose accepted rows were
    // rolled back. The table below carries a postsql statement against a table that does not
    // exist, which fails the task after its INSERT and before its commit.
    "Native DuckDB load whose second step task fails" should
    "fail the load rather than report it as successful" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvducksecondstepfail/dsvducksecondstepfail.sl.yml",
        datasetDomainName = "dsvducksecondstepfail",
        sourceDatasetPathName = "/sample/dsvducksecondstepfail/XDSVSECONDSTEPFAILTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvducksecondstepfail",
          "/sample/dsvducksecondstepfail/account_dsvducksecondstepfail.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe false

        // the target is created by updateJdbcTableSchema before the task runs, so what has to
        // be checked is that it holds none of this load's rows, not that it is absent
        queryDuckDb("SELECT count(*) AS cnt FROM dsvducksecondstepfail.account") { rs =>
          rs.next()
          rs.getInt("cnt")
        } shouldBe 0
      }
    }

    // reportRejects runs before the second step task on purpose, so that a reject sink failure
    // means nothing was committed. The cost is that a second step failing for a reason unrelated
    // to the data, a broken postsql here, leaves that attempt's replay file and audit rejected
    // rows behind. The retry once the SQL is fixed would then write them a second time, double
    // counting the rejects and stacking replay files, so the second step failure path cleans up
    // its own artifacts before rethrowing. The threshold breach path deliberately does not: there
    // the data is the problem and the replay file is the user's repair tool.
    "Native DuckDB load whose second step task fails" should
    "clean up its replay file and its audit rejected rows so the retry does not double count" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvducksecondstepfail/dsvducksecondstepfail.sl.yml",
        datasetDomainName = "dsvducksecondstepfail",
        sourceDatasetPathName = "/sample/dsvducksecondstepfail/XDSVSECONDSTEPFAILTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvducksecondstepfail",
          "/sample/dsvducksecondstepfail/account_dsvducksecondstepfail.sl.yml",
          Some("account.sl.yml")
        )

        // DatasetArea.replay survives cleanMetadata, as in the other replay blocks of this file
        storageHandler.delete(DatasetArea.replay("dsvducksecondstepfail"))

        loadPending.isSuccess shouldBe false

        // the input carries one malformed line, so the attempt did write a replay file before
        // its second step failed. It has to be gone again.
        storageHandler
          .list(
            DatasetArea.replay("dsvducksecondstepfail"),
            extension = ".replay",
            recursive = false
          )
          .map(_.path) shouldBe Nil
      }

      // the retry, with the postsql fixed, leaves exactly one set of artifacts rather than a
      // second one stacked on what the failed attempt left behind
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvducksecondstepfail/dsvducksecondstepfail.sl.yml",
        datasetDomainName = "dsvducksecondstepfail",
        sourceDatasetPathName = "/sample/dsvducksecondstepfail/XDSVSECONDSTEPFAILTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvducksecondstepfail",
          "/sample/dsvducksecondstepfail/account_dsvducksecondstepfixed.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true
        result.get.counters.get.acceptedCount shouldBe 2
        result.get.counters.get.rejectedCount shouldBe 1

        val replayFiles = storageHandler
          .list(
            DatasetArea.replay("dsvducksecondstepfail"),
            extension = ".replay",
            recursive = false
          )
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe "id;name;amount\n2;bob;NOTANUM\n"

        // this domain is used by no other block of this suite, so every audit rejected row it
        // carries was written by one of the two loads above. Only the retry's may remain.
        val jobid = result.get.counters.get.jobid.stripPrefix(",")
        queryAudit(
          "SELECT count(*) AS cnt FROM audit.rejected WHERE domain = 'dsvducksecondstepfail'"
        ) { rs =>
          rs.next()
          rs.getInt("cnt")
        } shouldBe 1
        queryAudit(
          "SELECT count(*) AS cnt FROM audit.rejected " +
          s"WHERE domain = 'dsvducksecondstepfail' AND jobid = '$jobid'"
        ) { rs =>
          rs.next()
          rs.getInt("cnt")
        } shouldBe 1
      }
    }
  }
}
