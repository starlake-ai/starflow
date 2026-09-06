package ai.starlake.job.ingest

import ai.starlake.TestHelper
import com.google.cloud.logging.Logging.EntryListOption
import com.google.cloud.logging.{LoggingOptions, Payload}
import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters.*

/** Smoke test for the GCPLOG audit sink path of NativeRejectedSink: a DuckDB native load whose
  * audit sink is a Cloud Logging connection must send one log entry per rejected line, with the
  * same routing, log name and payload shape as the Spark path (IngestionUtil.sinkRejected).
  *
  * Needs live GCP application-default credentials with logging read and write access on the
  * default project, so it only runs under SL_REMOTE_TEST=true, like the BigQuery specs.
  */
class DuckDbNativeGcpLogRejectSpec extends TestHelper {

  lazy val gcplogConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_gcplog_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |connections.gcplog {
         |    type = "gcplog"
         |    options {
         |      "authType": "APPLICATION_DEFAULT"
         |    }
         |}
         |audit.sink.connectionRef = "gcplog"
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(gcplogConfiguration) {

    "Native DuckDB load with a GCPLOG audit sink" should
    "send one Cloud Logging entry per rejected line" in {
      if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
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
          result.get.counters.get.rejectedCount shouldBe 2
          // IngestionWorkflow.load aggregates jobids as a comma-joined string starting from an
          // empty accumulator, so a single-table load's jobid comes back with a leading comma.
          val jobid = result.get.counters.get.jobid.stripPrefix(",")

          val logging = LoggingOptions.getDefaultInstance.getService
          val project = LoggingOptions.getDefaultInstance.getProjectId
          // the log name is the configured rejected domain ("rejected" via reference-audit.conf),
          // matching what NativeRejectedSink passes to sinkToGcpCloudLogging
          val logName = settings.appConfig.audit.getDomainRejected()
          val filter =
            s"""logName="projects/$project/logs/$logName" AND labels.type="rejected" AND jsonPayload.jobid="$jobid""""

          // Cloud Logging entries become listable with a small ingestion delay, so poll.
          def fetchErrors(): List[String] = logging
            .listLogEntries(EntryListOption.filter(filter))
            .iterateAll()
            .asScala
            .toList
            .map(_.getPayload[Payload.JsonPayload].getDataAsMap.get("error").toString)

          var errors = fetchErrors()
          val deadline = System.currentTimeMillis() + 120000
          while (errors.size < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5000)
            errors = fetchErrors()
          }

          errors.size shouldBe 2
          errors.count(_.contains("NOTANUM")) shouldBe 1
          errors.count(_.contains("MISSING COLUMNS")) shouldBe 1
        }
      }
    }
  }
}
