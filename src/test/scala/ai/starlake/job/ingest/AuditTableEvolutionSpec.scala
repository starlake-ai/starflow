package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.transform.AuditTableDdl
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  DatasetId,
  QueryJobConfiguration,
  TableId
}
import com.typesafe.config.ConfigFactory

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Using

/** An audit table created by an older Starflow lacks SCHEDULED_DATE: the audit sink adds it */
class AuditTableEvolutionSpec extends TestHelper {

  private def onAudit[T](f: java.sql.Connection => T)(implicit settings: Settings): T = {
    val options = settings.appConfig.audit.getConnection().options
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options)(f)
  }

  private def update(sql: String)(implicit settings: Settings): Unit =
    onAudit(conn => Using.resource(conn.createStatement())(_.executeUpdate(sql)))

  private def auditColumns()(implicit settings: Settings): List[String] =
    onAudit { conn =>
      Using.resource(conn.createStatement()) { st =>
        Using.resource(
          st.executeQuery(
            "SELECT column_name FROM information_schema.columns " +
            "WHERE table_schema = 'audit' AND table_name = 'audit'"
          )
        ) { rs =>
          Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1).toLowerCase).toList
        }
      }
    }

  private def rowsOf(jobid: String)(implicit settings: Settings): Int =
    onAudit { conn =>
      Using.resource(conn.createStatement()) { st =>
        Using.resource(
          st.executeQuery(s"SELECT count(*) FROM audit.audit WHERE jobid = '$jobid'")
        ) { rs =>
          rs.next()
          rs.getInt(1)
        }
      }
    }

  private def sampleLog(jobid: String) = AuditLog(
    jobid = jobid,
    paths = Some("/tmp/file.csv"),
    domain = "domain",
    schema = "table",
    success = true,
    count = 1,
    countAccepted = 1,
    countRejected = 0,
    timestamp = Timestamp.from(Instant.now()),
    duration = 1,
    message = "ok",
    step = "LOAD",
    database = None,
    tenant = "",
    test = false,
    scheduledDate = None
  )

  private val sparkAuditConfiguration =
    ConfigFactory
      .parseString("""audit.sink.connectionRef = "spark"""")
      .withFallback(testConfiguration)

  new WithSettings(sparkAuditConfiguration) {
    "The audit sink on Spark" should "add the columns an existing audit table lacks" in {
      implicit val sh: StorageHandler = storageHandler
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      val session = sparkSession
      session.sql("CREATE DATABASE IF NOT EXISTS audit")
      session.sql("DROP TABLE IF EXISTS audit.audit")
      session.sql(
        """CREATE TABLE audit.audit (
          |  JOBID STRING, PATHS STRING, DOMAIN STRING, SCHEMA STRING, SUCCESS BOOLEAN,
          |  COUNT BIGINT, COUNTACCEPTED BIGINT, COUNTREJECTED BIGINT, TIMESTAMP TIMESTAMP,
          |  DURATION LONG, MESSAGE STRING, STEP STRING, DATABASE STRING, TENANT STRING
          |) USING parquet""".stripMargin
      )
      AuditTableDdl.forgetChecked()
      val jobid = UUID.randomUUID().toString
      try {
        AuditLog.sink(List(sampleLog(jobid)), None).isSuccess shouldBe true
        session.table("audit.audit").columns.map(_.toLowerCase) should contain("scheduled_date")
        session.table("audit.audit").where(s"jobid = '$jobid'").count() shouldBe 1
      } finally {
        session.sql("DROP TABLE IF EXISTS audit.audit")
        AuditTableDdl.forgetChecked()
      }
    }
  }

  new WithSettings() {
    "The audit sink" should "add the columns an existing audit table lacks, then write its row" in {
      implicit val sh: StorageHandler = storageHandler
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()

      update("CREATE SCHEMA IF NOT EXISTS audit")
      update("DROP TABLE IF EXISTS audit.audit")
      // The PostgreSQL audit table as created before SCHEDULED_DATE
      update(
        """CREATE TABLE audit.audit (
          |  JOBID TEXT NOT NULL, PATHS TEXT NOT NULL, DOMAIN TEXT NOT NULL, SCHEMA TEXT NOT NULL,
          |  SUCCESS BOOLEAN NOT NULL, COUNT BIGINT NOT NULL, COUNTACCEPTED BIGINT NOT NULL,
          |  COUNTREJECTED BIGINT NOT NULL, TIMESTAMP TIMESTAMP NOT NULL, DURATION BIGINT NOT NULL,
          |  MESSAGE TEXT NOT NULL, STEP TEXT NOT NULL, DATABASE TEXT, TENANT TEXT
          |)""".stripMargin
      )
      AuditTableDdl.forgetChecked()
      auditColumns() should not contain "scheduled_date"

      val jobid = UUID.randomUUID().toString
      try {
        AuditLog.sink(List(sampleLog(jobid)), None).isSuccess shouldBe true
        auditColumns() should contain("scheduled_date")
        rowsOf(jobid) shouldBe 1
      } finally {
        // Later suites get a table created from the current DDL
        update("DROP TABLE IF EXISTS audit.audit")
        AuditTableDdl.forgetChecked()
      }
    }
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    // Its own dataset, dropped after: the shared audit dataset of the remote tests is left alone
    val auditDataset = "sl_audit_evolution_" + UUID.randomUUID().toString.replace("-", "")
    val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService()
    val bigQueryAuditConfiguration =
      ConfigFactory
        .parseString(s"""
          |audit.domain = "$auditDataset"
          |audit.sink.connectionRef = bqaudit
          |connections.bqaudit {
          |  type = "bigquery"
          |  options {
          |    location: europe-west1
          |    authType: APPLICATION_DEFAULT
          |  }
          |}
          |""".stripMargin)
        .withFallback(testConfiguration)

    new WithSettings(bigQueryAuditConfiguration) {
      "The audit sink on BigQuery" should "add the columns an existing audit table lacks" in {
        implicit val sh: StorageHandler = storageHandler
        implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
        def query(sql: String) = bigquery.query(QueryJobConfiguration.newBuilder(sql).build())
        try {
          query(s"CREATE SCHEMA `$auditDataset` OPTIONS(location = 'europe-west1')")
          query(
            s"""CREATE TABLE `$auditDataset.audit` (
              |  JOBID STRING, PATHS STRING, DOMAIN STRING, SCHEMA STRING, SUCCESS BOOLEAN,
              |  COUNT INT64, COUNTACCEPTED INT64, COUNTREJECTED INT64, TIMESTAMP TIMESTAMP,
              |  DURATION INT64, MESSAGE STRING, STEP STRING, DATABASE STRING, TENANT STRING
              |)""".stripMargin
          )
          AuditTableDdl.forgetChecked()
          val jobid = UUID.randomUUID().toString
          AuditLog.sink(List(sampleLog(jobid)), None).isSuccess shouldBe true
          val columns = bigquery
            .getTable(TableId.of(auditDataset, "audit"))
            .getDefinition[com.google.cloud.bigquery.StandardTableDefinition]
            .getSchema
            .getFields
            .asScala
            .map(_.getName.toLowerCase)
          columns should contain("scheduled_date")
          query(s"SELECT count(*) AS n FROM `$auditDataset.audit` WHERE jobid = '$jobid'")
            .iterateAll()
            .asScala
            .head
            .get("n")
            .getLongValue shouldBe 1
        } finally {
          bigquery.delete(DatasetId.of(auditDataset), BigQuery.DatasetDeleteOption.deleteContents())
          AuditTableDdl.forgetChecked()
        }
      }
    }
  }

}
