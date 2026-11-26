package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.schema.model.Engine

import java.sql.Timestamp
import java.time.Instant
import java.util.TimeZone

class AuditTimestampLiteralSpec extends TestHelper {

  private val instant = Instant.parse("2026-11-26T08:39:40Z")

  /** Timestamp.toString prints the wall clock of the JVM zone, which is the case to cover */
  private def inParis[T](body: => T): T = {
    val default = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Paris"))
    try body
    finally TimeZone.setDefault(default)
  }

  "AuditLog.timestampLiteral" should "give Spark and BigQuery the instant with a UTC offset" in {
    inParis {
      val ts = Timestamp.from(instant)
      AuditLog.timestampLiteral(ts, Engine.SPARK) shouldBe "2026-11-26 08:39:40+00:00"
      AuditLog.timestampLiteral(ts, Engine.BQ) shouldBe "2026-11-26 08:39:40+00:00"
    }
  }

  it should "keep the wall clock of the JVM zone for the JDBC engines" in {
    inParis {
      val ts = Timestamp.from(instant)
      AuditLog.timestampLiteral(ts, Engine.Custom("postgresql")) shouldBe "2026-11-26 09:39:40.0"
      AuditLog.timestampLiteral(ts, Engine.JDBC) shouldBe "2026-11-26 09:39:40.0"
    }
  }

  new WithSettings() {
    "The Spark audit row" should "hold the instant of the run whatever the JVM zone" in {
      val session = sparkSession
      session.conf.get("spark.sql.session.timeZone") shouldBe "UTC"
      inParis {
        val log = AuditLog(
          jobid = "job",
          paths = None,
          domain = "domain",
          schema = "table",
          success = true,
          count = 1,
          countAccepted = 1,
          countRejected = 0,
          timestamp = Timestamp.from(instant),
          duration = 1,
          message = "ok",
          step = Step.LOAD.toString,
          database = None,
          tenant = "",
          test = false,
          scheduledDate = None
        )
        val row = session.sql(log.asSelect(Engine.SPARK)).collect().head
        row.getAs[Timestamp]("TIMESTAMP").toInstant shouldBe instant
        row.getAs[Timestamp]("SCHEDULED_DATE").toInstant shouldBe instant
      }
    }
  }
}
