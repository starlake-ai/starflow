package ai.starlake.schema.handlers

import ai.starlake.TestHelper

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.TimeZone

class SlDateVarsSpec extends TestHelper {

  // 23:30 UTC on November 30 is 00:30 in Paris on December 1: the zone changes sl_date
  private val instant = Instant.parse("2026-11-30T23:30:15.042Z")

  "SchemaHandler.slDateVars" should "read the calendar fields in the zone of now" in {
    val vars = SchemaHandler.slDateVars(instant.atZone(ZoneId.of("UTC")))
    vars("sl_date") shouldBe "20261130"
    vars("sl_datetime") shouldBe "20261130233015"
    vars("sl_hour") shouldBe "23"
    vars("sl_milli") shouldBe "042"
    vars("sl_epoch_milli") shouldBe instant.toEpochMilli.toString
    vars("sl_epoch_second") shouldBe instant.getEpochSecond.toString

    val paris = SchemaHandler.slDateVars(instant.atZone(ZoneId.of("Europe/Paris")))
    paris("sl_date") shouldBe "20261201"
    paris("sl_datetime") shouldBe "20261201003015"
    paris("sl_epoch_milli") shouldBe instant.toEpochMilli.toString
  }

  new WithSettings() {
    "SchemaHandler.slDateVarsNow" should "use the timezone setting, not the JVM zone" in {
      settings.appConfig.timezone.getID shouldBe "UTC"
      val default = TimeZone.getDefault
      TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Kiritimati")) // UTC+14
      try {
        val before = ZonedDateTime.now(ZoneId.of("UTC")).withNano(0)
        val vars = SchemaHandler.slDateVarsNow()
        val after = ZonedDateTime.now(ZoneId.of("UTC"))
        val rendered = ZonedDateTime.of(
          vars("sl_year").toInt,
          vars("sl_month").toInt,
          vars("sl_day").toInt,
          vars("sl_hour").toInt,
          vars("sl_minute").toInt,
          vars("sl_second").toInt,
          0,
          ZoneId.of("UTC")
        )
        rendered.isBefore(before) shouldBe false
        rendered.isAfter(after) shouldBe false
      } finally TimeZone.setDefault(default)
    }
  }
}
