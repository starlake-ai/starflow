package ai.starlake.config

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone

class SparkEnvSessionTimeZoneSpec extends AnyFlatSpec with Matchers {

  "SparkEnv.sessionTimeZone" should "be Starflow's timezone setting" in {
    SparkEnv.sessionTimeZone(new SparkConf(false), TimeZone.getTimeZone("UTC")) shouldBe Some("UTC")
    SparkEnv.sessionTimeZone(
      new SparkConf(false),
      TimeZone.getTimeZone("Europe/Paris")
    ) shouldBe Some("Europe/Paris")
  }

  it should "leave an explicit spark.sql.session.timeZone alone" in {
    val conf = new SparkConf(false).set("spark.sql.session.timeZone", "America/New_York")
    SparkEnv.sessionTimeZone(conf, TimeZone.getTimeZone("UTC")) shouldBe None
  }
}
