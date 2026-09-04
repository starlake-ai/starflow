package ai.starlake.config

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The GCS connector defaults google.cloud.auth.type to COMPUTE_ENGINE, so a Spark session
  * that does not carry the connection's authType queries the GCE metadata server and fails
  * anywhere outside Google infrastructure.
  */
class SparkEnvGcsAuthSpec extends AnyFlatSpec with Matchers {

  private def settingsWith(connectionOptions: String): Settings = {
    val root = java.nio.file.Files.createTempDirectory("sl-sparkenv-gcsauth").toString
    val config = ConfigFactory.load(
      ConfigFactory.parseString(
        s"""
           |SL_ROOT="$root"
           |connectionRef: "bq"
           |connections.bq {
           |    type = "bigquery"
           |    options {
           |      "projectId": "test-project"
           |$connectionOptions
           |    }
           |}
           |""".stripMargin,
        ConfigParseOptions.defaults().setAllowMissing(false)
      )
    )
    Settings(config, None, None, None, None)
  }

  private def confFor(connectionOptions: String): Map[String, String] = {
    implicit val settings: Settings = settingsWith(connectionOptions)
    SparkEnv.bigQueryConf(settings.appConfig.connections("bq"))
  }

  "a BigQuery connection declaring authType" should "propagate it to the GCS connector" in {
    val conf = confFor("""      "authType": "APPLICATION_DEFAULT"""")
    conf("spark.hadoop.google.cloud.auth.type") shouldEqual "APPLICATION_DEFAULT"
    conf("spark.hadoop.fs.gs.project.id") shouldEqual "test-project"
    conf("parentProject") shouldEqual "test-project"
  }

  it should "propagate USER_CREDENTIALS and its secrets" in {
    val conf = confFor("""      "authType": "USER_CREDENTIALS"
                         |      "clientId": "id"
                         |      "clientSecret": "secret"
                         |      "refreshToken": "token"""".stripMargin)
    conf("spark.hadoop.google.cloud.auth.type") shouldEqual "USER_CREDENTIALS"
    conf("spark.hadoop.google.cloud.auth.client.id") shouldEqual "id"
    conf("spark.hadoop.google.cloud.auth.refresh.token") shouldEqual "token"
  }

  "a BigQuery connection without authType" should "default to APPLICATION_DEFAULT" in {
    confFor("")("spark.hadoop.google.cloud.auth.type") shouldEqual "APPLICATION_DEFAULT"
  }
}
