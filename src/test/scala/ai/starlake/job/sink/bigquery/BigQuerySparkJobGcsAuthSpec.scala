package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class BigQuerySparkJobGcsAuthSpec extends TestHelper {

  // a real file so the SERVICE_ACCOUNT_JSON_KEYFILE mapping passes its existence check; the
  // content is never parsed before a write actually reaches Google
  private val keyfile = Files.createTempFile("sl-bq-gcsauth", ".json")
  Files.write(keyfile, """{"type": "service_account"}""".getBytes(StandardCharsets.UTF_8))

  lazy val bqConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connections.bqtest {
         |    type = "bigquery"
         |    options {
         |      "authType": "SERVICE_ACCOUNT_JSON_KEYFILE"
         |      "jsonKeyfile": "$keyfile"
         |      "projectId": "test-project"
         |      "gcsBucket": "test-bucket"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(bqConfiguration) {

    // The session here is built for the default filesystem connection, so SparkEnv.bigQueryConf
    // never ran and the session's Hadoop configuration carries no google.cloud.auth.* keys. The
    // spark-bigquery connector's indirect write stages data to GCS through that configuration,
    // and without the keys gcs-connector falls back to COMPUTE_ENGINE and queries the GCE
    // metadata server, which only answers on Google infrastructure. prepareConf has to put the
    // target connection's auth there itself.
    "prepareConf on a session not built from a BigQuery connection" should
    "apply the connection's auth to the session Hadoop configuration" in {
      val job = new BigQuerySparkJob(BigQueryLoadConfig(connectionRef = Some("bqtest")))
      val conf = job.prepareConf()

      conf.get("google.cloud.auth.type") shouldBe "SERVICE_ACCOUNT_JSON_KEYFILE"
      conf.get("google.cloud.auth.service.account.enable") shouldBe "true"
      conf.get("google.cloud.auth.service.account.json.keyfile") shouldBe keyfile.toString
    }
  }
}
