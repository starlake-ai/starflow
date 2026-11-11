package ai.starlake.config

// The copy relocated inside the shaded gcs-connector jar, the one the connector runs
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HadoopCredentialsConfiguration
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType
import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The GCS connector defaults google.cloud.auth.type to COMPUTE_ENGINE, so a Spark session that
  * does not carry the connection's authType queries the GCE metadata server and fails anywhere
  * outside Google infrastructure.
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

  // The connector reads fs.gs.auth.type before google.cloud.auth.type, and Hadoop 3.5's
  // core-default.xml sets fs.gs.auth.type to COMPUTE_ENGINE: resolve it the way the connector does,
  // on a Hadoop conf that loads core-default.xml.
  private def connectorAuthType(sparkConf: Map[String, String]): AuthenticationType = {
    val hadoopConf = new Configuration()
    sparkConf.collect {
      case (key, value) if key.startsWith("spark.hadoop.") =>
        hadoopConf.set(key.stripPrefix("spark.hadoop."), value)
    }
    HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX
      .withPrefixes(HadoopCredentialsConfiguration.getConfigKeyPrefixes("fs.gs"))
      .get(hadoopConf, (key, default) => hadoopConf.getEnum(key, default))
  }

  "the GCS connector" should "use the connection's authType over Hadoop's COMPUTE_ENGINE default" in {
    new Configuration().get("fs.gs.auth.type") shouldEqual "COMPUTE_ENGINE"
    connectorAuthType(confFor("")) shouldEqual AuthenticationType.APPLICATION_DEFAULT
    connectorAuthType(
      confFor("""      "authType": "USER_CREDENTIALS"
                |      "clientId": "id"
                |      "clientSecret": "secret"
                |      "refreshToken": "token"""".stripMargin)
    ) shouldEqual AuthenticationType.USER_CREDENTIALS
  }
}
