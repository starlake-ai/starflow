package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.config.{ConnectionInfo, DatasetArea, Settings}
import ai.starlake.extract.StarlakeConnectionPool
import ai.starlake.job.ingest.{IngestConfig, LoadConfig}
import ai.starlake.job.sink.bigquery.{BigQueryLoadCliConfig, BigQueryLoadConfig}
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadConfig
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.model.ConnectionType
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, Logger}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

class SecretsInLogsSpec extends TestHelper {
  private val secret = "SENTINEL-c0ffee-SECRET"
  private val token = Some(secret)

  /** Everything Starflow and what it runs log during `body`, at debug level. SLF4J is bound to
    * reload4j here, so this is where the events go. Callers check that the logs are not empty, since
    * capturing nothing would make these tests pass.
    */
  private def logsOf(body: => Unit): String = {
    val events = new ConcurrentLinkedQueue[String]()
    val appender = new AppenderSkeleton {
      override def append(event: LoggingEvent): Unit = {
        events.add(event.getRenderedMessage)
        Option(event.getThrowableStrRep).foreach(lines => events.add(lines.mkString("\n")))
      }
      override def close(): Unit = ()
      override def requiresLayout(): Boolean = false
    }
    val loggers = List(Logger.getRootLogger, Logger.getLogger("ai.starlake"))
    val levels = loggers.map(_.getLevel)
    Logger.getRootLogger.addAppender(appender)
    loggers.foreach(_.setLevel(Level.DEBUG))
    try body
    finally {
      loggers.zip(levels).foreach { case (logger, level) => logger.setLevel(level) }
      Logger.getRootLogger.removeAppender(appender)
    }
    events.asScala.mkString("\n")
  }

  "A config" should "not print an access token" in {
    val configs: List[Product] = List(
      TransformConfig(name = "a.b", accessToken = token, scheduledDate = None),
      LoadConfig(accessToken = token, test = false, scheduledDate = None),
      IngestConfig(accessToken = token, scheduledDate = None),
      BigQueryLoadConfig(connectionRef = None, accessToken = token),
      BigQueryLoadCliConfig(accessToken = token)
    )
    configs.foreach { config =>
      withClue(config.productPrefix) {
        config.toString should not include secret
        config.toString should include("accessToken")
      }
    }
  }

  it should "show that no access token was given" in {
    TransformConfig(name = "a.b", scheduledDate = None).toString should include("accessToken=None")
  }

  it should "keep printing what helps to debug" in {
    val printed = TransformConfig(name = "a.b", truncate = true, scheduledDate = None).toString
    printed should startWith("TransformConfig(")
    printed should include("name=a.b")
    printed should include("truncate=true")
  }

  it should "mask the secrets of its options" in {
    val options = Map("sl_access_token" -> secret, "region" -> "eu")
    val printed = TransformConfig(name = "a.b", options = options, scheduledDate = None).toString
    printed should not include secret
    printed should include("eu")
  }

  "Utils.redact" should "mask every kind of credential" in {
    val secrets = List(
      "password",
      "sfPassword",
      "token",
      "sl_access_token",
      "gcpAccessToken",
      "client_secret",
      "s3SecretKey",
      "s3AccessKey",
      "privateKey",
      "credentials"
    )
    Utils.redact(secrets.map(_ -> secret).toMap).values.toSet shouldBe Set("********")
  }

  it should "keep what is not a credential" in {
    val options = Map("url" -> "jdbc:duckdb:x", "driver" -> "d", "user" -> "u")
    Utils.redact(options) shouldBe options
  }

  "A connection" should "not print the access token it was given" in {
    val connection =
      ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> "jdbc:postgresql://h/d"))
        .withAccessToken(token)
    connection.toString should not include secret
  }

  "JdbcConnectionLoadConfig" should "not print a token held in its options" in {
    JdbcConnectionLoadConfig(options = Map("token" -> secret)).toString should not include secret
  }

  "A connection without a driver" should "not put its options in the error" in {
    val error = intercept[IllegalArgumentException] {
      StarlakeConnectionPool.getConnection(None, Map("sl_access_token" -> secret))
    }
    error.getMessage should not include secret
  }

  "Loading the settings" should "not log the secrets of the connections" in {
    new WithSettings() {
      settings
        .storageHandler()
        .write(
          s"""version: 1
             |application:
             |  connections:
             |    flight:
             |      type: jdbc
             |      options:
             |        url: "jdbc:arrow-flight-sql://localhost:31337"
             |        driver: "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
             |        token: "$secret"
             |""".stripMargin,
          new Path(DatasetArea.metadata, "application.sl.yml")
        )
      val application = new Path(DatasetArea.metadata, "application.sl.yml")
      val logs =
        try logsOf(Settings(testConfiguration, None, Some(starlakeTestRoot), None, None))
        finally settings.storageHandler().delete(application)
      logs should not be empty
      logs should not include secret
    }
  }

  private lazy val duckDbConfiguration =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/secrets.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  "A transform given an access token" should "not log it, even at debug level" in {
    new WithSettings(duckDbConfiguration) {
      val storage = settings.storageHandler()
      val job = new Path(DatasetArea.transform, "sec")
      storage.mkdirs(job)
      List("a", "b").foreach(t => storage.write("SELECT 1 AS id", new Path(job, s"$t.sql")))
      val handler = settings.schemaHandler(reload = true)
      val workflow = new IngestionWorkflow(storage, handler)
      val logs = logsOf {
        workflow
          .autoJob(
            TransformConfig(name = "sec.a,sec.b", accessToken = token, scheduledDate = None)
          )
          .get
        workflow.transform(TransformConfig(name = "sec.a", accessToken = token, scheduledDate = None)).get
      }
      logs should include("Transforming")
      logs should not include secret
    }
  }
}
