package ai.starlake.job.transform

import com.dimafeng.testcontainers.{GenericContainer, MySQLContainer}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/** PostgreSQL, in its TestContainers container */
class PostgresSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("PostgreSQL") {
  override protected def engineConfiguration: Config =
    ConfigFactory.parseString("connectionRef: \"test-pg\"").withFallback(super.testConfiguration)
}

/** The Snowflake DELETE_THEN_INSERT template, rendered: no Snowflake account runs it here */
class SnowflakeDeleteThenInsertTemplateSpec extends ai.starlake.TestHelper {
  import ai.starlake.config.ConnectionInfo
  import ai.starlake.job.strategies.TransformStrategiesBuilder
  import ai.starlake.schema.model._

  new WithSettings() {
    private def render(dataBranch: Option[String]): String =
      new TransformStrategiesBuilder()
        .buildTransform(
          WriteStrategy(
            `type` = Some(WriteStrategyType.DELETE_THEN_INSERT),
            key = List("d", "k"),
            on = Some(MergeOn.TARGET)
          ),
          "SELECT d, k, n FROM selfdb.src",
          TransformStrategiesBuilder.TableComponents("", "selfdb", "target", List("d", "k", "n")),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines("snowflake"),
          AllSinks().getSink(),
          JdbcAutoTask.strategyTemplateVariables(
            ConnectionInfo(
              ConnectionType.JDBC,
              options = Map("url" -> "jdbc:snowflake://acme.snowflakecomputing.com")
            ),
            dataBranch
          )
        )
        .replaceAll("\\s+", " ")

    "The Snowflake DELETE_THEN_INSERT template" should "read the incoming rows from a temporary table, for a key of several columns" in {
      val sql = render(None)
      sql should include(
        "CREATE OR REPLACE TEMPORARY TABLE SL_INCOMING AS SELECT d, k, n FROM selfdb.src;"
      )
      sql should include("USING (SELECT DISTINCT")
      sql should include("FROM SL_INCOMING) AS SL_INCOMING")
      sql should include("DROP TABLE IF EXISTS SL_INCOMING;")
      sql should not include "SELECT d, k, n FROM selfdb.src) AS SL_INCOMING"
    }

    it should "run the SELECT in each statement on a data branch" in {
      val sql = render(Some("feature"))
      sql should not include "TEMPORARY TABLE"
      sql should include("FROM (SELECT d, k, n FROM selfdb.src)) AS SL_INCOMING")
    }
  }
}

/** Redshift. Runs when SL_REDSHIFT_TEST_URL names a Redshift JDBC url, with SL_REDSHIFT_TEST_USER
  * and SL_REDSHIFT_TEST_PASSWORD: every case drops and recreates the tables of the selfdb schema in
  * that database.
  */
class RedshiftSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("Redshift") {
  override protected def enabled: Boolean = sys.env.contains("SL_REDSHIFT_TEST_URL")
  override protected def engineConfiguration: Config = {
    val options = "connections.test-redshift.options"
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-redshift"
           |connections.test-redshift {
           |  type = "jdbc"
           |  options {
           |    "driver": "com.amazon.redshift.jdbc42.Driver"
           |    "quoteIdentifiers": false
           |  }
           |}
           |""".stripMargin
      )
      .withValue(s"$options.url", env("SL_REDSHIFT_TEST_URL"))
      .withValue(s"$options.user", env("SL_REDSHIFT_TEST_USER"))
      .withValue(s"$options.password", env("SL_REDSHIFT_TEST_PASSWORD"))
      .withFallback(super.testConfiguration)
  }

  private def env(name: String) = ConfigValueFactory.fromAnyRef(sys.env.getOrElse(name, ""))
}

/** MySQL, in its TestContainers container. The test user owns the starlake database only, which is
  * the schema of the tables.
  */
class MySqlSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("MySQL") {
  private lazy val mysql: MySQLContainer = {
    val container = MySQLContainer
      .Def(
        DockerImageName.parse("mysql:8.4"),
        databaseName = "starlake",
        username = "test",
        password = "test"
      )
      .createContainer()
    container.start()
    container
  }

  override def afterAll(): Unit = {
    super.afterAll()
    mysql.stop()
  }

  override protected def domain: String = "starlake"
  override protected def createSchema: List[String] = Nil
  override protected def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-mysql"
           |connections.test-mysql {
           |  type = "jdbc"
           |  options {
           |    "url": "${mysql.jdbcUrl}"
           |    "user": "test"
           |    "password": "test"
           |    "driver": "com.mysql.cj.jdbc.Driver"
           |    "quoteIdentifiers": false
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)
}

/** Trino, in its Docker image, on an Iceberg catalog kept in the container: Trino has no temporary
  * table, and its memory connector deletes no rows. No engine named trino ships in
  * reference-engines.conf: the test declares one, the PostgreSQL engine with the trino templates.
  * Trino writes to Iceberg only in autocommit, so the scenario runs the SQL of the write strategy,
  * not the task.
  */
class TrinoSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("Trino") {
  private lazy val trino: GenericContainer = {
    val container = GenericContainer(
      dockerImage = "trinodb/trino:484",
      exposedPorts = Seq(8080),
      waitStrategy = Wait
        .forLogMessage("(?s).*SERVER STARTED.*", 1)
        .withStartupTimeout(java.time.Duration.ofMinutes(4))
    )
    container.container.withClasspathResourceMapping(
      "trino/iceberg.properties",
      "/etc/trino/catalog/iceberg.properties",
      BindMode.READ_ONLY
    )
    container.start()
    container
  }

  override def afterAll(): Unit = {
    super.afterAll()
    trino.stop()
  }

  override protected def stringType: String = "VARCHAR"
  // the Iceberg catalog writes only in autocommit, and a task writes in a transaction
  override protected def runsStrategySqlOnly: Boolean = true
  override protected def engineConfiguration: Config = {
    val base = ConfigFactory.load().withFallback(super.testConfiguration)
    val trinoEngine = ConfigFactory
      .parseString("strategyBuilder = \"trino\"")
      .withFallback(base.getConfig("jdbcEngines.postgresql"))
      .atPath("jdbcEngines.trino")
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-trino"
           |connections.test-trino {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:trino://${trino.host}:${trino.mappedPort(8080)}/iceberg"
           |    "user": "test"
           |    "driver": "io.trino.jdbc.TrinoDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(trinoEngine)
      .withFallback(super.testConfiguration)
  }
}
