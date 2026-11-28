package ai.starlake.job.transform

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

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

class MySqlSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("MySQL") with MySqlEngine

class TrinoSelfReadingStrategiesSpec extends SelfReadingStrategiesSpecBase("Trino") with TrinoEngine
