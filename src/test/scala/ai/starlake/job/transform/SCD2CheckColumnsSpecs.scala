package ai.starlake.job.transform

import com.typesafe.config.{Config, ConfigFactory}

/** DuckDB, in a file of the test folder */
class DuckDbSCD2CheckColumnsSpec extends SCD2CheckColumnsSpecBase("DuckDB") {
  override protected def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/scd2_check.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)
}

/** PostgreSQL, in its TestContainers container */
class PostgresSCD2CheckColumnsSpec extends SCD2CheckColumnsSpecBase("PostgreSQL") {
  override protected def engineConfiguration: Config =
    ConfigFactory.parseString("connectionRef: \"test-pg\"").withFallback(super.testConfiguration)
}

class MySqlSCD2CheckColumnsSpec extends SCD2CheckColumnsSpecBase("MySQL") with MySqlEngine

class TrinoSCD2CheckColumnsSpec extends SCD2CheckColumnsSpecBase("Trino") with TrinoEngine
