package ai.starlake.schema.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The GRANT statements of an acl on each engine (#1985) */
class AccessControlEntrySpec extends AnyFlatSpec with Matchers {

  private val table = "dom.tbl"

  private def statements(engine: String, grants: String*): Set[String] =
    AccessControlEntry("SELECT", grants.toSet).asSql(table, Engine.fromString(engine))

  "The acl of a PostgreSQL table" should "grant to roles, GROUP being a noise word there" in {
    statements(
      "postgresql",
      "user:alice",
      "group:analysts",
      "role:readers",
      "user:me@me.com"
    ) shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO alice",
      "GRANT SELECT ON TABLE dom.tbl TO GROUP analysts",
      "GRANT SELECT ON TABLE dom.tbl TO readers",
      "GRANT SELECT ON TABLE dom.tbl TO \"me@me.com\""
    )
  }

  "The acl of a Redshift table" should "grant to users, groups and roles" in {
    statements("redshift", "user:alice", "group:analysts", "role:readers") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO alice",
      "GRANT SELECT ON TABLE dom.tbl TO GROUP analysts",
      "GRANT SELECT ON TABLE dom.tbl TO ROLE readers"
    )
  }

  "The acl of a table on another JDBC engine" should "keep the Hive syntax, with the whole role name" in {
    statements("h2", "user:alice", "group:analysts", "role:readers") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO USER alice",
      "GRANT SELECT ON TABLE dom.tbl TO ROLE analysts",
      "GRANT SELECT ON TABLE dom.tbl TO ROLE readers"
    )
  }

  "The acl of a Spark table outside Databricks" should "keep the Hive syntax, with the whole role name" in {
    assume(!sys.env.contains("DATABRICKS_RUNTIME_VERSION"))
    statements("spark", "role:readers") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO ROLE readers"
    )
  }

  "The acl of a DuckDB table" should "give no statement: DuckDB has no GRANT" in {
    statements("duckdb", "user:alice", "serviceAccount:reader@p.iam.gserviceaccount.com") shouldBe
    empty
    AccessControlEntry.supportsGrants(Engine.fromString("duckdb")) shouldBe false
    AccessControlEntry.supportsGrants(Engine.fromString("postgresql")) shouldBe true
  }

  "A grantee without user:, group: or role:" should "be refused by the engines that grant to typed principals" in {
    List("postgresql", "redshift", "h2", "spark").foreach { engine =>
      val error = intercept[IllegalArgumentException] {
        statements(engine, "serviceAccount:reader@p.iam.gserviceaccount.com")
      }
      error.getMessage should include("dom.tbl")
      error.getMessage should include("'serviceAccount:reader@p.iam.gserviceaccount.com'")
      error.getMessage should include(s"which $engine cannot express")
    }
    an[IllegalArgumentException] should be thrownBy statements("postgresql", "analysts")
    an[IllegalArgumentException] should be thrownBy statements("postgresql", "user:")
    an[IllegalArgumentException] should be thrownBy statements("postgresql", "user:  ")
  }

  it should "still be granted as written on BigQuery and Snowflake" in {
    statements("bigquery", "serviceAccount:reader@p.iam.gserviceaccount.com") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO \"serviceAccount:reader@p.iam.gserviceaccount.com\""
    )
    statements("snowflake", "analysts") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO DATABASE ROLE analysts"
    )
  }

  "The name of a grantee" should "be granted without the spaces around it" in {
    statements("postgresql", "user: alice ", "group: analysts") shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO alice",
      "GRANT SELECT ON TABLE dom.tbl TO GROUP analysts"
    )
  }

  "The acl of a Spark table that runs on Databricks" should "take the Databricks syntax, without the type of a grantee" in {
    AccessControlEntry(
      "SELECT",
      Set(
        "analysts",
        " me@me.com ",
        "user:alice@x.com",
        "group:readers",
        "group:data-engineers",
        "`quoted team`"
      )
    ).asSql(table, Engine.SPARK, databricks = true) shouldBe Set(
      "GRANT SELECT ON TABLE dom.tbl TO analysts",
      "GRANT SELECT ON TABLE dom.tbl TO `me@me.com`",
      "GRANT SELECT ON TABLE dom.tbl TO `alice@x.com`",
      "GRANT SELECT ON TABLE dom.tbl TO readers",
      "GRANT SELECT ON TABLE dom.tbl TO `data-engineers`",
      "GRANT SELECT ON TABLE dom.tbl TO `quoted team`"
    )
    val error = intercept[IllegalArgumentException] {
      AccessControlEntry("SELECT", Set("user: ")).asSql(table, Engine.SPARK, databricks = true)
    }
    error.getMessage should include("names no principal")
  }

  "A connection through the JDBC driver of Databricks" should "run its statements on Databricks" in {
    assume(!sys.env.contains("DATABRICKS_RUNTIME_VERSION"))
    def connection(url: String) =
      ai.starlake.config.ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> url))
    AccessControlEntry.runsOnDatabricks(
      connection("jdbc:databricks://example.cloud.databricks.com:443/default")
    ) shouldBe true
    AccessControlEntry.runsOnDatabricks(connection("jdbc:postgresql://localhost/db")) shouldBe
    false
  }
}
