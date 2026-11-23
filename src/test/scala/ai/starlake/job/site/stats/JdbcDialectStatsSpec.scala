package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JdbcDialectStatsSpec extends AnyFlatSpec with Matchers {
  import JdbcDialectStats._

  "lit" should "escape single quotes" in {
    lit("o'neil") shouldBe "'o''neil'"
  }

  "forEngine" should "pick the dialect from the engine string" in {
    forEngine("postgresql") shouldBe Some(Postgres)
    forEngine("redshift") shouldBe Some(Redshift)
    forEngine("duckdb") shouldBe Some(DuckDb)
    forEngine("snowflake") shouldBe Some(Snowflake)
    forEngine("mysql") shouldBe None
  }

  "Postgres" should "read pg_class estimates and never count rows" in {
    Postgres.rowCountEstimated shouldBe true
    Postgres.tablesSql(None, "sales") should include("information_schema.tables")
    Postgres.tablesSql(None, "sales") should include("lower(table_schema) = lower('sales')")
    val stats = Postgres.statsSql(None, "sales").get
    stats should include("reltuples")
    stats should include("pg_total_relation_size")
    stats.toLowerCase should not include "count("
    Postgres.columnsSql(None, "sales") should include("information_schema.columns")
  }

  "Redshift" should "read svv_table_info" in {
    val stats = Redshift.statsSql(None, "sales").get
    stats should include("svv_table_info")
    stats should include("tbl_rows")
    stats should include("1048576")
  }

  it should "list Spectrum tables, materialized views and late-binding view columns" in {
    val tables = Redshift.tablesSql(None, "sales")
    tables shouldBe
    "SELECT table_name, table_type FROM svv_tables WHERE lower(table_schema) = lower('sales')"
    val columns = Redshift.columnsSql(None, "sales")
    columns should include("FROM svv_columns WHERE lower(table_schema) = lower('sales')")
    columns should include("ORDER BY table_name, ordinal_position")
  }

  it should "map Spectrum and materialized view rows" in {
    val merged = InfoSchemaMapping.merge(
      List(
        Map("table_name" -> "ext", "table_type" -> "EXTERNAL TABLE"),
        Map("table_name" -> "mv", "table_type"  -> "VIEW"),
        Map("table_name" -> "mv", "table_type"  -> "MATERIALIZED VIEW")
      ),
      Nil,
      List(Map("table_name" -> "mv", "column_name" -> "id", "data_type" -> "integer")),
      estimated = true
    )
    merged.keySet shouldBe Set("ext", "mv")
    merged("ext").tableType shouldBe Some("EXTERNAL")
    merged("mv").tableType shouldBe Some("VIEW")
    merged("mv").columns.map(_.name) shouldBe List("id")
  }

  "DuckDb" should "read duckdb_tables() in the current database by default" in {
    val stats = DuckDb.statsSql(None, "sales").get
    stats should include("duckdb_tables()")
    stats should include("estimated_size")
    stats should include("database_name = current_database()")
    DuckDb.statsSql(Some("other"), "sales").get should include(
      "lower(database_name) = lower('other')"
    )
    DuckDb.tablesSql(None, "sales") should include("table_catalog = current_database()")
  }

  "Snowflake" should "qualify information_schema with the database" in {
    Snowflake.rowCountEstimated shouldBe false
    val tables = Snowflake.tablesSql(Some("ANALYTICS"), "sales")
    tables should include("ANALYTICS.information_schema.tables")
    tables should include("row_count")
    tables should include("last_altered AS last_modified")
    Snowflake.statsSql(Some("ANALYTICS"), "sales") shouldBe None
    Snowflake.tablesSql(Some("my db"), "s") should include("\"my db\".information_schema.tables")
    Snowflake.columnsSql(None, "s") should include(" information_schema.columns")
  }

  "Name safety" should "quote a schema or database containing a single quote" in {
    for (d <- List(Postgres, Redshift, DuckDb, Snowflake)) {
      d.tablesSql(None, "o'neil") should include("lower('o''neil')")
      d.columnsSql(None, "o'neil") should include("lower('o''neil')")
      d.statsSql(None, "o'neil").foreach(_ should include("lower('o''neil')"))
    }
    DuckDb.statsSql(Some("d'b"), "s").get should include("lower('d''b')")
    Snowflake.tablesSql(Some("a\"b"), "s") should include("\"a\"\"b\".information_schema")
  }

  "Literal quoting" should "escape backslashes on Snowflake and Redshift only" in {
    val name = "x\\' OR 1=1 --"
    Snowflake.tablesSql(None, name) should include("lower('x\\\\'' OR 1=1 --')")
    Snowflake.columnsSql(None, name) should include("lower('x\\\\'' OR 1=1 --')")
    Redshift.tablesSql(None, name) should include("lower('x\\\\'' OR 1=1 --')")
    Redshift.statsSql(None, name).get should include("lower('x\\\\'' OR 1=1 --')")
    Postgres.tablesSql(None, name) should include("lower('x\\'' OR 1=1 --')")
    Postgres.statsSql(None, name).get should include("lower('x\\'' OR 1=1 --')")
    DuckDb.tablesSql(None, name) should include("lower('x\\'' OR 1=1 --')")
    DuckDb.statsSql(Some(name), "s").get should include("lower('x\\'' OR 1=1 --')")
    Snowflake.lit("a\\b") shouldBe "'a\\\\b'"
    Postgres.lit("a\\b") shouldBe "'a\\b'"
  }

  "Postgres matviews and sizes" should "list materialized views and null the size of partitioned parents and foreign tables" in {
    val tables = Postgres.tablesSql(None, "sales")
    tables should include("pg_matviews")
    tables should include("'MATERIALIZED VIEW'")
    tables should include("UNION ALL")
    tables should include("lower(schemaname) = lower('sales')")
    val stats = Postgres.statsSql(None, "sales").get
    stats should include(
      "CASE WHEN c.relkind IN ('p', 'f') THEN NULL ELSE pg_total_relation_size(c.oid) END AS bytes"
    )
  }

  "Postgres columns" should "read materialized view columns from pg_attribute" in {
    val cols = Postgres.columnsSql(None, "sales")
    cols should include("information_schema.columns")
    cols should include("pg_attribute")
    cols should include("c.relkind = 'm'")
    cols should include("format_type(a.atttypid, a.atttypmod)")
    cols should include("lower(n.nspname) = lower('sales')")
    cols should include("ORDER BY table_name, ordinal_position")
  }
}
