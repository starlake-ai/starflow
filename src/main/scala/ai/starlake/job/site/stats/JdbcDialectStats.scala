package ai.starlake.job.site.stats

import ai.starlake.config.ConnectionInfo

trait JdbcDialectStats {
  def engine: String
  def rowCountEstimated: Boolean
  def defaultDatabase(conn: ConnectionInfo): Option[String] = None
  def tablesSql(database: Option[String], schema: String): String
  def statsSql(database: Option[String], schema: String): Option[String]
  def columnsSql(database: Option[String], schema: String): String

  /** Whether a backslash is an escape character inside single-quoted literals. */
  def backslashEscapes: Boolean = false

  def lit(s: String): String = JdbcDialectStats.lit(s, backslashEscapes)
}

object JdbcDialectStats {

  def lit(s: String): String = lit(s, backslashEscapes = false)

  def lit(s: String, backslashEscapes: Boolean): String = {
    val escaped = if (backslashEscapes) s.replace("\\", "\\\\") else s
    "'" + escaped.replace("'", "''") + "'"
  }

  private def schemaFilter(d: JdbcDialectStats, column: String, schema: String) =
    s"lower($column) = lower(${d.lit(schema)})"

  private def infoTables(
    d: JdbcDialectStats,
    prefix: String,
    schema: String,
    extra: String = ""
  ) =
    s"SELECT table_name, table_type FROM ${prefix}information_schema.tables " +
    s"WHERE ${schemaFilter(d, "table_schema", schema)}$extra"

  private def infoColumns(
    d: JdbcDialectStats,
    prefix: String,
    schema: String,
    extra: String = ""
  ) =
    "SELECT table_name, column_name, data_type, is_nullable, character_maximum_length, " +
    s"numeric_precision, numeric_scale FROM ${prefix}information_schema.columns " +
    s"WHERE ${schemaFilter(d, "table_schema", schema)}$extra ORDER BY table_name, ordinal_position"

  object Postgres extends JdbcDialectStats {
    val engine = "postgresql"
    val rowCountEstimated = true
    def tablesSql(database: Option[String], schema: String): String =
      infoTables(this, "", schema) +
      " UNION ALL SELECT matviewname AS table_name, 'MATERIALIZED VIEW' AS table_type " +
      s"FROM pg_matviews WHERE ${schemaFilter(this, "schemaname", schema)}"
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT c.relname AS table_name, c.reltuples::bigint AS row_count, " +
      "CASE WHEN c.relkind IN ('p', 'f') THEN NULL ELSE pg_total_relation_size(c.oid) END AS bytes " +
      "FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      s"WHERE ${schemaFilter(this, "n.nspname", schema)} AND c.relkind IN ('r', 'p', 'm', 'f')"
    )
    // information_schema.columns does not list materialized views: read them from pg_attribute,
    // with format_type giving the full spelling (numeric(18,2), character varying(20)).
    def columnsSql(database: Option[String], schema: String): String =
      "SELECT table_name, column_name, data_type, is_nullable, character_maximum_length, " +
      "numeric_precision, numeric_scale FROM (" +
      "SELECT table_name::text AS table_name, column_name::text AS column_name, " +
      "data_type::text AS data_type, is_nullable::text AS is_nullable, " +
      "character_maximum_length::int AS character_maximum_length, " +
      "numeric_precision::int AS numeric_precision, numeric_scale::int AS numeric_scale, " +
      "ordinal_position::int AS ordinal_position FROM information_schema.columns " +
      s"WHERE ${schemaFilter(this, "table_schema", schema)} " +
      "UNION ALL SELECT c.relname::text, a.attname::text, " +
      "format_type(a.atttypid, a.atttypmod), " +
      "CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END, NULL::int, NULL::int, NULL::int, " +
      "a.attnum::int FROM pg_attribute a " +
      "JOIN pg_class c ON c.oid = a.attrelid " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      s"WHERE ${schemaFilter(this, "n.nspname", schema)} AND c.relkind = 'm' " +
      "AND a.attnum > 0 AND NOT a.attisdropped" +
      ") cols ORDER BY table_name, ordinal_position"
  }

  object Redshift extends JdbcDialectStats {
    override def backslashEscapes = true
    val engine = "redshift"
    val rowCountEstimated = true // tbl_rows includes rows marked for deletion
    def tablesSql(database: Option[String], schema: String): String =
      infoTables(this, "", schema)
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT \"table\" AS table_name, tbl_rows AS row_count, " +
      "CAST(size AS BIGINT) * 1048576 AS bytes FROM svv_table_info " +
      s"WHERE ${schemaFilter(this, "\"schema\"", schema)}"
    )
    def columnsSql(database: Option[String], schema: String): String = infoColumns(this, "", schema)
  }

  object DuckDb extends JdbcDialectStats {
    val engine = "duckdb"
    val rowCountEstimated = true
    private def catalog(column: String, database: Option[String]) = database match {
      case Some(db) => s" AND lower($column) = lower(${this.lit(db)})"
      case None     => s" AND $column = current_database()"
    }
    def tablesSql(database: Option[String], schema: String): String =
      infoTables(this, "", schema, catalog("table_catalog", database))
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT table_name, estimated_size AS row_count FROM duckdb_tables() " +
      s"WHERE ${schemaFilter(this, "schema_name", schema)}${catalog("database_name", database)}"
    )
    def columnsSql(database: Option[String], schema: String): String =
      infoColumns(this, "", schema, catalog("table_catalog", database))
  }

  object Snowflake extends JdbcDialectStats {
    override def backslashEscapes = true
    val engine = "snowflake"
    val rowCountEstimated = false
    override def defaultDatabase(conn: ConnectionInfo): Option[String] = conn.getDatabaseName()
    private def prefix(database: Option[String]) = database match {
      case Some(db) if db.matches("[A-Za-z_][A-Za-z0-9_$]*") => s"$db."
      case Some(db) => "\"" + db.replace("\"", "\"\"") + "\"."
      case None     => ""
    }
    def tablesSql(database: Option[String], schema: String): String =
      "SELECT table_name, table_type, row_count, bytes, last_altered AS last_modified " +
      s"FROM ${prefix(database)}information_schema.tables WHERE ${schemaFilter(this, "table_schema", schema)}"
    def statsSql(database: Option[String], schema: String): Option[String] = None
    def columnsSql(database: Option[String], schema: String): String =
      infoColumns(this, prefix(database), schema)
  }

  def forEngine(engine: String): Option[JdbcDialectStats] = engine.toLowerCase match {
    case "postgresql" => Some(Postgres)
    case "redshift"   => Some(Redshift)
    case "duckdb"     => Some(DuckDb)
    case "snowflake"  => Some(Snowflake)
    case _            => None
  }
}
