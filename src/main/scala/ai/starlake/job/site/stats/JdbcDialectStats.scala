package ai.starlake.job.site.stats

import ai.starlake.config.ConnectionInfo

trait JdbcDialectStats {
  def engine: String
  def rowCountEstimated: Boolean
  def defaultDatabase(conn: ConnectionInfo): Option[String] = None
  def tablesSql(database: Option[String], schema: String): String
  def statsSql(database: Option[String], schema: String): Option[String]
  def columnsSql(database: Option[String], schema: String): String
}

object JdbcDialectStats {

  def lit(s: String): String = "'" + s.replace("'", "''") + "'"

  private def schemaFilter(column: String, schema: String) =
    s"lower($column) = lower(${lit(schema)})"

  private def infoTables(prefix: String, schema: String, extra: String = "") =
    s"SELECT table_name, table_type FROM ${prefix}information_schema.tables " +
    s"WHERE ${schemaFilter("table_schema", schema)}$extra"

  private def infoColumns(prefix: String, schema: String, extra: String = "") =
    "SELECT table_name, column_name, data_type, is_nullable, character_maximum_length, " +
    s"numeric_precision, numeric_scale FROM ${prefix}information_schema.columns " +
    s"WHERE ${schemaFilter("table_schema", schema)}$extra ORDER BY table_name, ordinal_position"

  object Postgres extends JdbcDialectStats {
    val engine = "postgresql"
    val rowCountEstimated = true
    def tablesSql(database: Option[String], schema: String): String = infoTables("", schema)
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT c.relname AS table_name, c.reltuples::bigint AS row_count, " +
      "pg_total_relation_size(c.oid) AS bytes FROM pg_class c " +
      "JOIN pg_namespace n ON n.oid = c.relnamespace " +
      s"WHERE ${schemaFilter("n.nspname", schema)} AND c.relkind IN ('r', 'p', 'm', 'f')"
    )
    def columnsSql(database: Option[String], schema: String): String = infoColumns("", schema)
  }

  object Redshift extends JdbcDialectStats {
    val engine = "redshift"
    val rowCountEstimated = true // tbl_rows includes rows marked for deletion
    def tablesSql(database: Option[String], schema: String): String = infoTables("", schema)
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT \"table\" AS table_name, tbl_rows AS row_count, " +
      "CAST(size AS BIGINT) * 1048576 AS bytes FROM svv_table_info " +
      s"WHERE ${schemaFilter("\"schema\"", schema)}"
    )
    def columnsSql(database: Option[String], schema: String): String = infoColumns("", schema)
  }

  object DuckDb extends JdbcDialectStats {
    val engine = "duckdb"
    val rowCountEstimated = true
    private def catalog(column: String, database: Option[String]) = database match {
      case Some(db) => s" AND lower($column) = lower(${lit(db)})"
      case None     => s" AND $column = current_database()"
    }
    def tablesSql(database: Option[String], schema: String): String =
      infoTables("", schema, catalog("table_catalog", database))
    def statsSql(database: Option[String], schema: String): Option[String] = Some(
      "SELECT table_name, estimated_size AS row_count FROM duckdb_tables() " +
      s"WHERE ${schemaFilter("schema_name", schema)}${catalog("database_name", database)}"
    )
    def columnsSql(database: Option[String], schema: String): String =
      infoColumns("", schema, catalog("table_catalog", database))
  }

  object Snowflake extends JdbcDialectStats {
    val engine = "snowflake"
    val rowCountEstimated = false
    override def defaultDatabase(conn: ConnectionInfo): Option[String] = conn.getDatabaseName()
    private def prefix(database: Option[String]) = database match {
      case Some(db) if db.matches("[A-Za-z_][A-Za-z0-9_$]*") => s"$db."
      case Some(db)                                       => "\"" + db.replace("\"", "\"\"") + "\"."
      case None                                           => ""
    }
    def tablesSql(database: Option[String], schema: String): String =
      "SELECT table_name, table_type, row_count, bytes, last_altered AS last_modified " +
      s"FROM ${prefix(database)}information_schema.tables WHERE ${schemaFilter("table_schema", schema)}"
    def statsSql(database: Option[String], schema: String): Option[String] = None
    def columnsSql(database: Option[String], schema: String): String =
      infoColumns(prefix(database), schema)
  }

  def forEngine(engine: String): Option[JdbcDialectStats] = engine.toLowerCase match {
    case "postgresql" => Some(Postgres)
    case "redshift"   => Some(Redshift)
    case "duckdb"     => Some(DuckDb)
    case "snowflake"  => Some(Snowflake)
    case _            => None
  }
}
