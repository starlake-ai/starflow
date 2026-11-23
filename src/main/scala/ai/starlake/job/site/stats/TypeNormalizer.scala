package ai.starlake.job.site.stats

case class NormalizedType(base: String, params: List[String])

object TypeNormalizer {

  private val Shape = """^([A-Z0-9_ ]+?)\s*(?:\(([^)]*)\))?$""".r

  private val common: Map[String, String] = Map(
    "INT"                         -> "INTEGER",
    "INT4"                        -> "INTEGER",
    "INTEGER"                     -> "INTEGER",
    "BIGINT"                      -> "BIGINT",
    "INT8"                        -> "BIGINT",
    "LONG"                        -> "BIGINT",
    "SMALLINT"                    -> "SMALLINT",
    "INT2"                        -> "SMALLINT",
    "SHORT"                       -> "SMALLINT",
    "TINYINT"                     -> "TINYINT",
    "INT1"                        -> "TINYINT",
    "BYTE"                        -> "TINYINT",
    "DOUBLE"                      -> "DOUBLE",
    "DOUBLE PRECISION"            -> "DOUBLE",
    "FLOAT8"                      -> "DOUBLE",
    "FLOAT"                       -> "DOUBLE",
    "REAL"                        -> "REAL",
    "FLOAT4"                      -> "REAL",
    "BOOL"                        -> "BOOLEAN",
    "BOOLEAN"                     -> "BOOLEAN",
    "VARCHAR"                     -> "VARCHAR",
    "CHARACTER VARYING"           -> "VARCHAR",
    "CHAR VARYING"                -> "VARCHAR",
    "NVARCHAR"                    -> "VARCHAR",
    "STRING"                      -> "VARCHAR",
    "TEXT"                        -> "VARCHAR",
    "CHAR"                        -> "CHAR",
    "CHARACTER"                   -> "CHAR",
    "BPCHAR"                      -> "CHAR",
    "NCHAR"                       -> "CHAR",
    "DECIMAL"                     -> "DECIMAL",
    "NUMERIC"                     -> "DECIMAL",
    "NUMBER"                      -> "DECIMAL",
    "DEC"                         -> "DECIMAL",
    "DATE"                        -> "DATE",
    "TIMESTAMP"                   -> "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE" -> "TIMESTAMP",
    "TIMESTAMP_NTZ"               -> "TIMESTAMP",
    "DATETIME"                    -> "TIMESTAMP",
    "TIMESTAMPTZ"                 -> "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE"    -> "TIMESTAMPTZ",
    "TIMESTAMP_TZ"                -> "TIMESTAMPTZ",
    "TIMESTAMP_LTZ"               -> "TIMESTAMPTZ",
    "TIME"                        -> "TIME",
    "TIME WITHOUT TIME ZONE"      -> "TIME",
    "BINARY"                      -> "BINARY",
    "VARBINARY"                   -> "BINARY",
    "BYTEA"                       -> "BINARY",
    "BYTES"                       -> "BINARY",
    "BLOB"                        -> "BINARY"
  )

  private def t(base: String, params: String*): NormalizedType = NormalizedType(base, params.toList)

  /** Engine-specific overrides, applied before the common aliases. */
  private val perEngine: Map[String, Map[String, NormalizedType]] = Map(
    "bigquery" -> Map(
      "INT64"      -> t("BIGINT"),
      "INTEGER"    -> t("BIGINT"),
      "INT"        -> t("BIGINT"),
      "SMALLINT"   -> t("BIGINT"),
      "TINYINT"    -> t("BIGINT"),
      "BYTEINT"    -> t("BIGINT"),
      "BIGINT"     -> t("BIGINT"),
      "FLOAT64"    -> t("DOUBLE"),
      "BIGNUMERIC" -> t("BIGDECIMAL"),
      "BIGDECIMAL" -> t("BIGDECIMAL"),
      "TIMESTAMP"  -> t("TIMESTAMPTZ"),
      "DATETIME"   -> t("TIMESTAMP")
    ),
    "snowflake" -> Map(
      "INT"              -> t("DECIMAL", "38", "0"),
      "INTEGER"          -> t("DECIMAL", "38", "0"),
      "BIGINT"           -> t("DECIMAL", "38", "0"),
      "SMALLINT"         -> t("DECIMAL", "38", "0"),
      "TINYINT"          -> t("DECIMAL", "38", "0"),
      "BYTEINT"          -> t("DECIMAL", "38", "0"),
      "FLOAT"            -> t("DOUBLE"),
      "FLOAT4"           -> t("DOUBLE"),
      "FLOAT8"           -> t("DOUBLE"),
      "REAL"             -> t("DOUBLE"),
      "DOUBLE"           -> t("DOUBLE"),
      "DOUBLE PRECISION" -> t("DOUBLE")
    ),
    "duckdb" -> Map(
      "FLOAT"  -> t("REAL"),
      "FLOAT4" -> t("REAL")
    ),
    "spark" -> Map(
      "FLOAT"         -> t("REAL"),
      "TIMESTAMP"     -> t("TIMESTAMPTZ"),
      "TIMESTAMP_LTZ" -> t("TIMESTAMPTZ"),
      "TIMESTAMP_NTZ" -> t("TIMESTAMP")
    )
  )

  def normalize(engine: String, raw: String): NormalizedType = {
    val cleaned =
      raw.trim.toUpperCase.replaceAll("\\s+", " ").replaceAll(" NOT NULL$", "").trim
    cleaned match {
      case Shape(base0, params0) =>
        val base = base0.trim
        val params =
          Option(params0).toList.flatMap(_.split(',').map(_.trim).filter(_.nonEmpty))
        perEngine.getOrElse(engine.toLowerCase, Map.empty).get(base) match {
          case Some(mapped) => if (params.nonEmpty) mapped.copy(params = params) else mapped
          case None         => NormalizedType(common.getOrElse(base, base), params)
        }
      case _ => NormalizedType(cleaned, Nil) // ARRAY<...>, STRUCT<...>, MAP<...>
    }
  }

  /** True when both types have the same family and, if the declared type has parameters, the same
    * parameters.
    */
  def same(engine: String, declared: String, actual: String): Boolean = {
    val d = normalize(engine, declared)
    val a = normalize(engine, actual)
    d.base == a.base && (d.params.isEmpty || d.params == a.params)
  }
}
