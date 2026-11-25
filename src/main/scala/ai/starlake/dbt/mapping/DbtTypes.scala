package ai.starlake.dbt.mapping

import ai.starlake.sql.SQLTypeMappings

/** A column `data_type` or seed `column_types` value as a Starlake type, through Starlake's SQL
  * type mapping (SQLTypeMappings: warehouse type -> Spark type), then Spark type -> the built-in
  * types of types/default.sl.yml
  */
object DbtTypes {
  private val sparkToStarlake = Map(
    "string"        -> "string",
    "varchar"       -> "string",
    "integer"       -> "integer",
    "int"           -> "int",
    "long"          -> "long",
    "bigint"        -> "long",
    "short"         -> "short",
    "smallint"      -> "short",
    "byte"          -> "byte",
    "tinyint"       -> "byte",
    "double"        -> "double",
    "float"         -> "double",
    "boolean"       -> "boolean",
    "date"          -> "date",
    "timestamp"     -> "timestamp",
    "timestamp_ntz" -> "timestamp",
    "variant"       -> "variant",
    "decimal"       -> "decimal"
  )

  def toStarlake(dataType: String, adapterType: String): Option[String] = {
    val base = dataType.trim.takeWhile(_ != '(').trim.toLowerCase
    val dialect = if (adapterType == "databricks") "spark" else adapterType.toLowerCase
    SQLTypeMappings
      .getSparkType(base, dialect)
      .map(_.toLowerCase.takeWhile(_ != '('))
      .flatMap(sparkToStarlake.get)
  }
}
