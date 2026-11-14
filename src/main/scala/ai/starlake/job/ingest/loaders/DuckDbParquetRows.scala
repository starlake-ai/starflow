package ai.starlake.job.ingest.loaders

import ai.starlake.schema.model.{PrimitiveType, TableAttribute, TransformInput, Type}

/** The SQL of the first step of a native DuckDB load of a Parquet file (#1952). As Spark, it reads
  * the columns of the attributes by name, and rejects the rows whose value does not convert to the
  * type of its column, or with no value for a required attribute. With `emptyIsNull`, an empty
  * string of the file is no value. A binary attribute held as text in the file is decoded from the
  * encoding of its type, as Spark decodes it.
  *
  * @param columns
  *   the attributes of the first step that are not scripted, ignored ones included as Spark checks
  *   them too
  * @param withValue
  *   true to show the values of the rejected rows, as `rejectWithValue` asks
  */
case class DuckDbParquetRows(
  columns: List[DuckDbParquetRows.Column],
  emptyIsNull: Boolean,
  withValue: Boolean
) {
  import DuckDbParquetRows._

  private val present: List[Column] = columns.filter(_.fileType.nonEmpty)

  /** The checks of the values of a row of the file, true when they are valid and never NULL */
  val checks: List[Check] = columns.flatMap { column =>
    val name = column.attribute.name
    val required =
      if (column.attribute.resolveRequired())
        List(Check(s"${input(column)} IS NOT NULL", literal(s"`$name`  can't be null")))
      else Nil
    val converted =
      if (column.fileType.isEmpty || column.isIdentity) Nil
      else {
        val error = literal(
          s"`$name` cannot be converted to `${column.tpe.primitiveType.value}` with sl type `${column.tpe.name}`"
        )
        List(
          Check(
            s"(${input(column)} IS NULL OR ${value(column)} IS NOT NULL)",
            if (withValue && !column.hidden)
              s"$error || ': ' || ${raw(column)}::VARCHAR"
            else error
          )
        )
      }
    required ++ converted
  }

  /** The condition of the rows to load */
  val condition: String =
    if (checks.isEmpty) "TRUE" else checks.map(_.condition).mkString(" AND ")

  /** @return
    *   the statement inserting the rows of the file to load into the table of the first step, whose
    *   columns have the source names of the attributes
    */
  def insertSql(table: String, file: String): String =
    s"""INSERT INTO $table (${present.map(column => quoted(column.attribute.name)).mkString(", ")})
       | SELECT ${present.map(value).mkString(", ")}
       | FROM read_parquet($file)
       | WHERE $condition""".stripMargin

  /** @return
    *   the query of the number of rejected rows of the file
    */
  def rejectedCountSql(file: String): String =
    s"SELECT count(*) AS rejected FROM read_parquet($file) WHERE NOT ($condition)"

  /** @return
    *   the query of the first `limit` rejected rows of the file: their error lines and, with
    *   `withValue`, the row as JSON, without the values of the ignored attributes and of those with
    *   a privacy
    */
  def rejectedRowsSql(file: String, limit: Int): String = {
    val errors = checks
      .map(check => s"CASE WHEN NOT (${check.condition}) THEN ${check.error} END")
      .mkString("concat_ws(chr(10), ", ", ", ")")
    val visible = present.filterNot(_.hidden)
    val row =
      if (!withValue) ""
      else if (visible.isEmpty) ", '{}' AS row_json"
      else
        visible
          .map(column => s"${quoted(column.attribute.name)} := ${raw(column)}")
          .mkString(", to_json(struct_pack(", ", ", ")) AS row_json")
    s"SELECT $errors AS errors$row FROM read_parquet($file) WHERE NOT ($condition) LIMIT $limit"
  }

  /** @return
    *   the query of the rejected rows of the file as it holds them, for a replay file that loads
    *   them again
    */
  def replaySql(file: String): String =
    s"SELECT ${present.map(column => raw(column)).mkString(", ")} FROM read_parquet($file) WHERE NOT ($condition)"

  /** The value of the file, NULL when it does not hold the column */
  private def raw(column: Column): String =
    if (column.fileType.isEmpty) "NULL" else quoted(column.attribute.name)

  /** The value of the file, as Spark reads it: an empty string is no value with `emptyIsNull` */
  private def input(column: Column): String =
    if (emptyIsNull && column.isText) s"NULLIF(${raw(column)}, '')" else raw(column)

  /** The value converted to the type of its column, NULL when it does not convert */
  private def value(column: Column): String = {
    val decoded = column.encoding match {
      // As Spark: whitespace is not part of the bytes, and the padding is not required
      case Some("base64") =>
        val text = s"regexp_replace(${input(column)}, '\\s', '', 'g')"
        s"TRY(from_base64(rpad($text, CAST((length($text) + 3) // 4 * 4 AS INTEGER), '=')))"
      // As Spark: an even number of hexadecimal digits, between spaces
      case Some("hex") =>
        val text = s"trim(${input(column)})"
        s"CASE WHEN length($text) % 2 = 0 THEN TRY(unhex($text)) END"
      case _ => input(column)
    }
    s"TRY_CAST($decoded AS ${column.ddlType})"
  }
}

object DuckDbParquetRows {

  /** @param tpe
    *   the type of the attribute
    * @param ddlType
    *   the DuckDB type of its column
    * @param fileType
    *   the DuckDB type of the column of the file that holds it, if any
    */
  case class Column(
    attribute: TableAttribute,
    tpe: Type,
    ddlType: String,
    fileType: Option[String]
  ) {
    val isText: Boolean = fileType.exists(_.equalsIgnoreCase("VARCHAR"))

    /** The encoding of a binary attribute held as text, which Spark decodes */
    val encoding: Option[String] =
      if (isText && tpe.primitiveType == PrimitiveType.binary)
        Some(tpe.pattern.toLowerCase(java.util.Locale.ROOT))
          .filter(PrimitiveType.binary.encodings.contains)
      else None

    /** true when the value of the file needs no conversion */
    val isIdentity: Boolean = encoding.isEmpty && fileType.exists(_.equalsIgnoreCase(ddlType))

    /** true when a rejected row must not show the value: an ignored attribute, or one with a
      * privacy, whose value the first step holds in clear
      */
    val hidden: Boolean =
      attribute.resolveIgnore() || attribute.resolvePrivacy() != TransformInput.None
  }

  /** A check of the values of a row
    *
    * @param condition
    *   its SQL condition, true for a valid row and never NULL
    * @param error
    *   the SQL of the error line of a row that it rejects
    */
  case class Check(condition: String, error: String)

  def quoted(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  private def literal(text: String): String = "'" + text.replace("'", "''") + "'"
}
