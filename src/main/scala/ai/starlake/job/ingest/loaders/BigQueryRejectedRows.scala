package ai.starlake.job.ingest.loaders

import ai.starlake.config.CometColumns
import ai.starlake.schema.model.{Format, Metadata, TableAttribute, TransformInput}
import ai.starlake.utils.conversion.{BigQueryNativeSchema, BigQueryType}
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.{Field, FieldValueList, Schema, StandardSQLTypeName}

import scala.jdk.CollectionConverters._

/** Finds the rows of the CSV and JSON files that a BigQuery load job rejected, which BigQuery only
  * counts (#1857).
  *
  * The files are loaded again into diagnostic tables: each CSV value, or each JSON line, as a
  * string. A query applies to them the rules of the load jobs for the types of the columns of the
  * load. The rows that it finds are the rejected rows when there are as many as the bad records of
  * the load job, less those that the diagnostic load rejected too: their errors are structural,
  * such as an unterminated quote, and they are only counted.
  */
object BigQueryRejectedRows {

  /** The column of the diagnostic table of a JSON file, with one of its lines */
  val lineColumn = "sl_line"

  /** @return
    *   true if the rejected rows of the load jobs of this format can be found: BigQuery reads the
    *   rows of the POSITION files as strings, and the rows of the Parquet files rejected by the
    *   native loader are in the tables of its first step
    */
  def supports(format: Format): Boolean =
    format == Format.DSV || format == Format.JSON || format == Format.JSON_FLAT

  /** @return
    *   the schema of the diagnostic load of a file: the columns of the load, as nullable strings,
    *   for a CSV file, and its lines for a JSON file
    */
  def diagnosticSchema(format: Format, loadSchema: Schema): Schema = {
    def string(name: String) =
      Field.newBuilder(name, StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build()
    if (format == Format.DSV) Schema.of(fieldsOf(loadSchema).map(f => string(f.getName)).asJava)
    else Schema.of(string(lineColumn))
  }

  /** @return
    *   the metadata of the diagnostic load of a file: the CSV options of the load for a CSV file. A
    *   JSON file is read as POSITION files are, one string per line, with neither separator nor
    *   quote, and in UTF-8, as JSON load jobs read it.
    */
  def diagnosticMetadata(metadata: Metadata): Metadata =
    if (metadata.resolveFormat() == Format.DSV) metadata
    else
      metadata.copy(
        format = Some(Format.POSITION),
        withHeader = Some(false),
        encoding = Some("UTF-8")
      )

  /** A check of the rows of a diagnostic table
    *
    * @param condition
    *   its SQL condition, true for a valid row and never NULL
    * @param error
    *   the error line of a row that it rejects
    */
  case class Check(condition: String, error: String)

  private def fieldsOf(schema: Schema): List[Field] =
    Option(schema.getFields).map(_.asScala.toList).getOrElse(Nil)

  private def required(path: String) = s"`$path`  can't be null"

  private def invalid(path: String, field: Field) =
    s"`$path` is not a valid ${Option(field.getType.getStandardType)
        .map(_ => BigQueryType.of(field).render)
        .getOrElse(field.getType.name())}"

  /** @param row
    *   the alias of a row of a diagnostic table of CSV files
    * @param emptyIsNull
    *   true when the CSV files have no null marker, or an empty one: an empty value, quoted or not,
    *   is then null in the columns of any type but STRING and BYTES, while the diagnostic table
    *   reads a quoted one as an empty string
    * @return
    *   the checks of its values, column by column: a required column has a value, and a value can
    *   be read into its column
    */
  def csvChecks(loadSchema: Schema, row: String, emptyIsNull: Boolean = true): List[Check] =
    fieldsOf(loadSchema).flatMap { field =>
      val text = Option(field.getType.getStandardType).exists(tpe =>
        tpe == StandardSQLTypeName.STRING || tpe == StandardSQLTypeName.BYTES
      )
      val column =
        if (emptyIsNull && !text) s"NULLIF($row.`${field.getName}`, '')"
        else s"$row.`${field.getName}`"
      val requiredCheck =
        if (field.getMode == Field.Mode.REQUIRED)
          List(Check(s"$column IS NOT NULL", required(field.getName)))
        else Nil
      val valueCheck = Option(field.getType.getStandardType)
        .flatMap(_ => BigQueryNativeSchema.loadableText(column, BigQueryType.of(field), csv = true))
        .map(condition =>
          Check(s"($column IS NULL OR ($condition))", invalid(field.getName, field))
        )
      requiredCheck ++ valueCheck
    }

  /** @param json
    *   the SQL of a line of a diagnostic table of JSON files, as a JSON value, NULL when the line
    *   is not valid JSON
    * @return
    *   the checks of the line: it is a JSON object, whose fields, at any level, have a value when
    *   they are required, an object when they are records, an array without null when they are
    *   repeated, and a value that can be read into their column. A missing field and a null are
    *   both no value.
    */
  def jsonChecks(loadSchema: Schema, json: String): List[Check] =
    List(
      Check(s"$json IS NOT NULL", "The line is not valid JSON"),
      Check(s"($json IS NULL OR JSON_TYPE($json) = 'object')", "The line is not a JSON object")
    ) ++ withinObject(json, fieldChecks(fieldsOf(loadSchema), json, "", 0))

  /** The checks of the fields of a JSON value apply only when it is an object */
  private def withinObject(json: String, checks: List[Check]): List[Check] =
    checks.map(check =>
      check.copy(condition = s"(IFNULL(JSON_TYPE($json), '') != 'object' OR ${check.condition})")
    )

  private def fieldChecks(
    fields: List[Field],
    parent: String,
    prefix: String,
    depth: Int
  ): List[Check] =
    fields.flatMap { field =>
      val path = prefix + field.getName
      val value = s"$parent[${BigQueryNativeLoader.bigQueryLiteral(field.getName)}]"
      val absent = s"IFNULL(JSON_TYPE($value), 'null') = 'null'"
      if (field.getMode == Field.Mode.REPEATED) {
        val element = s"sl_element_$depth"
        val elements = s"UNNEST(JSON_QUERY_ARRAY($value)) AS $element"
        List(
          Check(s"($absent OR JSON_TYPE($value) = 'array')", s"`$path` is not a JSON array"),
          Check(
            s"NOT EXISTS (SELECT 1 FROM $elements WHERE JSON_TYPE($element) = 'null')",
            s"`$path` holds a null element"
          )
        ) ++ valueChecks(field, element, path, depth + 1).map { check =>
          check.copy(condition =
            s"NOT EXISTS (SELECT 1 FROM $elements WHERE JSON_TYPE($element) != 'null' AND NOT (${check.condition}))"
          )
        }
      } else
        (if (field.getMode == Field.Mode.REQUIRED) List(Check(s"NOT ($absent)", required(path)))
         else Nil) ++ valueChecks(field, value, path, depth).map(check =>
          check.copy(condition = s"($absent OR ${check.condition})")
        )
    }

  /** The checks of a JSON value of a field, that is neither missing nor null */
  private def valueChecks(field: Field, value: String, path: String, depth: Int): List[Check] =
    Option(field.getSubFields).map(_.asScala.toList).getOrElse(Nil) match {
      case Nil =>
        Option(field.getType.getStandardType).toList.flatMap {
          // Any JSON value
          case StandardSQLTypeName.JSON => Nil
          // A string: BigQuery refuses a GeoJSON object ("JSON object specified for non-record
          // field")
          case StandardSQLTypeName.GEOGRAPHY =>
            List(
              Check(
                s"(JSON_TYPE($value) = 'string' AND SAFE.ST_GEOGFROM(JSON_VALUE($value)) IS NOT NULL)",
                invalid(path, field)
              )
            )
          case _ =>
            val scalar = s"JSON_TYPE($value) IN ('string', 'number', 'boolean')"
            val condition = BigQueryNativeSchema
              .loadableText(s"JSON_VALUE($value)", BigQueryType.of(field), csv = false)
              .map(condition => s"($scalar AND ($condition))")
              .getOrElse(scalar)
            List(Check(condition, invalid(path, field)))
        }
      case subFields =>
        Check(s"JSON_TYPE($value) = 'object'", s"`$path` is not a JSON object") ::
        withinObject(value, fieldChecks(subFields, value, path + ".", depth))
    }

  /** @return
    *   the SQL of the value of a line of a JSON file, as a JSON object, when the attributes have
    *   values that the rejected rows do not show: those of the ignored attributes and of the
    *   attributes with a privacy, or of the repeated record that holds them. The object is rebuilt
    *   from the fields that the attributes declare, in their case: BigQuery reads the keys of a
    *   line in any case, which JSON paths do not.
    */
  def jsonRowValue(attributes: List[TableAttribute], json: String): Option[String] = {
    def hidden(attribute: TableAttribute): Boolean =
      attribute.resolveIgnore() || attribute.resolvePrivacy() != TransformInput.None
    def holdsHidden(attribute: TableAttribute): Boolean =
      hidden(attribute) || attribute.attributes.exists(holdsHidden)
    def shown(attributes: List[TableAttribute], json: String): String =
      attributes
        .filter(_.script.isEmpty)
        .filterNot(attribute =>
          hidden(attribute) || (attribute.resolveArray() && holdsHidden(attribute))
        )
        .map { attribute =>
          val name = BigQueryNativeLoader.bigQueryLiteral(attribute.name)
          val value = s"$json[$name]"
          if (holdsHidden(attribute))
            s"$name, IF(JSON_TYPE($value) = 'object', ${shown(attribute.attributes, value)}, NULL)"
          else s"$name, $value"
        }
        .mkString("JSON_OBJECT(", ", ", ")")
    if (attributes.exists(holdsHidden)) Some(shown(attributes, json)) else None
  }

  /** @param tables
    *   the wildcard name of the diagnostic tables, whose suffix is the index of their file
    * @param jobs
    *   the index of the load job of each file, by index of file
    * @param withValue
    *   true to read the value of the rows, with `rejectWithValue`
    * @param attributes
    *   the attributes of the table, whose hidden values the value of a line of a JSON file does not
    *   show (see [[jsonRowValue]])
    * @param limitByJob
    *   the most rows to read by load job, if not all
    * @param emptyIsNull
    *   true when the CSV files have no null marker, or an empty one (see [[csvChecks]])
    * @return
    *   the query of the rejected rows of the diagnostic tables: their load job (`sl_job`), their
    *   file (`sl_file`), their error lines (`sl_errors`), their values as strings (`sl_raw`, a
    *   record of the columns of a CSV file, the line of a JSON file), and their value as JSON for
    *   JSON files (`sl_value`). With a limit, the rows hold the number of rejected rows of their
    *   load job (`sl_count`). The checks are written once, whatever the number of files, and no
    *   window function holds the rows of a load job.
    */
  def rejectedRowsSql(
    tables: String,
    jobs: List[Int],
    format: Format,
    loadSchema: Schema,
    withValue: Boolean,
    attributes: List[TableAttribute],
    limitByJob: Option[Int],
    emptyIsNull: Boolean = true
  ): String = {
    val file = CometColumns.cometInputFileNameColumn
    val row = "sl_row"
    def errors(checks: List[Check]): String =
      if (checks.isEmpty) "''"
      else
        checks
          .map(check =>
            s"IF(${check.condition}, '', ${BigQueryNativeLoader.bigQueryLiteral(check.error + "\n")})"
          )
          .mkString("CONCAT(", ", ", ")")
    val job = s"${jobs.mkString("[", ", ", "]")}[OFFSET(CAST(_TABLE_SUFFIX AS INT64))]"
    val (checked, fields) =
      if (format == Format.DSV) {
        val raw = fieldsOf(loadSchema)
          .map(field => s"$row.`${field.getName}` AS `${field.getName}`")
          .mkString("STRUCT(", ", ", ")")
        (
          s"SELECT $row.sl_job, $row.`$file` AS sl_file, ${errors(
              csvChecks(loadSchema, row, emptyIsNull)
            )} AS sl_errors, $raw AS sl_raw FROM (SELECT *, $job AS sl_job FROM $tables) AS $row",
          List("sl_file", "sl_errors", "sl_raw")
        )
      } else {
        val json = s"$row.sl_json"
        val value =
          if (!withValue) ""
          else
            jsonRowValue(attributes, json)
              .map(value => s", IF($json IS NULL, NULL, TO_JSON_STRING($value)) AS sl_value")
              .getOrElse(s", $row.`$lineColumn` AS sl_value")
        (
          // A blank line is no record
          s"SELECT $row.sl_job, $row.sl_file, ${errors(
              jsonChecks(loadSchema, json)
            )} AS sl_errors, $row.`$lineColumn` AS sl_raw$value FROM (SELECT $job AS sl_job, `$file` AS sl_file, `$lineColumn`, SAFE.PARSE_JSON(`$lineColumn`, wide_number_mode => 'round') AS sl_json FROM $tables WHERE TRIM(IFNULL(`$lineColumn`, '')) != '') AS $row",
          List("sl_file", "sl_errors", "sl_raw") ++ (if (withValue) List("sl_value") else Nil)
        )
      }
    val rejected = s"SELECT * FROM ($checked) WHERE sl_errors != ''"
    limitByJob match {
      case None => rejected
      case Some(limit) =>
        s"""SELECT sl_jobs.sl_job, sl_jobs.sl_count, sl_rejected.* FROM (
           |SELECT sl_job, COUNT(*) AS sl_count, ARRAY_AGG(STRUCT(${fields.mkString(
            ", "
          )}) LIMIT $limit) AS sl_rows
           |FROM ($rejected) GROUP BY sl_job
           |) AS sl_jobs, UNNEST(sl_jobs.sl_rows) AS sl_rejected""".stripMargin
    }
  }

  /** A rejected row, as [[rejectedRowsSql]] read it
    *
    * @param job
    *   the index of its load job
    * @param jobCount
    *   the number of rejected rows of its load job, when the query has a limit
    * @param values
    *   its values as strings: those of its columns for a CSV file, its line for a JSON file
    * @param value
    *   the row as JSON, without its hidden values, with `rejectWithValue`
    */
  case class RejectedRow(
    job: Int,
    jobCount: Option[Long],
    file: String,
    errors: List[String],
    values: List[Option[String]],
    value: Option[String]
  )

  private val mapper = new ObjectMapper()

  /** @param names
    *   the names of the columns of a CSV file
    * @param hidden
    *   the names of the columns of a CSV file whose values the value of a row does not show, in
    *   lower case
    * @param counted
    *   true if the query has a limit, and so the number of rejected rows of each load job
    * @return
    *   a row of the query of [[rejectedRowsSql]]
    */
  def rejectedRow(
    row: FieldValueList,
    format: Format,
    names: List[String],
    hidden: Set[String],
    withValue: Boolean,
    counted: Boolean
  ): RejectedRow = {
    def text(name: String): Option[String] =
      Option(row.get(name)).filterNot(_.isNull).map(_.getStringValue)
    val values =
      if (format == Format.DSV)
        row.get("sl_raw").getRecordValue.asScala.toList.map { field =>
          Option(field).filterNot(_.isNull).map(_.getStringValue)
        }
      else List(text("sl_raw"))
    RejectedRow(
      row.get("sl_job").getLongValue.toInt,
      if (counted) Some(row.get("sl_count").getLongValue) else None,
      text("sl_file").getOrElse(""),
      text("sl_errors").toList.flatMap(_.split("\n")).filter(_.nonEmpty),
      values,
      if (!withValue) None
      else if (format == Format.DSV) Some(csvRowJson(names, values, hidden))
      else text("sl_value")
    )
  }

  /** @return
    *   the values of a row of a CSV file as a JSON object of strings, without the hidden ones
    */
  def csvRowJson(names: List[String], values: List[Option[String]], hidden: Set[String]): String = {
    val node = mapper.createObjectNode()
    names.zip(values).filterNot { case (name, _) => hidden.contains(name.toLowerCase) }.foreach {
      case (name, Some(value)) => node.put(name, value)
      case (name, None)        => node.putNull(name)
    }
    mapper.writeValueAsString(node)
  }

  /** @param nullMarker
    *   the text of the null values, the empty text if None
    * @return
    *   a line of a CSV file with these values, that BigQuery reads as them: a value is quoted when
    *   it is empty, the null marker, or holds the separator, the quote or a line break, and the
    *   quotes of a quoted value are doubled. BigQuery has no escape character.
    */
  def csvLine(
    values: List[Option[String]],
    separator: String,
    quote: String,
    nullMarker: Option[String]
  ): String =
    values
      .map {
        case None => nullMarker.getOrElse("")
        case Some(value) =>
          val quoted = quote.nonEmpty && (value.isEmpty || nullMarker.contains(value) ||
          value.contains(separator) || value.contains(quote) ||
          value.exists(c => c == '\n' || c == '\r'))
          if (quoted) quote + value.replace(quote, quote + quote) + quote else value
      }
      .mkString(separator)
}
