package ai.starlake.job.ingest.loaders

import ai.starlake.schema.model.{Format, Metadata, TableAttribute, TransformInput}
import ai.starlake.utils.conversion.{BigQueryNativeSchema, BigQueryType}
import com.google.cloud.bigquery.{
  Field,
  FieldElementType,
  FieldList,
  FieldValue,
  FieldValueList,
  Schema,
  StandardSQLTypeName
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** The rejected rows of the native CSV and JSON loads, found again with the rules of the BigQuery
  * load jobs (#1857). SQL building only: no BigQuery access.
  */
class BigQueryRejectedRowsSpec extends AnyFlatSpec with Matchers {

  private def field(
    name: String,
    tpe: StandardSQLTypeName,
    mode: Field.Mode = Field.Mode.NULLABLE
  ): Field =
    Field.newBuilder(name, tpe).setMode(mode).build()

  private val csvSchema = Schema.of(
    field("id", StandardSQLTypeName.INT64, Field.Mode.REQUIRED),
    Field
      .newBuilder("code", StandardSQLTypeName.STRING)
      .setMaxLength(3L)
      .setMode(Field.Mode.NULLABLE)
      .build(),
    field("label", StandardSQLTypeName.STRING),
    Field
      .newBuilder("price", StandardSQLTypeName.NUMERIC)
      .setPrecision(10L)
      .setScale(2L)
      .build(),
    Field
      .newBuilder("days", StandardSQLTypeName.RANGE)
      .setRangeElementType(FieldElementType.newBuilder().setType("DATE").build())
      .build()
  )

  private val jsonSchema = Schema.of(
    field("id", StandardSQLTypeName.INT64, Field.Mode.REQUIRED),
    field("tags", StandardSQLTypeName.STRING, Field.Mode.REPEATED),
    Field
      .newBuilder(
        "record",
        StandardSQLTypeName.STRUCT,
        FieldList.of(
          field("code", StandardSQLTypeName.STRING, Field.Mode.REQUIRED),
          field("day", StandardSQLTypeName.DATE)
        )
      )
      .setMode(Field.Mode.NULLABLE)
      .build(),
    Field
      .newBuilder(
        "lines",
        StandardSQLTypeName.STRUCT,
        FieldList.of(field("flag", StandardSQLTypeName.BOOL, Field.Mode.REQUIRED))
      )
      .setMode(Field.Mode.REPEATED)
      .build(),
    field("doc", StandardSQLTypeName.JSON),
    field("area", StandardSQLTypeName.GEOGRAPHY)
  )

  "loadableText" should "give the rule of the load jobs for each type" in {
    def rule(tpe: BigQueryType, csv: Boolean = true) =
      BigQueryNativeSchema.loadableText("v", tpe, csv)
    rule(BigQueryType(StandardSQLTypeName.STRING)) shouldBe None
    rule(BigQueryType(StandardSQLTypeName.STRING, maxLength = Some(3))) shouldBe Some(
      "LENGTH(v) <= 3"
    )
    rule(BigQueryType(StandardSQLTypeName.BYTES, maxLength = Some(4))) shouldBe Some(
      "SAFE.FROM_BASE64(v) IS NOT NULL AND BYTE_LENGTH(SAFE.FROM_BASE64(v)) <= 4"
    )
    rule(BigQueryType(StandardSQLTypeName.BOOL)) shouldBe Some(
      "LOWER(v) IN ('true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0')"
    )
    rule(BigQueryType(StandardSQLTypeName.INT64)) shouldBe Some(
      "SAFE_CAST(v AS INT64) IS NOT NULL"
    )
    // BigQuery refuses parameterized types in CAST: the integer digits of the rounded value
    rule(
      BigQueryType(StandardSQLTypeName.BIGNUMERIC, precision = Some(40), scale = Some(2))
    ) shouldBe Some(
      s"SAFE_CAST(v AS BIGNUMERIC) IS NOT NULL AND ABS(ROUND(SAFE_CAST(v AS BIGNUMERIC), 2)) < BIGNUMERIC '1${"0" * 38}'"
    )
    rule(
      BigQueryType(StandardSQLTypeName.NUMERIC, precision = Some(5), scale = Some(2))
    ) shouldBe Some(
      "SAFE_CAST(v AS NUMERIC) IS NOT NULL AND ABS(ROUND(SAFE_CAST(v AS NUMERIC), 2)) < NUMERIC '1000'"
    )
    // The precision of NUMERIC
    rule(
      BigQueryType(StandardSQLTypeName.NUMERIC, precision = Some(38), scale = Some(9))
    ) shouldBe Some("SAFE_CAST(v AS NUMERIC) IS NOT NULL")
    rule(BigQueryType(StandardSQLTypeName.NUMERIC, precision = Some(3))) shouldBe Some(
      "SAFE_CAST(v AS NUMERIC) IS NOT NULL AND ABS(ROUND(SAFE_CAST(v AS NUMERIC), 0)) < NUMERIC '1000'"
    )
    rule(BigQueryType(StandardSQLTypeName.GEOGRAPHY)) shouldBe Some(
      "SAFE.ST_GEOGFROM(v) IS NOT NULL"
    )
    rule(BigQueryType(StandardSQLTypeName.JSON)) shouldBe Some(
      "SAFE.PARSE_JSON(v, wide_number_mode => 'round') IS NOT NULL"
    )
    // The canonical text in CSV files, ISO 8601 in JSON files
    rule(BigQueryType(StandardSQLTypeName.INTERVAL)) shouldBe Some(
      "NOT REGEXP_CONTAINS(v, r'^\\s*-?P') AND SAFE_CAST(v AS INTERVAL) IS NOT NULL"
    )
    rule(BigQueryType(StandardSQLTypeName.INTERVAL), csv = false) shouldBe Some(
      "REGEXP_CONTAINS(v, r'^\\s*-?P') AND SAFE_CAST(v AS INTERVAL) IS NOT NULL"
    )
    val timestamp = rule(BigQueryType(StandardSQLTypeName.TIMESTAMP)).get
    timestamp should include("SAFE_CAST(v AS TIMESTAMP) IS NOT NULL")
    // A slash date, a time without seconds, epoch seconds
    timestamp should include("r'\\1-\\2-\\3'")
    timestamp should include("r'\\1:00\\2'")
    timestamp should include("SAFE.TIMESTAMP_MICROS(")
    val range =
      rule(
        BigQueryType(StandardSQLTypeName.RANGE, rangeElementType = Some(StandardSQLTypeName.DATE))
      ).get
    range should include("SAFE_CAST(")
    range should include(" AS DATE)")
    range should not include "RANGE("
    // Without element type, a RANGE has no rule
    rule(BigQueryType(StandardSQLTypeName.RANGE)) shouldBe None
  }

  "csvChecks" should "check the required columns and the value of each column" in {
    val checks = BigQueryRejectedRows.csvChecks(csvSchema, "r")
    checks.map(_.error) shouldBe List(
      "`id`  can't be null",
      "`id` is not a valid INT64",
      "`code` is not a valid STRING(3)",
      "`price` is not a valid NUMERIC(10, 2)",
      "`days` is not a valid RANGE<DATE>"
    )
    checks.head.condition shouldBe "r.`id` IS NOT NULL"
    checks(1).condition shouldBe "(r.`id` IS NULL OR (SAFE_CAST(r.`id` AS INT64) IS NOT NULL))"
    checks(2).condition shouldBe "(r.`code` IS NULL OR (LENGTH(r.`code`) <= 3))"
  }

  "jsonChecks" should "check the line, then its fields at every level" in {
    val checks = BigQueryRejectedRows.jsonChecks(jsonSchema, "j")
    checks.map(_.error) shouldBe List(
      "The line is not valid JSON",
      "The line is not a JSON object",
      "`id`  can't be null",
      "`id` is not a valid INT64",
      "`tags` is not a JSON array",
      "`tags` holds a null element",
      // Not an object nor an array
      "`tags` is not a valid STRING",
      "`record` is not a JSON object",
      "`record.code`  can't be null",
      "`record.code` is not a valid STRING",
      "`record.day` is not a valid DATE",
      "`lines` is not a JSON array",
      "`lines` holds a null element",
      "`lines` is not a JSON object",
      "`lines.flag`  can't be null",
      "`lines.flag` is not a valid BOOL",
      "`area` is not a valid GEOGRAPHY"
    )
    val byError = checks.map(check => check.error -> check.condition).toMap
    byError("The line is not valid JSON") shouldBe "j IS NOT NULL"
    byError("`id`  can't be null") shouldBe
    "(IFNULL(JSON_TYPE(j), '') != 'object' OR NOT (IFNULL(JSON_TYPE(j[\"id\"]), 'null') = 'null'))"
    byError("`id` is not a valid INT64") shouldBe
    "(IFNULL(JSON_TYPE(j), '') != 'object' OR (IFNULL(JSON_TYPE(j[\"id\"]), 'null') = 'null' OR (JSON_TYPE(j[\"id\"]) IN ('string', 'number', 'boolean') AND (SAFE_CAST(JSON_VALUE(j[\"id\"]) AS INT64) IS NOT NULL))))"
    byError("`tags` holds a null element") shouldBe
    "(IFNULL(JSON_TYPE(j), '') != 'object' OR NOT EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(j[\"tags\"])) AS sl_element_0 WHERE JSON_TYPE(sl_element_0) = 'null'))"
    // A required field of a record applies when the record is an object
    byError("`record.code`  can't be null") shouldBe
    "(IFNULL(JSON_TYPE(j), '') != 'object' OR (IFNULL(JSON_TYPE(j[\"record\"]), 'null') = 'null' OR (IFNULL(JSON_TYPE(j[\"record\"]), '') != 'object' OR NOT (IFNULL(JSON_TYPE(j[\"record\"][\"code\"]), 'null') = 'null'))))"
    // Every element of a repeated record
    byError("`lines.flag`  can't be null") shouldBe
    "(IFNULL(JSON_TYPE(j), '') != 'object' OR NOT EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(j[\"lines\"])) AS sl_element_0 WHERE JSON_TYPE(sl_element_0) != 'null' AND NOT ((IFNULL(JSON_TYPE(sl_element_0), '') != 'object' OR NOT (IFNULL(JSON_TYPE(sl_element_0[\"flag\"]), 'null') = 'null')))))"
    byError("`area` is not a valid GEOGRAPHY") should include(
      "SAFE.ST_GEOGFROMGEOJSON(TO_JSON_STRING(j[\"area\"]))"
    )
  }

  "rejectedRowsSql" should "query the rejected rows of the diagnostic tables of CSV files" in {
    val sql = BigQueryRejectedRows.rejectedRowsSql(
      "`p`.`d`.`t_*`",
      List(0, 0, 1),
      Format.DSV,
      csvSchema,
      withValue = true,
      attributes = Nil,
      limitByJob = None
    )
    info(sql)
    // The checks are written once, for all the files: the load job of a file is given by its
    // table, and no window function holds the rows of a load job
    sql should startWith(
      "SELECT * FROM (SELECT sl_row.sl_job, sl_row.`sl_input_file_name` AS sl_file, CONCAT(IF(sl_row.`id` IS NOT NULL, '', \"`id`  can't be null\\n\"), IF((sl_row.`id` IS NULL OR (SAFE_CAST("
    )
    sql should include(
      "STRUCT(sl_row.`id` AS `id`, sl_row.`code` AS `code`, sl_row.`label` AS `label`, sl_row.`price` AS `price`, sl_row.`days` AS `days`) AS sl_raw FROM (SELECT *, [0, 0, 1][OFFSET(CAST(_TABLE_SUFFIX AS INT64))] AS sl_job FROM `p`.`d`.`t_*`) AS sl_row"
    )
    sql should endWith(") WHERE sl_errors != ''")
    "CONCAT\\(".r.findAllMatchIn(sql).size shouldBe 1
    sql should not include "OVER ("
    // The value of a CSV row is built from its values
    sql should not include "sl_value"
  }

  it should "query the rejected lines of JSON files, at most a number by load job" in {
    val sql = BigQueryRejectedRows.rejectedRowsSql(
      "`p`.`d`.`t_*`",
      List(0),
      Format.JSON,
      jsonSchema,
      withValue = true,
      attributes = List(
        TableAttribute("id", "long"),
        TableAttribute("secret", "string", privacy = Some(TransformInput("HIDE", false)))
      ),
      limitByJob = Some(100)
    )
    info(sql)
    sql should include(
      "FROM (SELECT [0][OFFSET(CAST(_TABLE_SUFFIX AS INT64))] AS sl_job, `sl_input_file_name` AS sl_file, `sl_line`, SAFE.PARSE_JSON(`sl_line`, wide_number_mode => 'round') AS sl_json FROM `p`.`d`.`t_*` WHERE TRIM(IFNULL(`sl_line`, '')) != '') AS sl_row"
    )
    // With hidden values, the value of a line is rebuilt from the declared fields
    sql should include(
      "sl_row.`sl_line` AS sl_raw, IF(sl_row.sl_json IS NULL, NULL, TO_JSON_STRING(JSON_OBJECT(\"id\", sl_row.sl_json[\"id\"]))) AS sl_value"
    )
    // The number of rejected rows of a load job, and at most a number of them
    sql should startWith(
      "SELECT sl_jobs.sl_job, sl_jobs.sl_count, sl_rejected.* FROM (\nSELECT sl_job, COUNT(*) AS sl_count, ARRAY_AGG(STRUCT(sl_file, sl_errors, sl_raw, sl_value) LIMIT 100) AS sl_rows\nFROM (SELECT * FROM ("
    )
    sql should endWith(
      ") WHERE sl_errors != '') GROUP BY sl_job\n) AS sl_jobs, UNNEST(sl_jobs.sl_rows) AS sl_rejected"
    )
    // Without hidden values, the value of a row is its line
    BigQueryRejectedRows.rejectedRowsSql(
      "t",
      List(0),
      Format.JSON,
      jsonSchema,
      withValue = true,
      attributes = List(TableAttribute("id", "long")),
      limitByJob = None
    ) should include("sl_row.`sl_line` AS sl_raw, sl_row.`sl_line` AS sl_value FROM")
  }

  "jsonRowValue" should "rebuild a line from its declared fields, without the hidden values" in {
    val hide = Some(TransformInput("HIDE", false))
    BigQueryRejectedRows.jsonRowValue(List(TableAttribute("a"), TableAttribute("b")), "j") shouldBe
    None
    BigQueryRejectedRows.jsonRowValue(
      List(
        TableAttribute("id", "long"),
        TableAttribute("secret", "string", privacy = hide),
        TableAttribute("skipped", "string", ignore = Some(true)),
        TableAttribute("computed", "string", script = Some("'x'")),
        TableAttribute(
          "record",
          "struct",
          attributes = List(TableAttribute("code"), TableAttribute("pin", privacy = hide))
        ),
        TableAttribute(
          "records",
          "struct",
          array = Some(true),
          attributes = List(TableAttribute("pin", privacy = hide))
        ),
        TableAttribute("tags", "string", array = Some(true))
      ),
      "j"
    ) shouldBe Some(
      "JSON_OBJECT(\"id\", j[\"id\"], \"record\", IF(JSON_TYPE(j[\"record\"]) = 'object', JSON_OBJECT(\"code\", j[\"record\"][\"code\"]), NULL), \"tags\", j[\"tags\"])"
    )
  }

  "diagnosticSchema" should "read CSV values and JSON lines as strings" in {
    BigQueryRejectedRows
      .diagnosticSchema(Format.DSV, csvSchema)
      .getFields
      .asScala
      .map(f => (f.getName, f.getType.getStandardType, f.getMode, f.getMaxLength)) shouldBe
    List("id", "code", "label", "price", "days").map(name =>
      (name, StandardSQLTypeName.STRING, Field.Mode.NULLABLE, null)
    )
    BigQueryRejectedRows
      .diagnosticSchema(Format.JSON, jsonSchema)
      .getFields
      .asScala
      .map(
        _.getName
      ) shouldBe List("sl_line")
  }

  "diagnosticMetadata" should "read the lines of a JSON file as POSITION loads do" in {
    val csv = Metadata(format = Some(Format.DSV), separator = Some("|"), encoding = Some("x"))
    BigQueryRejectedRows.diagnosticMetadata(csv) shouldBe csv
    val json = BigQueryRejectedRows.diagnosticMetadata(
      Metadata(format = Some(Format.JSON), encoding = Some("ISO-8859-1"))
    )
    json.resolveFormat() shouldBe Format.POSITION
    json.resolveWithHeader() shouldBe false
    json.resolveEncoding() shouldBe "UTF-8"
  }

  "csvLine" should "write the values as BigQuery reads them" in {
    BigQueryRejectedRows.csvLine(
      List(Some("a"), None, Some(""), Some("x;y"), Some("say \"hi\""), Some("l1\nl2")),
      ";",
      "\"",
      None
    ) shouldBe "a;;\"\";\"x;y\";\"say \"\"hi\"\"\";\"l1\nl2\""
    // A custom null value
    BigQueryRejectedRows.csvLine(List(None, Some("b")), ",", "\"", Some("NULL")) shouldBe "NULL,b"
    // A value that is the null value is quoted
    BigQueryRejectedRows.csvLine(List(Some("NULL")), ",", "\"", Some("NULL")) shouldBe "\"NULL\""
    // Without quote
    BigQueryRejectedRows.csvLine(List(Some(""), Some("b")), "|", "", None) shouldBe "|b"
  }

  "csvRowJson" should "write the values of a row without the hidden ones" in {
    BigQueryRejectedRows.csvRowJson(
      List("id", "Secret", "label"),
      List(Some("x"), Some("s"), None),
      Set("secret")
    ) shouldBe """{"id":"x","label":null}"""
  }

  "rejectedRow" should "read a rejected row of the query" in {
    val raw = FieldList.of(
      field("id", StandardSQLTypeName.STRING),
      field("secret", StandardSQLTypeName.STRING)
    )
    val schema = FieldList.of(
      field("sl_job", StandardSQLTypeName.INT64),
      field("sl_file", StandardSQLTypeName.STRING),
      field("sl_errors", StandardSQLTypeName.STRING),
      Field.newBuilder("sl_raw", StandardSQLTypeName.STRUCT, raw).build(),
      field("sl_count", StandardSQLTypeName.INT64)
    )
    def primitive(value: String) = FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)
    val row = FieldValueList.of(
      List(
        primitive("1"),
        primitive("gs://b/f.csv"),
        primitive("`id` is not a valid INT64\n"),
        FieldValue.of(
          FieldValue.Attribute.RECORD,
          FieldValueList.of(List(primitive("x"), primitive(null)).asJava, raw)
        ),
        primitive("3")
      ).asJava,
      schema
    )
    BigQueryRejectedRows.rejectedRow(
      row,
      Format.DSV,
      List("id", "secret"),
      Set("secret"),
      withValue = true,
      counted = true
    ) shouldBe BigQueryRejectedRows.RejectedRow(
      1,
      Some(3),
      "gs://b/f.csv",
      List("`id` is not a valid INT64"),
      List(Some("x"), None),
      Some("""{"id":"x"}""")
    )
  }
}
