package ai.starlake.job.ingest.loaders

import ai.starlake.job.ingest.loaders.BigQueryNativeLoader.{FirstStepChecks, RowCheck, TypedColumns}
import ai.starlake.schema.model.{
  Position,
  PrimitiveType,
  TableAttribute,
  TransformInput,
  Trim,
  Type
}
import ai.starlake.utils.conversion.BigQueryScriptChecks
import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The rows of the native BigQuery loads of DSV and POSITION files rejected for a typed value or a
  * scripted conversion that fails (#1935). Pure SQL: no BigQuery access.
  */
class BigQueryNativeDsvChecksSpec extends AnyFlatSpec with Matchers {

  private val yyyymmdd = Type(
    "date_yyyymmdd",
    "yyyyMMdd",
    PrimitiveType.date,
    ddlMapping = Some(Map("bigquery" -> "DATE"))
  )
  private val yesNo =
    Type("yes_no", "Y<-TF->N", PrimitiveType.boolean, ddlMapping = Some(Map("bigquery" -> "BOOL")))
  private val date =
    Type("date", "yyyy-MM-dd", PrimitiveType.date, ddlMapping = Some(Map("bigquery" -> "DATE")))
  private val long =
    Type(
      "long",
      "[-|\\+|0-9][0-9]*",
      PrimitiveType.long,
      ddlMapping = Some(Map("bigquery" -> "INT64"))
    )
  private val string = Type("string", ".+", PrimitiveType.string)
  private val byte = Type("byte", ".", PrimitiveType.byte)

  private val attributes = List(
    TableAttribute("ID", "string", required = Some(true)),
    TableAttribute("D_TYPED", "date_yyyymmdd", required = Some(true)),
    TableAttribute("D_PLAIN", "date"),
    TableAttribute("FLAG", "yes_no", privacy = Some(TransformInput("HIDE", false))),
    TableAttribute("N_TRIMMED", "long", trim = Some(Trim.BOTH)),
    TableAttribute("LABEL", "string", trim = Some(Trim.LEFT)),
    TableAttribute("B", "byte", trim = Some(Trim.BOTH)),
    TableAttribute("D_RAW", "string", ignore = Some(true)),
    TableAttribute("D_IGNORED", "date_yyyymmdd", ignore = Some(true)),
    TableAttribute("D_CALC", "date", script = Some("PARSE_DATE('%Y%m%d', D_RAW)"))
  )

  private val types = Map(
    "ID"        -> string,
    "D_TYPED"   -> yyyymmdd,
    "D_PLAIN"   -> date,
    "FLAG"      -> yesNo,
    "N_TRIMMED" -> long,
    "LABEL"     -> string,
    "B"         -> byte,
    "D_RAW"     -> string,
    "D_IGNORED" -> yyyymmdd,
    "D_CALC"    -> date
  )

  private def field(name: String, tpe: StandardSQLTypeName, required: Boolean = false) =
    Field
      .newBuilder(name, tpe)
      .setMode(if (required) Field.Mode.REQUIRED else Field.Mode.NULLABLE)
      .build()

  private val firstStep = Schema.of(
    field("ID", StandardSQLTypeName.STRING, required = true),
    field("D_TYPED", StandardSQLTypeName.DATE, required = true),
    field("D_PLAIN", StandardSQLTypeName.DATE),
    field("FLAG", StandardSQLTypeName.BOOL),
    field("N_TRIMMED", StandardSQLTypeName.INT64),
    field("LABEL", StandardSQLTypeName.STRING),
    field("B", StandardSQLTypeName.STRING),
    field("D_RAW", StandardSQLTypeName.STRING),
    field("D_IGNORED", StandardSQLTypeName.DATE),
    field("D_CALC", StandardSQLTypeName.DATE)
  )

  private def typed(emptyIsNull: Boolean = true): TypedColumns =
    BigQueryNativeLoader.dsvTypedColumns(attributes, firstStep, types, emptyIsNull).get

  private val dTypedValue =
    "IF(REGEXP_CONTAINS(`D_TYPED`, '^[0-9]{4}[0-9]{2}[0-9]{2}$'), SAFE.PARSE_DATE('%Y%m%d', `D_TYPED`), NULL)"
  private val flagValue =
    "CASE WHEN REGEXP_CONTAINS(`FLAG`, '(?s)^(?:Y)$') THEN TRUE WHEN REGEXP_CONTAINS(`FLAG`, '(?s)^(?:N)$') THEN FALSE END"

  "dsvTypedColumns" should "select the loaded attributes whose values the load job does not read as Spark does, or that declare trim" in {
    // An ignored attribute too, which the Spark loader checks and the scripts read
    typed().columns.map(_.attribute.name) shouldBe
    List("D_TYPED", "FLAG", "N_TRIMMED", "LABEL", "D_IGNORED")
    // A column that kept a STRING type reads the text
    BigQueryNativeLoader
      .dsvTypedColumns(
        attributes.filter(_.name == "D_TYPED"),
        Schema.of(field("D_TYPED", StandardSQLTypeName.STRING)),
        types,
        emptyIsNull = true
      ) shouldBe None
    BigQueryNativeLoader.dsvTypedColumns(
      attributes.filter(a => Set("ID", "D_PLAIN", "D_CALC")(a.name)),
      firstStep,
      types,
      emptyIsNull = true
    ) shouldBe None
  }

  "readAsStrings" should "read the selected fields as strings, with their mode" in {
    val schema = BigQueryNativeLoader.readAsStrings(firstStep, Set("D_TYPED", "N_TRIMMED"))
    schema.getFields.get("D_TYPED").getType.getStandardType shouldBe StandardSQLTypeName.STRING
    schema.getFields.get("D_TYPED").getMode shouldBe Field.Mode.REQUIRED
    schema.getFields.get("N_TRIMMED").getType.getStandardType shouldBe StandardSQLTypeName.STRING
    schema.getFields.get("D_PLAIN").getType.getStandardType shouldBe StandardSQLTypeName.DATE
    BigQueryNativeLoader.readAsStrings(firstStep, Set.empty) shouldBe firstStep
  }

  "TypedColumn" should "convert the trimmed text of a value, NULL when empty or invalid" in {
    val columns = typed().columns.map(column => column.attribute.name -> column).toMap
    columns("D_TYPED").value("`D_TYPED`") shouldBe dTypedValue
    columns("FLAG").value("`FLAG`") shouldBe flagValue
    columns("N_TRIMMED").value("`N_TRIMMED`") shouldBe "SAFE_CAST(TRIM(`N_TRIMMED`, ' ') AS INT64)"
    // An empty string is NULL when the null value is empty
    columns("LABEL").value("`LABEL`") shouldBe "NULLIF(LTRIM(`LABEL`, ' '), '')"
    typed(emptyIsNull = false).columns
      .find(_.attribute.name == "LABEL")
      .get
      .value("`LABEL`") shouldBe
    "LTRIM(`LABEL`, ' ')"
  }

  it should "reject an invalid value, or no value for a required attribute" in {
    val checks = typed().checks(withValue = true)
    checks shouldBe List(
      RowCheck("NOT (IFNULL(sl_raw_0, '') = '')", "\"`D_TYPED`  can't be null\\n\""),
      RowCheck(
        s"(IFNULL(sl_raw_0, '') = '' OR ${dTypedValue.replace("`D_TYPED`", "sl_raw_0")} IS NOT NULL)",
        "CONCAT(\"`D_TYPED` is not a valid DATE with the pattern `yyyyMMdd`: \", sl_raw_0, \"\\n\")"
      ),
      // The value of an attribute with a privacy is not shown
      RowCheck(
        s"(IFNULL(sl_raw_1, '') = '' OR ${flagValue.replace("`FLAG`", "sl_raw_1")} IS NOT NULL)",
        "\"`FLAG` is not a valid BOOL with the pattern `Y<-TF->N`\\n\""
      ),
      RowCheck(
        "(IFNULL(TRIM(sl_raw_2, ' '), '') = '' OR SAFE_CAST(TRIM(sl_raw_2, ' ') AS INT64) IS NOT NULL)",
        "CONCAT(\"`N_TRIMMED` is not a valid INT64: \", sl_raw_2, \"\\n\")"
      ),
      RowCheck(
        s"(IFNULL(sl_raw_4, '') = '' OR ${dTypedValue.replace("`D_TYPED`", "sl_raw_4")} IS NOT NULL)",
        "CONCAT(\"`D_IGNORED` is not a valid DATE with the pattern `yyyyMMdd`: \", sl_raw_4, \"\\n\")"
      )
    )
    typed().checks(withValue = false).map(_.error)(3) shouldBe
    "\"`N_TRIMMED` is not a valid INT64\\n\""
  }

  "TypedColumns" should "read the rows of the first step with their values converted, and their raw text" in {
    val columns =
      TypedColumns(typed().columns.filter(c => Set("D_TYPED", "LABEL")(c.attribute.name)))
    val rows =
      s"(SELECT * REPLACE ($dTypedValue AS `D_TYPED`, NULLIF(LTRIM(`LABEL`, ' '), '') AS `LABEL`), `D_TYPED` AS sl_raw_0, `LABEL` AS sl_raw_1 FROM `p`.`d`.`t_0`)"
    columns.rows("`p`.`d`.`t_0`", suffix = false) shouldBe rows
    columns.rows("`p`.`d`.`t_*`", suffix = true) should endWith(
      ", `LABEL` AS sl_raw_1, _TABLE_SUFFIX AS sl_suffix FROM `p`.`d`.`t_*`)"
    )
    columns.raw("LABEL") shouldBe Some("sl_raw_1")
    columns.raw("ID") shouldBe None
    // The second step loads the rows that pass the checks, without their raw text
    columns.loaded("`p`.`d`.`t_0`", Some("c")) shouldBe
    s"(SELECT * EXCEPT (sl_raw_0, sl_raw_1) FROM $rows WHERE c)"
    columns.loaded(
      "`p`.`d`.`t_0`",
      None
    ) shouldBe s"(SELECT * EXCEPT (sl_raw_0, sl_raw_1) FROM $rows)"
    // The checks read them, with the index of their file
    val checks = FirstStepChecks("c", Nil, None, "", typed = Some(columns))
    val wildcardRows = columns.rows("`p`.`d`.`t_*`", suffix = true)
    checks.rows("`p`.`d`.`t_*`") shouldBe (wildcardRows, "sl_suffix")
    checks.countsSql("`p`.`d`.`t_*`") shouldBe
    s"SELECT sl_suffix AS suffix, COUNT(*) AS rejected, 0 AS filtered FROM $wildcardRows WHERE NOT (c) GROUP BY 1"
    BigQueryNativeLoader.rejectedRowsSql(List("`p`.`d`.`t_*`"), checks, 5) should include(
      s"FROM $wildcardRows AS sl_rejected_row WHERE NOT (c)"
    )
  }

  "scriptChecks" should "reject the rows on which a strict conversion of a script fails" in {
    val scripts = List(
      attributes.last -> BigQueryScriptChecks.analyze("PARSE_DATE('%Y%m%d', D_RAW)").get
    )
    val check =
      "(('%Y%m%d') IS NULL OR (D_RAW) IS NULL OR (SAFE.PARSE_DATE('%Y%m%d', D_RAW)) IS NOT NULL)"
    BigQueryNativeLoader.scriptChecks(scripts, attributes, withValue = true) shouldBe List(
      RowCheck(
        check,
        "CONCAT(\"`D_CALC` is not computed: PARSE_DATE('%Y%m%d', D_RAW) fails: \", FORMAT('%t', D_RAW), \"\\n\")"
      )
    )
    BigQueryNativeLoader.scriptChecks(scripts, attributes, withValue = false) shouldBe List(
      RowCheck(check, "\"`D_CALC` is not computed: PARSE_DATE('%Y%m%d', D_RAW) fails\\n\"")
    )
    // Not when the value that it converts reads a column with a privacy
    val hidden = attributes.map(a =>
      if (a.name == "D_RAW") a.copy(privacy = Some(TransformInput("HIDE", false))) else a
    )
    BigQueryNativeLoader.scriptChecks(scripts, hidden, withValue = true).map(_.error) shouldBe
    List("\"`D_CALC` is not computed: PARSE_DATE('%Y%m%d', D_RAW) fails\\n\"")
    // Nor when it reads the line of a POSITION file with an attribute with a privacy
    val positionScript = List(
      TableAttribute("D", "date", script = Some("PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8))")) ->
      BigQueryScriptChecks.analyze("PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8))").get
    )
    val positioned = List(
      TableAttribute("ID", "string", position = Some(Position(0, 3))),
      TableAttribute(
        "SECRET",
        "string",
        position = Some(Position(4, 11)),
        privacy = Some(TransformInput("HIDE", false))
      )
    )
    BigQueryNativeLoader
      .scriptChecks(positionScript, positioned, withValue = true)
      .map(
        _.error
      ) shouldBe List(
      "\"`D` is not computed: PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8)) fails\\n\""
    )
    BigQueryNativeLoader
      .scriptChecks(positionScript, positioned.take(1), withValue = true)
      .map(_.error) shouldBe List(
      "CONCAT(\"`D` is not computed: PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8)) fails: \", FORMAT('%t', SUBSTR(value, 5, 8)), \"\\n\")"
    )
  }

  "dsvRowValue" should "show the raw text of the values, without the ignored, private and scripted ones" in {
    BigQueryNativeLoader.dsvRowValue(
      firstStep.getFields.toArray(new Array[Field](0)).toList.map(_.getName),
      attributes,
      Some(typed())
    ) shouldBe "TO_JSON_STRING(STRUCT(`ID`, sl_raw_0 AS `D_TYPED`, `D_PLAIN`, sl_raw_2 AS `N_TRIMMED`, sl_raw_3 AS `LABEL`, `B`))"
    BigQueryNativeLoader.dsvRowValue(List("D_RAW"), attributes, None) shouldBe "'{}'"
  }

  "rejectedCsvRowsSql" should "read the text of the columns of the rejected rows" in {
    val checks = FirstStepChecks("c", Nil, None, "")
    BigQueryNativeLoader.rejectedCsvRowsSql(
      List("`p`.`d`.`t_*`"),
      checks,
      List("`ID`", "sl_raw_0")
    ) shouldBe "SELECT `ID`, sl_raw_0 FROM `p`.`d`.`t_*` WHERE NOT (c)"
    BigQueryNativeLoader.replayText(field("a", StandardSQLTypeName.STRING)) shouldBe "`a`"
    BigQueryNativeLoader.replayText(field("a", StandardSQLTypeName.BYTES)) shouldBe
    "TO_BASE64(`a`)"
    BigQueryNativeLoader.replayText(field("a", StandardSQLTypeName.DATE)) shouldBe
    "CAST(`a` AS STRING)"
    BigQueryNativeLoader.replayText(field("a", StandardSQLTypeName.JSON)) shouldBe
    "TO_JSON_STRING(`a`)"
  }

  "positionRowValue" should "show the slices of the loaded attributes without privacy" in {
    BigQueryNativeLoader.positionRowValue(
      List(
        TableAttribute("a", "string", position = Some(Position(0, 1))),
        TableAttribute(
          "b",
          "string",
          position = Some(Position(2, 3)),
          privacy = Some(TransformInput("HIDE", false))
        ),
        TableAttribute("c", "string", position = Some(Position(4, 5)), ignore = Some(true))
      )
    ) shouldBe "TO_JSON_STRING(STRUCT(SUBSTR(value, 1, 2) AS `a`))"
  }
}
