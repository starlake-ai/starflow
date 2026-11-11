package ai.starlake.utils.conversion

import ai.starlake.utils.conversion.BigQueryScriptChecks.{Conversion, Script}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The strict conversions of the scripts, checked by the second step of a native BigQuery load
  * (#1935). Pure SQL: no BigQuery access.
  */
class BigQueryScriptChecksSpec extends AnyFlatSpec with Matchers {

  private def analyze(script: String): Script = BigQueryScriptChecks.analyze(script).get

  private def checks(script: String): List[String] = analyze(script).conversions.map(_.check)

  "analyze" should "keep a script without strict conversion as it is" in {
    List(
      "SAFE_CAST(N_RAW AS NUMERIC)",
      "SAFE.PARSE_DATE('%Y%m%d', D_RAW)",
      "CONCAT(a,   '-', b)",
      "TRIM(N_RAW)"
    ).foreach { script =>
      analyze(script) shouldBe Script(script, Nil)
    }
  }

  it should "check a CAST, and evaluate it as a SAFE_CAST, the script kept as it is written" in {
    analyze("CAST(N_RAW AS DECIMAL(18,2))") shouldBe Script(
      "SAFE_CAST(N_RAW AS DECIMAL(18,2))",
      List(
        Conversion(
          "CAST(N_RAW AS DECIMAL (18, 2))",
          "((N_RAW) IS NULL OR (SAFE_CAST(N_RAW AS DECIMAL (18, 2))) IS NOT NULL)",
          "N_RAW",
          List("N_RAW")
        )
      )
    )
    // With a format, and a quoted column
    val formatted = analyze("cast(`D RAW` as DATE FORMAT 'YYYYMMDD')")
    formatted.safe shouldBe "SAFE_cast(`D RAW` as DATE FORMAT 'YYYYMMDD')"
    formatted.conversions.map(_.check) shouldBe List(
      "((`D RAW`) IS NULL OR (SAFE_CAST(`D RAW` AS DATE FORMAT 'YYYYMMDD')) IS NOT NULL)"
    )
    formatted.conversions.map(_.columns) shouldBe List(List("D RAW"))
  }

  it should "check the PARSE functions on all their arguments, and evaluate them with SAFE" in {
    val script = analyze("PARSE_TIMESTAMP('%Y%m%d%H%M%S', TS_RAW, 'Europe/Paris')")
    script.safe shouldBe "SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S', TS_RAW, 'Europe/Paris')"
    script.conversions shouldBe List(
      Conversion(
        "PARSE_TIMESTAMP('%Y%m%d%H%M%S', TS_RAW, 'Europe/Paris')",
        "(('%Y%m%d%H%M%S') IS NULL OR (TS_RAW) IS NULL OR ('Europe/Paris') IS NULL OR (SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S', TS_RAW, 'Europe/Paris')) IS NOT NULL)",
        "TS_RAW",
        List("TS_RAW")
      )
    )
    // The text of PARSE_NUMERIC is its first argument, whatever the case of its name
    val numeric = analyze("parse_numeric(N_RAW)")
    numeric.safe shouldBe "SAFE.parse_numeric(N_RAW)"
    numeric.conversions.map(_.input) shouldBe List("N_RAW")
    List("PARSE_DATE", "PARSE_DATETIME", "PARSE_TIME", "PARSE_BIGNUMERIC").foreach { function =>
      analyze(s"$function('%Y', X)").safe shouldBe s"SAFE.$function('%Y', X)"
    }
  }

  it should "check the nested conversions on the safe value of their input" in {
    val script = analyze("CAST(CAST(N_RAW AS INT64) AS STRING)")
    script.safe shouldBe "SAFE_CAST(SAFE_CAST(N_RAW AS INT64) AS STRING)"
    script.conversions.map(_.check) shouldBe List(
      "((SAFE_CAST(N_RAW AS INT64)) IS NULL OR (SAFE_CAST(SAFE_CAST(N_RAW AS INT64) AS STRING)) IS NOT NULL)",
      "((N_RAW) IS NULL OR (SAFE_CAST(N_RAW AS INT64)) IS NOT NULL)"
    )
    script.conversions.map(_.sql) shouldBe List(
      "CAST(CAST(N_RAW AS INT64) AS STRING)",
      "CAST(N_RAW AS INT64)"
    )
    analyze("DATE_ADD(PARSE_DATE('%Y%m%d', D_RAW), INTERVAL 1 DAY)").safe shouldBe
    "DATE_ADD(SAFE.PARSE_DATE('%Y%m%d', D_RAW), INTERVAL 1 DAY)"
  }

  it should "check a conversion of a branch only when its branch is evaluated, as lazily" in {
    checks("CASE WHEN D_RAW = '00000000' THEN NULL ELSE PARSE_DATE('%Y%m%d', D_RAW) END") shouldBe
    List(
      "IF((D_RAW = '00000000') IS NOT TRUE, (('%Y%m%d') IS NULL OR (D_RAW) IS NULL OR (SAFE.PARSE_DATE('%Y%m%d', D_RAW)) IS NOT NULL), TRUE)"
    )
    checks("CASE D_RAW WHEN '0' THEN NULL WHEN '1' THEN CAST(X AS INT64) END") shouldBe List(
      "IF(((D_RAW) = ('0')) IS NOT TRUE, IF(((D_RAW) = ('1')) IS TRUE, ((X) IS NULL OR (SAFE_CAST(X AS INT64)) IS NOT NULL), TRUE), TRUE)"
    )
    // Either operand of a simple CASE may be a comparison
    checks("CASE TRUE WHEN X > 0 THEN CAST(X AS INT64) END") shouldBe List(
      "IF(((true) = (X > 0)) IS TRUE, ((X) IS NULL OR (SAFE_CAST(X AS INT64)) IS NOT NULL), TRUE)"
    )
    // The conditions are evaluated until one holds
    checks(
      "CASE WHEN CAST(A AS INT64) > 0 THEN 'P' WHEN CAST(B AS INT64) > 0 THEN 'Q' END"
    ) shouldBe
    List(
      "((A) IS NULL OR (SAFE_CAST(A AS INT64)) IS NOT NULL)",
      "IF((SAFE_CAST(A AS INT64) > 0) IS NOT TRUE, ((B) IS NULL OR (SAFE_CAST(B AS INT64)) IS NOT NULL), TRUE)"
    )
    val ifScript = analyze("IF(TRIM(N_RAW) = '', NULL, CAST(N_RAW AS NUMERIC))")
    ifScript.safe shouldBe "IF(TRIM(N_RAW) = '', NULL, SAFE_CAST(N_RAW AS NUMERIC))"
    ifScript.conversions.map(_.check) shouldBe List(
      "IF((Trim( N_RAW ) = '') IS NOT TRUE, ((N_RAW) IS NULL OR (SAFE_CAST(N_RAW AS NUMERIC)) IS NOT NULL), TRUE)"
    )
    checks("IF(A IS NULL, CAST(B AS INT64), 0)") shouldBe List(
      "IF((A IS NULL) IS TRUE, ((B) IS NULL OR (SAFE_CAST(B AS INT64)) IS NOT NULL), TRUE)"
    )
    // An argument of a branch is evaluated in its branch only: a / b is not when b = 0
    checks("IF(b = 0, NULL, CAST(a / b AS NUMERIC))") shouldBe List(
      "IF((b = 0) IS NOT TRUE, ((a / b) IS NULL OR (SAFE_CAST(a / b AS NUMERIC)) IS NOT NULL), TRUE)"
    )
    checks("IFNULL(A, CAST(B AS INT64))") shouldBe List(
      "IF((A) IS NULL, ((B) IS NULL OR (SAFE_CAST(B AS INT64)) IS NOT NULL), TRUE)"
    )
    checks("COALESCE(A, B, CAST(C AS INT64))") shouldBe List(
      "IF((A) IS NULL, IF((B) IS NULL, ((C) IS NULL OR (SAFE_CAST(C AS INT64)) IS NOT NULL), TRUE), TRUE)"
    )
    // A conversion that the condition of a branch reads is in safe form there too
    analyze("COALESCE(PARSE_DATE('%Y/%m/%d', D_RAW), DATE '1900-01-01')").safe shouldBe
    "COALESCE(SAFE.PARSE_DATE('%Y/%m/%d', D_RAW), DATE '1900-01-01')"
  }

  it should "check the second operand of AND and OR only when the first one does not decide" in {
    checks("REGEXP_CONTAINS(x, r'^\\d+$') AND CAST(x AS INT64) > 0") shouldBe List(
      "IF((REGEXP_CONTAINS(x, R'^\\d+$')) IS NOT FALSE, ((x) IS NULL OR (SAFE_CAST(x AS INT64)) IS NOT NULL), TRUE)"
    )
    checks("x IS NULL OR CAST(x AS INT64) > 0") shouldBe List(
      "IF((x IS NULL) IS NOT TRUE, ((x) IS NULL OR (SAFE_CAST(x AS INT64)) IS NOT NULL), TRUE)"
    )
  }

  it should "read the strings in double quotes, and keep them as they are written" in {
    analyze("CAST(REPLACE(N_RAW, \",\", \".\") AS FLOAT64)") shouldBe Script(
      "SAFE_CAST(REPLACE(N_RAW, \",\", \".\") AS FLOAT64)",
      List(
        Conversion(
          "CAST(REPLACE(N_RAW, ',', '.') AS FLOAT64)",
          "((REPLACE(N_RAW, ',', '.')) IS NULL OR (SAFE_CAST(REPLACE(N_RAW, ',', '.') AS FLOAT64)) IS NOT NULL)",
          "REPLACE(N_RAW, ',', '.')",
          List("N_RAW")
        )
      )
    )
    // A format in double quotes is a string, not a column
    val script = analyze("PARSE_DATE(\"%Y%m%d\", SUBSTR(value, 5, 8))")
    script.safe shouldBe "SAFE.PARSE_DATE(\"%Y%m%d\", SUBSTR(value, 5, 8))"
    script.conversions.map(c => (c.sql, c.input, c.columns)) shouldBe List(
      ("PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8))", "SUBSTR(value, 5, 8)", List("value"))
    )
  }

  it should "give the safe form of the script as it is written, before Jinja" in {
    val script =
      BigQueryScriptChecks.analyze("CAST(N_RAW AS INT64)", "CAST({{ column }} AS INT64)").get
    script.safe shouldBe "SAFE_CAST({{ column }} AS INT64)"
    script.conversions.map(_.check) shouldBe List(
      "((N_RAW) IS NULL OR (SAFE_CAST(N_RAW AS INT64)) IS NOT NULL)"
    )
    // Not when Jinja adds or removes conversions
    BigQueryScriptChecks.analyze(
      "CAST(a AS INT64)",
      "{% if x %}CAST(a AS INT64){% else %}CAST(b AS INT64){% endif %}"
    ) shouldBe None
    BigQueryScriptChecks.analyze("CAST(a AS INT64)", "{{ to_int('a') }}") shouldBe None
  }

  it should "not check a script that does not parse, or whose conversions are not all found" in {
    BigQueryScriptChecks.analyze("{{ some_macro }}") shouldBe None
    BigQueryScriptChecks.analyze("CAST(") shouldBe None
    // The conversions of a subquery
    BigQueryScriptChecks.analyze("(SELECT CAST(y AS INT64) FROM t)") shouldBe None
  }

  "safeText" should "prefix the strict calls outside of strings, quoted names and comments" in {
    BigQueryScriptChecks.safeText(
      """cast (a AS INT64) || 'CAST(b AS INT64)' || "PARSE_DATE(x)" || `CAST(c)` || SAFE.PARSE_DATE('%Y', d) || SAFE_CAST(e AS INT64) || parse_time('%H', f)"""
    ) shouldBe (
      """SAFE_cast (a AS INT64) || 'CAST(b AS INT64)' || "PARSE_DATE(x)" || `CAST(c)` || SAFE.PARSE_DATE('%Y', d) || SAFE_CAST(e AS INT64) || SAFE.parse_time('%H', f)""",
      2
    )
    BigQueryScriptChecks.safeText("CAST(y AS INT64) -- CAST(x)\n/* PARSE_DATE( */ + 1") shouldBe (
      "SAFE_CAST(y AS INT64) -- CAST(x)\n/* PARSE_DATE( */ + 1",
      1
    )
    BigQueryScriptChecks.safeText("'''CAST(x)''' || r'\\'CAST(' || cast_name") shouldBe (
      "'''CAST(x)''' || r'\\'CAST(' || cast_name",
      0
    )
  }

  "singleQuoted" should "quote the strings in double quotes with apostrophes" in {
    BigQueryScriptChecks.singleQuoted("""CONCAT("it's", 'a "b"', `c"d`, "e\"f")""") shouldBe
    Some("""CONCAT('it\'s', 'a "b"', `c"d`, 'e\"f')""")
    BigQueryScriptChecks.singleQuoted("""REGEXP_REPLACE(x, r"\s", '')""") shouldBe
    Some("""REGEXP_REPLACE(x, r'\s', '')""")
    // Nothing to quote, a raw string with an apostrophe, a triple-quoted string, no end
    List(
      "CONCAT('a', b)",
      """r"it's"""",
      "\"\"\"a\"\"\"",
      "\"a",
      "'a"
    ).foreach(script => BigQueryScriptChecks.singleQuoted(script) shouldBe None)
  }
}
