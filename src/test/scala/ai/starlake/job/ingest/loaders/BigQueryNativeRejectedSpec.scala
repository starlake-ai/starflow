package ai.starlake.job.ingest.loaders

import ai.starlake.job.ingest.RejectedRecord
import ai.starlake.schema.model.{TableAttribute, TransformInput}
import com.google.cloud.bigquery.{BigQueryError, QueryParameterValue}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

/** The rejected rows of the native BigQuery loads, kept as Spark keeps them (#1856). Pure SQL and
  * record building: no BigQuery access.
  */
class BigQueryNativeRejectedSpec extends AnyFlatSpec with Matchers {

  private val attributes = List(
    TableAttribute("id", "long", required = Some(true), rename = Some("identifier")),
    TableAttribute("name", "string"),
    TableAttribute(
      "address",
      "struct",
      attributes = List(TableAttribute("city", "string", required = Some(true)))
    ),
    TableAttribute(
      "lines",
      "struct",
      array = Some(true),
      attributes = List(
        TableAttribute("text", "string", required = Some(true)),
        TableAttribute(
          "words",
          "struct",
          array = Some(true),
          attributes = List(TableAttribute("w", "string", required = Some(true)))
        )
      )
    ),
    TableAttribute("tags", "string", array = Some(true), required = Some(true)),
    TableAttribute("ignored", "string", required = Some(true), ignore = Some(true)),
    TableAttribute("computed", "string", required = Some(true), script = Some("'x'"))
  )

  "requiredValueTerms" should "give the condition of each required attribute that a load checks" in {
    val terms = BigQueryNativeLoader.requiredValueTerms(attributes)
    terms.map(_._1) shouldBe BigQueryNativeLoader.requiredAttributePaths(attributes)
    terms shouldBe List(
      "id"           -> "`id` IS NOT NULL",
      "address.city" -> "(`address` IS NULL OR `address`.`city` IS NOT NULL)",
      "lines.text" -> "NOT EXISTS (SELECT 1 FROM UNNEST(`lines`) AS sl_element_0 WHERE NOT ((sl_element_0 IS NULL OR sl_element_0.`text` IS NOT NULL)))",
      "lines.words.w" -> "NOT EXISTS (SELECT 1 FROM UNNEST(`lines`) AS sl_element_0 WHERE NOT (NOT EXISTS (SELECT 1 FROM UNNEST(sl_element_0.`words`) AS sl_element_1 WHERE NOT ((sl_element_1 IS NULL OR sl_element_1.`w` IS NOT NULL)))))"
    )
    BigQueryNativeLoader.requiredValueTerms(List(TableAttribute("a"))) shouldBe Nil
    // A row without a value has an error line for each attribute without a value
    List(attributes, attributes.take(3), List(TableAttribute("a"))).foreach { attrs =>
      BigQueryNativeLoader.requiredValuesCondition(attrs).isDefined shouldBe
      BigQueryNativeLoader.requiredValueTerms(attrs).nonEmpty
    }
  }

  "bigQueryLiteral" should "quote any text" in {
    BigQueryNativeLoader.bigQueryLiteral(
      "`id`  can't be null\n"
    ) shouldBe "\"`id`  can't be null\\n\""
    BigQueryNativeLoader.bigQueryLiteral("a\"b\\c\r") shouldBe "\"a\\\"b\\\\c\\r\""
  }

  "rejectedRowsSql" should "read the file, the errors and the value of the first rejected rows" in {
    val terms = BigQueryNativeLoader.requiredValueTerms(attributes.take(3))
    val condition = BigQueryNativeLoader.requiredValuesCondition(attributes.take(3)).get
    val errors =
      """CONCAT(IF(`id` IS NOT NULL, '', "`id`  can't be null\n"), IF((`address` IS NULL OR `address`.`city` IS NOT NULL), '', "`address.city`  can't be null\n"))"""
    BigQueryNativeLoader.rejectedRowsSql(
      List("`p`.`d`.`t_*`"),
      condition,
      terms,
      None,
      100
    ) shouldBe s"SELECT * FROM (SELECT sl_rejected_row.`sl_input_file_name` AS file, $errors AS errors FROM `p`.`d`.`t_*` AS sl_rejected_row WHERE NOT ($condition)) LIMIT 100"
    BigQueryNativeLoader.rejectedRowsSql(
      List("`p`.`d`.`t_*`", "`p`.`d`.`u_*`"),
      condition,
      terms,
      Some(List("""$."secret"""")),
      5
    ) shouldBe s"SELECT * FROM (${List("t", "u")
        .map(table => s"""SELECT sl_rejected_row.`sl_input_file_name` AS file, $errors AS errors, TO_JSON_STRING(JSON_REMOVE(TO_JSON(sl_rejected_row, stringify_wide_numbers => TRUE), '$$."sl_input_file_name"', '$$."secret"')) AS row_json FROM `p`.`d`.`${table}_*` AS sl_rejected_row WHERE NOT ($condition)""")
        .mkString(" UNION ALL ")}) LIMIT 5"
  }

  "hiddenValuePaths" should "hide the ignored and private values, or their repeated record" in {
    BigQueryNativeLoader.hiddenValuePaths(
      List(
        TableAttribute("id", "long"),
        TableAttribute("skipped", "string", ignore = Some(true)),
        TableAttribute("email", "string", privacy = Some(TransformInput("SHA256", false))),
        TableAttribute(
          "address",
          "struct",
          attributes = List(
            TableAttribute("city", "string"),
            TableAttribute("street", "string", privacy = Some(TransformInput("HIDE", false)))
          )
        ),
        TableAttribute(
          "contacts",
          "struct",
          array = Some(true),
          attributes = List(
            TableAttribute("phone", "string", privacy = Some(TransformInput("HIDE", false)))
          )
        ),
        TableAttribute("tags", "string", array = Some(true))
      )
    ) shouldBe List(
      """$."skipped"""",
      """$."email"""",
      """$."address"."street"""",
      """$."contacts""""
    )
  }

  "rejectedRowFields" should "read the errors, the file and the value of a rejected row" in {
    BigQueryNativeLoader.rejectedRowFields(
      Some("gs://f"),
      Some("`a`  can't be null\n`b`  can't be null\n"),
      Some("{}")
    ) shouldBe (List("`a`  can't be null", "`b`  can't be null"), "gs://f", Some("{}"))
    BigQueryNativeLoader.rejectedRowFields(None, Some(""), None) shouldBe (
      List("A required attribute has no value"),
      "",
      None
    )
  }

  "replayExportSql" should "export all the rejected rows of the temporary tables, with the columns of the files" in {
    BigQueryNativeLoader.replayExportSql(
      "gs://bucket/replay/d.t.20261023101500.replay-*.parquet",
      List("`p`.`d`.`t_0`", "`p`.`d`.`t_1`"),
      "`id` IS NOT NULL",
      List("computed")
    ) shouldBe
    """EXPORT DATA OPTIONS (uri = "gs://bucket/replay/d.t.20261023101500.replay-*.parquet", format = 'PARQUET', compression = 'SNAPPY', overwrite = true) AS
      |SELECT * EXCEPT (`sl_input_file_name`, `computed`) FROM (
      |SELECT * FROM `p`.`d`.`t_0`
      |UNION ALL
      |SELECT * FROM `p`.`d`.`t_1`
      |) WHERE NOT (`id` IS NOT NULL)""".stripMargin
  }

  "temporaryBucket" should "be the temporary bucket of the connection, its bucket, then the one of the application" in {
    import BigQueryNativeLoader.temporaryBucket
    temporaryBucket(
      Map("temporaryGcsBucket" -> "gs://tmp/", "gcsBucket" -> "b"),
      Some("app")
    ) shouldBe
    Some("tmp")
    temporaryBucket(Map("gcsBucket" -> "b/folder"), Some("app")) shouldBe Some("b/folder")
    temporaryBucket(Map.empty, Some("gs://app/x")) shouldBe Some("app/x")
    temporaryBucket(Map("gcsBucket" -> " "), None) shouldBe None
    // A blank value hides no other bucket
    temporaryBucket(Map("temporaryGcsBucket" -> " ", "gcsBucket" -> "b"), Some("app")) shouldBe
    Some("b")
    temporaryBucket(Map("temporaryGcsBucket" -> ""), Some("app")) shouldBe Some("app")
  }

  "badRecords" should "keep the errors of the records that a load job skipped" in {
    val record = new BigQueryError(
      "invalid",
      "gs://bucket/a.csv",
      "Could not parse 'x' as INT64 for field id (position 0) starting at location 9"
    )
    val local = new BigQueryError("invalid", "file-00000000", "Could not parse 'y' as INT64")
    val failure = new BigQueryError("invalid", null, "CSV processing encountered too many errors")
    // The summaries of the bad records of a failed job
    val summaries = List(
      new BigQueryError(
        "invalid",
        null,
        "Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: 4; errors: 2; max bad: 0; error percent: 0"
      ),
      new BigQueryError(
        "invalid",
        null,
        "Error while reading data, error message: CSV table encountered too many errors, giving up. Rows: 4; errors: 2. Please look into the errors[] collection for more details."
      )
    )
    BigQueryNativeLoader.badRecords(
      List("file:/tmp/a.csv", "file:/tmp/b.csv"),
      List(record, local, new BigQueryError("invalid", null, null), failure) ++ summaries,
      Some(failure)
    ) shouldBe List(
      "gs://bucket/a.csv"               -> record.getMessage,
      "file:/tmp/a.csv,file:/tmp/b.csv" -> local.getMessage,
      "file:/tmp/a.csv,file:/tmp/b.csv" -> "invalid"
    )
  }

  "RejectedRecord.errorText" should "write the errors, the file and the value as the Spark validators" in {
    RejectedRecord.errorText(List("`a`  can't be null", "`b`  can't be null"), "gs://f") shouldBe
    "ERR -> \n`a`  can't be null\n`b`  can't be null\n\nFILE -> gs://f"
    RejectedRecord.errorText(List("e"), "f", Some("""{"a":null}""")) shouldBe
    "ERR -> \ne\n\nFILE -> f\n\nROW -> {\"a\":null}"
    val long = RejectedRecord.errorText(List("x" * 10000), "f")
    long.length should be < 8100
    long should endWith(
      s"...(truncated, original length=${"ERR -> \n".length + 10000 + "\n\nFILE -> f".length})"
    )
    // A character is not split
    val emoji = "\uD83D\uDE00"
    val cut = RejectedRecord.limited("a" * 7999 + emoji + "b" * 100)
    cut should startWith("a" * 7999 + "...")
    RejectedRecord.limited("short") shouldBe "short"
  }

  "RejectedRecord.batches" should "group the records by size" in {
    val timestamp = new Timestamp(0)
    val small = RejectedRecord("j", timestamp, "d", "t", "e", "p")
    val big = small.copy(error = "e" * 3000)
    RejectedRecord.batches(Nil) shouldBe Nil
    RejectedRecord.batches(List.fill(2500)(small)).map(_.size) shouldBe List(1000, 1000, 500)
    RejectedRecord.batches(List(big, big, small), maxSize = 6000).map(_.size) shouldBe List(1, 2)
  }

  "RejectedRecord.bigQueryInsert" should "insert the records with parameters, whatever their text" in {
    val timestamp = Timestamp.valueOf("2026-10-23 10:15:00.123456")
    val record = RejectedRecord("job", timestamp, "d", "t", "ERR -> \n`a`  can't be null\n", "p'1")
    val (sql, parameters) =
      RejectedRecord.bigQueryInsert("`p`.`audit`.`rejected`", List(record, record))
    sql shouldBe "INSERT INTO `p`.`audit`.`rejected` (JOBID, TIMESTAMP, DOMAIN, SCHEMA, ERROR, PATH) VALUES " +
    "(@jobid_0, @timestamp_0, @domain_0, @schema_0, @error_0, @path_0), " +
    "(@jobid_1, @timestamp_1, @domain_1, @schema_1, @error_1, @path_1)"
    parameters should have size 12
    parameters("error_1") shouldBe QueryParameterValue.string(record.error)
    parameters("path_0") shouldBe QueryParameterValue.string("p'1")
    parameters("timestamp_0") shouldBe QueryParameterValue.timestamp(
      timestamp.getTime / 1000 * 1000000 + 123456L
    )
  }
}
