package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{Format, Metadata, SchemaInfo, TableAttribute, Trim}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.conversion.BigQueryNativeSchema
import com.google.cloud.bigquery.{Field, FieldList, ParquetOptions}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

class BigQueryNativeParquetSpec extends TestHelper {

  private val localFiles = List(new Path("file:/tmp/a.parquet"), new Path("file:/tmp/b.parquet"))
  private val gcsFiles = List(new Path("gs://bucket/a.parquet"), new Path("gs://bucket/b.parquet"))
  private val parquet = Metadata(format = Some(Format.PARQUET))

  /** The test project defines its own datetime and time types: use the built-in ones */
  private def withBuiltInTypes(withSettings: WithSettings): SchemaHandler = {
    implicit val settings: Settings = withSettings.settings
    withSettings.deliverTestFile(
      "/sample/new-types/types.sl.yml",
      new Path(DatasetArea.types, "types.sl.yml")
    )
    settings.schemaHandler(reload = true)
  }

  private def describe(fields: FieldList): List[(String, String, String)] =
    fields.asScala.toList.flatMap { field =>
      (
        field.getName,
        field.getType.getStandardType.name(),
        Option(field.getMode).getOrElse(Field.Mode.NULLABLE).name()
      ) ::
      Option(field.getSubFields).map(describe).getOrElse(Nil)
    }

  // Every type that a Parquet load reads, at the top level and nested
  private val everyType = SchemaInfo(
    name = "parquet_table",
    pattern = Pattern.compile("parquet_table.*"),
    attributes = List(
      TableAttribute("id", "long", required = Some(true), rename = Some("identifier")),
      TableAttribute("name", "string"),
      TableAttribute("civil", "datetime"),
      TableAttribute("wall", "time"),
      TableAttribute("payload", "binary"),
      TableAttribute("big", "bignumeric"),
      TableAttribute("price", "decimal"),
      TableAttribute("area", "geography"),
      TableAttribute("duration", "interval"),
      TableAttribute("days", "range_date"),
      TableAttribute("instants", "range_timestamp"),
      TableAttribute("doc", "variant"),
      TableAttribute("tags", "string", array = Some(true)),
      TableAttribute(
        "address",
        "struct",
        attributes = List(
          TableAttribute("city", "string", required = Some(true)),
          TableAttribute("since", "interval")
        )
      ),
      TableAttribute("docs", "variant", array = Some(true)),
      TableAttribute("ignored", "string", ignore = Some(true)),
      TableAttribute("computed", "string", required = Some(true), script = Some("'x'"))
    ),
    metadata = Some(parquet)
  )

  "isNativelySupported" should "load PARQUET files natively on BigQuery" in {
    IngestionJob.isNativelySupported("bigquery", Format.PARQUET, array = false) shouldBe true
    List(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.POSITION).foreach { format =>
      IngestionJob.isNativelySupported("bigquery", format, array = false) shouldBe true
    }
    IngestionJob.isNativelySupported("bigquery", Format.XML, array = false) shouldBe false
    // BigQuery load jobs do not read JSON arrays
    IngestionJob.isNativelySupported("bigquery", Format.JSON, array = true) shouldBe false
    // The array metadata, a JSON setting inherited from the domain, does not apply to Parquet
    IngestionJob.isNativelySupported("bigquery", Format.PARQUET, array = true) shouldBe true
    IngestionJob.isNativelySupported("duckdb", Format.PARQUET, array = false) shouldBe false
    IngestionJob.isNativelySupported("snowflake", Format.PARQUET, array = false) shouldBe true
  }

  "parquetFeaturesNotLoadedByBigQuery" should "accept a plain Parquet table" in {
    IngestionJob.parquetFeaturesNotLoadedByBigQuery(
      parquet.copy(options = Some(Map("format" -> "PARQUET"))),
      everyType.attributes,
      gcsFiles,
      Nil
    ) shouldBe Nil
  }

  it should "list what a BigQuery load job would not apply" in {
    val attributes = List(
      TableAttribute("id", "long", rename = Some("identifier")),
      TableAttribute("name", "string", default = Some("none"), trim = Some(Trim.BOTH)),
      TableAttribute("code", "string", trim = Some(Trim.NONE)),
      TableAttribute("tags", "string", array = Some(true)),
      TableAttribute(
        "address",
        "struct",
        attributes = List(
          TableAttribute("city", "string", rename = Some("town")),
          TableAttribute("lines", "string", array = Some(true))
        )
      ),
      // In the files, not loaded into the target
      TableAttribute(
        "ignored",
        "string",
        array = Some(true),
        default = Some("x"),
        ignore = Some(true)
      ),
      // Computed by the second step, not in the files
      TableAttribute("computed", "string", array = Some(true), script = Some("['x']"))
    )
    IngestionJob.parquetFeaturesNotLoadedByBigQuery(
      parquet.copy(options = Some(Map("format" -> "orc", "mergeSchema" -> "true"))),
      attributes,
      localFiles,
      List("address.since", "docs")
    ) shouldBe List(
      "Spark read options (format orc, mergeSchema)",
      "array attributes in local files (tags, address.lines, ignored)",
      "nested renames (address.city)",
      "default values (name)",
      "trim (name)",
      "nested or repeated JSON, INTERVAL or RANGE fields (address.since, docs)"
    )
    // Load jobs reading Cloud Storage URIs infer the lists
    IngestionJob.parquetFeaturesNotLoadedByBigQuery(
      parquet,
      attributes,
      gcsFiles,
      Nil
    ) shouldBe List(
      "nested renames (address.city)",
      "default values (name)",
      "trim (name)"
    )
  }

  "loadFormatOptions" should "load PARQUET files with list inference and enums as strings" in {
    val formatOptions = BigQueryNativeJob.loadFormatOptions(parquet).asInstanceOf[ParquetOptions]
    formatOptions.getType shouldBe "PARQUET"
    formatOptions.getEnableListInference shouldBe true
    formatOptions.getEnumAsString shouldBe true
  }

  "supportsBadRecords" should "be false for PARQUET, whose invalid rows fail the load" in {
    BigQueryNativeJob.supportsBadRecords(Format.PARQUET) shouldBe false
    List(Format.DSV, Format.JSON, Format.JSON_FLAT, Format.POSITION).foreach { format =>
      BigQueryNativeJob.supportsBadRecords(format) shouldBe true
    }
  }

  "requiresOneLoadPerFile" should "load several local PARQUET files one by one" in {
    BigQueryNativeLoader.requiresOneLoadPerFile(Format.PARQUET, localFiles) shouldBe true
    BigQueryNativeLoader.requiresOneLoadPerFile(Format.PARQUET, localFiles.take(1)) shouldBe false
    // A single load job reads several Cloud Storage URIs
    BigQueryNativeLoader.requiresOneLoadPerFile(Format.PARQUET, gcsFiles) shouldBe false
    BigQueryNativeLoader.requiresOneLoadPerFile(Format.DSV, localFiles) shouldBe false
  }

  "hasRequiredAttributes" should "find required attributes at any level" in {
    BigQueryNativeLoader.hasRequiredAttributes(
      List(TableAttribute("a"), TableAttribute("b", required = Some(false)))
    ) shouldBe false
    BigQueryNativeLoader.hasRequiredAttributes(
      List(
        TableAttribute("a"),
        TableAttribute(
          "b",
          "struct",
          attributes = List(TableAttribute("c", required = Some(true)))
        )
      )
    ) shouldBe true
  }

  "parquetFirstStepSchema" should "give PARQUET loads an explicit and nullable schema, with source names" in {
    new WithSettings() {
      val schemaHandler = withBuiltInTypes(this)
      val firstStepSchema = BigQueryNativeLoader.firstStepSchema(
        everyType,
        None,
        schemaHandler,
        fromDdlMapping = true,
        log = false
      )
      describe(
        BigQueryNativeLoader.parquetFirstStepSchema(firstStepSchema).getFields
      ) shouldBe List(
        // Required: checked by the second step. Renamed by the second step
        ("id", "INT64", "NULLABLE"),
        ("name", "STRING", "NULLABLE"),
        // Not TIMESTAMP, as a schema-less load would read it
        ("civil", "DATETIME", "NULLABLE"),
        ("wall", "TIME", "NULLABLE"),
        ("payload", "BYTES", "NULLABLE"),
        // Not NUMERIC, as a schema-less load would read it
        ("big", "BIGNUMERIC", "NULLABLE"),
        ("price", "NUMERIC", "NULLABLE"),
        ("area", "GEOGRAPHY", "NULLABLE"),
        // Read as strings, converted by the second step
        ("duration", "STRING", "NULLABLE"),
        ("days", "STRING", "NULLABLE"),
        ("instants", "STRING", "NULLABLE"),
        ("doc", "STRING", "NULLABLE"),
        ("tags", "STRING", "REPEATED"),
        ("address", "STRUCT", "NULLABLE"),
        ("city", "STRING", "NULLABLE"),
        // Nested and repeated values keep their type
        ("since", "INTERVAL", "NULLABLE"),
        ("docs", "JSON", "REPEATED"),
        // Dropped by the second step
        ("ignored", "STRING", "NULLABLE"),
        // Computed by the second step, absent from the files
        ("computed", "STRING", "NULLABLE")
      )
      // The other formats keep the modes and types of the target columns
      describe(firstStepSchema.getFields).filter { case (name, _, _) =>
        Set("id", "duration", "days", "doc", "city")(name)
      } shouldBe List(
        ("id", "INT64", "REQUIRED"),
        ("duration", "INTERVAL", "NULLABLE"),
        ("days", "RANGE", "NULLABLE"),
        ("doc", "JSON", "NULLABLE"),
        ("city", "STRING", "REQUIRED")
      )
    }
  }

  "unconvertedParquetFields" should "list the nested and repeated JSON, INTERVAL and RANGE fields" in {
    new WithSettings() {
      val schemaHandler = withBuiltInTypes(this)
      BigQueryNativeLoader.unconvertedParquetFields(
        BigQueryNativeLoader.firstStepSchema(
          everyType,
          None,
          schemaHandler,
          fromDdlMapping = true,
          log = false
        )
      ) shouldBe List("address.since", "docs")
    }
  }

  "parquetConversions" should "convert the values that the first step reads as strings" in {
    new WithSettings() {
      val schemaHandler = withBuiltInTypes(this)
      val conversions = BigQueryNativeLoader.parquetConversions(
        BigQueryNativeLoader.firstStepSchema(
          everyType,
          None,
          schemaHandler,
          fromDdlMapping = true,
          log = false
        )
      )
      conversions shouldBe List(
        "duration" -> "CAST(`duration` AS INTERVAL)",
        // JSqlParser cannot parse a CAST to RANGE<T>
        "days"     -> BigQueryNativeSchema.rangeFromText("`days`", "DATE", safe = false),
        "instants" -> BigQueryNativeSchema.rangeFromText("`instants`", "TIMESTAMP", safe = false),
        "doc"      -> "PARSE_JSON(`doc`)"
      )
      // Without the mappings, these columns are strings: nothing to convert, but the JSON values
      BigQueryNativeLoader.parquetConversions(
        BigQueryNativeLoader.firstStepSchema(
          everyType,
          None,
          schemaHandler,
          fromDdlMapping = false,
          log = false
        )
      ) shouldBe List("doc" -> "PARSE_JSON(`doc`)")

      val table = "`project`.`dataset`.`parquet_table_*`"
      BigQueryNativeLoader.withConversions(table, Nil) shouldBe table
      val tableWithConversions = BigQueryNativeLoader.withConversions(table, conversions)
      tableWithConversions shouldBe conversions
        .map { case (name, sql) => s"$sql AS `$name`" }
        .mkString("(SELECT * REPLACE (", ", ", s") FROM $table)")

      // The second step SQL is parsed, then its references are substituted
      val secondStepSql = everyType.buildSecondStepSqlSelectOnLoad(
        tableWithConversions,
        settings.appConfig.jdbcEngines.get("bigquery")
      )
      SQLUtils.extractColumnNames(secondStepSql).map(SQLUtils.unquoteAgressive) shouldBe List(
        "identifier",
        "name",
        "civil",
        "wall",
        "payload",
        "big",
        "price",
        "area",
        "duration",
        "days",
        "instants",
        "doc",
        "tags",
        "address",
        "docs",
        "computed"
      )
      val substituted = schemaHandler.transpileAndSubstituteSelectStatement(
        secondStepSql,
        settings.appConfig.connections("bigquery"),
        test = false
      )
      conversions.foreach { case (_, sql) => substituted should include(sql) }
      substituted should include("REPLACE")
    }
  }
}
