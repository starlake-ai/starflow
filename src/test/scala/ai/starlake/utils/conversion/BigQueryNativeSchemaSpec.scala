package ai.starlake.utils.conversion

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{SchemaInfo, TableAttribute}
import ai.starlake.utils.YamlSerde
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{
  Field,
  FieldElementType,
  FieldList,
  Schema => BQSchema,
  StandardSQLTypeName
}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._

import java.util.regex.Pattern
import scala.io.Source
import scala.jdk.CollectionConverters._

class BigQueryNativeSchemaSpec extends TestHelper {

  "BigQueryType.parse" should "read BigQuery column types, their aliases and parameters" in {
    BigQueryType.parse("DATETIME") shouldBe Some(BigQueryType(StandardSQLTypeName.DATETIME))
    BigQueryType.parse(" time ") shouldBe Some(BigQueryType(StandardSQLTypeName.TIME))
    BigQueryType.parse("INT") shouldBe Some(BigQueryType(StandardSQLTypeName.INT64))
    BigQueryType.parse("boolean") shouldBe Some(BigQueryType(StandardSQLTypeName.BOOL))
    BigQueryType.parse("DECIMAL") shouldBe Some(BigQueryType(StandardSQLTypeName.NUMERIC))
    BigQueryType.parse("STRING(10)") shouldBe
    Some(BigQueryType(StandardSQLTypeName.STRING, maxLength = Some(10)))
    BigQueryType.parse("BYTES(5)") shouldBe
    Some(BigQueryType(StandardSQLTypeName.BYTES, maxLength = Some(5)))
    BigQueryType.parse("NUMERIC(10, 2)") shouldBe
    Some(BigQueryType(StandardSQLTypeName.NUMERIC, precision = Some(10), scale = Some(2)))
    BigQueryType.parse("BIGNUMERIC(50, 20)") shouldBe
    Some(BigQueryType(StandardSQLTypeName.BIGNUMERIC, precision = Some(50), scale = Some(20)))
    BigQueryType.parse("range<datetime>") shouldBe Some(
      BigQueryType(StandardSQLTypeName.RANGE, rangeElementType = Some(StandardSQLTypeName.DATETIME))
    )
  }

  it should "reject what is not a BigQuery column type" in {
    List(
      "STRUCT<a INT64>",
      "ARRAY<STRING>",
      "varchar(8000)",
      "INT64(3)",
      "RANGE",
      "RANGE<STRING>",
      "STRING(0)",
      "STRING(99999999999999999999)",
      "NUMERIC(30)",
      "NUMERIC(10, 10)",
      "BIGNUMERIC(77, 38)",
      // BIGNUMERIC(P) has no decimals: P is at most 38
      "BIGNUMERIC(50)",
      ""
    ).foreach(ddl => BigQueryType.parse(ddl) shouldBe None)
    BigQueryType.parse("NUMERIC(29)").map(_.render) shouldBe Some("NUMERIC(29)")
    BigQueryType.parse("BIGNUMERIC(76, 38)").map(_.render) shouldBe Some("BIGNUMERIC(76, 38)")
    BigQueryType.parse("range<date>").map(_.render) shouldBe Some("RANGE<DATE>")
  }

  private def legacySchema(attributes: List[TableAttribute])(implicit
    schemaHandler: SchemaHandler
  ): BQSchema =
    SchemaInfo("t", Pattern.compile("t"), attributes)
      .bigquerySchemaWithoutIgnore(schemaHandler, withFinalName = true)

  private def patched(
    attributes: List[TableAttribute],
    existingTarget: Option[BQSchema] = None,
    enabled: Boolean = true
  )(implicit schemaHandler: SchemaHandler): BQSchema =
    BigQueryNativeSchema.patch(
      legacySchema(attributes),
      attributes,
      _.getFinalName(),
      existingTarget,
      schemaHandler,
      enabled
    )

  private def field(schema: BQSchema, name: String): Field = schema.getFields.get(name)

  new WithSettings() {
    deliverTestFile(
      "/sample/bq-native-types/types.sl.yml",
      new Path(DatasetArea.types, "types.sl.yml")
    )
    implicit val schemaHandler: SchemaHandler = settings.schemaHandler(reload = true)

    "BigQueryNativeSchema.patch" should "keep the schema derived from Spark for every default type" in {
      val defaultTypes = YamlSerde.deserializeYamlTypes(
        Source.fromResource("types/default.sl.yml").mkString,
        "default.sl.yml"
      )
      defaultTypes should not be empty
      val attributes = defaultTypes.flatMap { tpe =>
        List(
          TableAttribute(s"${tpe.name}_col", tpe.name),
          TableAttribute(s"${tpe.name}_required", tpe.name, required = Some(true)),
          TableAttribute(s"${tpe.name}_array", tpe.name, array = Some(true)),
          TableAttribute(s"${tpe.name}_comment", tpe.name, comment = Some("A comment"))
        )
      } :+ TableAttribute(
        "record",
        "struct",
        attributes = defaultTypes.map(tpe => TableAttribute(s"${tpe.name}_field", tpe.name))
      )
      patched(attributes) shouldBe legacySchema(attributes)
    }

    it should "take the BigQuery type of the ddlMapping when it differs from the derived one" in {
      val schema = patched(
        List(
          TableAttribute("civil", "civil_datetime", required = Some(true), comment = Some("Civil")),
          TableAttribute("wall", "wall_time", array = Some(true)),
          TableAttribute("json", "json_text"),
          TableAttribute("area", "area"),
          TableAttribute("code", "code10"),
          TableAttribute("days", "day_range"),
          TableAttribute(
            "record",
            "struct",
            attributes = List(TableAttribute("nested", "civil_datetime", rename = Some("renamed")))
          )
        )
      )
      val civil = field(schema, "civil")
      (civil.getType.getStandardType, civil.getMode, civil.getDescription) shouldBe
      (StandardSQLTypeName.DATETIME, Field.Mode.REQUIRED, "Civil")
      val wall = field(schema, "wall")
      (wall.getType.getStandardType, wall.getMode) shouldBe
      (StandardSQLTypeName.TIME, Field.Mode.REPEATED)
      field(schema, "json").getType.getStandardType shouldBe StandardSQLTypeName.JSON
      field(schema, "area").getType.getStandardType shouldBe StandardSQLTypeName.GEOGRAPHY
      val code = field(schema, "code")
      (code.getType.getStandardType, code.getMaxLength) shouldBe
      (StandardSQLTypeName.STRING, 10L)
      val days = field(schema, "days")
      (days.getType.getStandardType, days.getRangeElementType) shouldBe
      (StandardSQLTypeName.RANGE, FieldElementType.newBuilder().setType("DATE").build())
      field(schema, "record").getSubFields.get("renamed").getType.getStandardType shouldBe
      StandardSQLTypeName.DATETIME
    }

    it should "keep the derived type for aliases, invalid mappings, decimals and variants" in {
      val attributes = List(
        TableAttribute("small", "small_int"),
        TableAttribute("odd", "odd_struct"),
        TableAttribute("money", "money"),
        TableAttribute("payload", "variant")
      )
      patched(attributes) shouldBe legacySchema(attributes)
      field(legacySchema(attributes), "payload").getType.getStandardType shouldBe
      StandardSQLTypeName.JSON
    }

    it should "keep the type of a target column that exists with the derived type" in {
      val attributes = List(
        TableAttribute("kept", "civil_datetime"),
        TableAttribute("created", "civil_datetime"),
        TableAttribute("source", "civil_datetime", rename = Some("target"))
      )
      val existing = BQSchema.of(
        Field.of("KEPT", StandardSQLTypeName.TIMESTAMP),
        Field.of("target", StandardSQLTypeName.TIMESTAMP)
      )
      val schema = patched(attributes, Some(existing))
      field(schema, "kept").getType.getStandardType shouldBe StandardSQLTypeName.TIMESTAMP
      field(schema, "created").getType.getStandardType shouldBe StandardSQLTypeName.DATETIME
      field(schema, "target").getType.getStandardType shouldBe StandardSQLTypeName.TIMESTAMP

      // The first step of a load uses source names, and the target table final names
      val firstStep = BigQueryNativeSchema.patch(
        SchemaInfo("t", Pattern.compile("t"), attributes)
          .bigquerySchemaWithIgnoreAndScript(schemaHandler, withFinalName = false),
        attributes,
        _.name,
        Some(existing),
        schemaHandler,
        enabled = true
      )
      field(firstStep, "source").getType.getStandardType shouldBe StandardSQLTypeName.TIMESTAMP
      field(firstStep, "created").getType.getStandardType shouldBe StandardSQLTypeName.DATETIME
    }

    it should "keep the derived schema when it is disabled" in {
      val attributes = List(TableAttribute("civil", "civil_datetime"))
      patched(attributes, enabled = false) shouldBe legacySchema(attributes)
    }

    it should "keep the mapped type and its parameters of the columns it created" in {
      val attributes = List(
        TableAttribute("civil", "civil_datetime"),
        TableAttribute("code", "code10")
      )
      // The next load of a table created with the mapped types
      val existing = patched(attributes)
      patched(attributes, Some(existing)) shouldBe existing
      field(patched(attributes, Some(existing)), "code").getMaxLength shouldBe 10L
      // The flag turned off afterwards: the existing columns keep the mapped types
      patched(attributes, Some(existing), enabled = false) shouldBe existing
    }

    it should "map a custom variant type to JSON" in {
      val attributes = List(TableAttribute("doc", "my_json"))
      field(legacySchema(attributes), "doc").getType.getStandardType shouldBe
      StandardSQLTypeName.STRING
      field(patched(attributes), "doc").getType.getStandardType shouldBe StandardSQLTypeName.JSON
    }

    "BigQueryNativeSchema.castTypes" should "cast POSITION values to the type of the columns that differ from their mapping" in {
      val attributes = List(
        TableAttribute("kept", "civil_datetime"),
        TableAttribute("created", "civil_datetime"),
        TableAttribute("code", "code10"),
        TableAttribute("count", "long"),
        TableAttribute("amount", "decimal"),
        TableAttribute("payload", "variant")
      )
      val existing = BQSchema.of(Field.of("kept", StandardSQLTypeName.TIMESTAMP))
      BigQueryNativeSchema.castTypes(
        patched(attributes, Some(existing)),
        attributes,
        schemaHandler
      ) shouldBe Map("kept" -> "TIMESTAMP")
      // Disabled: the columns keep the derived types
      BigQueryNativeSchema.castTypes(
        patched(attributes, enabled = false),
        attributes,
        schemaHandler
      ) shouldBe Map("kept" -> "TIMESTAMP", "created" -> "TIMESTAMP", "code" -> "STRING")
    }

    "BigQueryNativeSchema.patch" should "not hide a type change of an attribute" in {
      val attributes = List(TableAttribute("code", "string"))
      // The column was a long, the attribute is now a string: BigQuery rejects the change, as before
      val existing = BQSchema.of(Field.of("code", StandardSQLTypeName.INT64))
      patched(attributes, Some(existing)) shouldBe legacySchema(attributes)
    }
  }

  "BigQueryUtils.compatibleSchema" should "keep nullable the existing nullable columns, as normalizeCompatibleSchema" in {
    val incoming = StructType(
      List(
        StructField("required_was_nullable", LongType, nullable = false),
        StructField("required_was_required", StringType, nullable = false),
        StructField("required_was_repeated", StringType, nullable = false),
        StructField("nullable_was_required", DoubleType, nullable = true),
        StructField("new_required", BooleanType, nullable = false),
        StructField(
          "record",
          StructType(List(StructField("leaf", IntegerType, nullable = false))),
          nullable = false
        ),
        StructField("values", ArrayType(IntegerType), nullable = true)
      )
    )
    val existing = BQSchema.of(
      Field
        .newBuilder("required_was_nullable", StandardSQLTypeName.INT64)
        .setMode(Field.Mode.NULLABLE)
        .build(),
      Field
        .newBuilder("required_was_required", StandardSQLTypeName.STRING)
        .setMode(Field.Mode.REQUIRED)
        .build(),
      Field
        .newBuilder("required_was_repeated", StandardSQLTypeName.STRING)
        .setMode(Field.Mode.REPEATED)
        .build(),
      Field
        .newBuilder("nullable_was_required", StandardSQLTypeName.FLOAT64)
        .setMode(Field.Mode.REQUIRED)
        .build(),
      Field
        .newBuilder(
          "record",
          StandardSQLTypeName.STRUCT,
          FieldList.of(
            Field.newBuilder("leaf", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
          )
        )
        .build(),
      Field.newBuilder("values", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build()
    )
    val compatible = BigQueryUtils.compatibleSchema(BigQueryUtils.bqSchema(incoming), existing)
    compatible shouldBe BigQueryUtils.bqSchema(
      BigQueryUtils.normalizeCompatibleSchema(incoming, BigQuerySchemaConverters.toSpark(existing))
    )
    compatible.getFields.asScala.map(f => f.getName -> f.getMode).toList shouldBe List(
      "required_was_nullable" -> Field.Mode.NULLABLE,
      "required_was_required" -> Field.Mode.REQUIRED,
      "required_was_repeated" -> Field.Mode.NULLABLE,
      "nullable_was_required" -> Field.Mode.NULLABLE,
      "new_required"          -> Field.Mode.REQUIRED,
      "record"                -> Field.Mode.NULLABLE,
      "values"                -> Field.Mode.REPEATED
    )
  }

  it should "accept existing columns that Spark cannot represent" in {
    val existing = BQSchema.of(
      Field.of("amount", StandardSQLTypeName.BIGNUMERIC),
      Field
        .newBuilder("period", StandardSQLTypeName.RANGE)
        .setRangeElementType(FieldElementType.newBuilder().setType("DATE").build())
        .build()
    )
    val incoming = BQSchema.of(
      Field
        .newBuilder("amount", StandardSQLTypeName.BIGNUMERIC)
        .setMode(Field.Mode.REQUIRED)
        .build(),
      existing.getFields.get("period")
    )
    BigQueryUtils.compatibleSchema(incoming, existing) shouldBe BQSchema.of(
      Field
        .newBuilder("amount", StandardSQLTypeName.BIGNUMERIC)
        .setMode(Field.Mode.NULLABLE)
        .build(),
      existing.getFields.get("period")
    )
  }
}
