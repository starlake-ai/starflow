package ai.starlake.schema.model

import ai.starlake.extract.ExtractBigQuerySchema
import ai.starlake.schema.handlers.SchemaHandler
import com.google.cloud.bigquery.{Field, FieldElementType, Schema, StandardSQLTypeName}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** On BigQuery, the sync of the attributes of a task types the columns of its SELECT from a dry
  * run, with the types of extract-bq-schema, and keeps the type of an attribute that creates the
  * same BigQuery column (#1950)
  */
class BigQuerySyncTypesSpec extends AnyFlatSpec with Matchers {

  "ExtractBigQuerySchema.queryAttributes" should "name the columns of a dry run after the built-in type of their BigQuery type" in {
    val schema = Schema.of(
      Field
        .newBuilder("id", StandardSQLTypeName.INT64)
        .setMode(Field.Mode.REQUIRED)
        .setDescription("not written by the sync")
        .build(),
      Field.of("updated_at", StandardSQLTypeName.DATETIME),
      Field.of("loaded_at", StandardSQLTypeName.TIMESTAMP),
      Field.of("amount", StandardSQLTypeName.NUMERIC),
      Field.of("big_amount", StandardSQLTypeName.BIGNUMERIC),
      Field.of("ratio", StandardSQLTypeName.FLOAT64),
      Field.of("flag", StandardSQLTypeName.BOOL),
      Field.of("day", StandardSQLTypeName.DATE),
      Field.of("at", StandardSQLTypeName.TIME),
      Field.of("payload", StandardSQLTypeName.JSON),
      Field.of("place", StandardSQLTypeName.GEOGRAPHY),
      Field.of("raw", StandardSQLTypeName.BYTES),
      Field.of("duration", StandardSQLTypeName.INTERVAL),
      Field
        .newBuilder("period", StandardSQLTypeName.RANGE)
        .setRangeElementType(FieldElementType.newBuilder().setType("DATE").build())
        .build(),
      Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build(),
      Field.of(
        "address",
        StandardSQLTypeName.STRUCT,
        Field.of("city", StandardSQLTypeName.STRING),
        Field.newBuilder("zip", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
      )
    )
    ExtractBigQuerySchema.queryAttributes(schema, identity) shouldBe List(
      TableAttribute("id", "long"),
      TableAttribute("updated_at", "datetime"),
      TableAttribute("loaded_at", "timestamp"),
      TableAttribute("amount", "numeric"),
      TableAttribute("big_amount", "bignumeric"),
      TableAttribute("ratio", "double"),
      TableAttribute("flag", "boolean"),
      TableAttribute("day", "date"),
      TableAttribute("at", "time"),
      TableAttribute("payload", "variant"),
      TableAttribute("place", "geography"),
      TableAttribute("raw", "binary"),
      TableAttribute("duration", "interval"),
      TableAttribute("period", "range_date"),
      TableAttribute("tags", "string", array = Some(true)),
      TableAttribute(
        "address",
        "struct",
        attributes = List(TableAttribute("city", "string"), TableAttribute("zip", "long"))
      )
    )
    // The types of the project that replace the built-in ones
    ExtractBigQuerySchema
      .queryAttributes(
        Schema.of(Field.of("id", StandardSQLTypeName.INT64)),
        name => if (name == "long") "bigint" else name
      )
      .map(_.`type`) shouldBe List("bigint")
  }

  "AutoTaskInfo.sameBigQueryType" should "compare the BigQuery columns that two types create" in {
    val sameType = AutoTaskInfo.sameBigQueryType(
      AutoTaskInfo.bigQueryTypeOf(
        SchemaHandler.builtInTypes :+ Type(
          name = "decimal_10_2",
          primitiveType = PrimitiveType.decimal,
          pattern = "[-+]?\\d*\\.?\\d+",
          ddlMapping = Some(Map("bigquery" -> "NUMERIC(10, 2)"))
        )
      )
    ) _
    def same(existing: String, synced: String, array: Boolean = false): Boolean =
      sameType(
        TableAttribute("c", existing, array = if (array) Some(true) else None),
        TableAttribute("c", synced)
      )
    // INT64 and NUMERIC
    same("integer", "long") shouldBe true
    same("int", "long") shouldBe true
    same("decimal", "numeric") shouldBe true
    // A dry run gives no parameters: the declared ones are kept
    same("decimal_10_2", "numeric") shouldBe true
    same("struct", "struct") shouldBe true
    same("range_date", "range_date") shouldBe true
    // Other BigQuery types
    same("timestamp", "datetime") shouldBe false
    same("decimal", "bignumeric") shouldBe false
    same("string", "long") shouldBe false
    same("range_date", "range_timestamp") shouldBe false
    // A type that the project does not know is compared by its name
    same("unknown", "long") shouldBe false
    // A repeated column
    same("string", "string", array = true) shouldBe false
    sameType(
      TableAttribute("c", "string", array = Some(true)),
      TableAttribute("c", "string", array = Some(true))
    ) shouldBe true
  }

  "AutoTaskInfo.syncAttribute" should "sync the type, mode and fields of an attribute, and keep what does not change" in {
    val sameType =
      AutoTaskInfo.sameBigQueryType(AutoTaskInfo.bigQueryTypeOf(SchemaHandler.builtInTypes)) _
    def sync(existing: TableAttribute, synced: TableAttribute) =
      AutoTaskInfo.syncAttribute(existing, synced, sameType)
    // The same column
    sync(
      TableAttribute("id", "integer", comment = Some("the key")),
      TableAttribute("id", "long")
    ) shouldBe None
    // A change of mode only keeps the type of the attribute
    sync(TableAttribute("id", "integer"), TableAttribute("id", "long", array = Some(true))) shouldBe
    Some(TableAttribute("id", "integer", array = Some(true)))
    // The fields of a struct: a field whose type changes, a field added, a field removed, the
    // descriptions kept
    val address = TableAttribute(
      "address",
      "struct",
      comment = Some("where"),
      attributes = List(
        TableAttribute("city", "string", comment = Some("the city")),
        TableAttribute("moved_at", "timestamp"),
        TableAttribute("obsolete", "string")
      )
    )
    sync(
      address,
      TableAttribute(
        "address",
        "struct",
        attributes = List(
          TableAttribute("city", "string"),
          TableAttribute("moved_at", "datetime"),
          TableAttribute("zip", "long")
        )
      )
    ) shouldBe Some(
      address.copy(attributes =
        List(
          TableAttribute("city", "string", comment = Some("the city")),
          TableAttribute("moved_at", "datetime"),
          TableAttribute("zip", "long")
        )
      )
    )
    // The same fields
    sync(
      address,
      TableAttribute(
        "address",
        "struct",
        attributes = address.attributes.map(_.copy(comment = None))
      )
    ) shouldBe None
    // A struct that is no longer one has no field
    sync(address, TableAttribute("address", "variant")) shouldBe Some(
      address.copy(`type` = "variant", attributes = Nil)
    )
    // An engine that does not give the fields of its struct columns keeps them
    sync(address, TableAttribute("address", "struct")) shouldBe None
  }

  "AutoTaskInfo.fieldsOf" should "give the fields of an attribute updated by the sync" in {
    val existing =
      TableAttribute("address", "struct", attributes = List(TableAttribute("city", "string")))
    val zip = TableAttribute("zip", "long")
    AutoTaskInfo.fieldsOf(existing, existing.copy(attributes = List(zip))) shouldBe List(zip)
    AutoTaskInfo.fieldsOf(existing, existing.copy(attributes = Nil)) shouldBe existing.attributes
    AutoTaskInfo.fieldsOf(existing, TableAttribute("address", "string")) shouldBe Nil
    // A field keeps what the sync does not give, as an attribute does
    val written = TableAttribute(
      "address",
      "struct",
      attributes = List(
        TableAttribute("city", "string", required = Some(true), rename = Some("town"))
      )
    )
    AutoTaskInfo.fieldsOf(
      written,
      written.copy(attributes = List(TableAttribute("city", "variant", comment = Some("where"))))
    ) shouldBe List(
      TableAttribute(
        "city",
        "variant",
        required = Some(true),
        rename = Some("town"),
        comment = Some("where")
      )
    )
  }
}
