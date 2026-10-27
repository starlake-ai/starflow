package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{PrimitiveType, Type}
import ai.starlake.utils.YamlSerde
import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import org.apache.hadoop.fs.Path

/** The types of the parameterized BigQuery columns that extract-bq-schema generates (#1853) */
class BigQueryParameterizedTypesSpec extends TestHelper {

  private def numeric(name: String, precision: Long, scale: Option[Long] = None): Field = {
    val builder = Field.newBuilder(name, StandardSQLTypeName.NUMERIC).setPrecision(precision)
    scale.foreach(s => builder.setScale(s))
    builder.build()
  }

  private def withLength(name: String, tpe: StandardSQLTypeName, length: Long): Field =
    Field.newBuilder(name, tpe).setMaxLength(length).build()

  "typeOf" should "give a type to each parameter set, from the built-in type of the column" in {
    def described(field: Field) =
      BigQueryParameterizedTypes
        .typeOf(field)
        .map(t => (t.name, t.primitiveType, t.zone, t.getDdlMappingForDB("bigquery")))
    described(numeric("a", 10, Some(2))) shouldBe Some(
      ("numeric_10_2", PrimitiveType.decimal, Some("10,2"), Some("NUMERIC(10, 2)"))
    )
    described(numeric("a", 10)) shouldBe Some(
      ("numeric_10", PrimitiveType.decimal, Some("10,0"), Some("NUMERIC(10)"))
    )
    described(
      Field
        .newBuilder("a", StandardSQLTypeName.BIGNUMERIC)
        .setPrecision(50L)
        .setScale(20L)
        .build()
    ) shouldBe Some(
      ("bignumeric_50_20", PrimitiveType.decimal, Some("50,20"), Some("BIGNUMERIC(50, 20)"))
    )
    described(withLength("a", StandardSQLTypeName.STRING, 10)) shouldBe Some(
      ("string_10", PrimitiveType.string, None, Some("STRING(10)"))
    )
    described(withLength("a", StandardSQLTypeName.BYTES, 16)) shouldBe Some(
      ("binary_16", PrimitiveType.binary, None, Some("BYTES(16)"))
    )
    // The other engines keep the mapping of the built-in type, and its pattern
    val string10 =
      BigQueryParameterizedTypes.typeOf(withLength("a", StandardSQLTypeName.STRING, 10)).get
    string10.getDdlMappingForDB("postgres") shouldBe Some("varchar(8000)")
    string10.pattern shouldBe ".*"
    // The column that decimal creates, and the columns without parameters
    BigQueryParameterizedTypes.typeOf(numeric("a", 38, Some(9))) shouldBe None
    BigQueryParameterizedTypes.typeOf(Field.of("a", StandardSQLTypeName.NUMERIC)) shouldBe None
    BigQueryParameterizedTypes.typeOf(Field.of("a", StandardSQLTypeName.STRING)) shouldBe None
    BigQueryParameterizedTypes.typeOf(Field.of("a", StandardSQLTypeName.INT64)) shouldBe None
  }

  "typeNameOf" should "reuse a type of the same definition, and suffix a name taken by another" in {
    val project = List(
      // The same definition
      BigQueryParameterizedTypes.typeOf(withLength("a", StandardSQLTypeName.STRING, 10)).get,
      // Another definition
      Type("numeric_10_2", ".*", PrimitiveType.string)
    )
    val types = new BigQueryParameterizedTypes(project)
    types.typeNameOf(withLength("a", StandardSQLTypeName.STRING, 10)) shouldBe Some("string_10")
    types.typeNameOf(numeric("b", 10, Some(2))) shouldBe Some("numeric_10_2_extracted")
    types.typeNameOf(numeric("c", 10, Some(2))) shouldBe Some("numeric_10_2_extracted")
    types.typeNameOf(withLength("d", StandardSQLTypeName.BYTES, 16)) shouldBe Some("binary_16")
    types.typeNameOf(Field.of("e", StandardSQLTypeName.NUMERIC)) shouldBe None
    // Each generated type once, in the order met
    val added = types.takeNewTypes()
    added.map(_.name) shouldBe List("numeric_10_2_extracted", "binary_16")
    BigQueryParameterizedTypes.sameType(
      added.head,
      BigQueryParameterizedTypes.typeOf(numeric("b", 10, Some(2))).get
    ) shouldBe true
    types.takeNewTypes() shouldBe Nil
  }

  "sameType" should "compare the pattern, the zone and the BigQuery type, a NUMERIC(P) being a NUMERIC(P, 0)" in {
    val numeric10 = BigQueryParameterizedTypes.typeOf(numeric("a", 10)).get
    val numeric10Scale0 = BigQueryParameterizedTypes.typeOf(numeric("a", 10, Some(0))).get
    BigQueryParameterizedTypes.sameType(numeric10, numeric10Scale0) shouldBe true
    BigQueryParameterizedTypes.sameType(numeric10, numeric10.copy(pattern = "\\d+")) shouldBe false
    BigQueryParameterizedTypes.sameType(
      numeric10,
      numeric10.copy(zone = Some("10, 0"))
    ) shouldBe true
    BigQueryParameterizedTypes.isParameterized(numeric10) shouldBe true
    BigQueryParameterizedTypes.isParameterized(
      BigQueryParameterizedTypes.builtIn("numeric")
    ) shouldBe
    false
  }

  it should "generate a type from the type of the project that a built-in type names" in {
    val projectString = Type(
      "string",
      "[A-Z]*",
      PrimitiveType.string,
      ddlMapping = Some(Map("bigquery" -> "STRING", "postgres" -> "TEXT"))
    )
    val string10 = BigQueryParameterizedTypes
      .typeOf(withLength("a", StandardSQLTypeName.STRING, 10), _ => projectString)
      .get
    (string10.pattern, string10.ddlMapping) shouldBe (
      "[A-Z]*",
      Some(Map("bigquery" -> "STRING(10)", "postgres" -> "TEXT"))
    )
  }

  new WithSettings() {
    private val typesPath = new Path(DatasetArea.types, "types.sl.yml")

    private def projectTypes(): List[Type] =
      YamlSerde.deserializeYamlTypes(storageHandler.read(typesPath), typesPath.toString)

    private val generated = List(
      BigQueryParameterizedTypes.typeOf(numeric("a", 10, Some(2))).get,
      BigQueryParameterizedTypes.typeOf(withLength("b", StandardSQLTypeName.STRING, 10)).get,
      BigQueryParameterizedTypes.typeOf(withLength("c", StandardSQLTypeName.BYTES, 16)).get
    )

    "addToProject" should "create the types file of the project" in {
      storageHandler.delete(typesPath)
      BigQueryParameterizedTypes.addToProject(generated)
      projectTypes() shouldBe generated
      val schemaHandler = settings.schemaHandler(reload = true)
      schemaHandler.types().map(_.name) should contain allOf (
        "numeric_10_2", "string_10", "binary_16"
      )
      // The copies of the built-in types do not find the type of a SQL column: binary_16 would
      // be the first one of a Snowflake BINARY column
      val lookupTypes = schemaHandler.typesForSqlTypeLookup().map(_.name)
      lookupTypes should contain allOf ("string", "decimal")
      lookupTypes should contain noneOf ("numeric_10_2", "string_10", "binary_16", "binary")
    }

    it should "append the types to the text of the types file, keeping its comments" in {
      val existing =
        """---
          |# The types of the project
          |version: 1
          |types:
          |    # An email
          |    - name: "email"
          |      primitiveType: "string"
          |      pattern: ".+@.+"
          |""".stripMargin
      storageHandler.write(existing, typesPath)
      BigQueryParameterizedTypes.addToProject(generated)
      val text = storageHandler.read(typesPath)
      text should startWith(existing)
      text should include("\n    - name: \"numeric_10_2\"\n      primitiveType: \"decimal\"\n")
      projectTypes().map(_.name) shouldBe List("email", "numeric_10_2", "string_10", "binary_16")
      projectTypes().tail shouldBe generated
    }

    it should "not add a type that the file already has" in {
      storageHandler.delete(typesPath)
      BigQueryParameterizedTypes.addToProject(generated)
      val text = storageHandler.read(typesPath)
      BigQueryParameterizedTypes.addToProject(generated)
      storageHandler.read(typesPath) shouldBe text
    }

    it should "write the types file again when its text cannot be appended to" in {
      storageHandler.write("version: 1\ntypes: []\n", typesPath)
      BigQueryParameterizedTypes.addToProject(generated)
      projectTypes() shouldBe generated
    }
  }
}
