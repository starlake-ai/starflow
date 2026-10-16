package ai.starlake.extract

import ai.starlake.schema.model.JDBCSchema
import ai.starlake.utils.YamlSerde
import com.google.cloud.bigquery._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ExtractBigQuerySchemaSpec extends AnyFlatSpec with Matchers {

  private val bqSchema = Schema.of(
    Field
      .newBuilder("id", StandardSQLTypeName.INT64)
      .setMode(Field.Mode.REQUIRED)
      .setDescription("Identifier")
      .build(),
    Field
      .newBuilder("name", StandardSQLTypeName.STRING)
      .setMode(Field.Mode.NULLABLE)
      .setDescription("Name")
      .build(),
    Field
      .newBuilder("amount", StandardSQLTypeName.NUMERIC)
      .setMode(Field.Mode.NULLABLE)
      .build(),
    Field
      .newBuilder("tags", StandardSQLTypeName.STRING)
      .setMode(Field.Mode.REPEATED)
      .build()
  )

  private def tableInfo(name: String, definition: TableDefinition): TableInfo =
    TableInfo
      .newBuilder(TableId.of("my_dataset", name), definition)
      .setDescription(s"$name description")
      .build()

  private val table = tableInfo("my_table", StandardTableDefinition.of(bqSchema))
  private val view =
    tableInfo("my_view", ViewDefinition.newBuilder("SELECT 1").setSchema(bqSchema).build())
  private val materializedView = tableInfo(
    "my_materialized_view",
    MaterializedViewDefinition.newBuilder("SELECT 1").setSchema(bqSchema).build()
  )
  private val externalTable = tableInfo(
    "my_external_table",
    ExternalTableDefinition
      .newBuilder("gs://my-bucket/*.parquet", bqSchema, FormatOptions.parquet())
      .build()
  )
  private val snapshot = tableInfo(
    "my_snapshot",
    SnapshotTableDefinition
      .newBuilder()
      .setBaseTableId(TableId.of("my_dataset", "my_table"))
      .setSnapshotTime("2026-10-14T00:00:00Z")
      .setSchema(bqSchema)
      .build()
  )
  private val allObjects = List(table, view, materializedView)

  /** listTables returns the object type without the schema */
  private def listed(table: TableInfo): TableInfo =
    table.toBuilder
      .setDefinition(
        table.getDefinition[TableDefinition]().getType match {
          case TableDefinition.Type.VIEW => ViewDefinition.of("SELECT 1")
          case TableDefinition.Type.MATERIALIZED_VIEW =>
            MaterializedViewDefinition.of("SELECT 1")
          case _ => StandardTableDefinition.newBuilder().build()
        }
      )
      .build()

  private def getTable(tables: TableInfo*)(tableId: TableId): TableInfo =
    tables.find(_.getTableId == tableId).orNull

  /** Dry run of views returning the given effective schemas, recording the dry run objects */
  private class DryRun(schemas: (String, Schema)*) extends (TableInfo => Schema) {
    val dryRunTables = scala.collection.mutable.ListBuffer.empty[String]
    def apply(view: TableInfo): Schema = {
      dryRunTables += view.getTableId.getTable()
      schemas.toMap.getOrElse(view.getTableId.getTable(), null)
    }
  }

  /** The schema of the view once its underlying table changed: amount is now an INT64 */
  private val effectiveSchema = Schema.of(
    Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
    Field.of("name", StandardSQLTypeName.STRING),
    Field.of("amount", StandardSQLTypeName.INT64),
    Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()
  )

  "parseTableTypes" should "default to tables only" in {
    ExtractBigQuerySchema.parseTableTypes(Nil) shouldBe Set(TableDefinition.Type.TABLE)
  }

  it should "be case insensitive and accept spaces instead of underscores" in {
    ExtractBigQuerySchema.parseTableTypes(
      List("table", " View ", "materialized  view", "external", "Snapshot")
    ) shouldBe Set(
      TableDefinition.Type.TABLE,
      TableDefinition.Type.VIEW,
      TableDefinition.Type.MATERIALIZED_VIEW,
      TableDefinition.Type.EXTERNAL,
      TableDefinition.Type.SNAPSHOT
    )
  }

  it should "ignore table types not supported by BigQuery" in {
    ExtractBigQuerySchema.parseTableTypes(List("TABLE", "SYSTEM TABLE", "SYNONYM")) shouldBe Set(
      TableDefinition.Type.TABLE
    )
  }

  it should "fail when no table type is supported by BigQuery" in {
    an[IllegalArgumentException] should be thrownBy
    ExtractBigQuerySchema.parseTableTypes(List("SYSTEM TABLE"))
  }

  "isExtractable" should "skip views and materialized views by default" in {
    val tableTypes = ExtractBigQuerySchema.parseTableTypes(Nil)
    allObjects.filter(ExtractBigQuerySchema.isExtractable(_, tableTypes)) shouldBe List(table)
  }

  it should "keep the configured table types" in {
    val tableTypes = ExtractBigQuerySchema.parseTableTypes(List("VIEW", "MATERIALIZED_VIEW"))
    allObjects.filter(ExtractBigQuerySchema.isExtractable(_, tableTypes)) shouldBe List(
      view,
      materializedView
    )
  }

  "extractTable" should "read the same schema from tables, views and materialized views" in {
    val schemas = allObjects.flatMap(ExtractBigQuerySchema.extractTable)
    schemas.map(_.name) shouldBe List("my_table", "my_view", "my_materialized_view")
    schemas.map(_.comment) shouldBe List(
      Some("my_table description"),
      Some("my_view description"),
      Some("my_materialized_view description")
    )
    val attributes = schemas.map(_.attributes)
    attributes.distinct should have size 1
    attributes.head.map(a => (a.name, a.`type`, a.required, a.array, a.comment)) shouldBe List(
      ("id", "long", Some(true), None, Some("Identifier")),
      ("name", "string", Some(false), None, Some("Name")),
      ("amount", "decimal", Some(false), None, None),
      ("tags", "string", Some(false), Some(true), None)
    )
  }

  it should "read the schema of external tables and snapshots" in {
    List(externalTable, snapshot)
      .flatMap(ExtractBigQuerySchema.extractTable)
      .map(_.attributes) shouldBe List.fill(2)(
      ExtractBigQuerySchema.extractTable(table).map(_.attributes).getOrElse(Nil)
    )
  }

  it should "skip definitions without schema" in {
    ExtractBigQuerySchema.extractTable(
      tableInfo("no_schema", ViewDefinition.of("SELECT 1"))
    ) shouldBe None
  }

  "extractTables" should "extract tables only by default" in {
    ExtractBigQuerySchema
      .extractTables(
        allObjects.map(listed),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        getTable(allObjects: _*),
        new DryRun()
      )
      .map(_.name) shouldBe List("my_table")
  }

  it should "extract tables, views and materialized views with identical schemas" in {
    val schemas = ExtractBigQuerySchema.extractTables(
      allObjects.map(listed),
      ExtractBigQuerySchema.parseTableTypes(List("TABLE", "VIEW", "MATERIALIZED_VIEW")),
      getTable(allObjects: _*),
      new DryRun("my_view" -> bqSchema)
    )
    schemas.map(_.name) shouldBe List("my_table", "my_view", "my_materialized_view")
    schemas.map(_.attributes).distinct should have size 1
  }

  it should "check the type of the fetched object when the listing does not tell it" in {
    // An EXTERNAL object listed as a standard table
    ExtractBigQuerySchema
      .extractTables(
        List(listed(externalTable)),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        getTable(externalTable),
        new DryRun()
      ) shouldBe Nil
  }

  it should "skip objects that cannot be fetched or converted" in {
    val unsupportedColumn = tableInfo(
      "unsupported_column",
      StandardTableDefinition.of(
        Schema.of(Field.of("bytes", StandardSQLTypeName.BYTES))
      )
    )
    val failing = tableInfo("failing", StandardTableDefinition.of(bqSchema))
    val deleted = tableInfo("deleted", StandardTableDefinition.of(bqSchema))
    ExtractBigQuerySchema
      .extractTables(
        List(failing, deleted, unsupportedColumn, table).map(listed),
        ExtractBigQuerySchema.parseTableTypes(Nil),
        tableId =>
          if (tableId.getTable() == "failing") throw new BigQueryException(500, "failure")
          else getTable(unsupportedColumn, table)(tableId),
        new DryRun()
      )
      .map(_.name) shouldBe List("my_table")
  }

  it should "read the types of views from their effective schema, keeping their descriptions" in {
    val dryRun = new DryRun("my_view" -> effectiveSchema)
    val schemas = ExtractBigQuerySchema.extractTables(
      List(table, view, materializedView).map(listed),
      ExtractBigQuerySchema.parseTableTypes(List("TABLE", "VIEW", "MATERIALIZED_VIEW")),
      getTable(table, view, materializedView),
      dryRun
    )
    dryRun.dryRunTables shouldBe List("my_view")
    schemas.tail.map(schema =>
      schema.name -> schema.attributes.map(a => (a.name, a.`type`, a.comment))
    ) shouldBe List(
      "my_view" -> List(
        ("id", "long", Some("Identifier")),
        ("name", "string", Some("Name")),
        ("amount", "long", None),
        ("tags", "string", None)
      ),
      // Materialized views are not dry run: their data is stored with their schema
      "my_materialized_view" -> List(
        ("id", "long", Some("Identifier")),
        ("name", "string", Some("Name")),
        ("amount", "decimal", None),
        ("tags", "string", None)
      )
    )
  }

  it should "fall back to the stored schema of a view only when its dry run fails" in {
    def extractView(getViewSchema: TableInfo => Schema): List[String] =
      ExtractBigQuerySchema
        .extractTables(
          List(listed(view)),
          ExtractBigQuerySchema.parseTableTypes(List("VIEW")),
          getTable(view),
          getViewSchema
        )
        .flatMap(_.attributes.map(_.`type`))
    val storedTypes = List("long", "string", "decimal", "string")
    // No bigquery.jobs.create permission
    extractView(_ => throw new BigQueryException(403, "Access Denied")) shouldBe storedTypes
    // No schema
    extractView(new DryRun()) shouldBe storedTypes
    extractView(_ => Schema.of()) shouldBe storedTypes
    // An effective type that cannot be converted fails the view instead of extracting stale types
    extractView(_ => Schema.of(Field.of("id", StandardSQLTypeName.BYTES))) shouldBe Nil
  }

  it should "not dry run legacy SQL views, whose dry runs return no schema" in {
    val legacySqlView = tableInfo(
      "my_view",
      ViewDefinition.newBuilder("SELECT 1").setUseLegacySql(true).setSchema(bqSchema).build()
    )
    val dryRun = new DryRun("my_view" -> effectiveSchema)
    ExtractBigQuerySchema
      .extractTables(
        List(listed(legacySqlView)),
        ExtractBigQuerySchema.parseTableTypes(List("VIEW")),
        getTable(legacySqlView),
        dryRun
      )
      .flatMap(_.attributes.map(_.`type`)) shouldBe List("long", "string", "decimal", "string")
    dryRun.dryRunTables shouldBe Nil
  }

  "dryRunJob" should "query the view in standard SQL, the dataset location and the given project" in {
    val standardSqlView = tableInfo(
      "my_view",
      ViewDefinition.newBuilder("SELECT 1").setUseLegacySql(false).build()
    ).toBuilder.setTableId(TableId.of("source-project", "my_dataset", "my_view")).build()
    val job = ExtractBigQuerySchema.dryRunJob(standardSqlView, "billing-project", Some("EU"))
    job.getJobId.getProject shouldBe "billing-project"
    job.getJobId.getLocation shouldBe "EU"
    val query = job.getConfiguration[QueryJobConfiguration]()
    query.getQuery shouldBe "SELECT * FROM `source-project.my_dataset.my_view`"
    query.dryRun() shouldBe true
    query.useLegacySql() shouldBe false
  }

  "mergeViewSchema" should "take the effective types and the stored descriptions at every level" in {
    val stored = Schema.of(
      Field.newBuilder("ID", StandardSQLTypeName.NUMERIC).setDescription("Identifier").build(),
      Field
        .newBuilder(
          "address",
          StandardSQLTypeName.STRUCT,
          Field.newBuilder("zip", StandardSQLTypeName.BYTES).setDescription("Zip code").build()
        )
        .setDescription("Address")
        .build(),
      Field.of("dropped", StandardSQLTypeName.STRING)
    )
    val effective = Schema.of(
      Field.of("id", StandardSQLTypeName.INT64),
      Field.of(
        "address",
        StandardSQLTypeName.STRUCT,
        Field.of("zip", StandardSQLTypeName.STRING)
      ),
      Field.of("added", StandardSQLTypeName.DATE)
    )
    val merged =
      ExtractBigQuerySchema.mergeViewSchema("my_dataset.my_view", Some(stored), effective)
    def describe(fields: FieldList): List[(String, String, String)] =
      fields.asScala.toList.flatMap { field =>
        (field.getName, field.getType.name(), field.getDescription) ::
        Option(field.getSubFields).map(describe).getOrElse(Nil)
      }
    describe(merged.getFields) shouldBe List(
      ("id", "INTEGER", "Identifier"),
      ("address", "RECORD", "Address"),
      ("zip", "STRING", "Zip code"),
      ("added", "DATE", null)
    )
  }

  it should "prefer the description of the view column to the one of the underlying column" in {
    val merged = ExtractBigQuerySchema.mergeViewSchema(
      "my_dataset.my_view",
      Some(
        Schema.of(Field.newBuilder("id", StandardSQLTypeName.INT64).setDescription("View").build())
      ),
      Schema.of(Field.newBuilder("id", StandardSQLTypeName.INT64).setDescription("Table").build())
    )
    merged.getFields.get("id").getDescription shouldBe "View"
  }

  it should "keep the effective schema as is when the stored one is missing" in {
    ExtractBigQuerySchema.mergeViewSchema(
      "my_dataset.my_view",
      None,
      effectiveSchema
    ) shouldBe effectiveSchema
  }

  "extract-bq-schema" should "parse the --table-types option" in {
    ExtractBigQuerySchemaCmd
      .parse(Seq("--tables", "my_dataset", "--table-types", "TABLE,VIEW"))
      .map(_.tableTypes) shouldBe Some(List("TABLE", "VIEW"))
    ExtractBigQuerySchemaCmd.parse(Seq("--tables", "my_dataset")).map(_.tableTypes) shouldBe Some(
      Nil
    )
  }

  "fromExtractSchemaConfig" should "propagate the table types and the connection" in {
    val config = ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(
      ExtractSchemaConfig(connectionRef = Some("bigquery-source"), accessToken = Some("token")),
      JDBCSchema(schema = "my_dataset", tableTypes = List("TABLE", "VIEW"))
    )
    config.tableTypes shouldBe List("TABLE", "VIEW")
    config.connectionRef shouldBe Some("bigquery-source")
    config.accessToken shouldBe Some("token")
  }

  it should "prefer the connection of the extract config to the one of the command" in {
    def connectionRef(command: Option[String], extractConfig: Option[String]): Option[String] =
      ExtractBigQuerySchemaCmd
        .fromExtractSchemaConfig(
          ExtractSchemaConfig(connectionRef = command),
          JDBCSchema(schema = "my_dataset"),
          extractConfig
        )
        .connectionRef
    connectionRef(Some("command"), Some("extract-config")) shouldBe Some("extract-config")
    connectionRef(Some("command"), None) shouldBe Some("command")
    connectionRef(None, None) shouldBe None
  }

  it should "keep extracting tables only when the table types are the JDBC default ones" in {
    val extractConfig = YamlSerde.deserializeYamlExtractConfig(
      """version: 1
        |extract:
        |  connectionRef: "bigquery-source"
        |  default:
        |    tableRemarks: "ignored"
        |  jdbcSchemas:
        |    - schema: "dataset_with_default_types"
        |    - schema: "dataset_with_views"
        |      tableTypes: ["TABLE", "VIEW", "MATERIALIZED VIEW"]
        |""".stripMargin,
      "extract.sl.yml"
    )
    val tableTypes = extractConfig.jdbcSchemas.getOrElse(Nil).map { jdbcSchema =>
      ExtractBigQuerySchemaCmd.fromExtractSchemaConfig(ExtractSchemaConfig(), jdbcSchema).tableTypes
    }
    tableTypes shouldBe List(Nil, List("TABLE", "VIEW", "MATERIALIZED VIEW"))
    tableTypes.map(ExtractBigQuerySchema.parseTableTypes) shouldBe List(
      Set(TableDefinition.Type.TABLE),
      Set(
        TableDefinition.Type.TABLE,
        TableDefinition.Type.VIEW,
        TableDefinition.Type.MATERIALIZED_VIEW
      )
    )
  }

  "schemaDifferences" should "list type, parameter and mode changes, new and missing columns" in {
    ExtractBigQuerySchema.schemaDifferences(
      List(
        Field.of("id", StandardSQLTypeName.NUMERIC),
        Field
          .newBuilder("amount", StandardSQLTypeName.NUMERIC)
          .setPrecision(10L)
          .setScale(2L)
          .build(),
        Field.of("record", StandardSQLTypeName.STRUCT, Field.of("zip", StandardSQLTypeName.BYTES)),
        Field.of("dropped", StandardSQLTypeName.STRING)
      ),
      List(
        Field.newBuilder("ID", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build(),
        Field.of("amount", StandardSQLTypeName.NUMERIC),
        Field.of("record", StandardSQLTypeName.STRUCT, Field.of("zip", StandardSQLTypeName.STRING)),
        Field.of("added", StandardSQLTypeName.DATE)
      )
    ) shouldBe List(
      "ID NUMERIC -> NUMERIC REQUIRED",
      "amount NUMERIC(10,2) -> NUMERIC",
      "record.zip BYTES -> STRING",
      "new column added",
      "missing column dropped"
    )
  }
}
