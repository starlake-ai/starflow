package ai.starlake.semantic

import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TMDLConverterSpec extends AnyFlatSpec with Matchers {

  private val modelYaml =
    """name: ecommerce_analytics
      |description: E-commerce analytics model
      |tables:
      |  - name: orders
      |    description: Order transactions
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: ORDERS
      |    dimensions:
      |      - name: order_id
      |        expr: ORDER_ID
      |        data_type: NUMBER
      |      - name: order_status
      |        data_type: TEXT
      |        synonyms: ["status"]
      |    time_dimensions:
      |      - name: order_date
      |        expr: ORDER_DATE
      |        data_type: DATE
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |        synonyms: ["AOV"]
      |      - name: order_count
      |        expr: COUNT(*)
      |      - name: last_order_date
      |        expr: MAX(order_date)
      |    filters:
      |      - name: recent_orders
      |        expr: order_date >= CURRENT_DATE - 30
      |    primary_key:
      |      columns: [order_id]
      |  - name: customers
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: CUSTOMERS
      |    dimensions:
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    primary_key:
      |      columns: [customer_id]
      |metrics:
      |  - name: customer_count
      |    expr: COUNT(DISTINCT customers.customer_id)
      |  - name: total_revenue
      |    expr: SUM(a)/NULLIF(SUM(b),0)
      |verified_queries:
      |  - name: top
      |    question: Who?
      |    sql: SELECT 1
      |""".stripMargin

  private def files(): Map[String, String] =
    TMDLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), None)
      .toMap

  "convert" should "produce database, model and one table file per table" in {
    files().keySet shouldBe Set(
      "database.tmdl",
      "model.tmdl",
      "tables/orders.tmdl",
      "tables/customers.tmdl"
    )
  }

  it should "render database.tmdl and model.tmdl" in {
    val db = files()("database.tmdl")
    db should include("database ecommerce_analytics")
    db should include("\tcompatibilityLevel: 1600")
    val model = files()("model.tmdl")
    model should include("/// E-commerce analytics model")
    model should include("model Model")
    model should include("\tculture: en-US")
  }

  it should "render columns with dataType, sourceColumn, summarizeBy and isKey" in {
    val orders = files()("tables/orders.tmdl")
    orders should include("/// Order transactions")
    orders should include("table orders")
    orders should include("\tcolumn order_id")
    orders should include("\t\tdataType: decimal")
    orders should include("\t\tisKey")
    orders should include("\t\tsourceColumn: order_id")
    orders should include("\t/// Synonyms: status")
    orders should include("\tcolumn order_status")
    orders should include("\t\tdataType: string")
    orders should include("\tcolumn order_date")
    orders should include("\t\tdataType: dateTime")
    orders should include("\tcolumn order_total")
    orders should include("\t\tsummarizeBy: sum")
    val customers = files()("tables/customers.tmdl")
    customers should include("\t\tisKey")
  }

  it should "give summarizeBy none to dimensions and time dimensions" in {
    val orders = files()("tables/orders.tmdl")
    // 3 non-fact columns -> three "none" lines; the single fact is "sum"
    orders.split("\n").count(_ == "\t\tsummarizeBy: none") shouldBe 3
    orders.split("\n").count(_ == "\t\tsummarizeBy: sum") shouldBe 1
  }

  it should "quote names that are not plain identifiers" in {
    val yaml =
      """name: Mixed Model
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: item id
        |        data_type: NUMBER
        |""".stripMargin
    val out = TMDLConverter.convert("Mixed Model", YamlSerde.mapper.readTree(yaml), None).toMap
    out.keySet should contain("tables/Order Items.tmdl")
    val table = out("tables/Order Items.tmdl")
    table should include("table 'Order Items'")
    table should include("\tcolumn 'item id'")
    out("database.tmdl") should include("database 'Mixed Model'")
    table should include(
      "Value.NativeQuery(Source, \"SELECT \"\"item id\"\" FROM Order Items\")"
    )
  }

  it should "skip isKey for composite primary keys" in {
    val yaml =
      """name: composite
        |tables:
        |  - name: line_items
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |      - name: line_no
        |        data_type: NUMBER
        |    primary_key:
        |      columns: [order_id, line_no]
        |""".stripMargin
    val table =
      TMDLConverter
        .convert("composite", YamlSerde.mapper.readTree(yaml), None)
        .toMap
        .apply("tables/line_items.tmdl")
    table should not include "isKey"
  }

  it should "default unknown data types to string" in {
    val yaml =
      """name: unknowns
        |tables:
        |  - name: t
        |    dimensions:
        |      - name: mystery
        |        data_type: GEOGRAPHY
        |      - name: untyped
        |""".stripMargin
    val table =
      TMDLConverter
        .convert("unknowns", YamlSerde.mapper.readTree(yaml), None)
        .toMap
        .apply("tables/t.tmdl")
    table.split("\n").count(_ == "\t\tdataType: string") shouldBe 2
  }

  private def pg: Option[ConnectionInfo] = Some(
    ConnectionInfo(
      `type` = ConnectionType.JDBC,
      options = Map("url" -> "jdbc:postgresql://myhost:5432/mydb")
    )
  )

  "partitions" should "emit an import-mode native query with expr AS name projections" in {
    val orders =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), pg)
        .toMap
        .apply("tables/orders.tmdl")
    orders should include("\tpartition orders = m")
    orders should include("\t\tmode: import")
    orders should include("\t\tsource =")
    orders should include("\t\t\tlet")
    orders should include("\t\t\t\tSource = PostgreSQL.Database(\"myhost:5432\", \"mydb\"),")
    orders should include(
      "\t\t\t\tResult = Value.NativeQuery(Source, \"SELECT ORDER_ID AS order_id, order_status, ORDER_DATE AS order_date, ORDER_TOTAL AS order_total FROM ANALYTICS_DB.ECOMMERCE.ORDERS\")"
    )
    orders should include("\t\t\tin")
    orders should include("\t\t\t\tResult")
  }

  it should "emit SELECT * when the table has no fields and fall back to the table name" in {
    val yaml =
      """name: bare
        |tables:
        |  - name: audit_log
        |""".stripMargin
    val table = TMDLConverter
      .convert("bare", YamlSerde.mapper.readTree(yaml), pg)
      .toMap
      .apply("tables/audit_log.tmdl")
    table should include("Value.NativeQuery(Source, \"SELECT * FROM audit_log\")")
  }

  it should "derive the snowflake source with database navigation" in {
    val sf = Some(
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map(
          "url"       -> "jdbc:snowflake://acme.snowflakecomputing.com",
          "warehouse" -> "COMPUTE_WH"
        )
      )
    )
    val orders =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), sf)
        .toMap
        .apply("tables/orders.tmdl")
    orders should include(
      "\t\t\t\tSource = Snowflake.Databases(\"acme.snowflakecomputing.com\", \"COMPUTE_WH\"),"
    )
    orders should include("\t\t\t\tDB = Source{[Name=\"ANALYTICS_DB\"]}[Data],")
    orders should include("Result = Value.NativeQuery(DB, ")
  }

  it should "derive the bigquery source from the billing project option" in {
    val bq = Some(
      ConnectionInfo(`type` = ConnectionType.BQ, options = Map("gcpProjectId" -> "my-project"))
    )
    val orders =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), bq)
        .toMap
        .apply("tables/orders.tmdl")
    orders should include(
      "\t\t\t\tSource = GoogleBigQuery.Database([BillingProject=\"my-project\"]),"
    )
  }

  it should "derive the sqlserver source from a semicolon-style JDBC url" in {
    val withPort = Some(
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map("url" -> "jdbc:sqlserver://sqlhost:1433;databaseName=mydb")
      )
    )
    val orders = TMDLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), withPort)
      .toMap
      .apply("tables/orders.tmdl")
    orders should include("\t\t\t\tSource = Sql.Database(\"sqlhost\", \"mydb\"),")

    val noPort = Some(
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map("url" -> "jdbc:sqlserver://sqlhost;databaseName=mydb")
      )
    )
    val orders2 = TMDLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), noPort)
      .toMap
      .apply("tables/orders.tmdl")
    orders2 should include("\t\t\t\tSource = Sql.Database(\"sqlhost\", \"mydb\"),")
  }

  it should "fall back to a generic TODO source for missing or unmapped connections" in {
    val none =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), None)
        .toMap
        .apply("tables/orders.tmdl")
    none should include("\t\t\t\t// TODO Starflow: set the connector for your warehouse")
    none should include("\t\t\t\tSource = Sql.Database(\"SERVER_TODO\", \"DATABASE_TODO\"),")

    val duck = Some(
      ConnectionInfo(`type` = ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:/tmp/db"))
    )
    val d =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), duck)
        .toMap
        .apply("tables/orders.tmdl")
    d should include("// TODO Starflow: set the connector for your warehouse")
  }

  it should "double embedded double quotes in the M query string" in {
    val yaml =
      """name: esc
        |tables:
        |  - name: t
        |    dimensions:
        |      - name: trimmed
        |        expr: TRIM("COL")
        |        data_type: TEXT
        |""".stripMargin
    val table = TMDLConverter
      .convert("esc", YamlSerde.mapper.readTree(yaml), pg)
      .toMap
      .apply("tables/t.tmdl")
    table should include("SELECT TRIM(\"\"COL\"\") AS trimmed FROM t")
  }

  "measures" should "translate simple aggregates to DAX" in {
    val orders = files()("tables/orders.tmdl")
    orders should include("\t/// Synonyms: AOV")
    orders should include("\tmeasure avg_order_value = AVERAGE('orders'[order_total])")
    orders should include("\tmeasure order_count = COUNTROWS('orders')")
    orders should include("\tmeasure last_order_date = MAX('orders'[order_date])")
  }

  it should "attach owned model metrics to their table with DISTINCTCOUNT" in {
    val customers = files()("tables/customers.tmdl")
    customers should include("\tmeasure customer_count = DISTINCTCOUNT('customers'[customer_id])")
    files()("tables/orders.tmdl") should not include "customer_count"
  }

  it should "fall back to BLANK() with the original SQL for untranslatable metrics" in {
    val orders = files()("tables/orders.tmdl")
    orders should include(
      "\t/// TODO Starflow: translate original SQL to DAX: SUM(a)/NULLIF(SUM(b),0)"
    )
    orders should include("\tmeasure total_revenue = BLANK()")
  }

  it should "fall back when the aggregate argument is not a column of the table" in {
    val yaml =
      """name: stray
        |tables:
        |  - name: sales
        |    dimensions:
        |      - name: amount
        |        data_type: NUMBER
        |    metrics:
        |      - name: stray_sum
        |        expr: SUM(profit)
        |""".stripMargin
    val table = TMDLConverter
      .convert("stray", YamlSerde.mapper.readTree(yaml), None)
      .toMap
      .apply("tables/sales.tmdl")
    table should include("\t/// TODO Starflow: translate original SQL to DAX: SUM(profit)")
    table should include("\tmeasure stray_sum = BLANK()")
  }

  it should "quote table and escape column names in DAX references" in {
    val yaml =
      """name: daxq
        |tables:
        |  - name: Order Items
        |    facts:
        |      - name: qty
        |        data_type: NUMBER
        |    metrics:
        |      - name: total_qty
        |        expr: SUM(qty)
        |""".stripMargin
    val table = TMDLConverter
      .convert("daxq", YamlSerde.mapper.readTree(yaml), None)
      .toMap
      .apply("tables/Order Items.tmdl")
    table should include("\tmeasure total_qty = SUM('Order Items'[qty])")
  }

  private val relationshipYaml =
    """name: rels
      |tables:
      |  - name: orders
      |    dimensions:
      |      - name: customer_id
      |        data_type: NUMBER
      |      - name: order_id
      |        data_type: NUMBER
      |      - name: company_id
      |        data_type: NUMBER
      |  - name: customers
      |    dimensions:
      |      - name: customer_id
      |        data_type: NUMBER
      |  - name: line_items
      |    dimensions:
      |      - name: order_id
      |        data_type: NUMBER
      |      - name: company_id
      |        data_type: NUMBER
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |    join_type: left_outer
      |    relationship_type: many_to_one
      |  - name: items_to_orders
      |    left_table: line_items
      |    right_table: orders
      |    relationship_columns:
      |      - left_column: order_id
      |        right_column: order_id
      |      - left_column: company_id
      |        right_column: company_id
      |  - name: broken
      |    left_table: orders
      |    right_table: customers
      |""".stripMargin

  private def relFiles(): Map[String, String] =
    TMDLConverter.convert("rels", YamlSerde.mapper.readTree(relationshipYaml), None).toMap

  "relationships" should "render single-column relationships" in {
    val rels = relFiles()("relationships.tmdl")
    rels should include("relationship orders_to_customers")
    rels should include("\tfromColumn: orders.customer_id")
    rels should include("\ttoColumn: customers.customer_id")
  }

  it should "generate hidden COMBINEVALUES key columns for composite relationships" in {
    val items = relFiles()("tables/line_items.tmdl")
    items should include(
      "\tcolumn _sl_items_to_orders_key = COMBINEVALUES(\"|\", [order_id], [company_id])"
    )
    items should include("\t\tisHidden")
    val orders = relFiles()("tables/orders.tmdl")
    orders should include(
      "\tcolumn _sl_items_to_orders_key = COMBINEVALUES(\"|\", [order_id], [company_id])"
    )
    val rels = relFiles()("relationships.tmdl")
    rels should include("relationship items_to_orders")
    rels should include("\tfromColumn: line_items._sl_items_to_orders_key")
    rels should include("\ttoColumn: orders._sl_items_to_orders_key")
  }

  it should "skip relationships without columns and omit the file when none are valid" in {
    relFiles()("relationships.tmdl") should not include "broken"

    val yaml =
      """name: norel
        |tables:
        |  - name: t
        |relationships:
        |  - name: broken
        |    left_table: t
        |    right_table: t
        |""".stripMargin
    val out = TMDLConverter.convert("norel", YamlSerde.mapper.readTree(yaml), None).toMap
    out.keySet should not contain "relationships.tmdl"
  }

  it should "not emit relationships.tmdl when the model has no relationships" in {
    files().keySet should not contain "relationships.tmdl"
  }

  it should "quote non-plain table names in relationship references" in {
    val yaml =
      """name: q
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |  - name: orders
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |relationships:
        |  - name: items_to_orders
        |    left_table: Order Items
        |    right_table: orders
        |    relationship_columns:
        |      - left_column: order_id
        |        right_column: order_id
        |""".stripMargin
    val rels = TMDLConverter
      .convert("q", YamlSerde.mapper.readTree(yaml), None)
      .toMap
      .apply("relationships.tmdl")
    rels should include("\tfromColumn: 'Order Items'.order_id")
  }
}
