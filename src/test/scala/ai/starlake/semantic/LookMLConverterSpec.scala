package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LookMLConverterSpec extends AnyFlatSpec with Matchers {

  import LookMLConverter.sanitize
  import SemanticModelOps.{parseAggregate, ParsedAggregate}

  "sanitize" should "lowercase and replace invalid characters" in {
    sanitize("order_id") shouldBe "order_id"
    sanitize("Order Status") shouldBe "order_status"
    sanitize("CUSTOMERS") shouldBe "customers"
    sanitize("weird-name.1") shouldBe "weird_name_1"
  }

  "parseAggregate" should "map simple aggregates to native LookML measure types" in {
    parseAggregate("SUM(order_total)") shouldBe Some(ParsedAggregate("sum", Some("order_total")))
    parseAggregate("avg(order_total)") shouldBe Some(
      ParsedAggregate("average", Some("order_total"))
    )
    parseAggregate("MIN(x)") shouldBe Some(ParsedAggregate("min", Some("x")))
    parseAggregate("MAX(x)") shouldBe Some(ParsedAggregate("max", Some("x")))
    parseAggregate("COUNT(*)") shouldBe Some(ParsedAggregate("count", None))
    parseAggregate("COUNT(DISTINCT customers.customer_id)") shouldBe Some(
      ParsedAggregate("count_distinct", Some("customers.customer_id"))
    )
  }

  it should "reject expressions that are not a single simple aggregate" in {
    parseAggregate("SUM(a)/NULLIF(SUM(b),0)") shouldBe None
    parseAggregate("SUM(a) + SUM(b)") shouldBe None
    parseAggregate("COUNT(order_id)") shouldBe None
    parseAggregate("CASE WHEN x THEN 1 ELSE 0 END") shouldBe None
    parseAggregate("order_total") shouldBe None
  }

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
      |        unique: true
      |      - name: order_status
      |        expr: ORDER_STATUS
      |        data_type: TEXT
      |        is_enum: true
      |        synonyms: ["status"]
      |        sample_values: ["Pending", "Shipped"]
      |    time_dimensions:
      |      - name: order_date
      |        expr: ORDER_DATE
      |        data_type: DATE
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |        access_modifier: public_access
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |        synonyms: ["AOV"]
      |    filters:
      |      - name: recent_orders
      |        expr: order_date >= DATEADD(day, -30, CURRENT_DATE())
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
      |        unique: true
      |    primary_key:
      |      columns: [customer_id]
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |    join_type: left_outer
      |    relationship_type: many_to_one
      |metrics:
      |  - name: customer_count
      |    expr: COUNT(DISTINCT customers.customer_id)
      |verified_queries:
      |  - name: top_customers
      |    question: Who are the top 10 customers by revenue?
      |    sql: SELECT 1
      |""".stripMargin

  private def convertSample(): Map[String, String] =
    LookMLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), "analytics_wh")
      .toMap

  "convert" should "produce one view file per table plus a model file" in {
    val files = convertSample()
    files.keySet shouldBe Set(
      "ecommerce_analytics.model.lkml",
      "orders.view.lkml",
      "customers.view.lkml"
    )
  }

  it should "render dimensions, primary key, suggestions and sql_table_name" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("view: orders {")
    orders should include("sql_table_name: ANALYTICS_DB.ECOMMERCE.ORDERS ;;")
    orders should include("dimension: order_id {")
    orders should include("primary_key: yes")
    orders should include("type: number")
    orders should include("sql: ORDER_ID ;;")
    orders should include("dimension: order_status {")
    orders should include("type: string")
    orders should include("""description: "Synonyms: status"""")
    orders should include("""suggestions: ["Pending", "Shipped"]""")
    orders should include("# filters (not translated to LookML):")
    orders should include("#   recent_orders: order_date >= DATEADD(day, -30, CURRENT_DATE())")
  }

  it should "render time dimensions as dimension_groups" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("dimension_group: order_date {")
    orders should include("type: time")
    orders should include("timeframes: [raw, date, week, month, quarter, year]")
    orders should include("datatype: date")
    orders should include("sql: ORDER_DATE ;;")
  }

  it should "render table metrics as native measures referencing view fields" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("measure: avg_order_value {")
    orders should include("type: average")
    orders should include("sql: ${order_total} ;;")
    orders should include("""description: "Synonyms: AOV"""")
  }

  it should "attach model-level metrics to the view owning the aggregate argument" in {
    val customers = convertSample()("customers.view.lkml")
    customers should include("measure: customer_count {")
    customers should include("type: count_distinct")
    customers should include("sql: ${customer_id} ;;")
    val orders = convertSample()("orders.view.lkml")
    orders should not include "measure: customer_count"
  }

  it should "not substitute ${...} for a metric argument naming a time dimension" in {
    val yaml =
      """name: time_metric_model
        |tables:
        |  - name: orders
        |    time_dimensions:
        |      - name: order_date
        |        expr: ORDER_DATE
        |        data_type: DATE
        |    metrics:
        |      - name: latest_order_date
        |        expr: MAX(order_date)
        |""".stripMargin
    val files =
      LookMLConverter.convert("time_metric_model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val orders = files("orders.view.lkml")
    orders should include("measure: latest_order_date {")
    orders should include("type: max")
    orders should include("sql: order_date ;;")
    orders should not include "sql: ${order_date} ;;"
  }

  it should "fall back to type number for model-level metrics whose owning table cannot be determined" in {
    val yaml =
      """name: unowned_metric_model
        |tables:
        |  - name: orders
        |  - name: customers
        |metrics:
        |  - name: total_revenue
        |    expr: SUM(revenue)
        |""".stripMargin
    val files =
      LookMLConverter.convert("unowned_metric_model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val orders = files("orders.view.lkml")
    orders should include(
      "# starflow: verify this measure, expression could not be mapped to a native LookML type"
    )
    orders should include("measure: total_revenue {")
    orders should include("type: number")
    orders should include("sql: SUM(revenue) ;;")
  }

  it should "fall back to type number with raw SQL for unmappable metrics" in {
    val yaml =
      """name: fallback_model
        |tables:
        |  - name: sales
        |    dimensions:
        |      - name: a
        |        data_type: NUMBER
        |    metrics:
        |      - name: weird_ratio
        |        expr: SUM(a)/NULLIF(SUM(b),0)
        |""".stripMargin
    val files =
      LookMLConverter.convert("fallback_model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val sales = files("sales.view.lkml")
    sales should include(
      "# starflow: verify this measure, expression could not be mapped to a native LookML type"
    )
    sales should include("measure: weird_ratio {")
    sales should include("type: number")
    sales should include("sql: SUM(a)/NULLIF(SUM(b),0) ;;")
    // no base_table: sql_table_name falls back to the table name
    sales should include("sql_table_name: sales ;;")
  }

  it should "sanitize mixed-case table and column names" in {
    val yaml =
      """name: Mixed Model
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: Item-Id
        |        data_type: NUMBER
        |""".stripMargin
    val files = LookMLConverter.convert("Mixed Model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    files.keySet should contain("order_items.view.lkml")
    files.keySet should contain("mixed_model.model.lkml")
    files("order_items.view.lkml") should include("dimension: item_id {")
  }

  it should "skip primary_key flag for composite primary keys with a comment" in {
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
    val files = LookMLConverter.convert("composite", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val view = files("line_items.view.lkml")
    view should include("# composite primary key (order_id, line_no) not representable in LookML")
    view should not include "primary_key: yes"
  }

  "renderModel" should "emit connection, include and one explore per left table" in {
    val model = convertSample()("ecommerce_analytics.model.lkml")
    model should include("""connection: "analytics_wh"""")
    model should include("""include: "*.view.lkml"""")
    model should include("explore: orders {")
    model should include("join: customers {")
    model should include("type: left_outer")
    model should include("relationship: many_to_one")
    model should include("sql_on: ${orders.customer_id} = ${customers.customer_id} ;;")
    // customers is joined from orders, no explore of its own
    model should not include "explore: customers"
  }

  it should "emit bare explores for tables in no relationship and default join attributes" in {
    val yaml =
      """name: multi
        |tables:
        |  - name: orders
        |  - name: customers
        |  - name: audit_log
        |relationships:
        |  - name: orders_to_customers
        |    left_table: orders
        |    right_table: customers
        |    relationship_columns:
        |      - left_column: customer_id
        |        right_column: customer_id
        |""".stripMargin
    val files =
      LookMLConverter
        .convert("multi", YamlSerde.mapper.readTree(yaml), "wh")
        .toMap
    val model = files("multi.model.lkml")
    model should include("explore: audit_log {}")
    // join_type / relationship_type absent: defaults apply
    model should include("type: left_outer")
    model should include("relationship: many_to_one")
  }

  it should "join composite relationship columns with AND" in {
    val yaml =
      """name: composite_rel
        |tables:
        |  - name: line_items
        |  - name: orders
        |relationships:
        |  - name: items_to_orders
        |    left_table: line_items
        |    right_table: orders
        |    relationship_columns:
        |      - left_column: order_id
        |        right_column: order_id
        |      - left_column: company_id
        |        right_column: company_id
        |""".stripMargin
    val files = LookMLConverter
      .convert("composite_rel", YamlSerde.mapper.readTree(yaml), "wh")
      .toMap
    val model = files("composite_rel.model.lkml")
    model should include(
      "sql_on: ${line_items.order_id} = ${orders.order_id} AND ${line_items.company_id} = ${orders.company_id} ;;"
    )
  }
}
