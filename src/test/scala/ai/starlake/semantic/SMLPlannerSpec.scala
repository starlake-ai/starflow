package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SMLPlannerSpec extends AnyFlatSpec with Matchers {

  import SMLPlan._

  private def model(yaml: String): JsonNode = YamlSerde.mapper.readTree(yaml.stripMargin)

  "SMLPlanner.smlDataType" should "map Starflow data types to SML data types" in {
    val cases = List(
      Some("NUMBER(38,2)")      -> "decimal(38,2)",
      Some("numeric(10)")       -> "decimal(10,0)",
      Some("DECIMAL")           -> "decimal",
      Some("INT")               -> "int",
      Some("integer")           -> "int",
      Some("SMALLINT")          -> "int",
      Some("BIGINT")            -> "long",
      Some("TINYINT")           -> "tinyint",
      Some("FLOAT")             -> "float",
      Some("REAL")              -> "float",
      Some("DOUBLE")            -> "double",
      Some("VARCHAR(16777216)") -> "string",
      Some("TEXT")              -> "string",
      Some("CHAR(1)")           -> "string",
      Some("BOOLEAN")           -> "boolean",
      Some("DATE")              -> "date",
      Some("TIMESTAMP_NTZ(9)")  -> "datetime",
      Some("DATETIME")          -> "datetime",
      Some("TIME")              -> "string",
      Some("GEOGRAPHY")         -> "string",
      None                      -> "string"
    )
    cases.foreach { case (raw, expected) =>
      withClue(raw) { SMLPlanner.smlDataType(raw) shouldBe expected }
    }
  }

  "SMLPlanner.plan" should "build physical and calculated columns, datasets and connections" in {
    val plan = SMLPlanner.plan(
      "shop",
      model(
        """name: shop
          |description: Shop model
          |tables:
          |  - name: items
          |    description: Sold items
          |    synonyms: [articles]
          |    base_table: {database: DB, schema: SALES, table: ITEMS}
          |    dimensions:
          |      - {name: id, expr: ID, data_type: INT}
          |      - {name: label, data_type: TEXT}
          |      - {name: id_copy, expr: ID, data_type: INT}
          |      - {name: upper_label, expr: UPPER(LABEL), data_type: TEXT}
          |    facts:
          |      - {name: price, expr: PRICE, data_type: "NUMBER(10,2)"}
          |  - name: refs
          |    base_table: {database: DB, table: REFS}
          |    dimensions: [{name: code}]
          |  - name: loose
          |    dimensions: [{name: k}]
          |"""
      )
    )
    plan.description shouldBe Some("Shop model")
    plan.datasets.map(_.name) shouldBe List("items", "refs", "loose")
    plan.datasets.head shouldBe Dataset(
      "items",
      Some("Sold items. Synonyms: articles"),
      "shop - DB.SALES",
      "ITEMS",
      List(
        Column("ID", "int", None),
        Column("label", "string", None),
        Column("id_copy", "int", Some("ID")),
        Column("upper_label", "string", Some("UPPER(LABEL)")),
        Column("PRICE", "decimal(10,2)", None)
      )
    )
    plan.datasets(1).connection shouldBe "shop - DB"
    plan.datasets(1).table shouldBe "REFS"
    plan.datasets(2) shouldBe Dataset(
      "loose",
      None,
      "shop - default",
      "loose",
      List(Column("k", "string", None))
    )
    plan.connections shouldBe List(
      Connection("shop - DB.SALES", Some("DB"), Some("SALES")),
      Connection("shop - DB", Some("DB"), None),
      Connection("shop - default", None, None)
    )
  }

  it should "classify metrics as native metrics, MDX calculations or NULL fallbacks" in {
    val plan = SMLPlanner.plan(
      "sales",
      model(
        """name: sales
          |tables:
          |  - name: shop_a
          |    primary_key: {columns: [id]}
          |    dimensions: [{name: id, expr: ID, data_type: INT}]
          |    facts:
          |      - {name: price, expr: PRICE, data_type: "NUMBER(10,2)"}
          |      - {name: net, expr: PRICE - DISCOUNT, data_type: "NUMBER(10,2)"}
          |    metrics:
          |      - {name: revenue, expr: SUM(price), description: Gross, synonyms: [sales]}
          |      - {name: net_total, expr: SUM(net * 2), access_modifier: private_access}
          |      - {name: orders, expr: COUNT(*)}
          |      - {name: ranked, expr: RANK() OVER (ORDER BY price), description: Rank}
          |  - name: shop_b
          |    facts: [{name: amount, data_type: DOUBLE}]
          |    metrics:
          |      - {name: revenue, expr: SUM(amount)}
          |      - {name: foreign, expr: SUM(shop_a.price)}
          |"""
      )
    )
    plan.metrics shouldBe List(
      Metric(
        "shop_a_revenue",
        "revenue",
        Some("Gross. Synonyms: sales"),
        "shop_a",
        "PRICE",
        "sum",
        hidden = false
      ),
      Metric("net_total", "net_total", None, "shop_a", "_sl_net_total", "sum", hidden = true),
      Metric("orders", "orders", None, "shop_a", "ID", "count non-null", hidden = false),
      Metric("shop_b_revenue", "revenue", None, "shop_b", "amount", "sum", hidden = false),
      Metric("_sl_foreign_1", "_sl_foreign_1", None, "shop_a", "PRICE", "sum", hidden = true)
    )
    plan.calculations shouldBe List(
      Calculation(
        "ranked",
        "ranked",
        Some("Rank. TODO Starflow: translate original SQL to MDX: RANK() OVER (ORDER BY price)"),
        "NULL",
        hidden = false
      ),
      Calculation("foreign", "foreign", None, "[Measures].[_sl_foreign_1]", hidden = false)
    )
    plan.stats.fallbackCalculations shouldBe 1
    plan.datasets.head.columns.last shouldBe Column(
      "_sl_net_total",
      "double",
      Some("(PRICE - DISCOUNT) * 2")
    )
  }

  it should "turn unowned arithmetic over qualified aggregates into a calculation with hidden metrics" in {
    val plan = SMLPlanner.plan(
      "m",
      model(
        """name: m
          |tables:
          |  - name: orders
          |    facts: [{name: total, expr: TOTAL, data_type: DOUBLE}]
          |  - name: customers
          |    dimensions: [{name: id, expr: ID}]
          |metrics:
          |  - name: basket
          |    expr: SUM(orders.total) / COUNT(DISTINCT customers.id)
          |  - name: vague
          |    expr: SUM(total) / 2
          |"""
      )
    )
    plan.metrics shouldBe List(
      Metric("_sl_basket_1", "_sl_basket_1", None, "orders", "TOTAL", "sum", hidden = true),
      Metric(
        "_sl_basket_2",
        "_sl_basket_2",
        None,
        "customers",
        "ID",
        "count distinct",
        hidden = true
      )
    )
    plan.calculations.map(c => (c.name, c.expression)) shouldBe List(
      "basket" -> "[Measures].[_sl_basket_1] / [Measures].[_sl_basket_2]",
      "vague"  -> "NULL"
    )
  }

  private def tpchMini: JsonNode =
    YamlSerde.mapper.readTree(getClass.getResourceAsStream("/semantic/sml/tpch_mini.yaml"))

  it should "assign roles, build dimensions and route relationships for a star schema" in {
    val plan = SMLPlanner.plan("tpch_mini", tpchMini)
    plan.dimensions.map(d => (d.name, d.isTime, d.isDegenerate)) shouldBe List(
      ("lineitem l_returnflag Dimension", false, true),
      ("orders Dimension", false, false),
      ("orders order_calendar Dimension", true, true),
      ("customers Dimension", false, false),
      ("nations Dimension", false, false)
    )
    plan.degenerateDimensions shouldBe List(
      "lineitem l_returnflag Dimension",
      "orders order_calendar Dimension"
    )
    plan.relationships shouldBe List(
      ModelRelationship(
        "lineitem_to_orders",
        "lineitem",
        List("L_ORDERKEY"),
        "orders Dimension",
        "orders o_orderkey",
        None
      ),
      ModelRelationship(
        "orders_to_customers",
        "orders",
        List("O_CUSTKEY"),
        "customers Dimension",
        "customers c_custkey",
        None
      ),
      ModelRelationship(
        "orders_self",
        "orders",
        List("O_ORDERKEY"),
        "orders Dimension",
        "orders o_orderkey",
        None
      )
    )
    val customers = plan.dimensions(3)
    customers.hierarchies shouldBe List(
      Hierarchy(
        "customers segment Hierarchy",
        "segment",
        None,
        List("customers c_mktsegment", "customers c_custkey")
      )
    )
    customers.leafLevel shouldBe "customers c_custkey"
    customers.secondaryAttributes.map(_.name) shouldBe List(
      "customers c_name",
      "customers c_nationkey"
    )
    customers.relationships shouldBe List(
      EmbeddedRelationship(
        "customers_to_nations",
        "customers",
        List("C_NATIONKEY"),
        "customers segment Hierarchy",
        "customers c_custkey",
        "nations Dimension",
        "nations n_nationkey",
        None
      )
    )
    val calendar = plan.dimensions(2)
    calendar.levelAttributes.map(a => (a.name, a.keyColumns, a.timeUnit)) shouldBe List(
      ("orders order_calendar year", List("o_orderdate_year"), Some("year")),
      (
        "orders order_calendar quarter",
        List("o_orderdate_year", "o_orderdate_quarter"),
        Some("quarter")
      ),
      ("orders order_calendar month", List("o_orderdate_year", "o_orderdate_month"), Some("month")),
      ("orders order_calendar day", List("O_ORDERDATE"), Some("day"))
    )
    plan.datasets(1).columns.takeRight(3) shouldBe List(
      Column("o_orderdate_year", "int", Some("EXTRACT(YEAR FROM O_ORDERDATE)")),
      Column("o_orderdate_quarter", "int", Some("EXTRACT(QUARTER FROM O_ORDERDATE)")),
      Column("o_orderdate_month", "int", Some("EXTRACT(MONTH FROM O_ORDERDATE)"))
    )
    plan.usesTimeHierarchies shouldBe true
    plan.stats shouldBe Stats(0, 0, 0, 1)
  }

  it should "handle role playing, keys from relationships, hidden-metric roles and skips" in {
    val plan = SMLPlanner.plan(
      "misc",
      model(
        """name: misc
          |tables:
          |  - name: flights
          |    dimensions:
          |      - {name: origin}
          |      - {name: destination}
          |      - {name: carrier}
          |      - {name: country}
          |      - {name: region}
          |    facts:
          |      - {name: miles, data_type: NUMBER}
          |    metrics:
          |      - {name: total_miles, expr: SUM(miles)}
          |    hierarchies:
          |      - name: geo
          |        levels: [{field: country}, {field: region}]
          |  - name: airports
          |    dimensions: [{name: code}]
          |    time_dimensions: [{name: opened, data_type: DATE}]
          |    hierarchies:
          |      - {name: opening, time: opened, levels: [year]}
          |  - name: carriers
          |    dimensions: [{name: carrier_code}, {name: carrier_name}]
          |  - name: regions
          |    dimensions: [{name: region_name}]
          |    hierarchies:
          |      - name: h
          |        levels: [{field: region_name}]
          |relationships:
          |  - {name: origin_airport, left_table: flights, right_table: airports, relationship_columns: [{left_column: origin, right_column: code}]}
          |  - {name: dest_airport, left_table: flights, right_table: airports, relationship_columns: [{left_column: destination, right_column: code}]}
          |  - {name: flights_carriers, left_table: flights, right_table: carriers, relationship_columns: [{left_column: carrier, right_column: carrier_code}]}
          |  - {name: to_nowhere, left_table: flights, right_table: planets, relationship_columns: [{left_column: origin, right_column: id}]}
          |  - {name: bad_column, left_table: flights, right_table: carriers, relationship_columns: [{left_column: tail_number, right_column: carrier_code}]}
          |metrics:
          |  - name: share
          |    expr: COUNT(DISTINCT carriers.carrier_code) / COUNT(DISTINCT flights.carrier)
          |"""
      )
    )
    plan.dimensions.map(_.name) shouldBe List(
      "flights geo Dimension",
      "airports Dimension",
      "carriers Dimension"
    )
    plan.dimensions.head.levelAttributes.map(a => (a.name, a.keyColumns)) shouldBe List(
      ("flights country", List("country")),
      ("flights region", List("region"))
    )
    val airports = plan.dimensions(1)
    airports.leafLevel shouldBe "airports code"
    airports.levelAttributes.head.keyColumns shouldBe List("code")
    airports.secondaryAttributes.map(_.name) shouldBe List("airports opened")
    plan.relationships shouldBe List(
      ModelRelationship(
        "origin_airport",
        "flights",
        List("origin"),
        "airports Dimension",
        "airports code",
        Some("origin_airport {0}")
      ),
      ModelRelationship(
        "dest_airport",
        "flights",
        List("destination"),
        "airports Dimension",
        "airports code",
        Some("dest_airport {0}")
      ),
      ModelRelationship(
        "flights_carriers",
        "flights",
        List("carrier"),
        "carriers Dimension",
        "carriers carrier_code",
        None
      ),
      ModelRelationship(
        "carriers_self",
        "carriers",
        List("carrier_code"),
        "carriers Dimension",
        "carriers carrier_code",
        None
      )
    )
    plan.metrics.map(m => (m.name, m.dataset, m.column, m.method)) shouldBe List(
      ("total_miles", "flights", "miles", "sum"),
      ("_sl_share_1", "carriers", "carrier_code", "count distinct"),
      ("_sl_share_2", "flights", "carrier", "count distinct")
    )
    plan.usesTimeHierarchies shouldBe false
    plan.stats shouldBe Stats(1, 1, 2, 0)
  }

  it should "generate time columns for timestamps, avoid name collisions and count rows without a key" in {
    val plan = SMLPlanner.plan(
      "ev",
      model(
        """name: ev
          |tables:
          |  - name: events
          |    dimensions:
          |      - {name: ts_year, expr: TS_YEAR, data_type: INT}
          |    time_dimensions:
          |      - {name: ts, expr: TS, data_type: TIMESTAMP}
          |    metrics:
          |      - {name: event_count, expr: COUNT(*)}
          |    hierarchies:
          |      - {name: cal, time: ts, levels: [month, day]}
          |"""
      )
    )
    plan.datasets.head.columns shouldBe List(
      Column("TS_YEAR", "int", None),
      Column("TS", "datetime", None),
      Column("_sl_row_count", "int", Some("1")),
      Column("ts_year_sl", "int", Some("EXTRACT(YEAR FROM TS)")),
      Column("ts_month", "int", Some("EXTRACT(MONTH FROM TS)")),
      Column("ts_day", "date", Some("CAST(TS AS DATE)"))
    )
    plan.metrics shouldBe List(
      Metric("event_count", "event_count", None, "events", "_sl_row_count", "sum", hidden = false)
    )
    plan.dimensions.map(_.name) shouldBe List("events cal Dimension", "events ts_year Dimension")
    plan.dimensions.head.levelAttributes.map(_.keyColumns) shouldBe List(
      List("ts_year_sl", "ts_month"),
      List("ts_day")
    )
  }
}
