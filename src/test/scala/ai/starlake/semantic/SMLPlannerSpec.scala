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
}
