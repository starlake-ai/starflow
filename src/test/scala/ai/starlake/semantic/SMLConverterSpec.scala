package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SMLConverterSpec extends AnyFlatSpec with Matchers {

  private def read(files: Seq[(String, String)], path: String): JsonNode =
    SMLConverter.yaml.readTree(files.toMap.apply(path))

  private def tpchMini: JsonNode =
    YamlSerde.mapper.readTree(getClass.getResourceAsStream("/semantic/sml/tpch_mini.yaml"))

  "SMLConverter.convert" should "write a complete SML repository" in {
    val files = SMLConverter.convert("tpch_mini", tpchMini, "snowflake_prod")
    files.map(_._1) shouldBe List(
      "catalog.yml",
      "connections/tpch_mini - TPCH.SF1.yml",
      "datasets/lineitem.yml",
      "datasets/orders.yml",
      "datasets/customers.yml",
      "datasets/nations.yml",
      "dimensions/lineitem l_returnflag Dimension.yml",
      "dimensions/orders Dimension.yml",
      "dimensions/orders order_calendar Dimension.yml",
      "dimensions/customers Dimension.yml",
      "dimensions/nations Dimension.yml",
      "metrics/revenue.yml",
      "metrics/order_revenue.yml",
      "metrics/order_count.yml",
      "metrics/_sl_revenue_per_order_1.yml",
      "metrics/_sl_revenue_per_order_2.yml",
      "calculations/revenue_per_order.yml",
      "calculations/running_total.yml",
      "models/tpch_mini.yml"
    )
    files.head._2 should not startWith "---"
    val catalog = read(files, "catalog.yml")
    catalog.get("version").isNumber shouldBe true
    catalog.get("version").asText() shouldBe "1.8"
    read(files, "connections/tpch_mini - TPCH.SF1.yml").get("as_connection").asText() shouldBe
    "snowflake_prod"
    val running = read(files, "calculations/running_total.yml")
    running.get("object_type").asText() shouldBe "metric_calc"
    running.get("expression").isTextual shouldBe true
    running.get("expression").asText() shouldBe "NULL"
  }

  it should "render embedded relationships, secondary attributes and hidden metrics" in {
    val files = SMLConverter.convert("tpch_mini", tpchMini, "wh")
    val customers = read(files, "dimensions/customers Dimension.yml")
    val levels = customers.get("hierarchies").get(0).get("levels")
    levels.get(0).has("secondary_attributes") shouldBe false
    levels
      .get(1)
      .get("secondary_attributes")
      .get(0)
      .get("description")
      .asText() shouldBe "Synonyms: customer name"
    val embedded = customers.get("relationships").get(0)
    embedded.get("type").asText() shouldBe "embedded"
    embedded.get("from").get("hierarchy").asText() shouldBe "customers segment Hierarchy"
    customers.get("level_attributes").get(1).get("is_unique_key").asBoolean() shouldBe true
    customers.get("level_attributes").get(0).has("is_unique_key") shouldBe false
    read(files, "metrics/_sl_revenue_per_order_1.yml").get("is_hidden").asBoolean() shouldBe true
    read(files, "metrics/revenue.yml").has("is_hidden") shouldBe false
    val model = read(files, "models/tpch_mini.yml")
    model.get("metrics").size() shouldBe 7
    model.get("dimensions").get(1).asText() shouldBe "orders order_calendar Dimension"
    read(files, "dimensions/orders order_calendar Dimension.yml").get("type").asText() shouldBe
    "time"
  }

  it should "add role_play to relationships sharing a dataset and a dimension" in {
    val model = YamlSerde.mapper.readTree(
      """name: air
        |tables:
        |  - name: flights
        |    dimensions: [{name: origin}, {name: destination}]
        |    facts: [{name: miles, data_type: DOUBLE}]
        |  - name: airports
        |    dimensions: [{name: code}]
        |relationships:
        |  - {name: origin_airport, left_table: flights, right_table: airports, relationship_columns: [{left_column: origin, right_column: code}]}
        |  - {name: dest_airport, left_table: flights, right_table: airports, relationship_columns: [{left_column: destination, right_column: code}]}
        |""".stripMargin
    )
    val relationships = read(SMLConverter.convert("air", model, "wh"), "models/air.yml")
      .get("relationships")
    relationships.get(0).get("role_play").asText() shouldBe "origin_airport {0}"
    relationships.get(1).get("role_play").asText() shouldBe "dest_airport {0}"
    relationships.get(0).get("to").get("level").asText() shouldBe "airports code"
  }

  it should "write only the catalog and the model for a model without tables" in {
    val files = SMLConverter.convert("empty", YamlSerde.mapper.readTree("name: empty\n"), "wh")
    files.map(_._1) shouldBe List("catalog.yml", "models/empty.yml")
    read(files, "models/empty.yml").get("relationships").size() shouldBe 0
  }

  "SMLConverter.fileNames" should "sanitize names and disambiguate collisions" in {
    SMLConverter.fileNames(List("a/b", "a:b", "A_b", "ok name-1.x")) shouldBe List(
      "a_b.yml",
      "a_b_2.yml",
      "A_b_3.yml",
      "ok name-1.x.yml"
    )
  }
}
