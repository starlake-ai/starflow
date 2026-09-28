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
}
