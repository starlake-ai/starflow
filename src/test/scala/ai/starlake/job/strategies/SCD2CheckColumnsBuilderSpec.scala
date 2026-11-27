package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model._

import scala.jdk.CollectionConverters._

/** The columns that SCD2 compares (`checkColumns`), resolved against the columns of the SELECT */
class SCD2CheckColumnsBuilderSpec extends TestHelper {
  new WithSettings() {
    private def parameters(
      checkColumns: Option[List[String]],
      columns: List[String] = List("id", "Label", "note", "valid_from")
    ): Map[String, Any] =
      TransformStrategiesBuilder
        .StrategiesGenerationContext(
          WriteStrategy(
            `type` = Some(WriteStrategyType.SCD2),
            key = List("id"),
            checkColumns = checkColumns,
            startTs = Some("valid_from"),
            endTs = Some("valid_to")
          ),
          "SELECT 1",
          TransformStrategiesBuilder.TableComponents("", "dom", "t", columns),
          targetTableExists = true,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines("duckdb"),
          AllSinks().getSink()
        )
        .asMap()

    private def list(parameters: Map[String, Any], name: String): List[String] =
      parameters(name).asInstanceOf[java.util.List[String]].asScala.toList

    "checkColumns '*'" should "name the columns of the query without the key and the validity columns" in {
      list(parameters(Some(List("*"))), "strategyCheckColumns") shouldBe List("Label", "note")
    }

    it should "stop the run when it leaves no column to compare" in {
      val error = intercept[IllegalArgumentException](
        parameters(Some(List("*")), columns = List("id", "valid_from"))
      )
      error.getMessage shouldBe "checkColumns: '*' leaves no column to compare for dom.t"
    }

    it should "stop the run when Starlake cannot list the columns of the query" in {
      val error = intercept[IllegalArgumentException](parameters(Some(List("*")), columns = Nil))
      error.getMessage shouldBe
      "checkColumns: '*' needs the columns of the query, which Starlake cannot list for dom.t"
    }

    "A listed check column" should "take the spelling of the query, whatever its case or quotes" in {
      list(parameters(Some(List("LABEL", "\"note\""))), "strategyCheckColumns") shouldBe List(
        "Label",
        "note"
      )
    }

    it should "be quoted with the quote of the engine" in {
      list(parameters(Some(List("label", "note"))), "quotedStrategyCheckColumns") shouldBe List(
        "\"Label\"",
        "\"note\""
      )
    }

    it should "stop the run when the query does not return it" in {
      val error = intercept[IllegalArgumentException](parameters(Some(List("nope"))))
      error.getMessage shouldBe "checkColumns: 'nope' is not a column of the query for dom.t"
    }

    it should "be kept as written when Starlake cannot list the columns of the query" in {
      list(parameters(Some(List("label")), columns = Nil), "strategyCheckColumns") shouldBe List(
        "label"
      )
    }

    "A strategy without checkColumns" should "pass no check column" in {
      val map = parameters(None)
      list(map, "strategyCheckColumns") shouldBe Nil
      list(map, "quotedStrategyCheckColumns") shouldBe Nil
    }
  }
}
