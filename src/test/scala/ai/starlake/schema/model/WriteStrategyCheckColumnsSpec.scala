package ai.starlake.schema.model

import ai.starlake.TestHelper

import java.util.regex.Pattern

/** The validation of `checkColumns`, which lets SCD2 detect a new version by comparing columns */
class WriteStrategyCheckColumnsSpec extends TestHelper {
  new WithSettings() {
    private val table = SchemaInfo(
      name = "t",
      pattern = Pattern.compile("t.*"),
      attributes = List(
        TableAttribute("id"),
        TableAttribute("label"),
        TableAttribute("note"),
        TableAttribute("updated_at", "timestamp")
      )
    )

    private def scd2(
      timestamp: Option[String] = None,
      checkColumns: Option[List[String]] = None,
      on: Option[MergeOn] = None
    ): WriteStrategy =
      WriteStrategy(
        `type` = Some(WriteStrategyType.SCD2),
        key = List("id"),
        timestamp = timestamp,
        checkColumns = checkColumns,
        on = on
      )

    private def messages(
      strategy: WriteStrategy,
      schema: Option[SchemaInfo] = Some(table)
    ): List[ValidationMessage] =
      strategy.checkValidity("dom", schema).left.getOrElse(Nil)

    private def texts(strategy: WriteStrategy, schema: Option[SchemaInfo] = Some(table)) =
      messages(strategy, schema).map(_.message)

    "An SCD2 strategy" should "be valid with checkColumns and no timestamp" in {
      texts(scd2(checkColumns = Some(List("label", "note")))) shouldBe Nil
      texts(scd2(checkColumns = Some(List("*")))) shouldBe Nil
    }

    it should "still be valid with a timestamp and no checkColumns" in {
      texts(scd2(timestamp = Some("updated_at"))) shouldBe Nil
    }

    it should "need a timestamp or checkColumns" in {
      texts(scd2()) should contain(
        "timestamp or checkColumns must be defined for scd2 strategy in table: t"
      )
    }

    it should "refuse a timestamp with checkColumns" in {
      texts(
        scd2(timestamp = Some("updated_at"), checkColumns = Some(List("label")))
      ) should contain(
        "timestamp and checkColumns cannot be used together in table: t"
      )
    }

    it should "refuse an empty checkColumns" in {
      texts(scd2(checkColumns = Some(Nil))) should contain(
        "checkColumns cannot be empty in table: t"
      )
    }

    it should "refuse '*' with other columns" in {
      texts(scd2(checkColumns = Some(List("*", "label")))) should contain(
        "checkColumns: '*' must be its only entry in table: t"
      )
    }

    it should "refuse checkColumns with on: SOURCE_AND_TARGET" in {
      texts(
        scd2(checkColumns = Some(List("label")), on = Some(MergeOn.SOURCE_AND_TARGET))
      ) should contain(
        "checkColumns cannot be used with on: SOURCE_AND_TARGET, which needs a timestamp to keep one incoming row per key, in table: t"
      )
    }

    it should "refuse a check column that the table does not have, whatever its case" in {
      texts(scd2(checkColumns = Some(List("nope", "LABEL")))) shouldBe List(
        "checkColumns: 'nope' does not exist in table: t"
      )
    }

    it should "not check the columns of a strategy without a table, as for a transform" in {
      texts(scd2(checkColumns = Some(List("nope"))), schema = None) shouldBe Nil
    }

    "A strategy other than SCD2" should "warn that its checkColumns are ignored" in {
      val strategy = WriteStrategy(
        `type` = Some(WriteStrategyType.UPSERT_BY_KEY),
        key = List("id"),
        checkColumns = Some(List("label"))
      )
      messages(strategy) shouldBe List(
        ValidationMessage(
          Severity.Warning,
          "WriteStrategy",
          "checkColumns is only used by the scd2 strategy and is ignored in table: t"
        )
      )
    }

    "checkColumns" should "be read from YAML and shown by toString" in {
      val strategy = mapper.readValue(
        """type: "SCD2"
          |key: ["id"]
          |checkColumns: ["label", "note"]
          |""".stripMargin,
        classOf[WriteStrategy]
      )
      strategy.checkColumns shouldBe Some(List("label", "note"))
      strategy.toString should include("checkColumns=label,note")
    }
  }
}
