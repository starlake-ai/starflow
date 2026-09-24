package ai.starlake.job.strategies

import ai.starlake.schema.model.{AllSinks, Materialization, WriteStrategy, WriteStrategyType}

class BigQueryTransformStrategiesBuilderSpec extends TransformStrategiesBuilderSpec {
  def engine = "bigquery"

  new WithSettings() {
    // A presql creating the target forces targetTableExists = true on the native path (#1803)
    "buildTransform" should "insert into an existing target and create an absent one" in {
      def plan(targetTableExists: Boolean): String =
        new TransformStrategiesBuilder().buildTransform(
          WriteStrategy(`type` = Some(WriteStrategyType.APPEND)),
          "SELECT 1 AS id",
          TransformStrategiesBuilder.TableComponents("", "my_domain", "my_table", List("id")),
          targetTableExists = targetTableExists,
          truncate = false,
          materializedView = Materialization.TABLE,
          settings.appConfig.jdbcEngines(engine),
          AllSinks().getSink()
        )
      val existing = plan(targetTableExists = true)
      existing should include("INSERT INTO my_domain.my_table")
      existing should not include "CREATE TABLE"
      plan(targetTableExists = false) should include("CREATE TABLE my_domain.my_table")
    }
  }
}
