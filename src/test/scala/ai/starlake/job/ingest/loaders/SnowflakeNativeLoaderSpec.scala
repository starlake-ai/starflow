package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.ExpectationItem

/** The second step of a native Snowflake load checks its expectations before its commit, unless its
  * COPY purges the files of a Snowflake stage, which a rolled-back write would lose (#1976).
  */
class SnowflakeNativeLoaderSpec extends TestHelper {
  private val expectations = List(ExpectationItem("is_unique('id')"))

  "The second step of a native Snowflake load" should "check its expectations before its commit" in {
    SnowflakeNativeLoader.checkedBeforeCommit(
      expectations,
      purges = false,
      filesInSnowflakeStage = true
    ) shouldBe expectations
  }

  it should "leave them after the write when its COPY purges the files of a Snowflake stage" in {
    SnowflakeNativeLoader.checkedBeforeCommit(
      expectations,
      purges = true,
      filesInSnowflakeStage = true
    ) shouldBe Nil
  }

  it should "check them before its commit when the purge removes only the copies of local files" in {
    SnowflakeNativeLoader.checkedBeforeCommit(
      expectations,
      purges = true,
      filesInSnowflakeStage = false
    ) shouldBe expectations
  }
}
