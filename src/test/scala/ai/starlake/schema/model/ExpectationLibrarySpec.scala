package ai.starlake.schema.model

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

/** The library lists the expectation macros of a project, not the SQL fragments that they share */
class ExpectationLibrarySpec extends TestHelper {
  new WithSettings() {
    storageHandler.write(
      """{% macro expect_one(table_name) %}
        |SELECT 0
        |{% endmacro %}
        |{% macro ignore_me(table_name) %}
        |SELECT 0
        |{% endmacro %}
        |""".stripMargin,
      new Path(DatasetArea.expectations, "Checks.j2")
    )
    storageHandler.write(
      """{% macro sl_ratio(num, den) -%}
        |SAFE_DIVIDE({{ num }}, {{ den }})
        |{%- endmacro %}
        |""".stripMargin,
      new Path(DatasetArea.expectations, "Engine.j2")
    )

    "The expectation library" should "list the expectation macros of a file" in {
      ExpectationLibrary.expectations("Checks") shouldBe List("expect_one(table_name)")
      ExpectationLibrary.expectations("Engine") shouldBe Nil
    }

    it should "not list a file of SQL fragments as a category" in {
      ExpectationLibrary.categories() should contain("Checks")
      ExpectationLibrary.categories() should not contain "Engine"
    }
  }
}
