package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.schema.model.WriteMode

class UtilsSpec extends TestHelper {
  new WithSettings() {
    "Exceptions" should "be returned as string" in {
      val expected = "java.lang.Exception: test"
      Utils.exceptionAsString(new Exception("test")) should startWith(expected)
    }
    "moduleExists" should "find a Scala object on the classpath and not a missing one" in {
      Utils.moduleExists("ai.starlake.utils.Utils") shouldBe true
      Utils.moduleExists("ai.starlake.utils.NoSuchObject") shouldBe false
      Utils.moduleExists("java.lang.String") shouldBe false
    }

    "BigQuery Table Creation / Write Mapping" should "Map to correct BQ Mappings" in {
      Utils.getDBDisposition(WriteMode.OVERWRITE) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
      Utils.getDBDisposition(
        WriteMode.APPEND
      ) should equal(
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      )
      Utils.getDBDisposition(
        WriteMode.OVERWRITE
      ) should equal(
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      )
    }

    "Custom format should" should "match patterns" in {
      import ai.starlake.utils.Formatter._
      assert("${key}_and_${key}".richFormat(Map.empty, Map("key" -> "value")) == "value_and_value")
    }

    "Parse Jinja using env vars" should "match result" in {
      val result =
        Utils.parseJinja(
          "{%if ok %}{{ 'hello' | upper }}{% endif %}",
          Map("ok" -> true)
        )
      assert(result.trim == "HELLO")
    }
  }
}
