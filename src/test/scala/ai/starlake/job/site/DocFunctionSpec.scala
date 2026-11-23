package ai.starlake.job.site

import ai.starlake.TestHelper
import ai.starlake.utils.Utils

class DocFunctionSpec extends TestHelper {
  "parseJinja" should "keep doc() references intact" in {
    new WithSettings() {
      val in = """comment: '{{ doc("customers_desc") }}' and {{ doc('other') }}"""
      val out = Utils.parseJinja(in, Map.empty[String, Any])
      out shouldBe """comment: '{{ doc("customers_desc") }}' and {{ doc("other") }}"""
    }
  }
}
