package ai.starlake.job.site

import ai.starlake.TestHelper
import ai.starlake.utils.Utils

class DocFunctionSpec extends TestHelper {
  "parseJinja" should "emit a quote-free marker that keeps both YAML quoting styles valid" in {
    new WithSettings() {
      val in = "a: '{{ doc(\"customers_desc\") }}'\nb: \"see {{ doc('other') }}\""
      val out = Utils.parseJinja(in, Map.empty[String, Any])
      out shouldBe "a: '<!--sl-doc:customers_desc-->'\nb: \"see <!--sl-doc:other-->\""
      val tree = Utils.newYamlMapper().readTree(out)
      tree.get("a").asText() shouldBe "<!--sl-doc:customers_desc-->"
      tree.get("b").asText() shouldBe "see <!--sl-doc:other-->"
    }
  }
}
