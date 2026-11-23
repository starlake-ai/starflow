package ai.starlake.job.site

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DocBlocksSpec extends AnyFlatSpec with Matchers {
  private val md =
    """intro text ignored
      |{% docs customers_desc %}
      |**Customers** master data.
      |{% enddocs %}
      |{%- docs nested -%}Before {{ doc("customers_desc") }} after{%- enddocs -%}
      |{% docs loop_a %}{{ doc('loop_b') }}{% enddocs %}
      |{% docs loop_b %}{{ doc("loop_a") }}{% enddocs %}
      |""".stripMargin

  "parse" should "extract trimmed blocks by name" in {
    val (blocks, warnings) = DocBlocks.parse(Seq(md))
    blocks.blocks("customers_desc") shouldBe "**Customers** master data."
    blocks.blocks.keySet shouldBe Set("customers_desc", "nested", "loop_a", "loop_b")
    warnings shouldBe Nil
  }

  it should "warn on duplicate block names and keep the first" in {
    val (blocks, warnings) =
      DocBlocks.parse(Seq("{% docs a %}one{% enddocs %}", "{% docs a %}two{% enddocs %}"))
    blocks.blocks("a") shouldBe "one"
    warnings shouldBe List("Duplicate doc block 'a' ignored")
  }

  "resolve" should "replace references, including nested ones" in {
    val (blocks, _) = DocBlocks.parse(Seq(md))
    blocks.resolve("""See {{ doc("nested") }}.""") shouldBe
      ("See Before **Customers** master data. after.", Nil)
  }

  it should "keep unknown references and warn" in {
    DocBlocks.Empty.resolve("""x {{ doc("missing") }}""") shouldBe
      ("""x {{ doc("missing") }}""", List("Unknown doc block 'missing'"))
  }

  it should "stop on cycles and warn" in {
    val (blocks, _) = DocBlocks.parse(Seq(md))
    val (text, warnings) = blocks.resolve("""{{ doc("loop_a") }}""")
    text shouldBe """{{ doc("loop_a") }}"""
    warnings shouldBe List("Cyclic doc block reference 'loop_a'")
  }
}
