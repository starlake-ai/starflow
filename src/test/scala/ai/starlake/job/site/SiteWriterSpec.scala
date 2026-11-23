package ai.starlake.job.site

import better.files.File
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class SiteWriterSpec extends AnyFlatSpec with Matchers {
  private val model = SiteModel(
    1,
    "2026-11-23T00:00:00Z",
    "test",
    SiteProject("p", "", "duckdb", None),
    TreeMap(
      "load:a.b" -> SiteNode(
        "load:a.b",
        "load",
        "b",
        "a",
        "b",
        description = Some("bad </script><script>alert(1)</script>")
      )
    ),
    Nil,
    Nil,
    SiteGroups(Nil, Nil, Nil),
    TreeMap.empty
  )

  // Built by concatenation so scalac never decodes the unicode escape in this source file.
  private val esc = "\\" + "u003c"

  "escapeForScript" should "escape every < as a unicode escape" in {
    SiteWriter.escapeForScript("""{"a":"</script><!--x"}""") shouldBe
    s"""{"a":"${esc}/script>${esc}!--x"}"""
  }

  "inline" should "insert blocks at the marker, or before </body> when absent" in {
    SiteWriter.inline("<head><!--sl-data--></head>", List("sl-model" -> "{}")) shouldBe
    """<head><script type="application/json" id="sl-model">{}</script></head>"""
    SiteWriter.inline("<body>x</body>", List("sl-model" -> "{}")) shouldBe
    """<body>x<script type="application/json" id="sl-model">{}</script></body>"""
  }

  "write" should "produce sl-model.json and an index.html with the model inlined" in {
    val out = File.newTemporaryDirectory("sl-site-writer-")
    try {
      SiteWriter.write(out, model, stats = None)
      (out / "sl-model.json").exists shouldBe true
      (out / "sl-stats.json").exists shouldBe false
      val html = (out / "index.html").contentAsString
      html should include("""id="sl-model"""")
      html should include("load:a.b")
      html should include(esc + "/script>")
      html should not include "bad </script>"
      html should not include "</script><script>alert(1)"
      (out / "sl-model.json").contentAsString should include("bad </script>")
    } finally out.delete(swallowIOExceptions = true)
  }

  it should "write sl-stats.json when stats are given" in {
    val out = File.newTemporaryDirectory("sl-site-writer-")
    try {
      SiteWriter.write(out, model, stats = Some(Map("schemaVersion" -> 1)))
      (out / "sl-stats.json").exists shouldBe true
      (out / "index.html").contentAsString should include("""id="sl-stats"""")
    } finally out.delete(swallowIOExceptions = true)
  }
}
