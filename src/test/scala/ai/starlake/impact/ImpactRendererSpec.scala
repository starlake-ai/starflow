package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.impact.ChangeClass._

import scala.io.Source

class ImpactRendererSpec extends TestHelper {

  private val report = ImpactReport(
    "origin/master (abc1234)",
    "working tree",
    List(
      DirectChange(
        NodeRef("raw", "orders"),
        NodeKind.LoadTable,
        Breaking,
        Cols(Set("amount")),
        List(ColumnRemoved("amount")),
        Nil
      )
    ),
    List(
      ImpactedNode(
        NodeRef("stg", "orders"),
        NodeKind.Task,
        Breaking,
        AllColumns,
        List("raw.orders[amount]", "stg.orders"),
        likelyFailure = true,
        viaUnresolvedEdge = false
      )
    ),
    List(UnresolvedEdge(NodeRef("stg", "orders"), NodeRef("kpi", "py"), "python task")),
    List("dags/default.sl.yml (changed)")
  )

  private def expected(name: String): String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream(s"/impact/expected/$name"))
    try s.mkString
    finally s.close()
  }

  private def single(d: DirectChange): ImpactReport =
    ImpactReport("a", "b", List(d), Nil, Nil, Nil)

  private val analyzable = DirectChange(
    NodeRef("stg", "orders"),
    NodeKind.Task,
    NonBreaking,
    AllColumns,
    Nil,
    List("SQL now analyzable (previously: python task)"),
    unresolved = Some("python task")
  )

  "render" should "produce the golden text report" in {
    new WithSettings() {
      ImpactRenderer.render(report, "text", None).trim shouldBe expected("report.txt").trim
    }
  }

  it should "produce the golden markdown report" in {
    new WithSettings() {
      ImpactRenderer.render(report, "markdown", None).trim shouldBe expected("report.md").trim
    }
  }

  it should "produce the golden json report" in {
    new WithSettings() {
      ImpactRenderer.render(report, "json", None).trim shouldBe expected("report.json").trim
    }
  }

  it should "render html with every section" in {
    new WithSettings() {
      val html = ImpactRenderer.render(report, "html", None)
      html should include("raw.orders")
      html should include("Unresolved")
      html should include("likely failure")
      html should include("""class="BREAKING">BREAKING""")
      html should include("breaking: yes")
    }
  }

  it should "say there are no changes" in {
    new WithSettings() {
      ImpactRenderer.render(
        ImpactReport("a", "b", Nil, Nil, Nil, Nil),
        "text",
        None
      ) should include("No changes")
    }
  }

  it should "print no column list for a change without columns" in {
    new WithSettings() {
      val metadataOnly = DirectChange(
        NodeRef("raw", "products"),
        NodeKind.LoadTable,
        Metadata,
        Cols(Set.empty),
        Nil,
        List("metadata only")
      )
      val text = ImpactRenderer.render(
        ImpactReport("a", "b", List(metadataOnly), Nil, Nil, Nil),
        "text",
        None
      )
      text should include("raw.products\n")
      text should not include "[]"
    }
  }

  it should "reject an unknown format" in {
    new WithSettings() {
      an[IllegalArgumentException] should be thrownBy ImpactRenderer.render(report, "xml", None)
    }
  }

  it should "not repeat an unresolved reason already explained by 'SQL now analyzable'" in {
    new WithSettings() {
      val r = single(analyzable)
      List("text", "markdown", "html").foreach { f =>
        val out = ImpactRenderer.render(r, f, None)
        out should include("SQL now analyzable")
        out should not include "unresolved: python task"
      }
      ImpactRenderer.render(r, "html", None) should not include "unresolved:"
      val other = single(analyzable.copy(reasons = List("row-level: WHERE changed")))
      ImpactRenderer.render(other, "text", None) should include("unresolved: python task")
      ImpactRenderer.render(other, "markdown", None) should include("unresolved: python task")
      ImpactRenderer.render(other, "html", None) should include("unresolved: python task")
    }
  }

  it should "escape pipes and newlines in markdown table cells" in {
    new WithSettings() {
      val d = analyzable.copy(reasons = List("a | b\nsecond line"), unresolved = None)
      val out = ImpactRenderer.render(single(d), "markdown", None)
      out should include("a \\| b second line")
      val row = out.linesIterator.find(_.contains("stg.orders")).get
      row.replace("\\|", "").count(_ == '|') shouldBe 6
    }
  }

  it should "escape pipes in node, columns, path, sides, unresolved and other lists" in {
    new WithSettings() {
      val r = ImpactReport(
        "o|ld",
        "n|ew",
        List(
          DirectChange(NodeRef("d", "t|1"), NodeKind.Task, Breaking, Cols(Set("c|1")), Nil, Nil)
        ),
        List(
          ImpactedNode(
            NodeRef("s", "t|2"),
            NodeKind.Task,
            Breaking,
            Cols(Set("x|y")),
            List("p|1", "p\n2"),
            likelyFailure = true,
            viaUnresolvedEdge = false
          )
        ),
        List(UnresolvedEdge(NodeRef("a", "b|c"), NodeRef("d", "e"), "why | not\nreally")),
        List("f|ile\nname")
      )
      val out = ImpactRenderer.render(r, "markdown", None)
      out should include("`o\\|ld` -> `n\\|ew`")
      out should include("d.t\\|1")
      out should include("c\\|1")
      out should include("x\\|y")
      out should include("p\\|1 -> p 2")
      out should include("why \\| not really")
      out should include("f\\|ile name")
      out.linesIterator.filter(_.startsWith("| BREAKING")).foreach { row =>
        row.replace("\\|", "").count(_ == '|') should (be(6) or be(7))
      }
    }
  }

  it should "always show the Unresolved section, even for an empty report" in {
    new WithSettings() {
      val empty = ImpactReport("a", "b", Nil, Nil, Nil, Nil)
      ImpactRenderer.render(empty, "text", None) should include("Unresolved")
      ImpactRenderer.render(empty, "markdown", None) should include("Unresolved")
      ImpactRenderer.render(empty, "html", None) should include("Unresolved")
      ImpactRenderer.render(empty, "json", None) should include("unresolved")
    }
  }

  it should "show unresolved edges when there are no direct changes" in {
    new WithSettings() {
      val r = ImpactReport(
        "a",
        "b",
        Nil,
        Nil,
        List(UnresolvedEdge(NodeRef("stg", "orders"), NodeRef("kpi", "py"), "python task")),
        Nil
      )
      List("text", "markdown", "html", "json").foreach { f =>
        val out = ImpactRenderer.render(r, f, None)
        out should include("python task")
        out should include("kpi")
      }
      ImpactRenderer.render(r, "text", None) should include("No changes")
    }
  }

  it should "escape html in every interpolated value" in {
    new WithSettings() {
      val r = ImpactReport(
        "<b>old</b>",
        "n&w",
        List(
          DirectChange(
            NodeRef("d", "<script>t"),
            NodeKind.Task,
            Breaking,
            Cols(Set("<i>")),
            Nil,
            List("a < b & c > d"),
            unresolved = Some("<u>x</u>")
          )
        ),
        List(
          ImpactedNode(
            NodeRef("s", "<img>"),
            NodeKind.Task,
            Breaking,
            AllColumns,
            List("p<1>", "q&2"),
            likelyFailure = false,
            viaUnresolvedEdge = false
          )
        ),
        List(UnresolvedEdge(NodeRef("a", "<e>"), NodeRef("d", "e"), "<why>")),
        List("<o>")
      )
      val html = ImpactRenderer.render(r, "html", None)
      html should include("&lt;")
      html should include("a &lt; b &amp; c &gt; d")
      html should include("p&lt;1&gt;")
      html should not include "<script>"
      html should not include "<img>"
      html should not include "<why>"
      html should not include "<o>"
      html should not include "<b>old"
      // the json contract stays raw
      ImpactRenderer.render(r, "json", None) should include("<script>t")
    }
  }

  it should "show None in html only for empty sections" in {
    new WithSettings() {
      val html = ImpactRenderer.render(report, "html", None)
      html should not include "<li>None</li>"
      ImpactRenderer.render(
        ImpactReport("a", "b", Nil, Nil, Nil, Nil),
        "html",
        None
      ) should include("<li>None</li>")
    }
  }

  it should "mark deleted direct changes in html" in {
    new WithSettings() {
      val html = ImpactRenderer.render(single(analyzable.copy(deleted = true)), "html", None)
      html should include("(deleted)")
      ImpactRenderer.render(single(analyzable), "html", None) should not include "(deleted)"
      ImpactRenderer.render(single(analyzable), "html", None) should include("breaking: no")
    }
  }

  it should "fail clearly when the template file is missing" in {
    new WithSettings() {
      val e = the[IllegalArgumentException] thrownBy
        ImpactRenderer.render(report, "html", Some("/nonexistent/tpl.j2"))
      e.getMessage shouldBe "Template not found: /nonexistent/tpl.j2"
    }
  }
}
