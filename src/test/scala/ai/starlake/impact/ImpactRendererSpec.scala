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
      val other = single(analyzable.copy(reasons = List("row-level: WHERE changed")))
      ImpactRenderer.render(other, "text", None) should include("unresolved: python task")
      ImpactRenderer.render(other, "markdown", None) should include("unresolved: python task")
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
}
