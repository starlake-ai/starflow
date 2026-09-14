package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TextRendererSpec extends AnyFlatSpec with Matchers {

  private val taskNode = RunNode("sales.report", "sales.report", RunNodeType.Task)
  private val boundaryNode = RunNode("ext.tbl", "ext.tbl", RunNodeType.Boundary)

  "TextRenderer" should "render start and finish progress lines" in {
    TextRenderer.progressLine(NodeStarted(taskNode)) should include("sales.report")
    val finished = NodeFinished(NodeResult(taskNode, NodeStatus.Succeeded, 1234L))
    val line = TextRenderer.progressLine(finished)
    line should include("SUCCEEDED")
    line should include("sales.report")
  }

  it should "render a summary table with one row per executable node" in {
    val summary = RunSummary(
      List(
        NodeResult(boundaryNode, NodeStatus.Satisfied, 0L),
        NodeResult(taskNode, NodeStatus.Succeeded, 42L),
        NodeResult(
          RunNode("sales.failed", "sales.failed", RunNodeType.Task),
          NodeStatus.Failed(new RuntimeException("table not found")),
          7L
        ),
        NodeResult(
          RunNode("sales.skipped", "sales.skipped", RunNodeType.Task),
          NodeStatus.SkippedUpstreamFailed,
          0L
        )
      )
    )
    val table = TextRenderer.summaryTable(summary)
    table should include("sales.report")
    table should include("SUCCEEDED")
    table should include("sales.failed")
    table should include("table not found")
    table should include("SKIPPED_UPSTREAM_FAILED")
    (table should not).include("ext.tbl")
  }

  it should "keep a multi-line error message on a single row" in {
    // A JDBC driver typically reports the position of the syntax error on a second line. The
    // formatter pads every cell to the widest raw cell, so an unprocessed newline would both break
    // the borders and stretch the Cause column to the longest line.
    val jdbcMessage =
      "ERROR: relation \"orders\" does not exist\n  Position: 15\n  Hint: check the schema"
    val summary = RunSummary(
      List(
        NodeResult(taskNode, NodeStatus.Succeeded, 42L),
        NodeResult(
          RunNode("sales.failed", "sales.failed", RunNodeType.Task),
          NodeStatus.Failed(new RuntimeException(jdbcMessage)),
          7L
        )
      )
    )
    val table = TextRenderer.summaryTable(summary)

    // header plus two rows plus the three separators: no stray newline from the message
    table.linesIterator.size shouldBe 6
    table should include("relation \"orders\" does not exist Position: 15")
    // the separators bound the column, so a stable width proves the padding did not blow up
    val widths = table.linesIterator.map(_.length).toList.distinct
    widths.size shouldBe 1
    widths.head should be < 200
  }

  it should "truncate an overlong error message" in {
    val long = "x" * 500
    val summary = RunSummary(
      List(
        NodeResult(taskNode, NodeStatus.Failed(new RuntimeException(long)), 1L)
      )
    )
    val table = TextRenderer.summaryTable(summary)
    table.linesIterator.map(_.length).max should be < 200
    (table should not).include(long)
    table should include("x" * 120)
  }

  it should "fall back to the exception class when the message is null" in {
    val summary = RunSummary(
      List(NodeResult(taskNode, NodeStatus.Failed(new RuntimeException()), 1L))
    )
    TextRenderer.summaryTable(summary) should include("RuntimeException")
  }

  private def planDag: RunDag = RunDag(
    nodes = Map(
      "sales.orders"  -> RunNode("sales.orders", "sales.orders", RunNodeType.LoadTable),
      "sales.clean"   -> RunNode("sales.clean", "sales.clean", RunNodeType.Task),
      "sales.revenue" -> RunNode("sales.revenue", "sales.revenue", RunNodeType.Task)
    ),
    parents = Map(
      "sales.orders"  -> Set.empty[String],
      "sales.clean"   -> Set("sales.orders"),
      "sales.revenue" -> Set("sales.clean")
    )
  )

  "TextRenderer.levels" should "number a chain from zero" in {
    val levels = TextRenderer.levels(planDag)
    levels("sales.orders") shouldBe 0
    levels("sales.clean") shouldBe 1
    levels("sales.revenue") shouldBe 2
  }

  it should "take the deepest parent when a node has several" in {
    val dag = RunDag(
      nodes = List("d.a", "d.b", "d.c")
        .map(id => id -> RunNode(id, id, RunNodeType.Task))
        .toMap,
      parents = Map("d.a" -> Set.empty[String], "d.b" -> Set("d.a"), "d.c" -> Set("d.a", "d.b"))
    )
    TextRenderer.levels(dag)("d.c") shouldBe 2
  }

  "TextRenderer.planTable" should "list every node with its level and type" in {
    val selection = Selection(
      ids = planDag.nodes.keySet,
      selectMatches = List(SelectorMatch("+sales.revenue", 3)),
      excludeMatches = Nil
    )
    val plan = TextRenderer.planTable(planDag, selection)
    plan should include("Execution plan: 3 tasks, 3 levels")
    plan should include("sales.orders")
    plan should include("load")
    plan should include("sales.revenue")
    plan should include("transform")
    plan should include("Selectors:")
    plan should include("+sales.revenue")
    plan should include("matched 3 tasks")
  }

  it should "order rows by level then by name" in {
    val selection = Selection(planDag.nodes.keySet, Nil, Nil)
    val plan = TextRenderer.planTable(planDag, selection)
    plan.indexOf("sales.orders") should be < plan.indexOf("sales.clean")
    plan.indexOf("sales.clean") should be < plan.indexOf("sales.revenue")
  }

  it should "report what an exclusion removed" in {
    val selection = Selection(
      ids = Set("sales.orders"),
      selectMatches = List(SelectorMatch("sales.*", 3)),
      excludeMatches = List(SelectorMatch("tag:wip", 2))
    )
    val plan = TextRenderer.planTable(planDag.restrictTo(Set("sales.orders")), selection)
    plan should include("--exclude tag:wip")
    plan should include("removed 2 tasks")
  }

  it should "say so when the whole project was selected" in {
    val plan = TextRenderer.planTable(planDag, Selection(planDag.nodes.keySet, Nil, Nil))
    plan should include("whole project")
  }

  it should "use the singular for a single task and level" in {
    val single = RunDag(
      nodes = Map("a.one" -> RunNode("a.one", "a.one", RunNodeType.Task)),
      parents = Map("a.one" -> Set.empty[String])
    )
    val plan = TextRenderer.planTable(single, Selection(Set("a.one"), Nil, Nil))
    plan should include("1 task, 1 level")
    (plan should not).include("1 tasks")
  }

  "TextRenderer.emptySelectionMessage" should "name only the selectors that matched nothing" in {
    val selection = Selection(
      ids = Set.empty,
      selectMatches = List(SelectorMatch("sales.clean", 1), SelectorMatch("nope.nothing", 0)),
      excludeMatches = Nil
    )
    val message = TextRenderer.emptySelectionMessage(selection)
    message should include("nope.nothing")
    (message should not).include("sales.clean")
  }

  it should "blame the exclusions when every select matched" in {
    val selection = Selection(
      ids = Set.empty,
      selectMatches = List(SelectorMatch("sales.*", 3)),
      excludeMatches = List(SelectorMatch("tag:daily", 3))
    )
    val message = TextRenderer.emptySelectionMessage(selection)
    message should include("tag:daily")
    message.toLowerCase should include("exclude")
  }

  it should "say the project has nothing to run when no selector was given" in {
    val message = TextRenderer.emptySelectionMessage(Selection(Set.empty, Nil, Nil))
    message.toLowerCase should include("no executable task")
    message should include("transform")
  }
}
