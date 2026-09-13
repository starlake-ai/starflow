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
}
