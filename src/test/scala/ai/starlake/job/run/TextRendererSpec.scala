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
}
