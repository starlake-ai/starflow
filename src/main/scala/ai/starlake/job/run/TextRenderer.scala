package ai.starlake.job.run

import ai.starlake.utils.TableFormatter

object TextRenderer {

  def statusLabel(status: NodeStatus): String =
    status match {
      case NodeStatus.Succeeded             => "SUCCEEDED"
      case NodeStatus.Failed(_)             => "FAILED"
      case NodeStatus.SkippedUpstreamFailed => "SKIPPED_UPSTREAM_FAILED"
      case NodeStatus.Satisfied             => "SATISFIED"
    }

  def progressLine(event: RunEvent): String =
    event match {
      case NodeStarted(node) =>
        s"[run] STARTED ${node.displayName}"
      case NodeFinished(result) =>
        s"[run] ${statusLabel(result.status)} ${result.node.displayName} (${result.durationMillis} ms)"
    }

  private val MaxCauseLength = 120

  private[run] def condense(message: String): String =
    message.replaceAll("\\s+", " ").trim.take(MaxCauseLength)

  def summaryTable(summary: RunSummary): String = {
    val headers = List("Task", "Type", "Status", "Duration (ms)", "Cause")
    val rows = summary.results
      .filterNot(_.status == NodeStatus.Satisfied)
      .map { result =>
        val cause = result.status match {
          case NodeStatus.Failed(e) =>
            // TableFormatter sizes every column on its longest raw cell and pads with %Ns, so a
            // multi-line message (JDBC errors carry a "Position:" line) would embed newlines in
            // the borders and stretch the column to the longest line. Keep it to one short line.
            condense(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
          case _ => ""
        }
        val typeLabel = result.node.typ match {
          case RunNodeType.Task      => "transform"
          case RunNodeType.LoadTable => "load"
          case RunNodeType.Boundary  => "boundary"
        }
        List(
          result.node.displayName,
          typeLabel,
          statusLabel(result.status),
          result.durationMillis.toString,
          cause
        )
      }
    if (rows.isEmpty) "No tasks executed."
    else TableFormatter.format(headers :: rows)
  }
}
