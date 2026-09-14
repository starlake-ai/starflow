package ai.starlake.job.run

import ai.starlake.utils.TableFormatter

import scala.collection.mutable

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
        val nodeType = typeLabel(result.node.typ)
        List(
          result.node.displayName,
          nodeType,
          statusLabel(result.status),
          result.durationMillis.toString,
          cause
        )
      }
    if (rows.isEmpty) "No tasks executed."
    else TableFormatter.format(headers :: rows)
  }

  private def typeLabel(typ: RunNodeType): String =
    typ match {
      case RunNodeType.Task      => "transform"
      case RunNodeType.LoadTable => "load"
      case RunNodeType.Boundary  => "boundary"
    }

  /** Topological level of every node: 0 with no parents, else one more than the deepest parent.
    * Memoized by hand rather than with getOrElseUpdate, which is not safe to call reentrantly.
    */
  private[run] def levels(dag: RunDag): Map[String, Int] = {
    val memo = mutable.Map[String, Int]()
    def level(id: String): Int =
      memo.get(id) match {
        case Some(value) => value
        case None =>
          val ups = dag.parents.getOrElse(id, Set.empty)
          val value = if (ups.isEmpty) 0 else ups.map(level).max + 1
          memo(id) = value
          value
      }
    dag.nodes.keys.map(id => id -> level(id)).toMap
  }

  private def plural(count: Int, word: String): String =
    if (count == 1) s"$count $word" else s"$count ${word}s"

  /** The `--dry-run` output. Goes to stdout: it is this invocation's machine-consumable result, the
    * same role the summary table plays in a real run. Rows are sorted by level then name so the
    * output is stable enough to diff in a pull request (spec section 7).
    *
    * `dag` is expected to be already restricted to the selected executable nodes, so boundaries do
    * not appear and levels have no gaps.
    */
  def planTable(dag: RunDag, selection: Selection): String = {
    val level = levels(dag)
    val nodes = dag.nodes.values.toList.sortBy(node => (level(node.id), node.displayName))
    val levelCount = if (nodes.isEmpty) 0 else level.values.max + 1
    val headline =
      s"Execution plan: ${plural(nodes.size, "task")}, ${plural(levelCount, "level")}"
    val table = TableFormatter.format(
      List("Level", "Task", "Type") ::
      nodes.map(node => List(level(node.id).toString, node.displayName, typeLabel(node.typ)))
    )
    val selectorLines =
      selection.selectMatches.map(m => s"  ${m.expr}  matched ${plural(m.matched, "task")}") ++
      selection.excludeMatches.map(m =>
        s"  --exclude ${m.expr}  removed ${plural(m.matched, "task")}"
      )
    val selectors =
      if (selectorLines.isEmpty) "Selectors:\n  none given, the whole project was selected"
      else ("Selectors:" :: selectorLines).mkString("\n")
    s"$headline\n\n$table\n$selectors"
  }

  /** Explains an empty selection. Spec section 4: never silently succeed on an empty run, and name
    * the selectors that matched nothing. Which selectors those are depends on how the set emptied,
    * so blaming the wrong half would send the user hunting a typo that is not there.
    */
  def emptySelectionMessage(selection: Selection): String = {
    val unmatched = selection.selectMatches.filter(_.matched == 0).map(_.expr)
    if (unmatched.nonEmpty)
      s"Selection matched no task. These selectors matched nothing: ${unmatched.mkString(", ")}." +
      " Run with --dry-run and no selector to list what the project graph contains."
    else if (selection.excludeMatches.exists(_.matched > 0)) {
      // Presence is not effect: `--exclude foo.bar` on a project with no transform is present and
      // removed nothing, and blaming it would send the user hunting a typo that is not there. Only
      // exclusions that actually shrank the set emptied it, and only those are named.
      val effective = selection.excludeMatches.filter(_.matched > 0).map(_.expr)
      "Selection matched no task: --exclude " + effective.mkString(", ") +
      " removed every selected task."
    } else
      "The project graph contains no executable task. `starlake run` builds its graph from" +
      " transform lineage, so a project with no transform has nothing to run."
  }
}
