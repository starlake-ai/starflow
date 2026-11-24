package ai.starlake.impact

import ai.starlake.schema.model.WriteStrategyType

/** What `transform --impacted` does with a node of the change impact report. */
sealed abstract class RebuildAction(val label: String, val rank: Int)

object RebuildAction {
  case object Run extends RebuildAction("run", 0)

  /** Reached only through changed load tables: the task would read the old physical table. */
  final case class Blocked(by: List[NodeRef]) extends RebuildAction("blocked", 1)

  /** A changed load table: only loading its files again changes it. */
  case object ReloadRequired extends RebuildAction("reload required", 2)
  final case class NotRun(reason: String) extends RebuildAction("not run", 3)
}

final case class PlannedNode(
  node: NodeRef,
  kind: NodeKind,
  changeClass: ChangeClass,
  action: RebuildAction,
  notes: List[String]
) {

  /** Why the node is not run, then its notes, on one line. */
  def detail: String = {
    val why = action match {
      case RebuildAction.Blocked(by) =>
        List(s"blocked by reload of ${by.map(_.name).mkString(", ")}")
      case RebuildAction.NotRun(reason) => List(reason)
      case _                            => Nil
    }
    (why ++ notes).mkString("; ")
  }
}

/** The entries are sorted by action (run, blocked, reload required, not run), then by name. */
final case class RebuildPlan(entries: List[PlannedNode]) {
  def tasksToRun: List[NodeRef] =
    entries.collect { case e if e.action == RebuildAction.Run && e.kind == NodeKind.Task => e.node }
  def isEmpty: Boolean = entries.isEmpty
}

object RebuildPlan {
  val empty: RebuildPlan = RebuildPlan(Nil)
}

/** Decides from a change impact report which tasks to run. A task runs once, as a normal run would:
  * an incremental task only rewrites its current slice, and its entry says so.
  */
object RebuildPlanner {
  import RebuildAction._

  val Deleted = "deleted, drop the table by hand"
  val MetadataOnly = "metadata only"
  val LikelyFailure = "likely failure: reads a column that no longer exists"
  val PythonUnknown = "python task, inputs unknown"

  def plan(report: ImpactReport, view: SnapshotView): RebuildPlan = {
    val directByRef = report.direct.map(d => d.node -> d).toMap
    val impactedByRef = report.impacted.filter(_.kind == NodeKind.Task).map(n => n.node -> n).toMap
    val taskChanges = report.direct.filter(_.kind == NodeKind.Task)
    val loadChanges = report.direct.filter(d =>
      d.kind == NodeKind.LoadTable && d.changeClass != ChangeClass.Metadata
    )

    // the impact of the task changes alone: what it reaches can run, what only load changes reach
    // would read tables not loaded again yet
    val reachedFromTasks: Set[NodeRef] =
      ImpactPropagator.propagate(taskChanges, view)._1.map(_.node).toSet
    lazy val reachedFromLoads: List[(NodeRef, Set[NodeRef])] =
      loadChanges.map(l => l.node -> ImpactPropagator.propagate(List(l), view)._1.map(_.node).toSet)

    def blockers(ref: NodeRef): List[NodeRef] =
      reachedFromLoads
        .collect { case (load, reached) if reached.contains(ref) => load }
        .sortBy(_.name)

    def historyNote(ref: NodeRef, cls: ChangeClass): List[String] =
      if (cls != ChangeClass.Breaking && cls != ChangeClass.NonBreaking) Nil
      else
        view.nodes
          .get(ref)
          .flatMap(_.writeStrategy)
          .filter(_ != WriteStrategyType.OVERWRITE.value)
          .map(ws => s"history not rebuilt ($ws)")
          .toList

    // python tasks read tables nobody can tell offline: the report lists them as unresolved edges
    val pythonRefs: Set[NodeRef] =
      report.unresolved.filter(_.from == ImpactPropagator.AnyNode).map(_.to).toSet
    val breakingTaskChange = taskChanges.exists(_.changeClass == ChangeClass.Breaking)
    val breakingLoads =
      loadChanges.filter(_.changeClass == ChangeClass.Breaking).map(_.node).sortBy(_.name)

    def pythonEntry(ref: NodeRef): PlannedNode =
      if (breakingTaskChange)
        PlannedNode(
          ref,
          NodeKind.Task,
          ChangeClass.Breaking,
          Run,
          PythonUnknown :: historyNote(ref, ChangeClass.Breaking)
        )
      else PlannedNode(ref, NodeKind.Task, ChangeClass.Breaking, Blocked(breakingLoads), Nil)

    def entry(ref: NodeRef): PlannedNode = {
      val d = directByRef.get(ref)
      val onlyPythonUnknown =
        pythonRefs.contains(ref) && d.forall(x =>
          !x.deleted && x.changeClass == ChangeClass.Metadata
        )
      if (onlyPythonUnknown) pythonEntry(ref) else plain(ref)
    }

    def plain(ref: NodeRef): PlannedNode = {
      val d = directByRef.get(ref)
      val i = impactedByRef.get(ref)
      val kind = d.map(_.kind).getOrElse(NodeKind.Task)
      val cls = (d.map(_.changeClass).toList ++ i.map(_.changeClass)).reduce(ChangeClass.worst)
      val action =
        if (d.exists(_.deleted)) NotRun(Deleted)
        else if (kind == NodeKind.LoadTable) {
          if (d.exists(_.changeClass != ChangeClass.Metadata)) ReloadRequired
          else NotRun(MetadataOnly)
        } else if (d.exists(_.changeClass != ChangeClass.Metadata) || reachedFromTasks(ref)) Run
        else if (i.isEmpty) NotRun(MetadataOnly)
        else
          blockers(ref) match {
            case Nil => Run // over-report rather than miss impact
            case by  => Blocked(by)
          }
      val notes =
        if (action != Run) Nil
        else historyNote(ref, cls) ++ i.filter(_.likelyFailure).map(_ => LikelyFailure)
      PlannedNode(ref, kind, cls, action, notes)
    }

    val pythonEntries = pythonRefs.toList
      .filterNot(r => directByRef.contains(r) || impactedByRef.contains(r))
      .map(pythonEntry)

    val entries = (directByRef.keySet ++ impactedByRef.keySet).toList.map(entry) ++ pythonEntries
    RebuildPlan(entries.sortBy(e => (e.action.rank, e.node.name)))
  }
}
