package ai.starlake.impact

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RebuildPlannerSpec extends AnyFlatSpec with Matchers {
  import ChangeClass._
  import RebuildAction._

  private val raw = NodeRef("raw", "orders")
  private val customers = NodeRef("raw", "customers")
  private val stg = NodeRef("stg", "orders")
  private val kpi = NodeRef("kpi", "revenue")
  private val dash = NodeRef("kpi", "dashboard")
  private val py = NodeRef("kpi", "py")

  private def col(name: String, from: (NodeRef, String)*) =
    ColumnDef(name, name, from.map { case (n, c) => SourceCol(n, c) }.toSet)

  private def taskNode(
    ref: NodeRef,
    inputs: Set[NodeRef],
    columns: List[ColumnDef],
    writeStrategy: String = "OVERWRITE"
  ) = {
    val allIds = columns.flatMap(c => c.name :: c.sources.map(_.column).toList).toSet
    NodeView(
      ref,
      NodeKind.Task,
      Nil,
      Some(TaskShape(inputs, columns, Set.empty, "", Set.empty, allIds, Map.empty, None)),
      "",
      "",
      "",
      Some(writeStrategy)
    )
  }

  private def loadNode(ref: NodeRef, columns: String*) =
    NodeView(
      ref,
      NodeKind.LoadTable,
      columns.toList.map(c => AttributeView(c, c, "string")),
      None,
      "",
      ""
    )

  private val pythonNode = NodeView(
    py,
    NodeKind.Task,
    Nil,
    Some(TaskShape.unresolved(TaskShape.PythonTask, Set.empty, Set.empty)),
    "",
    "",
    "",
    Some("OVERWRITE")
  )

  // raw.orders -> stg.orders(id, amount) -> kpi.revenue(id, total <- amount) -> kpi.dashboard(rev <- total)
  private def chain(kpiStrategy: String = "OVERWRITE", extra: List[NodeView] = Nil) =
    SnapshotView(
      "new",
      (List(
        loadNode(raw, "id", "amount"),
        taskNode(stg, Set(raw), List(col("id", raw -> "id"), col("amount", raw -> "amount"))),
        taskNode(
          kpi,
          Set(stg),
          List(col("id", stg -> "id"), col("total", stg -> "amount")),
          kpiStrategy
        ),
        taskNode(dash, Set(kpi), List(col("rev", kpi -> "total")))
      ) ++ extra).map(n => n.ref -> n).toMap,
      Map.empty
    )

  private def change(
    ref: NodeRef,
    kind: NodeKind,
    cls: ChangeClass,
    cols: Columns = AllColumns,
    deleted: Boolean = false
  ) = DirectChange(ref, kind, cls, cols, Nil, Nil, deleted = deleted)

  private def planFor(direct: List[DirectChange], view: SnapshotView = chain()): RebuildPlan = {
    val (impacted, edges) = ImpactPropagator.propagate(direct, view)
    RebuildPlanner.plan(ImpactReport("old", "new", direct, impacted, edges, Nil), view)
  }

  private def actions(plan: RebuildPlan): Map[String, RebuildAction] =
    plan.entries.map(e => e.node.name -> e.action).toMap

  private def notesOf(plan: RebuildPlan, ref: NodeRef): List[String] =
    plan.entries.find(_.node == ref).get.notes

  "RebuildPlanner" should "run a changed task and every task it impacts" in {
    val plan = planFor(List(change(stg, NodeKind.Task, Breaking, Cols(Set("amount")))))
    plan.tasksToRun shouldBe List(dash, kpi, stg)
    actions(plan).values.toSet shouldBe Set(Run)
  }

  it should "block the tasks reached only through a changed load table" in {
    val plan = planFor(List(change(raw, NodeKind.LoadTable, Breaking, Cols(Set("amount")))))
    plan.tasksToRun shouldBe Nil
    actions(plan) shouldBe Map(
      "raw.orders"    -> ReloadRequired,
      "stg.orders"    -> Blocked(List(raw)),
      "kpi.revenue"   -> Blocked(List(raw)),
      "kpi.dashboard" -> Blocked(List(raw))
    )
  }

  it should "run a task that a changed task reaches, even when a load change reaches it too" in {
    val plan = planFor(
      List(
        change(raw, NodeKind.LoadTable, Breaking, Cols(Set("amount"))),
        change(kpi, NodeKind.Task, Breaking, Cols(Set("total")))
      )
    )
    plan.tasksToRun shouldBe List(dash, kpi)
    plan.entries.map(e => e.action.label -> e.node.name) shouldBe List(
      "run"             -> "kpi.dashboard",
      "run"             -> "kpi.revenue",
      "blocked"         -> "stg.orders",
      "reload required" -> "raw.orders"
    )
  }

  it should "name every load table that blocks a task" in {
    val stgOfTwo = taskNode(
      stg,
      Set(raw, customers),
      List(col("id", raw -> "id"), col("name", customers -> "name"))
    )
    val view = chain(extra = List(loadNode(customers, "id", "name"), stgOfTwo))
    val plan = planFor(
      List(
        change(raw, NodeKind.LoadTable, Breaking),
        change(customers, NodeKind.LoadTable, Breaking)
      ),
      view
    )
    actions(plan)("stg.orders") shouldBe Blocked(List(customers, raw))
  }

  it should "not run a metadata-only change" in {
    val plan = planFor(List(change(kpi, NodeKind.Task, Metadata, Cols(Set.empty))))
    plan.tasksToRun shouldBe Nil
    actions(plan) shouldBe Map("kpi.revenue" -> NotRun("metadata only"))
  }

  it should "run a task whose own change is metadata only when an upstream change impacts it" in {
    val plan = planFor(
      List(
        change(stg, NodeKind.Task, Breaking, Cols(Set("amount"))),
        change(kpi, NodeKind.Task, Metadata, Cols(Set.empty))
      )
    )
    actions(plan)("kpi.revenue") shouldBe Run
    plan.entries.find(_.node == kpi).get.changeClass shouldBe Breaking
  }

  it should "not run a deleted task, and run what read it" in {
    val view = chain()
    val withoutKpi = view.copy(nodes = view.nodes - kpi)
    val plan = planFor(List(change(kpi, NodeKind.Task, Breaking, deleted = true)), withoutKpi)
    actions(plan)("kpi.revenue") shouldBe NotRun("deleted, drop the table by hand")
    actions(plan)("kpi.dashboard") shouldBe Run
    notesOf(plan, dash) should contain("likely failure: reads a column that no longer exists")
  }

  it should "warn that the history of an incremental task is not rebuilt" in {
    val plan = planFor(
      List(change(stg, NodeKind.Task, Breaking, Cols(Set("amount")))),
      chain(kpiStrategy = "APPEND")
    )
    notesOf(plan, kpi) shouldBe List("history not rebuilt (APPEND)")
    notesOf(plan, stg) shouldBe Nil
  }

  it should "run an added task without a history note" in {
    val added = NodeRef("kpi", "added")
    val view =
      chain(extra = List(taskNode(added, Set(kpi), List(col("id", kpi -> "id")), "APPEND")))
    val plan = planFor(List(change(added, NodeKind.Task, Added)), view)
    plan.tasksToRun shouldBe List(added)
    notesOf(plan, added) shouldBe Nil
  }

  it should "run a python task when a task change is breaking" in {
    val plan = planFor(
      List(change(stg, NodeKind.Task, Breaking, Cols(Set("amount")))),
      chain(extra = List(pythonNode))
    )
    actions(plan)("kpi.py") shouldBe Run
    notesOf(plan, py) shouldBe List("python task, inputs unknown")
  }

  it should "block a python task when only load changes are breaking" in {
    val plan =
      planFor(List(change(raw, NodeKind.LoadTable, Breaking)), chain(extra = List(pythonNode)))
    actions(plan)("kpi.py") shouldBe Blocked(List(raw))
  }

  it should "run a python task that also carries a metadata change" in {
    val plan = planFor(
      List(
        change(stg, NodeKind.Task, Breaking, Cols(Set("amount"))),
        change(py, NodeKind.Task, Metadata)
      ),
      chain(extra = List(pythonNode))
    )
    actions(plan)("kpi.py") shouldBe Run
    notesOf(plan, py) shouldBe List("python task, inputs unknown")
  }

  it should "plan nothing without changes" in {
    planFor(Nil) shouldBe RebuildPlan.empty
  }
}
