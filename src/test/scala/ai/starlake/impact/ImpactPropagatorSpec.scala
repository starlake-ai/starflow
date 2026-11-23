package ai.starlake.impact

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImpactPropagatorSpec extends AnyFlatSpec with Matchers {
  import ChangeClass._

  private val src = NodeRef("raw", "orders")
  private val stg = NodeRef("stg", "orders")
  private val kpi = NodeRef("kpi", "revenue")
  private val dash = NodeRef("kpi", "dashboard")

  private def col(name: String, from: (NodeRef, String)*) =
    ColumnDef(name, name, from.map { case (n, c) => SourceCol(n, c) }.toSet)

  private def taskNode(
    ref: NodeRef,
    inputs: Set[NodeRef],
    columns: List[ColumnDef],
    rowIds: Set[String] = Set.empty,
    star: Set[NodeRef] = Set.empty,
    unresolvedInputs: Map[NodeRef, String] = Map.empty,
    unresolved: Option[String] = None
  ) = {
    val allIds = columns.flatMap(c => c.name :: c.sources.map(_.column).toList).toSet ++ rowIds
    NodeView(
      ref,
      NodeKind.Task,
      Nil,
      Some(TaskShape(inputs, columns, star, "", rowIds, allIds, unresolvedInputs, unresolved)),
      "",
      ""
    )
  }

  private val loadNode = NodeView(
    src,
    NodeKind.LoadTable,
    List(
      AttributeView("id", "id", "long"),
      AttributeView("amount", "amount", "decimal"),
      AttributeView("status", "status", "string")
    ),
    None,
    "",
    ""
  )

  // raw.orders -> stg.orders(id, amount; WHERE status) -> kpi.revenue(total <- amount) -> kpi.dashboard(rev <- total)
  private def chain(
    stgNode: NodeView = taskNode(
      stg,
      Set(src),
      List(col("id", src -> "id"), col("amount", src -> "amount")),
      rowIds = Set("status")
    )
  ) =
    SnapshotView(
      "new",
      List(
        loadNode,
        stgNode,
        taskNode(kpi, Set(stg), List(col("total", stg -> "amount"), col("id", stg -> "id"))),
        taskNode(dash, Set(kpi), List(col("rev", kpi -> "total")))
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )

  private def change(
    cls: ChangeClass,
    cols: Columns,
    details: List[ColumnChange] = Nil,
    deleted: Boolean = false
  ) =
    DirectChange(src, NodeKind.LoadTable, cls, cols, details, Nil, deleted = deleted)

  private def run(c: DirectChange, view: SnapshotView = chain()) =
    ImpactPropagator.propagate(List(c), view)._1.map(n => n.node -> n).toMap

  "propagate" should "not trace METADATA or ADDED" in {
    run(change(Metadata, Cols(Set.empty))) shouldBe empty
    run(change(Added, Cols(Set.empty))) shouldBe empty
  }

  it should "follow a column through the chain and stop where it is not read" in {
    val r = run(change(Breaking, Cols(Set("amount")), List(ExpressionChanged("amount"))))
    r(stg).columns shouldBe Cols(Set("amount"))
    r(kpi).columns shouldBe Cols(Set("total"))
    r(dash).columns shouldBe Cols(Set("rev"))
    r(dash).path shouldBe List(
      "raw.orders[amount]",
      "stg.orders[amount]",
      "kpi.revenue[total]",
      "kpi.dashboard[rev]"
    )
  }

  it should "not impact consumers of an unrelated column" in {
    val r = run(change(Breaking, Cols(Set("id")), List(ExpressionChanged("id"))))
    r(kpi).columns shouldBe Cols(Set("id"))
    r.contains(dash) shouldBe false
  }

  it should "turn a change on a row-level column into all columns" in {
    val r = run(change(Breaking, Cols(Set("status")), List(TypeChanged("status", "string", "int"))))
    r(stg).columns shouldBe AllColumns
    r(dash).columns shouldBe AllColumns
  }

  it should "flag a consumer that mentions a removed column as a likely failure" in {
    val r = run(change(Breaking, Cols(Set("amount")), List(ColumnRemoved("amount"))))
    r(stg).likelyFailure shouldBe true
    r(stg).columns shouldBe AllColumns
  }

  it should "propagate a deleted node to all its consumers as likely failures" in {
    val r = run(change(Breaking, AllColumns, deleted = true))
    r(stg).likelyFailure shouldBe true
    r(dash).changeClass shouldBe Breaking
  }

  it should "only propagate added columns to star consumers" in {
    val starStg = taskNode(
      stg,
      Set(src),
      List(col("id", src -> "id"), col("amount", src -> "amount")),
      star = Set(src)
    )
    run(change(NonBreaking, Cols(Set("note")), List(ColumnAdded("note"))), chain(starStg))(
      stg
    ).changeClass shouldBe NonBreaking
    run(change(NonBreaking, Cols(Set("note")), List(ColumnAdded("note")))).isEmpty shouldBe true
  }

  it should "fall back to table level through an unresolved edge and record it" in {
    val opaque =
      taskNode(stg, Set(src), Nil, unresolvedInputs = Map(src -> "columns unknown offline"))
    val (nodes, edges) =
      ImpactPropagator.propagate(List(change(Breaking, Cols(Set("id")))), chain(opaque))
    val stgNode = nodes.find(_.node == stg).get
    stgNode.columns shouldBe AllColumns
    stgNode.viaUnresolvedEdge shouldBe true
    edges shouldBe List(UnresolvedEdge(src, stg, "columns unknown offline"))
  }

  it should "keep the worst class through a diamond" in {
    val left = NodeRef("stg", "left")
    val right = NodeRef("stg", "right")
    val sink = NodeRef("kpi", "sink")
    val view = SnapshotView(
      "new",
      List(
        loadNode,
        taskNode(left, Set(src), List(col("note", src -> "note")), star = Set(src)),
        taskNode(right, Set(src), List(col("amount", src -> "amount"))),
        taskNode(
          sink,
          Set(left, right),
          List(col("a", left -> "note"), col("b", right -> "amount")),
          star = Set(left)
        )
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    val changes = List(
      change(NonBreaking, Cols(Set("note")), List(ColumnAdded("note"))),
      change(Breaking, Cols(Set("amount")), List(ExpressionChanged("amount")))
    )
    val r = ImpactPropagator.propagate(changes, view)._1.map(n => n.node -> n).toMap
    r(sink).changeClass shouldBe Breaking
  }

  it should "show the path that made a node impacted on all its columns" in {
    val byColumn = NodeRef("stg", "a_by_column") // reached first: consumers are sorted by name
    val byRow = NodeRef("stg", "b_by_row")
    val sink = NodeRef("kpi", "sink")
    val view = SnapshotView(
      "new",
      List(
        loadNode,
        taskNode(byColumn, Set(src), List(col("x", src -> "amount"))),
        taskNode(byRow, Set(src), List(col("id", src -> "id")), rowIds = Set("status")),
        taskNode(
          sink,
          Set(byColumn, byRow),
          List(col("v", byColumn -> "x"), col("w", byRow -> "id"))
        )
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    val r =
      run(change(Breaking, Cols(Set("amount", "status")), List(ExpressionChanged("amount"))), view)
    r(sink).columns shouldBe AllColumns
    // the route through the row-level change explains "all columns", not the first one found
    r(sink).path shouldBe List("raw.orders[amount, status]", "stg.b_by_row", "kpi.sink")
  }

  it should "keep the shortest path when a later route adds nothing worse" in {
    val r = run(change(Breaking, Cols(Set("amount")), List(ExpressionChanged("amount"))))
    r(dash).path shouldBe List(
      "raw.orders[amount]",
      "stg.orders[amount]",
      "kpi.revenue[total]",
      "kpi.dashboard[rev]"
    )
  }

  it should "terminate on cycles" in {
    val a = NodeRef("c", "a")
    val b = NodeRef("c", "b")
    val view = SnapshotView(
      "new",
      List(
        loadNode,
        taskNode(a, Set(src, b), List(col("x", src -> "amount"), col("y", b -> "x"))),
        taskNode(b, Set(a), List(col("x", a -> "x")))
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    val r = ImpactPropagator.propagate(List(change(Breaking, Cols(Set("amount")))), view)._1
    r.map(_.node).toSet shouldBe Set(a, b)
  }

  // Beyond the brief: the review points of the propagation.

  it should "merge the column sets of a node reached through two paths and carry the union downstream" in {
    val left = NodeRef("stg", "left")
    val right = NodeRef("stg", "right")
    val sink = NodeRef("kpi", "sink")
    val top = NodeRef("kpi", "top")
    val view = SnapshotView(
      "new",
      List(
        loadNode,
        taskNode(left, Set(src), List(col("id", src -> "id"))),
        taskNode(right, Set(src), List(col("amount", src -> "amount"))),
        taskNode(sink, Set(left, right), List(col("a", left -> "id"), col("b", right -> "amount"))),
        taskNode(top, Set(sink), List(col("ta", sink -> "a"), col("tb", sink -> "b")))
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    val r = run(change(Breaking, Cols(Set("id", "amount"))), view)
    r(sink).columns shouldBe Cols(Set("a", "b"))
    r(top).columns shouldBe Cols(Set("ta", "tb"))
  }

  it should "flag an unresolved consumer that mentions a removed column as a likely failure" in {
    val opaque = NodeView(
      stg,
      NodeKind.Task,
      Nil,
      Some(TaskShape.unresolved("python task", Set(src), Set("amount"))),
      "",
      ""
    )
    val r = run(change(Breaking, Cols(Set("amount")), List(ColumnRemoved("amount"))), chain(opaque))
    r(stg).likelyFailure shouldBe true
    r(stg).viaUnresolvedEdge shouldBe true
    r(stg).changeClass shouldBe Breaking
  }

  it should "flag consumers of a deleted node reached through an unresolved edge as likely failures" in {
    val opaque = taskNode(stg, Set(src), Nil, unresolvedInputs = Map(src -> "table unknown"))
    val r = run(change(Breaking, AllColumns, deleted = true), chain(opaque))
    r(stg).likelyFailure shouldBe true
    r(stg).changeClass shouldBe Breaking
  }

  it should "keep the unresolved flag when a node is reached again through an unresolved edge" in {
    val other = NodeRef("raw", "other")
    val otherNode =
      NodeView(other, NodeKind.LoadTable, List(AttributeView("k", "k", "long")), None, "", "")
    val mixed = taskNode(
      stg,
      Set(src, other),
      List(col("amount", src -> "amount")),
      unresolvedInputs = Map(other -> "columns unknown offline")
    )
    val view = SnapshotView("new", chain(mixed).nodes + (other -> otherNode), Map.empty)
    val changes = List(
      change(Breaking, AllColumns),
      DirectChange(other, NodeKind.LoadTable, Breaking, Cols(Set("k")), Nil, Nil)
    )
    val (nodes, edges) = ImpactPropagator.propagate(changes, view)
    val stgNode = nodes.find(_.node == stg).get
    stgNode.columns shouldBe AllColumns
    stgNode.viaUnresolvedEdge shouldBe true
    edges shouldBe List(UnresolvedEdge(other, stg, "columns unknown offline"))
  }

  it should "record each unresolved edge once" in {
    val a = NodeRef("c", "a")
    val opaqueB = NodeRef("c", "b")
    val view = SnapshotView(
      "new",
      List(
        loadNode,
        taskNode(
          a,
          Set(src, opaqueB),
          List(col("x", src -> "amount"), col("y", src -> "id"), col("z", opaqueB -> "x"))
        ),
        NodeView(
          opaqueB,
          NodeKind.Task,
          Nil,
          Some(TaskShape.unresolved("parse error", Set(a), Set.empty)),
          "",
          ""
        )
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    val changes = List(
      change(Breaking, Cols(Set("amount"))),
      change(Breaking, Cols(Set("id")))
    )
    val (_, edges) = ImpactPropagator.propagate(changes, view)
    edges shouldBe List(UnresolvedEdge(a, opaqueB, "parse error"))
  }

  it should "match upstream column names case-insensitively" in {
    val r = run(change(Breaking, Cols(Set("Amount")), List(ExpressionChanged("Amount"))))
    r(stg).columns shouldBe Cols(Set("amount"))
    r(dash).columns shouldBe Cols(Set("rev"))
    run(change(Breaking, Cols(Set("STATUS"))))(stg).columns shouldBe AllColumns
    run(change(Breaking, Cols(Set("Amount")), List(ColumnRemoved("Amount"))))(
      stg
    ).likelyFailure shouldBe true
  }

  it should "match an ambiguous or unknown single-part input by table name and record the edge" in {
    val rawCustomers = NodeRef("raw", "customers")
    val stgCustomers = NodeRef("stg", "customers")
    val reader = NodeRef("kpi", "customers")
    // FROM customers: two tables have that name, the input keeps no domain
    val byName = NodeRef("", "customers")
    val view = SnapshotView(
      "new",
      List(
        NodeView(
          rawCustomers,
          NodeKind.LoadTable,
          List(AttributeView("id", "id", "long")),
          None,
          "",
          ""
        ),
        taskNode(stgCustomers, Set(rawCustomers), List(col("id", rawCustomers -> "id"))),
        taskNode(
          reader,
          Set(byName),
          List(col("id", byName -> "id")),
          unresolvedInputs = Map(byName -> "columns unknown")
        )
      ).map(n => n.ref -> n).toMap,
      Map.empty
    )
    view.consumersOf(rawCustomers).map(_.ref) shouldBe List(reader, stgCustomers)
    val deleted =
      DirectChange(rawCustomers, NodeKind.LoadTable, Breaking, AllColumns, Nil, Nil, deleted = true)
    val (nodes, edges) = ImpactPropagator.propagate(List(deleted), view)
    val r = nodes.map(n => n.node -> n).toMap
    r(reader).likelyFailure shouldBe true
    r(reader).viaUnresolvedEdge shouldBe true
    edges should contain(
      UnresolvedEdge(rawCustomers, reader, "ambiguous or unknown reference 'customers'")
    )
  }

  it should "list every python task as not traced when a change is breaking" in {
    val py = NodeRef("kpi", "py")
    val python = NodeView(
      py,
      NodeKind.Task,
      Nil,
      Some(TaskShape.unresolved(TaskShape.PythonTask, Set.empty, Set.empty)),
      "",
      ""
    )
    val view = SnapshotView(
      "new",
      chain().nodes + (py -> python),
      Map.empty
    )
    val notTraced =
      UnresolvedEdge(NodeRef("", "(any)"), py, "python task, inputs unknown, not traced")
    ImpactPropagator.propagate(List(change(Breaking, AllColumns)), view)._2 should contain(
      notTraced
    )
    ImpactPropagator
      .propagate(
        List(change(NonBreaking, Cols(Set("note")), List(ColumnAdded("note")))),
        view
      )
      ._2 should not contain notTraced
  }
}
