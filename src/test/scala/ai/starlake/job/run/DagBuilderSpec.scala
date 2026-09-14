package ai.starlake.job.run

import ai.starlake.lineage.TaskViewDependency
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DagBuilderSpec extends AnyFlatSpec with Matchers {

  private def dep(
    name: String,
    typ: String,
    parent: String = "",
    parentTyp: String = ""
  ): TaskViewDependency =
    TaskViewDependency(
      name = name,
      typ = typ,
      parent = parent,
      parentTyp = parentTyp,
      parentRef = parent,
      writeStrategy = None,
      dataset_triggering_strategy = None
    )

  private def buildOrFail(
    deps: List[TaskViewDependency],
    loadTables: Set[String] = Set.empty
  ): RunDag =
    DagBuilder.build(deps, loadTables) match {
      case Right(dag)  => dag
      case Left(cycle) => fail(s"unexpected cycle: ${cycle.mkString(" -> ")}")
    }

  "DagBuilder" should "build a single-node dag" in {
    val dag = buildOrFail(List(dep("sales.report", "task")))
    dag.nodes.keySet shouldBe Set("sales.report")
    dag.nodes("sales.report").typ shouldBe RunNodeType.Task
    dag.parents("sales.report") shouldBe Set.empty[String]
  }

  it should "build a diamond without duplicating nodes" in {
    val deps = List(
      dep("d.top", "task"),
      dep("d.left", "task", "d.top", "task"),
      dep("d.right", "task", "d.top", "task"),
      dep("d.bottom", "task", "d.left", "task"),
      dep("d.bottom", "task", "d.right", "task")
    )
    val dag = buildOrFail(deps)
    dag.nodes.size shouldBe 4
    dag.parents("d.bottom") shouldBe Set("d.left", "d.right")
    dag.children("d.top") shouldBe Set("d.left", "d.right")
  }

  it should "handle disconnected components" in {
    val deps = List(
      dep("a.one", "task"),
      dep("a.two", "task", "a.one", "task"),
      dep("b.solo", "task")
    )
    val dag = buildOrFail(deps)
    dag.nodes.size shouldBe 3
    dag.parents("b.solo") shouldBe Set.empty[String]
  }

  it should "detect a cycle and return its path" in {
    val deps = List(
      dep("c.a", "task", "c.b", "task"),
      dep("c.b", "task", "c.a", "task")
    )
    DagBuilder.build(deps, Set.empty) match {
      case Left(cycle) =>
        cycle.size should be >= 3
        cycle.head shouldBe cycle.last
        cycle.toSet shouldBe Set("c.a", "c.b")
      case Right(_) => fail("expected a cycle")
    }
  }

  it should "drop self-referencing edges instead of reporting a cycle" in {
    // A task reading its own output table (incremental pattern) is not a scheduling dependency.
    val dag = buildOrFail(List(dep("inc.merge", "task", "inc.merge", "table")))
    dag.parents("inc.merge") shouldBe Set.empty[String]
  }

  it should "classify table parents as LoadTable when they belong to the project" in {
    val deps = List(dep("t.job", "task", "dom.tbl", "table"))
    val dag = buildOrFail(deps, loadTables = Set("dom.tbl"))
    dag.nodes("dom.tbl").typ shouldBe RunNodeType.LoadTable
  }

  it should "classify unknown table parents as Boundary" in {
    val deps = List(dep("t.job", "task", "ext.tbl", "table"))
    val dag = buildOrFail(deps)
    dag.nodes("ext.tbl").typ shouldBe RunNodeType.Boundary
  }

  it should "match names case-insensitively and keep one node" in {
    val deps = List(
      dep("D.Top", "task"),
      dep("d.child", "task", "d.top", "task")
    )
    val dag = buildOrFail(deps)
    dag.nodes.size shouldBe 2
    dag.parents("d.child") shouldBe Set("d.top")
  }

  it should "prefer the Task type when a name is seen as both task and table" in {
    val deps = List(
      dep("x.out", "task"),
      dep("x.next", "task", "x.out", "table")
    )
    val dag = buildOrFail(deps, loadTables = Set("x.out"))
    dag.nodes("x.out").typ shouldBe RunNodeType.Task
  }

  it should "resolve a transform/load name collision to the transform, in either order" in {
    // Defensively pins `register`'s ranking rule in isolation: Task outranks LoadTable, so two
    // declarations for the same id collapse to a single Task node, in both registration orders.
    // This is a unit invariant, not a reproduction of real lineage: TaskViewDependency resolves a
    // two-part parent reference against schemaHandler.tasks() by fullName() or by the
    // (domain, table) pair and emits TASK_TYPE on a hit, so when a transform named dual.thing
    // exists, lineage never emits a TABLE_TYPE dependency for dual.thing in the first place - the
    // load table never becomes a node here to be outranked. Pinning the ranking rule anyway guards
    // against it becoming order-dependent, which would be a latent nondeterminism.
    val taskFirst = List(
      dep("dual.thing", "task"),
      dep("reader.a", "task", "dual.thing", "table")
    )
    val tableFirst = List(
      dep("reader.a", "task", "dual.thing", "table"),
      dep("dual.thing", "task")
    )
    buildOrFail(taskFirst, loadTables = Set("dual.thing")).nodes("dual.thing").typ shouldBe
    RunNodeType.Task
    buildOrFail(tableFirst, loadTables = Set("dual.thing")).nodes("dual.thing").typ shouldBe
    RunNodeType.Task
  }
}
