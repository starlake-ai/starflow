package ai.starlake.lineage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Cycles are reachable from ordinary projects: a task that reads its own sink for an incremental
  * reload is a self loop, and two marts cross referencing each other through a third is a 3-cycle.
  * Neither must hang or blow the stack during DAG generation.
  */
class DependencyGraphCycleSpec extends AnyFlatSpec with Matchers {

  private def dep(name: String, parent: String): TaskViewDependency =
    TaskViewDependency(
      name = name,
      typ = TaskViewDependency.TASK_TYPE,
      parent = parent,
      parentTyp = TaskViewDependency.TASK_TYPE,
      parentRef = parent,
      writeStrategy = None,
      dataset_triggering_strategy = None
    )

  private def entity(name: String): TaskViewDependency = dep(name, "")

  private def collectNames(node: TaskViewDependencyNode): List[String] =
    node.data.name :: node.children.flatMap(collectNames)

  "TaskViewDependencyNode.dependencies" should "terminate on a self referencing task" in {
    val entities = List(entity("kpi.revenue"))
    val relations = List(dep("kpi.revenue", "kpi.revenue"))

    val node = TaskViewDependencyNode.dependencies(entities.head, entities, relations)

    node.data.name shouldBe "kpi.revenue"
    node.children shouldBe empty
  }

  it should "terminate on a three node cycle" in {
    val entities = List(entity("d.a"), entity("d.b"), entity("d.c"))
    val relations = List(dep("d.a", "d.b"), dep("d.b", "d.c"), dep("d.c", "d.a"))

    val node = TaskViewDependencyNode.dependencies(entities.head, entities, relations)

    // a -> b -> c, and c's parent a is cut because it is already on the path
    collectNames(node) should contain theSameElementsAs List("d.a", "d.b", "d.c")
  }

  it should "still expand a diamond on both sides" in {
    val entities = List(entity("d.top"), entity("d.left"), entity("d.right"), entity("d.base"))
    val relations = List(
      dep("d.top", "d.left"),
      dep("d.top", "d.right"),
      dep("d.left", "d.base"),
      dep("d.right", "d.base")
    )

    val node = TaskViewDependencyNode.dependencies(entities.head, entities, relations)

    collectNames(node) should contain theSameElementsAs
      List("d.top", "d.left", "d.base", "d.right", "d.base")
  }

  "TaskViewDependency.getHierarchy" should "terminate on a three node cycle" in {
    val allDeps = List(dep("d.a", "d.b"), dep("d.b", "d.c"), dep("d.c", "d.a"))
    val result = scala.collection.mutable.ListBuffer[TaskViewDependency]()
    val roots = allDeps.filter(_.name == "d.a")
    result ++= roots

    TaskViewDependency.getHierarchy(roots, allDeps, result)

    result.map(_.name).distinct should contain theSameElementsAs List("d.a", "d.b", "d.c")
  }

  it should "walk a linear chain to its root" in {
    val allDeps = List(dep("d.a", "d.b"), dep("d.b", "d.c"))
    val result = scala.collection.mutable.ListBuffer[TaskViewDependency]()
    val roots = allDeps.filter(_.name == "d.a")
    result ++= roots

    TaskViewDependency.getHierarchy(roots, allDeps, result)

    result.map(_.name).distinct should contain theSameElementsAs List("d.a", "d.b")
  }
}
