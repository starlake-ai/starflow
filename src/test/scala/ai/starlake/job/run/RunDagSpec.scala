package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunDagSpec extends AnyFlatSpec with Matchers {

  /** a -> b -> c, plus an independent d */
  private def chain: RunDag = {
    val ids = List("x.a", "x.b", "x.c", "x.d")
    RunDag(
      nodes = ids.map(id => id -> RunNode(id, id, RunNodeType.Task)).toMap,
      parents = Map(
        "x.a" -> Set.empty[String],
        "x.b" -> Set("x.a"),
        "x.c" -> Set("x.b"),
        "x.d" -> Set.empty[String]
      )
    )
  }

  "RunDag.restrictTo" should "be the identity when everything is selected" in {
    // Guards the contract that a run with no selectors behaves exactly as P1 did.
    val dag = chain
    val restricted = dag.restrictTo(dag.nodes.keySet)
    restricted.nodes shouldBe dag.nodes
    restricted.parents shouldBe dag.parents
  }

  it should "drop unselected nodes" in {
    val restricted = chain.restrictTo(Set("x.a", "x.b"))
    restricted.nodes.keySet shouldBe Set("x.a", "x.b")
  }

  it should "rewire through an unselected node so ordering survives" in {
    // THE POINT OF THIS METHOD. With a -> b -> c and b excluded, deleting b's edges outright
    // would leave a and c independent, free to run concurrently or in the wrong order.
    // Assert the edge, not an observed execution order: the scheduler's alphabetical tie-break
    // would produce a before c either way, so an order assertion could not fail for the right
    // reason (spec section 12).
    val restricted = chain.restrictTo(Set("x.a", "x.c"))
    restricted.nodes.keySet shouldBe Set("x.a", "x.c")
    restricted.parents("x.c") shouldBe Set("x.a")
    restricted.parents("x.a") shouldBe Set.empty[String]
  }

  it should "rewire through a chain of several unselected nodes" in {
    val dag = RunDag(
      nodes = List("y.1", "y.2", "y.3", "y.4")
        .map(id => id -> RunNode(id, id, RunNodeType.Task))
        .toMap,
      parents = Map(
        "y.1" -> Set.empty[String],
        "y.2" -> Set("y.1"),
        "y.3" -> Set("y.2"),
        "y.4" -> Set("y.3")
      )
    )
    dag.restrictTo(Set("y.1", "y.4")).parents("y.4") shouldBe Set("y.1")
  }

  it should "keep both branches of a diamond when the apex is excluded" in {
    val dag = RunDag(
      nodes = List("d.top", "d.left", "d.right", "d.bottom")
        .map(id => id -> RunNode(id, id, RunNodeType.Task))
        .toMap,
      parents = Map(
        "d.top"    -> Set.empty[String],
        "d.left"   -> Set("d.top"),
        "d.right"  -> Set("d.top"),
        "d.bottom" -> Set("d.left", "d.right")
      )
    )
    val restricted = dag.restrictTo(Set("d.top", "d.bottom"))
    restricted.parents("d.bottom") shouldBe Set("d.top")
  }

  it should "give every kept node a parents entry, possibly empty" in {
    // RunScheduler indexes dag.parents(id) for every node without a default, so a missing entry
    // would be a NoSuchElementException at dispatch time.
    val restricted = chain.restrictTo(Set("x.b", "x.d"))
    restricted.nodes.keySet.foreach { id =>
      restricted.parents.keySet should contain(id)
    }
    restricted.parents("x.b") shouldBe Set.empty[String]
  }

  it should "ignore ids that are not in the graph" in {
    val restricted = chain.restrictTo(Set("x.a", "nope.nope"))
    restricted.nodes.keySet shouldBe Set("x.a")
  }

  it should "return an empty dag when nothing is selected" in {
    val restricted = chain.restrictTo(Set.empty)
    restricted.nodes shouldBe empty
    restricted.executableCount shouldBe 0
  }

  it should "recompute children from the rewired parents" in {
    val restricted = chain.restrictTo(Set("x.a", "x.c"))
    restricted.children("x.a") shouldBe Set("x.c")
  }
}
