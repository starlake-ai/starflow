package ai.starlake.impact

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImpactModelSpec extends AnyFlatSpec with Matchers {
  import ChangeClass._

  "ChangeClass.worst" should "follow METADATA < ADDED < NON_BREAKING < BREAKING" in {
    worst(Metadata, Added) shouldBe Added
    worst(NonBreaking, Added) shouldBe NonBreaking
    worst(NonBreaking, Breaking) shouldBe Breaking
    worst(Breaking, Metadata) shouldBe Breaking
  }

  "Columns.union" should "absorb into AllColumns" in {
    Cols(Set("a")).union(Cols(Set("b"))) shouldBe Cols(Set("a", "b"))
    Cols(Set("a")).union(AllColumns) shouldBe AllColumns
    AllColumns.union(Cols(Set("a"))) shouldBe AllColumns
  }

  "Columns.covers" should "tell whether a state already contains another" in {
    AllColumns.covers(Cols(Set("a"))) shouldBe true
    Cols(Set("a", "b")).covers(Cols(Set("a"))) shouldBe true
    Cols(Set("a")).covers(AllColumns) shouldBe false
  }

  "NodeRef.of" should "lowercase both parts" in {
    NodeRef.of("Sales", "Orders") shouldBe NodeRef("sales", "orders")
    NodeRef.of("Sales", "Orders").name shouldBe "sales.orders"
  }

  "ImpactReport.summary" should "flag breaking and non metadata changes" in {
    val direct = List(
      DirectChange(NodeRef("a", "t"), NodeKind.Task, Metadata, Cols(Set.empty), Nil, List("comment"))
    )
    val impacted = List(
      ImpactedNode(NodeRef("b", "u"), NodeKind.Task, Breaking, AllColumns, List("a.t", "b.u"), likelyFailure = true, viaUnresolvedEdge = false)
    )
    val s = ImpactReport("old", "new", direct, impacted, Nil, Nil).summary
    s.hasBreaking shouldBe true
    s.hasNonMetadata shouldBe true
    s.likelyFailures shouldBe 1
    s.direct("METADATA") shouldBe 1
    s.impacted("BREAKING") shouldBe 1
  }
}
