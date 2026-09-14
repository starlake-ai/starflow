package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunFingerprintSpec extends AnyFlatSpec with Matchers {

  private def node(id: String, typ: RunNodeType = RunNodeType.Task) = RunNode(id, id, typ)

  private val dag = RunDag(
    nodes = Map("a" -> node("a"), "b" -> node("b")),
    parents = Map("a" -> Set.empty[String], "b" -> Set("a"))
  )

  private def parts(
    d: RunDag = dag,
    selection: Set[String] = Set("a", "b"),
    options: Map[String, String] = Map("k" -> "v"),
    envName: String = "prod",
    envVars: Map[String, String] = Map("REGION" -> "eu")
  ) = RunFingerprint.parts(d, selection, options, envName, envVars)

  "RunFingerprint" should "be stable across two identical resolutions" in {
    parts().digest shouldBe parts().digest
  }

  it should "not depend on map or set iteration order" in {
    parts(selection = Set("b", "a")).digest shouldBe parts().digest
  }

  it should "change when an edge is added" in {
    val extra = dag.copy(parents = dag.parents.updated("a", Set("b")))
    parts(d = extra).graph should not be parts().graph
  }

  it should "change when a node's type changes" in {
    val retyped = dag.copy(nodes = dag.nodes.updated("b", node("b", RunNodeType.LoadTable)))
    parts(d = retyped).graph should not be parts().graph
  }

  it should "change when the selection changes" in {
    parts(selection = Set("a")).selection should not be parts().selection
  }

  it should "change when an option changes" in {
    parts(options = Map("k" -> "other")).options should not be parts().options
  }

  it should "change when the env name or an env var changes" in {
    parts(envName = "dev").env should not be parts().env
    parts(envVars = Map("REGION" -> "us")).env should not be parts().env
  }

  it should "keep each part independent of the others" in {
    val other = parts(options = Map("k" -> "other"))
    other.graph shouldBe parts().graph
    other.selection shouldBe parts().selection
    other.env shouldBe parts().env
  }

  "RunFingerprint.changed" should "name only the parts that differ" in {
    val recorded = parts().toMap
    val current = parts(options = Map("k" -> "other"), envName = "dev")
    RunFingerprint.changed(recorded, current) shouldBe List("env", "options")
  }

  it should "report nothing when everything matches" in {
    RunFingerprint.changed(parts().toMap, parts()) shouldBe Nil
  }

  it should "fall back to a single unknown part when the log recorded no breakdown" in {
    // An old log, or one written before the breakdown existed: we know the digests differ, we
    // cannot say where, and naming a cause we did not verify would send the user to the wrong file.
    RunFingerprint.changed(Map.empty, parts()) shouldBe List("unknown")
  }

  "RunFingerprint.taskDigests" should "give each task its own digest" in {
    val digests = RunFingerprint.taskDigests(Map("a" -> "select 1", "b" -> "select 2"))
    digests.keySet shouldBe Set("a", "b")
    digests("a") should not be digests("b")
    RunFingerprint.taskDigests(Map("a" -> "select 1"))("a") shouldBe digests("a")
  }

  "RunFingerprint.changedTasks" should "ignore a task the resume will run again anyway" in {
    // The ordinary recovery: a transform failed, its SQL was fixed, the run is resumed. The
    // changed task is re-executed, so nothing stale survives and there is nothing to refuse.
    val recorded = RunFingerprint.taskDigests(Map("a" -> "select 1", "b" -> "select 2"))
    val current = RunFingerprint.taskDigests(Map("a" -> "select 1", "b" -> "select 2 fixed"))
    RunFingerprint.changedTasks(recorded, current, amongst = Set("a")) shouldBe Nil
  }

  it should "name a changed task whose recorded success would be kept" in {
    val recorded = RunFingerprint.taskDigests(Map("a" -> "select 1", "b" -> "select 2"))
    val current = RunFingerprint.taskDigests(Map("a" -> "select 99", "b" -> "select 2"))
    RunFingerprint.changedTasks(recorded, current, amongst = Set("a", "b")) shouldBe List("a")
  }

  it should "say nothing about a task the recorded run never knew" in {
    val recorded = RunFingerprint.taskDigests(Map("a" -> "select 1"))
    val current = RunFingerprint.taskDigests(Map("a" -> "select 1", "b" -> "select 2"))
    RunFingerprint.changedTasks(recorded, current, amongst = Set("a", "b")) shouldBe Nil
  }

  "RunFingerprint.label" should "turn a part name into something a user can act on" in {
    RunFingerprint.label("graph") should include("graph")
    RunFingerprint.label("env") should include("env")
  }
}
