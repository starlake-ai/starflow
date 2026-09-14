package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The behaviour every RunStore must have, written once against the trait.
  *
  * Store tests live here rather than in each implementation's spec on purpose: it is what stops
  * the trait from quietly acquiring the vocabulary of whichever backend was written first.
  * Subclasses supply a fresh, empty store per test.
  */
abstract class RunStoreContractSpec extends AnyFlatSpec with Matchers {

  /** Runs `test` against a store whose backing state is empty, then closes it. */
  def withStore(test: RunStore => Unit): Unit

  protected def header(runId: String, fingerprint: String = "fp1") =
    RunLogEvent(
      runId = runId,
      attempt = 1,
      seq = 1,
      ts = "2026-09-14T18:30:42Z",
      `type` = RunLogEventType.RunStarted,
      fingerprint = Some(fingerprint),
      fingerprintParts = Some(Map("graph" -> "g1", "selection" -> "s1")),
      selectExprs = Some(List("sales.*")),
      options = Some(Map("k" -> "v")),
      env = Some("prod")
    )

  protected def event(
    runId: String,
    attempt: Int,
    seq: Int,
    typ: String,
    taskId: Option[String] = None
  ) =
    RunLogEvent(
      runId = runId,
      attempt = attempt,
      seq = seq,
      ts = "2026-09-14T18:30:43Z",
      `type` = typ,
      taskId = taskId,
      taskName = taskId,
      nodeType = taskId.map(_ => "Task")
    )

  "a RunStore" should "read back what it recorded, header included" in withStore { store =>
    store.start(header("r1")) shouldBe 1
    store.append(event("r1", 1, 2, RunLogEventType.TaskStarted, Some("a")))
    store.append(event("r1", 1, 3, RunLogEventType.TaskSucceeded, Some("a")))

    val history = store.read("r1").getOrElse(fail("expected a history"))
    history.runId shouldBe "r1"
    history.header.fingerprint shouldBe Some("fp1")
    history.header.selectExprs shouldBe Some(List("sales.*"))
    history.header.options shouldBe Some(Map("k" -> "v"))
    history.succeeded shouldBe Set("a")
    history.attempts shouldBe 1
    history.finished shouldBe false
  }

  it should "return None for a run it never saw" in withStore { store =>
    store.read("nope") shouldBe None
  }

  it should "number a reopened run's attempts upwards" in withStore { store =>
    store.start(header("r1"))
    store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))
    store.reopen("r1") shouldBe 2
    store.append(header("r1").copy(attempt = 2))
    store.reopen("r1") shouldBe 3
  }

  it should "refuse to reopen a run it never saw" in withStore { store =>
    a[RunStoreException] should be thrownBy store.reopen("nope")
  }

  it should "accumulate successes across attempts" in withStore { store =>
    store.start(header("r1"))
    store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))
    store.append(event("r1", 1, 3, RunLogEventType.TaskFailed, Some("b")))
    store.reopen("r1")
    store.append(header("r1").copy(attempt = 2))
    store.append(event("r1", 2, 2, RunLogEventType.TaskSucceeded, Some("b")))

    val history = store.read("r1").getOrElse(fail("expected a history"))
    history.succeeded shouldBe Set("a", "b")
    history.attempts shouldBe 2
  }

  it should "report the most recently started run as the latest" in withStore { store =>
    // No close() between the two: a connection-backed store would have nothing left to write
    // with, and every implementation must accept a second run on a live store.
    store.start(header("20260914-183042-aaaaaa"))
    store.start(header("20260914-190000-bbbbbb"))
    store.latest().map(_.runId) shouldBe Some("20260914-190000-bbbbbb")
  }

  it should "have no latest run when nothing was ever recorded" in withStore { store =>
    store.latest() shouldBe None
  }

  it should "mark a run finished only when its last attempt finished" in withStore { store =>
    store.start(header("r1"))
    store.append(event("r1", 1, 2, RunLogEventType.RunFinished).copy(exitCode = Some(0)))
    store.read("r1").map(_.finished) shouldBe Some(true)

    store.reopen("r1")
    store.append(header("r1").copy(attempt = 2))
    store.read("r1").map(_.finished) shouldBe Some(false)
  }
}
