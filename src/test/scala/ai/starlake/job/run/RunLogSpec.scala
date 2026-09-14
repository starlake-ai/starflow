package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.Random

class RunLogSpec extends AnyFlatSpec with Matchers {

  private def header(runId: String, attempt: Int = 1) =
    RunLogEvent(
      runId = runId,
      attempt = attempt,
      seq = 1,
      ts = "2026-09-14T18:30:42Z",
      `type` = RunLogEventType.RunStarted,
      fingerprint = Some("abc123"),
      selectExprs = Some(List("sales.*")),
      options = Some(Map("k" -> "v"))
    )

  private def taskEvent(runId: String, attempt: Int, seq: Int, typ: String, taskId: String) =
    RunLogEvent(
      runId = runId,
      attempt = attempt,
      seq = seq,
      ts = "2026-09-14T18:30:43Z",
      `type` = typ,
      taskId = Some(taskId),
      taskName = Some(taskId),
      nodeType = Some("Task")
    )

  "RunLog" should "round trip an event through JSON" in {
    val event = header("20260914-183042-a1b2c3")
    val parsed = RunLog.fromJson(RunLog.toJson(event))
    parsed shouldBe Right(event)
  }

  it should "write the schema version on every event" in {
    RunLog.toJson(header("r1")) should include("\"schemaVersion\":1")
  }

  it should "serialize a plain value for a typed column" in {
    RunLog.toJsonValue(List("a", "b")) shouldBe "[\"a\",\"b\"]"
    RunLog.toJsonValue(Map("k" -> "v")) shouldBe "{\"k\":\"v\"}"
  }

  it should "reject a line that is not an event" in {
    RunLog.fromJson("{ this is not json").isLeft shouldBe true
  }

  it should "ignore a field it does not know" in {
    val json = """{"runId":"r1","attempt":1,"seq":1,"ts":"t","type":"RunStarted","schemaVersion":1,"futureField":42}"""
    RunLog.fromJson(json).map(_.runId) shouldBe Right("r1")
  }

  "RunHistory.fold" should "return None when there is no RunStarted" in {
    RunHistory.fold(Nil) shouldBe None
    RunHistory.fold(List(taskEvent("r1", 1, 1, RunLogEventType.TaskSucceeded, "a"))) shouldBe None
  }

  it should "collect the task ids that succeeded" in {
    val events = List(
      header("r1"),
      taskEvent("r1", 1, 2, RunLogEventType.TaskStarted, "a"),
      taskEvent("r1", 1, 3, RunLogEventType.TaskSucceeded, "a"),
      taskEvent("r1", 1, 4, RunLogEventType.TaskFailed, "b")
    )
    val history = RunHistory.fold(events).getOrElse(fail("expected a history"))
    history.succeeded shouldBe Set("a")
    history.attempts shouldBe 1
    history.finished shouldBe false
  }

  it should "accumulate successes across attempts and never retract one" in {
    val events = List(
      header("r1"),
      taskEvent("r1", 1, 2, RunLogEventType.TaskSucceeded, "a"),
      header("r1", attempt = 2).copy(seq = 1),
      taskEvent("r1", 2, 2, RunLogEventType.TaskSkipped, "a").copy(reason = Some(SkipReason.AlreadySucceeded)),
      taskEvent("r1", 2, 3, RunLogEventType.TaskSucceeded, "b")
    )
    val history = RunHistory.fold(events).getOrElse(fail("expected a history"))
    history.succeeded shouldBe Set("a", "b")
    history.attempts shouldBe 2
  }

  it should "take the header from the first attempt" in {
    val events = List(header("r1"), header("r1", attempt = 2).copy(fingerprint = Some("changed")))
    RunHistory.fold(events).map(_.header.fingerprint) shouldBe Some(Some("abc123"))
  }

  it should "report finished only when the last attempt has a RunFinished" in {
    val attempt1Finished = List(
      header("r1"),
      taskEvent("r1", 1, 2, RunLogEventType.RunFinished, "a").copy(taskId = None, exitCode = Some(0))
    )
    RunHistory.fold(attempt1Finished).map(_.finished) shouldBe Some(true)

    val resumedNotFinished = attempt1Finished :+ header("r1", attempt = 2)
    RunHistory.fold(resumedNotFinished).map(_.finished) shouldBe Some(false)
  }

  "RunId.generate" should "be sortable by start time and unique within a second" in {
    val now = Instant.parse("2026-09-14T18:30:42Z")
    val first = RunId.generate(now, new Random(1))
    val second = RunId.generate(now, new Random(2))
    first should fullyMatch regex "\\d{8}-\\d{6}-[0-9a-f]{6}"
    first should startWith("20260914-183042-")
    first should not be second
    RunId.generate(Instant.parse("2026-09-14T18:30:43Z"), new Random(1)) should be > first
  }
}
