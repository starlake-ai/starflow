package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NoopRunStoreSpec extends AnyFlatSpec with Matchers {

  private val header = RunLogEvent(
    runId = "r1",
    attempt = 1,
    seq = 1,
    ts = "2026-09-14T18:30:42Z",
    `type` = RunLogEventType.RunStarted
  )

  "NoopRunStore" should "accept writes and keep nothing" in {
    NoopRunStore.start(header) shouldBe 1
    NoopRunStore.append(header.copy(seq = 2, `type` = RunLogEventType.TaskStarted))
    NoopRunStore.read("r1") shouldBe None
    NoopRunStore.latest() shouldBe None
    NoopRunStore.close()
  }

  it should "refuse to reopen, since it has nothing to reopen" in {
    // --resume against a disabled log is a usage error, not an empty history: the difference
    // matters because an empty history would silently re-execute everything.
    a[RunStoreException] should be thrownBy NoopRunStore.reopen("r1")
  }
}
