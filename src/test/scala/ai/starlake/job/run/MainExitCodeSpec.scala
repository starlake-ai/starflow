package ai.starlake.job.run

import ai.starlake.job.Main
import ai.starlake.utils.{EmptyJobResult, FailedJobResult, PreLoadJobResult}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

/** Pins the mapping from a command result to the process exit code. Main.runWithExitCode builds its
  * own Settings from the reference config, so the mapping itself is tested here rather than through
  * the CLI entry point.
  */
class MainExitCodeSpec extends AnyFlatSpec with Matchers {

  private def runResult(exitCode: Int) =
    RunJobResult(exitCode = exitCode, summary = None, errorMessage = None)

  "Main.exitCodeOf" should "return a RunJobResult's own exit code" in {
    Main.exitCodeOf(Success(runResult(0))) shouldBe 0
    Main.exitCodeOf(Success(runResult(1))) shouldBe 1
    Main.exitCodeOf(Success(runResult(2))) shouldBe 2
  }

  it should "pass through the empty-selection exit code" in {
    // P2 adds code 3. It needs no new arm in exitCodeOf because RunJobResult carries its own code,
    // which is exactly the property worth pinning: a refactor that starts interpreting the result
    // instead of forwarding it would break this.
    Main.exitCodeOf(Success(runResult(3))) shouldBe 3
  }

  it should "never collapse a failed run onto the generic success code" in {
    // The RunJobResult arm must stay above the catch-all `case Success(_) => 0`. If it were
    // removed or reordered below it, every failed run would silently exit 0.
    List(1, 2, 3, 42).foreach { code =>
      Main.exitCodeOf(Success(runResult(code))) should not be 0
      Main.exitCodeOf(Success(runResult(code))) shouldBe code
    }
  }

  it should "treat an empty PreLoadJobResult as a soft failure" in {
    val empty = PreLoadJobResult("sales", Map("orders" -> 0))
    empty.empty shouldBe true
    Main.exitCodeOf(Success(empty)) shouldBe 1
  }

  it should "treat a loadable PreLoadJobResult as a success" in {
    val loadable = PreLoadJobResult("sales", Map("orders" -> 3))
    loadable.empty shouldBe false
    Main.exitCodeOf(Success(loadable)) shouldBe 0
  }

  it should "treat FailedJobResult as a soft failure" in {
    Main.exitCodeOf(Success(FailedJobResult)) shouldBe 1
  }

  it should "return 0 for any other successful result" in {
    Main.exitCodeOf(Success(EmptyJobResult)) shouldBe 0
    Main.exitCodeOf(Success(())) shouldBe 0
    Main.exitCodeOf(Success("anything")) shouldBe 0
  }

  it should "return 1 for a failure" in {
    Main.exitCodeOf(Failure(new RuntimeException("boom"))) shouldBe 1
    Main.exitCodeOf(Failure(new IllegalArgumentException("bad args"))) shouldBe 1
  }
}
