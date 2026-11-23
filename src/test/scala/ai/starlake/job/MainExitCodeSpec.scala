package ai.starlake.job

import ai.starlake.utils.ExitCodeJobResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class MainExitCodeSpec extends AnyFlatSpec with Matchers {
  private case class Coded(exitCode: Int) extends ExitCodeJobResult

  "Main.exitCodeOf" should "return the code an ExitCodeJobResult carries" in {
    Main.exitCodeOf(Success(Coded(0))) shouldBe 0
    Main.exitCodeOf(Success(Coded(1))) shouldBe 1
    Main.exitCodeOf(Success(Coded(2))) shouldBe 2
  }
}
