package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunConfigSpec extends AnyFlatSpec with Matchers {

  "RunCmd parser" should "parse defaults" in {
    val config = RunCmd.parse(Seq.empty)
    config shouldBe defined
    config.get.parallelism shouldBe None
    config.get.failFast shouldBe false
    config.get.options shouldBe Map.empty
  }

  it should "parse all options" in {
    val config = RunCmd.parse(
      Seq("--parallelism", "4", "--fail-fast", "--options", "k1=v1,k2=v2")
    )
    config shouldBe defined
    config.get.parallelism shouldBe Some(4)
    config.get.failFast shouldBe true
    config.get.options shouldBe Map("k1" -> "v1", "k2" -> "v2")
  }

  it should "reject a non-positive parallelism" in {
    RunCmd.parse(Seq("--parallelism", "0")) shouldBe None
  }
}
