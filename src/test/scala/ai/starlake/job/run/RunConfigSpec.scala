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
    config.get.select shouldBe empty
    config.get.exclude shouldBe empty
    config.get.dryRun shouldBe false
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

  it should "parse a comma-separated selector list" in {
    val config = RunCmd.parse(Seq("--select", "sales.a,sales.b"))
    config.get.select shouldBe Seq("sales.a", "sales.b")
  }

  it should "accumulate repeated selector flags" in {
    // The spec asks for a repeatable flag; the codebase convention (LoadCmd, TransformCmd) is a
    // comma-separated list. Selector expressions contain no commas, so both forms work.
    val config = RunCmd.parse(Seq("--select", "sales.a", "--select", "+sales.b"))
    config.get.select shouldBe Seq("sales.a", "+sales.b")
  }

  it should "mix both selector forms in one invocation" in {
    val config = RunCmd.parse(Seq("--select", "a.one,a.two", "--select", "b.three"))
    config.get.select shouldBe Seq("a.one", "a.two", "b.three")
  }

  it should "parse exclusions the same way" in {
    val config = RunCmd.parse(Seq("--exclude", "tag:wip", "--exclude", "ops.audit"))
    config.get.exclude shouldBe Seq("tag:wip", "ops.audit")
  }

  it should "parse --dry-run" in {
    RunCmd.parse(Seq("--dry-run")).get.dryRun shouldBe true
  }

  it should "not validate selector syntax at parse time" in {
    // Selector syntax is rejected during resolution so the message can name the graph it was
    // checked against, and so the failure maps to exit 2 through one path only.
    RunCmd.parse(Seq("--select", "a.b.c")) shouldBe defined
  }
}
