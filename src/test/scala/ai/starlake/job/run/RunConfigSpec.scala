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
    // Every flag the command declares, in one invocation. The cases below cover the selector
    // flags in isolation; this one exists to fail when a new flag is added without being parsed
    // alongside the others, so it has to stay exhaustive.
    val config = RunCmd.parse(
      Seq(
        "--parallelism",
        "4",
        "--fail-fast",
        "--select",
        "sales.a",
        "--exclude",
        "tag:wip",
        "--dry-run",
        "--options",
        "k1=v1,k2=v2",
        "--resume-id",
        "20260914-183042-a1b2c3",
        "--force",
        "--no-run-log"
      )
    )
    config shouldBe defined
    config.get.parallelism shouldBe Some(4)
    config.get.failFast shouldBe true
    config.get.select shouldBe Seq("sales.a")
    config.get.exclude shouldBe Seq("tag:wip")
    config.get.dryRun shouldBe true
    config.get.options shouldBe Map("k1" -> "v1", "k2" -> "v2")
    config.get.resumeId shouldBe Some("20260914-183042-a1b2c3")
    config.get.force shouldBe true
    config.get.noRunLog shouldBe true
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

  it should "parse the resume flags" in {
    val latest = RunCmd.parse(Seq("--resume")).getOrElse(fail("expected a config"))
    latest.resumeLatest shouldBe true
    latest.resumeId shouldBe None

    val byId = RunCmd
      .parse(Seq("--resume-id", "20260914-183042-a1b2c3"))
      .getOrElse(fail("expected a config"))
    byId.resumeLatest shouldBe false
    byId.resumeId shouldBe Some("20260914-183042-a1b2c3")

    val forced = RunCmd
      .parse(Seq("--resume", "--force", "--no-run-log"))
      .getOrElse(fail("expected a config"))
    forced.force shouldBe true
    forced.noRunLog shouldBe true
  }

  "RunConfig.runLogEnabled" should "be on by default and off when either switch says so" in {
    RunConfig.runLogEnabled(RunConfig(), Map.empty) shouldBe true
    RunConfig.runLogEnabled(RunConfig(noRunLog = true), Map.empty) shouldBe false
    List("false", "FALSE", "0", "no", "off", " off ").foreach { value =>
      withClue(s"SL_RUN_LOG=$value: ") {
        RunConfig.runLogEnabled(RunConfig(), Map(RunConfig.RunLogEnv -> value)) shouldBe false
      }
    }
  }

  it should "ignore a value that does not mean off" in {
    // Only an explicit off disables it. A typo must not silently cost someone their run log, which
    // they discover when a resume tells them there is nothing to resume.
    List("true", "1", "yes", "", "maybe").foreach { value =>
      withClue(s"SL_RUN_LOG=$value: ") {
        RunConfig.runLogEnabled(RunConfig(), Map(RunConfig.RunLogEnv -> value)) shouldBe true
      }
    }
  }

  "RunConfig.resumeRequest" should "return no target when no resume flag is given" in {
    RunConfig.resumeRequest(RunConfig()) shouldBe Right(None)
  }

  it should "resolve each resume flag to its target" in {
    RunConfig.resumeRequest(RunConfig(resumeLatest = true)) shouldBe Right(
      Some(ResumeTarget.Latest)
    )
    RunConfig.resumeRequest(RunConfig(resumeId = Some("r1"))) shouldBe
    Right(Some(ResumeTarget.Id("r1")))
  }

  it should "refuse both resume flags at once" in {
    val result = RunConfig.resumeRequest(RunConfig(resumeLatest = true, resumeId = Some("r1")))
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("--resume-id")
  }

  it should "refuse to re-scope a resumed run" in {
    // The selection and the options are part of the recorded run and are replayed from it.
    // Accepting new ones here and silently ignoring them would be the worst of both.
    List(
      RunConfig(resumeLatest = true, select = Seq("sales.a")),
      RunConfig(resumeLatest = true, exclude = Seq("tag:wip")),
      RunConfig(resumeLatest = true, options = Map("k" -> "v"))
    ).foreach { config =>
      val result = RunConfig.resumeRequest(config)
      result.isLeft shouldBe true
      result.left.getOrElse("") should include("replayed")
    }
  }

  it should "allow a resume to change how the remaining work runs" in {
    // --parallelism, --fail-fast and --force change how what is left executes, not what is left.
    val config =
      RunConfig(resumeLatest = true, parallelism = Some(4), failFast = true, force = true)
    RunConfig.resumeRequest(config) shouldBe Right(Some(ResumeTarget.Latest))
  }
}
