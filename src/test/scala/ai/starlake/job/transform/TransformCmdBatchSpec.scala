package ai.starlake.job.transform

import ai.starlake.schema.model.Engine
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformCmdBatchSpec extends AnyFlatSpec with Matchers {

  "transform --name" should "keep a single name as a single task" in {
    val config = TransformCmd.parse(Seq("--name", "a.b")).get
    config.names shouldBe List("a.b")
    config.isMultiTask shouldBe false
  }

  it should "accept a comma separated list of tasks" in {
    val config = TransformCmd.parse(Seq("--name", "a.b, a.c")).get
    config.names shouldBe List("a.b", "a.c")
    config.isMultiTask shouldBe true
  }

  it should "keep a name listed twice" in {
    TransformCmd.parse(Seq("--name", "a.b,a.b")).get.names shouldBe List("a.b", "a.b")
  }

  "transform --tags" should "be usable without --name" in {
    val config = TransformCmd.parse(Seq("--tags", "x,y")).get
    config.tags shouldBe Seq("x", "y")
    config.isMultiTask shouldBe true
  }

  it should "still refuse to be combined with --name" in {
    val e = intercept[IllegalArgumentException] {
      TransformCmd.parse(Seq("--name", "a.b", "--tags", "x"))
    }
    e.getMessage should include("Cannot specify both tags and task name")
  }

  "transform" should "require either --name or --tags" in {
    val e = intercept[IllegalArgumentException](TransformCmd.parse(Seq("--truncate", "true")))
    e.getMessage should include("--name or --tags")
  }

  private val singleTaskOnly = List(
    List("--compile")            -> "--compile",
    List("--sync-apply")         -> "--sync-apply",
    List("--sync-preview")       -> "--sync-preview",
    List("--format")             -> "--format",
    List("--dry-run")            -> "--dry-run",
    List("--interactive", "csv") -> "--interactive",
    List("--query", "select 1")  -> "--query"
  )

  singleTaskOnly.foreach { case (args, flag) =>
    it should s"reject $flag with several tasks, which it would otherwise ignore" in {
      val e = intercept[IllegalArgumentException] {
        TransformCmd.parse(Seq("--name", "a.b,a.c") ++ args)
      }
      e.getMessage should include(flag)
    }

    it should s"reject $flag with tags, which it would otherwise ignore" in {
      val e = intercept[IllegalArgumentException] {
        TransformCmd.parse(Seq("--tags", "x") ++ args)
      }
      e.getMessage should include(flag)
    }
  }

  // Each task of a recursive run gets a config of its own, so these flags could not apply. They used
  // to be dropped silently: a preview wrote its tables and a dry run really ran.
  List(
    List("--dry-run")            -> "--dry-run",
    List("--interactive", "csv") -> "--interactive",
    List("--query", "select 1")  -> "--query"
  ).foreach { case (args, flag) =>
    it should s"reject $flag with --recursive" in {
      val e = intercept[IllegalArgumentException] {
        TransformCmd.parse(Seq("--name", "a.b", "--recursive") ++ args)
      }
      e.getMessage should include(flag)
    }
  }

  it should "keep accepting --recursive with --compile, which reads a single task" in {
    val config = TransformCmd.parse(Seq("--name", "a.b", "--recursive", "--compile")).get
    config.compile shouldBe true
  }

  it should "keep accepting a single task with the flags --recursive refuses" in {
    val config = TransformCmd
      .parse(Seq("--name", "a.b", "--interactive", "csv", "--query", "select 1", "--dry-run"))
      .get
    config.incompatibleFlags shouldBe Nil
  }

  "dryRunSupportedOn" should "accept BigQuery only when dry run is asked" in {
    val dryRun = TransformConfig(name = "a.b", dryRun = true, scheduledDate = None)
    dryRun.dryRunSupportedOn(Engine.BQ) shouldBe true
    dryRun.dryRunSupportedOn(Engine.JDBC) shouldBe false
    dryRun.dryRunSupportedOn(Engine.SPARK) shouldBe false
    dryRun.dryRunSupportedOn(Engine.SNOWFLAKE) shouldBe false
  }

  it should "not matter when dry run is not asked" in {
    val run = TransformConfig(name = "a.b", scheduledDate = None)
    run.dryRunSupportedOn(Engine.JDBC) shouldBe true
  }
}
