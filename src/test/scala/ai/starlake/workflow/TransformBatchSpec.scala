package ai.starlake.workflow

import ai.starlake.TestHelper
import ai.starlake.job.transform.{TransformBatchSummary, TransformConfig, TransformTaskStatus}
import ai.starlake.lineage.{TaskViewDependency, TaskViewDependencyNode}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class TransformBatchSpec extends TestHelper {

  private def task(name: String, upstream: TaskViewDependencyNode*) =
    TaskViewDependencyNode(
      TaskViewDependency(name, "task", "", "", "", None, None),
      upstream.toList
    )

  private def table(name: String, upstream: TaskViewDependencyNode*) =
    TaskViewDependencyNode(
      TaskViewDependency(name, "table", "", "", "", None, None),
      upstream.toList
    )

  private case class Outcome(ran: List[String], summary: TransformBatchSummary) {
    def status(name: String): TransformTaskStatus =
      summary.results.find(_.name == name).map(_.status).getOrElse(fail(s"no result for $name"))
  }

  private def runTrees(
    trees: List[TaskViewDependencyNode],
    failing: Set[String] = Set.empty,
    throwing: Set[String] = Set.empty,
    selected: String => Boolean = _ => true
  ): Outcome = {
    var outcome: Option[Outcome] = None
    new WithSettings() {
      val ran = ListBuffer.empty[String]
      val wf = new IngestionWorkflow(settings.storageHandler(), settings.schemaHandler()) {
        override def transform(c: TransformConfig): Try[String] = {
          ran += c.name
          if (throwing(c.name)) throw new RuntimeException(s"thrown ${c.name}")
          if (failing(c.name)) Failure(new RuntimeException(s"boom ${c.name}")) else Success("")
        }
      }
      val summary = wf.runTaskTrees(trees, selected, TransformConfig(scheduledDate = None))
      outcome = Some(Outcome(ran.toList, summary))
    }
    outcome.get
  }

  "runTaskTrees" should "run upstream tasks before the tasks reading them" in {
    val out = runTrees(List(task("down", task("mid", task("up")))))
    out.ran shouldBe List("up", "mid", "down")
    out.summary.isSuccess shouldBe true
  }

  it should "not run a task whose upstream failed, and report the batch as failed" in {
    val out = runTrees(List(task("down", task("up"))), failing = Set("up"))
    out.ran shouldBe List("up")
    out.status("up") shouldBe a[TransformTaskStatus.Failed]
    out.status("down") shouldBe TransformTaskStatus.Skipped("up")
    out.summary.isSuccess shouldBe false
  }

  it should "report the failure of the last sibling" in {
    val out = runTrees(List(task("a"), task("b")), failing = Set("b"))
    out.ran shouldBe List("a", "b")
    out.summary.isSuccess shouldBe false
  }

  it should "report the real error of a failed sibling and still run independent tasks" in {
    val out = runTrees(List(task("a"), task("b")), failing = Set("a"))
    out.ran shouldBe List("a", "b")
    out.status("a") match {
      case TransformTaskStatus.Failed(e) => e.getMessage shouldBe "boom a"
      case other                         => fail(s"unexpected $other")
    }
    out.status("b") shouldBe TransformTaskStatus.Succeeded
  }

  it should "run only once a task shared by two branches" in {
    val shared = task("shared")
    val out = runTrees(List(task("x", shared), task("y", shared), task("z", task("x", shared))))
    out.ran shouldBe List("shared", "x", "y", "z")
  }

  it should "skip every branch reading a failed shared task" in {
    val shared = task("shared")
    val out = runTrees(List(task("x", shared), task("y", shared)), failing = Set("shared"))
    out.ran shouldBe List("shared")
    out.status("x") shouldBe TransformTaskStatus.Skipped("shared")
    out.status("y") shouldBe TransformTaskStatus.Skipped("shared")
  }

  it should "propagate a skip through several levels, naming the task that failed" in {
    val out = runTrees(List(task("c", task("b", task("a")))), failing = Set("a"))
    out.status("b") shouldBe TransformTaskStatus.Skipped("a")
    out.status("c") shouldBe TransformTaskStatus.Skipped("a")
  }

  it should "treat an exception thrown by a task as a failure of that task" in {
    val out = runTrees(List(task("a"), task("b")), throwing = Set("a"))
    out.ran shouldBe List("a", "b")
    out.status("a") shouldBe a[TransformTaskStatus.Failed]
    out.summary.isSuccess shouldBe false
  }

  it should "only run the selected tasks, keeping the order through the others" in {
    val out = runTrees(
      List(task("down", table("t", task("skipped", task("up"))))),
      selected = Set("down", "up")
    )
    out.ran shouldBe List("up", "down")
    out.summary.results.map(_.name) shouldBe List("up", "down")
  }

  it should "skip a selected task behind a failure reached through an unselected task" in {
    val out = runTrees(
      List(task("down", task("unselected", task("up")))),
      failing = Set("up"),
      selected = Set("down", "up")
    )
    out.ran shouldBe List("up")
    out.status("down") shouldBe TransformTaskStatus.Skipped("up")
  }

  it should "not run table nodes" in {
    val out = runTrees(List(task("t1", table("raw.orders"))))
    out.ran shouldBe List("t1")
  }

  it should "not run twice a task spelled differently by the trees" in {
    val out = runTrees(List(task("x", task("Dom.A"), task("dom.a"))))
    out.ran shouldBe List("Dom.A", "x")
  }

  it should "run a task as many times as it is listed" in {
    val out = runTrees(List(task("a"), task("a"), task("a")))
    out.ran shouldBe List("a", "a", "a")
    out.summary.results.map(_.name) shouldBe List("a", "a", "a")
  }

  it should "treat names differing by case as the same listed task" in {
    val out = runTrees(List(task("Dom.A"), task("dom.a")))
    out.ran shouldBe List("Dom.A", "dom.a")
  }

  it should "run a repeated task at its place in the list" in {
    val up = task("up")
    val out = runTrees(List(up, task("down", up), up))
    out.ran shouldBe List("up", "down", "up")
  }

  it should "run once what is only read by a task listed twice" in {
    val down = task("down", task("up"))
    val out = runTrees(List(down, down))
    out.ran shouldBe List("up", "down", "down")
  }

  it should "count a run as upstream as one of the times a task is listed" in {
    val up = task("up")
    runTrees(List(task("down", up), up)).ran shouldBe List("up", "down")
    runTrees(List(task("down", up), up, up)).ran shouldBe List("up", "down", "up")
  }

  it should "run a listed task again after it failed, and report the batch as failed" in {
    val out = runTrees(List(task("a"), task("a")), failing = Set("a"))
    out.ran shouldBe List("a", "a")
    out.summary.isSuccess shouldBe false
  }

  it should "skip each listing of a task whose upstream failed" in {
    val down = task("down", task("up"))
    val out = runTrees(List(down, down), failing = Set("up"))
    out.ran shouldBe List("up")
    out.summary.results.map(_.status).count(_ == TransformTaskStatus.Skipped("up")) shouldBe 2
  }

  // starlake-api calls autoJob directly, so the parser alone would not protect this path.
  private def autoJobOutcome(config: TransformConfig): (Try[String], List[String]) = {
    var outcome: Option[(Try[String], List[String])] = None
    new WithSettings() {
      val ran = ListBuffer.empty[String]
      val wf = new IngestionWorkflow(settings.storageHandler(), settings.schemaHandler()) {
        override def transform(c: TransformConfig): Try[String] = {
          ran += c.name
          Success("")
        }
      }
      val result = wf.autoJob(config)
      outcome = Some(result -> ran.toList)
    }
    outcome.get
  }

  private val recursive = TransformConfig(name = "a.b", recursive = true, scheduledDate = None)

  List(
    "--dry-run"     -> recursive.copy(dryRun = true),
    "--interactive" -> recursive.copy(interactive = Some("json-array")),
    "--query"       -> recursive.copy(query = Some("select 1"))
  ).foreach { case (flag, config) =>
    "autoJob" should s"refuse --recursive with $flag without running anything" in {
      val (result, ran) = autoJobOutcome(config)
      ran shouldBe Nil
      result match {
        case Failure(e: IllegalArgumentException) => e.getMessage should include(flag)
        case other                                => fail(s"unexpected $other")
      }
    }
  }

  "autoJob" should "not refuse --recursive for the compile flag starlake-api always sets" in {
    val (result, _) = autoJobOutcome(recursive.copy(compile = true))
    result.failed.get.getMessage should include("Unknown task")
  }

  "TransformBatchSummary" should "render one line per task with its status" in {
    val out = runTrees(List(task("down", task("up")), task("other")), failing = Set("up"))
    val text = out.summary.render
    text should include("3 tasks")
    text should include("1 succeeded")
    text should include("1 failed")
    text should include("1 skipped")
    text should include("boom up")
    text should include("upstream up failed")
  }

  it should "convert a failed batch to a Failure carrying the first error as its cause" in {
    val out = runTrees(List(task("a"), task("b")), failing = Set("a", "b"))
    out.summary.toTry match {
      case Failure(e) =>
        e.getMessage should include("a")
        e.getCause.getMessage shouldBe "boom a"
      case other => fail(s"unexpected $other")
    }
  }
}
