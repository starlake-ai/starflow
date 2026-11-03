package ai.starlake.workflow

import ai.starlake.TestHelper
import ai.starlake.job.transform.{TransformBatchSummary, TransformConfig, TransformTaskStatus}
import ai.starlake.lineage.{TaskViewDependency, TaskViewDependencyNode}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier, TimeUnit}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
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

  private def runParallel(
    trees: List[TaskViewDependencyNode],
    parallelism: Int,
    selected: String => Boolean = _ => true
  )(body: String => Try[String]): TransformBatchSummary = {
    var summary: Option[TransformBatchSummary] = None
    new WithSettings() {
      val wf = new IngestionWorkflow(settings.storageHandler(), settings.schemaHandler()) {
        override def transform(c: TransformConfig): Try[String] = body(c.name)
      }
      summary = Some(
        wf.runTaskTrees(trees, selected, TransformConfig(scheduledDate = None), parallelism)
      )
    }
    summary.get
  }

  "runTaskTrees with a parallelism" should "run independent tasks at the same time" in {
    // Only passes when the two tasks are inside the barrier together
    val barrier = new CyclicBarrier(2)
    val summary = runParallel(List(task("a"), task("b")), parallelism = 2) { _ =>
      Try { barrier.await(30, TimeUnit.SECONDS); "" }
    }
    summary.isSuccess shouldBe true
  }

  it should "not run more tasks at the same time than asked" in {
    val running = new AtomicInteger(0)
    val highest = new AtomicInteger(0)
    val trees = (1 to 8).toList.map(i => task(s"t$i"))
    val summary = runParallel(trees, parallelism = 3) { _ =>
      highest.accumulateAndGet(running.incrementAndGet(), (a, b) => math.max(a, b))
      java.lang.Thread.sleep(50)
      running.decrementAndGet()
      Success("")
    }
    summary.isSuccess shouldBe true
    highest.get() shouldBe 3
  }

  it should "start a task only once what it reads has finished" in {
    val events = new ConcurrentLinkedQueue[String]()
    val tree = task("down", task("mid", task("up")))
    runParallel(List(tree), parallelism = 4) { name =>
      events.add(s"start $name")
      java.lang.Thread.sleep(50)
      events.add(s"end $name")
      Success("")
    }.isSuccess shouldBe true
    events.asScala.toList shouldBe List(
      "start up",
      "end up",
      "start mid",
      "end mid",
      "start down",
      "end down"
    )
  }

  it should "wait for a task it reads through something that does not run" in {
    val events = new ConcurrentLinkedQueue[String]()
    val tree = task("down", table("view", task("up")))
    runParallel(List(tree), parallelism = 4) { name =>
      events.add(s"start $name")
      java.lang.Thread.sleep(50)
      events.add(s"end $name")
      Success("")
    }.isSuccess shouldBe true
    events.asScala.toList shouldBe List("start up", "end up", "start down", "end down")
  }

  it should "run once a task that several tasks read" in {
    val ran = new ConcurrentLinkedQueue[String]()
    val up = task("up")
    runParallel(List(task("a", up), task("b", up)), parallelism = 4) { name =>
      ran.add(name)
      Success("")
    }.isSuccess shouldBe true
    ran.asScala.toList.count(_ == "up") shouldBe 1
    ran.asScala.toList.head shouldBe "up"
    ran.asScala.toSet shouldBe Set("up", "a", "b")
  }

  it should "skip what reads a failed task and still run the other branches" in {
    val ran = new ConcurrentLinkedQueue[String]()
    val trees = List(task("down", task("mid", task("up"))), task("other"))
    val summary = runParallel(trees, parallelism = 2) { name =>
      ran.add(name)
      if (name == "up") Failure(new RuntimeException("boom")) else Success("")
    }
    ran.asScala.toSet shouldBe Set("up", "other")
    val status = summary.results.map(r => r.name -> r.status).toMap
    status("up") shouldBe a[TransformTaskStatus.Failed]
    status("mid") shouldBe TransformTaskStatus.Skipped("up")
    status("down") shouldBe TransformTaskStatus.Skipped("up")
    status("other") shouldBe TransformTaskStatus.Succeeded
  }

  it should "run a task listed twice after everything listed before it" in {
    val events = new ConcurrentLinkedQueue[String]()
    // a, b reading a, then a again: the second run of a must not write while b reads
    val trees = List(task("a"), task("b", task("a")), task("a"))
    runParallel(trees, parallelism = 4) { name =>
      events.add(s"start $name")
      java.lang.Thread.sleep(50)
      events.add(s"end $name")
      Success("")
    }.isSuccess shouldBe true
    events.asScala.toList shouldBe List(
      "start a",
      "end a",
      "start b",
      "end b",
      "start a",
      "end a"
    )
  }

  it should "run a task listed again even when its first run failed" in {
    val ran = new AtomicInteger(0)
    val summary = runParallel(List(task("a"), task("a")), parallelism = 2) { _ =>
      if (ran.incrementAndGet() == 1) Failure(new RuntimeException("boom")) else Success("")
    }
    ran.get() shouldBe 2
    summary.results.map(_.status).last shouldBe TransformTaskStatus.Succeeded
  }

  it should "report the tasks in the order they would run serially" in {
    val summary = runParallel(List(task("slow"), task("fast")), parallelism = 2) { name =>
      if (name == "slow") java.lang.Thread.sleep(200)
      Success("")
    }
    summary.results.map(_.name) shouldBe List("slow", "fast")
  }

  it should "stay on the calling thread when the parallelism is 1" in {
    val threads = new ConcurrentLinkedQueue[java.lang.Thread]()
    val caller = java.lang.Thread.currentThread()
    runParallel(List(task("a"), task("b")), parallelism = 1) { _ =>
      threads.add(java.lang.Thread.currentThread())
      Success("")
    }.isSuccess shouldBe true
    threads.asScala.toSet shouldBe Set(caller)
  }

  "autoJob with several names" should "default to the maxParTask of the settings" in {
    val config = com.typesafe.config.ConfigFactory
      .parseString("maxParTask = 2")
      .withFallback(super.testConfiguration)
    new WithSettings(config) {
      val storage = settings.storageHandler()
      val job = new org.apache.hadoop.fs.Path(ai.starlake.config.DatasetArea.transform, "par")
      storage.mkdirs(job)
      List("a", "b").foreach(t =>
        storage.write("SELECT 1 AS id", new org.apache.hadoop.fs.Path(job, s"$t.sql"))
      )
      val barrier = new CyclicBarrier(2)
      val wf = new IngestionWorkflow(storage, settings.schemaHandler(reload = true)) {
        override def transform(c: TransformConfig): Try[String] =
          Try { barrier.await(30, TimeUnit.SECONDS); "" }
      }
      wf.autoJob(TransformConfig(name = "par.a,par.b", scheduledDate = None))
        .isSuccess shouldBe true
    }
  }

  it should "let --parallelism override the maxParTask of the settings" in {
    new WithSettings() {
      val storage = settings.storageHandler()
      val job = new org.apache.hadoop.fs.Path(ai.starlake.config.DatasetArea.transform, "par")
      storage.mkdirs(job)
      List("a", "b").foreach(t =>
        storage.write("SELECT 1 AS id", new org.apache.hadoop.fs.Path(job, s"$t.sql"))
      )
      settings.appConfig.maxParTask shouldBe 1
      val barrier = new CyclicBarrier(2)
      val wf = new IngestionWorkflow(storage, settings.schemaHandler(reload = true)) {
        override def transform(c: TransformConfig): Try[String] =
          Try { barrier.await(30, TimeUnit.SECONDS); "" }
      }
      wf.autoJob(
        TransformConfig(name = "par.a,par.b", scheduledDate = None, parallelism = Some(2))
      ).isSuccess shouldBe true
    }
  }
}
