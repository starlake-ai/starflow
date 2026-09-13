package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class RunSchedulerSpec extends AnyFlatSpec with Matchers {

  private def node(id: String, typ: RunNodeType = RunNodeType.Task): RunNode =
    RunNode(id, id, typ)

  /** dagOf("b" -> "a", "c" -> "b") builds a<-b<-c (b depends on a). Extra isolated
    * nodes are listed in `alone`.
    */
  private def dagOf(edges: (String, String)*)(alone: String*): RunDag = {
    val ids =
      (edges.flatMap(e => List(e._1, e._2)) ++ alone).distinct
    val parents =
      ids.map(id => id -> edges.collect { case (c, p) if c == id => p }.toSet).toMap
    RunDag(ids.map(id => id -> node(id)).toMap, parents)
  }

  private val alwaysOk: RunNode => Try[Unit] = _ => Success(())

  private def statusOf(summary: RunSummary, id: String): NodeStatus =
    summary.byId(id).status

  "RunScheduler" should "return an empty successful summary for an empty dag" in {
    val summary =
      new RunScheduler(RunDag(Map.empty, Map.empty), 1, failFast = false, alwaysOk, _ => ())
        .run()
    summary.results shouldBe empty
    summary.exitCode shouldBe 0
  }

  it should "execute a chain in topological order with parallelism 1" in {
    val order = mutable.ListBuffer[String]()
    val exec: RunNode => Try[Unit] = { n => order.synchronized(order += n.id); Success(()) }
    val dag = dagOf("b" -> "a", "c" -> "b")()
    val summary = new RunScheduler(dag, 1, failFast = false, exec, _ => ()).run()
    order.toList shouldBe List("a", "b", "c")
    summary.exitCode shouldBe 0
  }

  it should "be deterministic across repeated runs with parallelism 1" in {
    val dag = dagOf("z.end" -> "m.mid", "m.mid" -> "a.start")("k.solo", "b.solo")
    def completionIds(): List[String] = {
      val order = mutable.ListBuffer[String]()
      val exec: RunNode => Try[Unit] = { n => order.synchronized(order += n.id); Success(()) }
      new RunScheduler(dag, 1, failFast = false, exec, _ => ()).run()
      order.toList
    }
    val first = completionIds()
    (1 to 5).foreach(_ => completionIds() shouldBe first)
  }

  it should "never start a child before its parent finished" in {
    val events = mutable.ListBuffer[RunEvent]()
    val dag = dagOf("d.b" -> "d.a", "d.c" -> "d.a", "d.d" -> "d.b", "d.d" -> "d.c")()
    new RunScheduler(dag, 4, failFast = false, alwaysOk, e => events += e).run()
    def startIndex(id: String) =
      events.indexWhere { case NodeStarted(n) => n.id == id; case _ => false }
    def finishIndex(id: String) =
      events.indexWhere { case NodeFinished(r) => r.node.id == id; case _ => false }
    startIndex("d.b") should be > finishIndex("d.a")
    startIndex("d.c") should be > finishIndex("d.a")
    startIndex("d.d") should be > finishIndex("d.b")
    startIndex("d.d") should be > finishIndex("d.c")
  }

  it should "cap concurrency at the configured parallelism" in {
    val current = new AtomicInteger(0)
    val maxSeen = new AtomicInteger(0)
    val exec: RunNode => Try[Unit] = { _ =>
      val now = current.incrementAndGet()
      maxSeen.getAndUpdate(m => math.max(m, now))
      Thread.sleep(50)
      current.decrementAndGet()
      Success(())
    }
    val dag = dagOf()("n1", "n2", "n3", "n4", "n5", "n6")
    new RunScheduler(dag, 2, failFast = false, exec, _ => ()).run()
    maxSeen.get() should be <= 2
  }

  it should "skip exactly the transitive downstream of a failure" in {
    val exec: RunNode => Try[Unit] = {
      case n if n.id == "f.a" => Failure(new RuntimeException("boom"))
      case _                  => Success(())
    }
    val dag = dagOf("f.b" -> "f.a", "f.c" -> "f.b")("f.solo")
    val summary = new RunScheduler(dag, 2, failFast = false, exec, _ => ()).run()
    statusOf(summary, "f.a") shouldBe a[NodeStatus.Failed]
    statusOf(summary, "f.b") shouldBe NodeStatus.SkippedUpstreamFailed
    statusOf(summary, "f.c") shouldBe NodeStatus.SkippedUpstreamFailed
    statusOf(summary, "f.solo") shouldBe NodeStatus.Succeeded
    summary.exitCode shouldBe 1
  }

  it should "stop dispatching after the first failure when failFast is set" in {
    val executed = mutable.ListBuffer[String]()
    val exec: RunNode => Try[Unit] = { n =>
      executed.synchronized(executed += n.id)
      if (n.id == "a.first") Failure(new RuntimeException("boom")) else Success(())
    }
    // 'a.first' sorts first, so with parallelism 1 it runs alone; everything else must be skipped
    val dag = dagOf("b.child" -> "a.first")("c.other", "d.other")
    val summary = new RunScheduler(dag, 1, failFast = true, exec, _ => ()).run()
    executed.toList shouldBe List("a.first")
    statusOf(summary, "b.child") shouldBe NodeStatus.SkippedUpstreamFailed
    statusOf(summary, "c.other") shouldBe NodeStatus.SkippedUpstreamFailed
    statusOf(summary, "d.other") shouldBe NodeStatus.SkippedUpstreamFailed
    summary.exitCode shouldBe 1
  }

  it should "satisfy boundary nodes without executing them and pass ordering through" in {
    val executed = mutable.ListBuffer[String]()
    val exec: RunNode => Try[Unit] = { n => executed.synchronized(executed += n.id); Success(()) }
    val boundary = node("ext.tbl", RunNodeType.Boundary)
    val task = node("t.job")
    val dag = RunDag(
      Map(boundary.id -> boundary, task.id -> task),
      Map(task.id -> Set(boundary.id), boundary.id -> Set.empty[String])
    )
    val summary = new RunScheduler(dag, 2, failFast = false, exec, _ => ()).run()
    executed.toList shouldBe List("t.job")
    statusOf(summary, "ext.tbl") shouldBe NodeStatus.Satisfied
    statusOf(summary, "t.job") shouldBe NodeStatus.Succeeded
    summary.exitCode shouldBe 0
  }

  it should "treat an executor exception as a failure rather than crashing" in {
    val exec: RunNode => Try[Unit] = { n =>
      if (n.id == "x.a") throw new IllegalStateException("escaped") else Success(())
    }
    val dag = dagOf("x.b" -> "x.a")()
    val summary = new RunScheduler(dag, 1, failFast = false, exec, _ => ()).run()
    statusOf(summary, "x.a") shouldBe a[NodeStatus.Failed]
    statusOf(summary, "x.b") shouldBe NodeStatus.SkippedUpstreamFailed
  }
}
