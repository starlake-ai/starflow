package ai.starlake.job.run

import java.util.concurrent.{Callable, ExecutorCompletionService, Executors}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

sealed trait NodeStatus

object NodeStatus {
  case object Succeeded extends NodeStatus
  final case class Failed(error: Throwable) extends NodeStatus
  case object SkippedUpstreamFailed extends NodeStatus

  /** Boundary node: nothing to execute, ordering constraint only */
  case object Satisfied extends NodeStatus
}

final case class NodeResult(node: RunNode, status: NodeStatus, durationMillis: Long)

sealed trait RunEvent
final case class NodeStarted(node: RunNode) extends RunEvent
final case class NodeFinished(result: NodeResult) extends RunEvent

final case class RunSummary(results: List[NodeResult]) {
  lazy val byId: Map[String, NodeResult] = results.map(r => r.node.id -> r).toMap

  def failures: List[NodeResult] = results.filter(_.status.isInstanceOf[NodeStatus.Failed])

  def skipped: List[NodeResult] = results.filter(_.status == NodeStatus.SkippedUpstreamFailed)

  def exitCode: Int = if (failures.nonEmpty || skipped.nonEmpty) 1 else 0
}

/** Executes a RunDag on a fixed-size thread pool. A node is dispatched only once every
  * parent reached Succeeded or Satisfied. The listener is always invoked from the thread
  * that called run(), never from worker threads.
  */
class RunScheduler(
  dag: RunDag,
  parallelism: Int,
  failFast: Boolean,
  executor: RunNode => Try[Unit],
  listener: RunEvent => Unit = _ => ()
) {
  require(parallelism >= 1, "parallelism must be >= 1")

  def run(): RunSummary = {
    val children = dag.children
    val pendingParents =
      mutable.Map[String, Int]() ++= dag.nodes.keys.map(id => id -> dag.parents(id).size)
    val readyQueue = mutable.TreeSet[String]() ++= pendingParents.collect {
      case (id, 0) => id
    }
    val terminal = mutable.Map[String, NodeResult]()
    val running = mutable.Set[String]()
    val completionOrder = mutable.ListBuffer[NodeResult]()
    var aborted = false

    val pool = Executors.newFixedThreadPool(parallelism)
    val completionService = new ExecutorCompletionService[NodeResult](pool)

    def record(result: NodeResult): Unit = {
      terminal(result.node.id) = result
      completionOrder += result
      listener(NodeFinished(result))
    }

    def unblockChildren(id: String): Unit =
      children.getOrElse(id, Set.empty).foreach { child =>
        val remaining = pendingParents(child) - 1
        pendingParents(child) = remaining
        if (remaining == 0 && !terminal.contains(child)) readyQueue += child
      }

    /** Transitive downstream of a failed or skipped node. A running node can never be in
      * this set: it only started because all its parents had already succeeded.
      */
    def skipDownstream(roots: Set[String]): Unit = {
      val queue = mutable.Queue[String]() ++= roots
      while (queue.nonEmpty) {
        val id = queue.dequeue()
        if (!terminal.contains(id)) {
          readyQueue -= id
          record(NodeResult(dag.nodes(id), NodeStatus.SkippedUpstreamFailed, 0L))
          queue ++= children.getOrElse(id, Set.empty)
        }
      }
    }

    def dispatch(): Unit = {
      var progressed = true
      while (progressed) {
        progressed = false
        readyQueue.find(id => dag.nodes(id).typ == RunNodeType.Boundary).foreach { id =>
          readyQueue -= id
          record(NodeResult(dag.nodes(id), NodeStatus.Satisfied, 0L))
          unblockChildren(id)
          progressed = true
        }
        while (!aborted && running.size < parallelism && readyQueue.nonEmpty) {
          val id = readyQueue.head
          readyQueue -= id
          val node = dag.nodes(id)
          if (node.typ == RunNodeType.Boundary) {
            record(NodeResult(node, NodeStatus.Satisfied, 0L))
            unblockChildren(id)
            progressed = true
          } else {
            running += id
            listener(NodeStarted(node))
            completionService.submit(new Callable[NodeResult] {
              def call(): NodeResult = {
                val start = System.nanoTime()
                val status =
                  Try(executor(node)).flatten match {
                    case Success(_) => NodeStatus.Succeeded
                    case Failure(e) => NodeStatus.Failed(e)
                  }
                NodeResult(node, status, (System.nanoTime() - start) / 1000000L)
              }
            })
          }
        }
      }
    }

    try {
      dispatch()
      while (running.nonEmpty) {
        val result = completionService.take().get()
        running -= result.node.id
        record(result)
        result.status match {
          case NodeStatus.Succeeded =>
            unblockChildren(result.node.id)
          case NodeStatus.Failed(_) =>
            if (failFast) aborted = true
            skipDownstream(children.getOrElse(result.node.id, Set.empty))
          case _ => ()
        }
        dispatch()
      }
      // fail-fast leftovers: ready but never dispatched, or still blocked
      dag.nodes.keys.filterNot(terminal.contains).toList.sorted.foreach { id =>
        record(NodeResult(dag.nodes(id), NodeStatus.SkippedUpstreamFailed, 0L))
      }
      RunSummary(completionOrder.toList)
    } finally {
      pool.shutdown()
    }
  }
}
