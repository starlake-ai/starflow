package ai.starlake.job.transform

import ai.starlake.utils.StarlakeException

import java.util.concurrent.{Executor, ExecutorCompletionService, Executors}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

sealed trait TransformTaskStatus

object TransformTaskStatus {
  case object Succeeded extends TransformTaskStatus
  final case class Failed(error: Throwable) extends TransformTaskStatus

  /** Not run, because a task it reads from failed
    *
    * @param failedUpstream
    *   the task that failed, which may be several levels upstream
    */
  final case class Skipped(failedUpstream: String) extends TransformTaskStatus
}

final case class TransformTaskResult(
  name: String,
  status: TransformTaskStatus,
  durationMillis: Long
)

/** One run of a task in a batch
  *
  * @param name
  *   the task, as the trees spell it
  * @param reads
  *   the runs it reads from, in the order of the tree: the first that fails or is skipped gets this
  *   one skipped
  * @param after
  *   the runs that must finish first, whatever their outcome
  */
final case class TransformStep(name: String, reads: List[Int], after: Set[Int])

object TransformStep {

  /** Runs the steps, none before those it depends on, and at most `parallelism` at the same time.
    * At most one runs at a time, on the calling thread and in the order of the list, when
    * `parallelism` is 1. Skipping a step takes no slot.
    *
    * @return
    *   what happened to each step, in the order of the list
    */
  def runAll(steps: Vector[TransformStep], parallelism: Int)(
    run: String => TransformTaskResult
  ): List[TransformTaskResult] = {
    val pool =
      if (parallelism > 1 && steps.size > 1)
        Some(Executors.newFixedThreadPool(math.min(parallelism, steps.size)))
      else
        None
    val executor: Executor = pool.getOrElse((task: Runnable) => task.run())
    val running = new ExecutorCompletionService[(Int, TransformTaskResult)](executor)
    val results = new Array[TransformTaskResult](steps.size)
    // Step -> the failed task blocking the steps that read it
    val settled = mutable.Map.empty[Int, Option[String]]
    val waiting = mutable.LinkedHashSet.from(steps.indices)
    var runningCount = 0

    def settle(index: Int, result: TransformTaskResult, blocking: Option[String]): Unit = {
      results(index) = result
      settled(index) = blocking
    }

    // Goes over the steps again after a skip, as it may settle steps that read the skipped one
    @tailrec
    def startReadySteps(): Unit = {
      val ready = waiting.toList.filter { index =>
        val step = steps(index)
        (step.reads ++ step.after).forall(settled.contains)
      }
      var skipped = false
      ready.foreach { index =>
        steps(index).reads.flatMap(settled).headOption match {
          case Some(failed) =>
            waiting -= index
            val status = TransformTaskStatus.Skipped(failed)
            settle(index, TransformTaskResult(steps(index).name, status, 0L), Some(failed))
            skipped = true
          case None if runningCount < parallelism.max(1) =>
            waiting -= index
            runningCount += 1
            running.submit(() => index -> run(steps(index).name))
          case None =>
        }
      }
      if (skipped) startReadySteps()
    }

    try {
      startReadySteps()
      while (runningCount > 0) {
        val (index, result) = running.take().get()
        runningCount -= 1
        val blocking =
          if (result.status == TransformTaskStatus.Succeeded) None else Some(result.name)
        settle(index, result, blocking)
        startReadySteps()
      }
    } finally pool.foreach(_.shutdownNow())
    results.toList
  }
}

class TransformBatchException(message: String, cause: Throwable)
    extends StarlakeException(message, cause)

/** What happened to each task of a batch, in the order they were reached */
final case class TransformBatchSummary(results: List[TransformTaskResult]) {
  import TransformTaskStatus.*

  private def failures: List[(String, Throwable)] =
    results.collect { case TransformTaskResult(name, Failed(error), _) => name -> error }

  private def skippedCount: Int = results.count(_.status.isInstanceOf[Skipped])

  def isSuccess: Boolean = failures.isEmpty && skippedCount == 0

  def render: String = {
    val succeeded = results.count(_.status == Succeeded)
    val header =
      s"Transform summary: ${results.size} tasks: $succeeded succeeded, ${failures.size} failed, " +
      s"$skippedCount skipped"
    val width = results.map(_.name.length).maxOption.getOrElse(0)
    val lines = results.map { result =>
      val name = result.name.padTo(width, ' ')
      val seconds = f"${result.durationMillis / 1000.0}%.1fs"
      result.status match {
        case Succeeded => s"  SUCCEEDED  $name  $seconds"
        case Failed(error) =>
          val message = Option(error.getMessage).getOrElse(error.getClass.getName)
          s"  FAILED     $name  $seconds  ${message.linesIterator.nextOption().getOrElse("")}"
        case Skipped(failedUpstream) =>
          s"  SKIPPED    $name  upstream $failedUpstream failed"
      }
    }
    (header +: lines).mkString("\n")
  }

  def toTry: Try[String] =
    failures.headOption match {
      case Some((_, firstError)) =>
        val failedNames = failures.map { case (name, _) => name }.mkString(", ")
        Failure(
          new TransformBatchException(
            s"${failures.size} task(s) failed, $skippedCount skipped: $failedNames",
            firstError
          )
        )
      case None => Success("")
    }
}
