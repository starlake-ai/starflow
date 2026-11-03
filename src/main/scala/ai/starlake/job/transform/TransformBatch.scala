package ai.starlake.job.transform

import ai.starlake.utils.StarlakeException

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
