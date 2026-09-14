package ai.starlake.job.run

import ai.starlake.utils.Utils
import com.fasterxml.jackson.databind.ObjectMapper

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.Random
import scala.util.control.NonFatal

object RunLogEventType {
  val RunStarted = "RunStarted"
  val TaskStarted = "TaskStarted"
  val TaskSucceeded = "TaskSucceeded"
  val TaskFailed = "TaskFailed"
  val TaskSkipped = "TaskSkipped"
  val RunFinished = "RunFinished"
}

object SkipReason {
  val UpstreamFailed = "UPSTREAM_FAILED"
  val AlreadySucceeded = "ALREADY_SUCCEEDED"
}

/** One line of the run log.
  *
  * Deliberately flat, with a `type` discriminator, rather than a sealed hierarchy: Jackson then
  * needs no polymorphic configuration and every consumer (starlake-api, `jq`, a future
  * `--output jsonl`) can read a line without knowing our class graph. Fields that do not apply to
  * an event type are None and are omitted on write.
  */
final case class RunLogEvent(
  runId: String,
  attempt: Int,
  seq: Int,
  ts: String,
  `type`: String,
  schemaVersion: Int = RunLog.SchemaVersion,
  // RunStarted only
  fingerprint: Option[String] = None,
  fingerprintParts: Option[Map[String, String]] = None,
  selectExprs: Option[List[String]] = None,
  excludeExprs: Option[List[String]] = None,
  selection: Option[List[String]] = None,
  taskDigests: Option[Map[String, String]] = None,
  options: Option[Map[String, String]] = None,
  env: Option[String] = None,
  correlationId: Option[String] = None,
  parallelism: Option[Int] = None,
  failFast: Option[Boolean] = None,
  // task events only
  taskId: Option[String] = None,
  taskName: Option[String] = None,
  nodeType: Option[String] = None,
  durationMillis: Option[Long] = None,
  errorType: Option[String] = None,
  message: Option[String] = None,
  reason: Option[String] = None,
  // RunFinished only
  exitCode: Option[Int] = None,
  counts: Option[Map[String, Int]] = None
)

object RunLog {

  /** Frozen. Bump only on a breaking change to the event shape, never to add an optional field. */
  val SchemaVersion: Int = 1

  private val mapper: ObjectMapper = Utils.newJsonMapper()

  def toJson(event: RunLogEvent): String = mapper.writeValueAsString(event)

  def fromJson(line: String): Either[String, RunLogEvent] =
    try Right(mapper.readValue(line, classOf[RunLogEvent]))
    catch {
      case NonFatal(e) =>
        Left(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
    }

  /** Serializes a plain value with the same mapper the events use. JdbcRunStore fills its typed
    * JSONB columns with this, so the store owns no second serializer that could disagree with the
    * payload it writes beside them.
    */
  def toJsonValue(value: Any): String = mapper.writeValueAsString(value)

  def nowTs(): String = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
}

/** The state a run log folds down to. The only place events become state: both stores call this,
  * so neither can drift into its own notion of what "succeeded" means.
  *
  * @param succeeded
  *   task ids that reached TaskSucceeded in any attempt. Never retracted: a task that succeeded in
  *   attempt 1 stays satisfied even if a later attempt never touched it.
  */
final case class RunHistory(
  runId: String,
  header: RunLogEvent,
  attempts: Int,
  succeeded: Set[String],
  finished: Boolean,
  events: List[RunLogEvent]
)

object RunHistory {

  def fold(events: List[RunLogEvent]): Option[RunHistory] =
    events.find(_.`type` == RunLogEventType.RunStarted).map { header =>
      val attempts = events.map(_.attempt).maxOption.getOrElse(1)
      val succeeded = events.collect {
        case e if e.`type` == RunLogEventType.TaskSucceeded => e.taskId
      }.flatten.toSet
      val finished = events.exists(e =>
        e.`type` == RunLogEventType.RunFinished && e.attempt == attempts
      )
      RunHistory(header.runId, header, attempts, succeeded, finished, events)
    }
}

object RunId {

  private val formatter =
    DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC)

  /** `yyyyMMdd-HHmmss-<6 hex>`: sortable by start time, which is what makes "the latest run" a
    * name comparison in FileRunStore, with a random suffix so two runs in the same second stay
    * distinct.
    */
  def generate(now: Instant = Instant.now(), random: Random = new Random()): String = {
    val suffix = f"${random.nextInt(1 << 24)}%06x"
    s"${formatter.format(now)}-$suffix"
  }
}
