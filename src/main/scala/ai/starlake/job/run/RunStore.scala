package ai.starlake.job.run

/** Anything that went wrong talking to a run store. Carries a message a user can act on: a store
  * failure is reported to the user, never only logged.
  */
class RunStoreException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

/** Where a run log is kept.
  *
  * The trait speaks in runs and events only: no paths, no files, no line offsets, no SQL. That is
  * what makes FileRunStore and JdbcRunStore peers rather than one being a retrofit of the other,
  * and it is deliberate, not incidental.
  *
  * Implementations are single-writer. RunScheduler invokes its listener only from the thread that
  * called run(), so no implementation needs locking.
  *
  * Two caller obligations the backends cannot both enforce cheaply, so they are stated rather than
  * defended: `start` is called once per run id, and an attempt's header is appended before that run
  * is reopened again. `reopen` numbers attempts from what has been recorded, so reopening twice
  * with nothing written in between is undefined and the backends will disagree.
  */
trait RunStore {

  /** Records a new run and opens attempt 1 for writing. Returns the attempt number, always 1. */
  def start(header: RunLogEvent): Int

  /** Opens a further attempt on an existing run. Returns the new attempt number.
    * @throws RunStoreException
    *   if the run is unknown
    */
  def reopen(runId: String): Int

  /** Records one event on the attempt opened by the last `start` or `reopen`.
    * @throws RunStoreException
    *   if no attempt is open, or the write fails
    */
  def append(event: RunLogEvent): Unit

  /** Every attempt of that run, folded, or None when the run is unknown. */
  def read(runId: String): Option[RunHistory]

  /** The most recently *started* run, or None when nothing was ever recorded. Resuming an older run
    * does not make it the latest again.
    */
  def latest(): Option[RunHistory]

  def close(): Unit
}

/** The off switch, used for `--dry-run` and `--no-run-log`. Not a third backend: it stores nothing.
  * Reading through it is always empty, and reopening through it fails, because `--resume` against a
  * disabled log is a usage error rather than a run with no history.
  */
object NoopRunStore extends RunStore {
  def start(header: RunLogEvent): Int = 1
  def reopen(runId: String): Int =
    throw new RunStoreException(
      s"Cannot resume run '$runId': the run log is disabled for this invocation."
    )
  def append(event: RunLogEvent): Unit = ()
  def read(runId: String): Option[RunHistory] = None
  def latest(): Option[RunHistory] = None
  def close(): Unit = ()
}
