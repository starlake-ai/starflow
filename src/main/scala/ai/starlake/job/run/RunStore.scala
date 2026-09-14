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

  /** Releases whatever this store holds open. After it, the instance is spent: a fresh store over
    * the same backing state is a new instance, never this one reused.
    */
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

object RunStore {

  /** Set by starlake-api when it spawns the CLI, following the same convention as SL_API and the
    * QoD credential: the spawned process has no session, so what it needs arrives by environment.
    */
  val UrlEnv = "SL_RUN_STORE_URL"
  val UserEnv = "SL_RUN_STORE_USER"
  val PasswordEnv = "SL_RUN_STORE_PASSWORD"

  /** The caller's own job identifier, stored beside the run rather than as the run id: the runner
    * keeps ownership of the run id, which is what keeps one log to one writer.
    */
  val CorrelationEnv = "SL_RUN_CORRELATION_ID"
  val LogDirEnv = "SL_RUN_LOG_DIR"

  /** @param notice
    *   a line to show the user once, when the log did not go where they would expect
    */
  final case class FileLogRoot(path: java.nio.file.Path, notice: Option[String])

  private val RemoteUri = "^\\w+://.*"

  def fileLogRoot(root: String, env: Map[String, String]): FileLogRoot =
    env.get(LogDirEnv).filter(_.trim.nonEmpty) match {
      case Some(dir) => FileLogRoot(java.nio.file.Paths.get(dir.trim), None)
      case None =>
        localPathOf(root) match {
          case Some(local) =>
            FileLogRoot(local.resolve(".starlake").resolve("runs"), None)
          case None =>
            val fallback =
              java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")).resolve("starlake-runs")
            FileLogRoot(
              fallback,
              Some(
                s"Run log: $root is not a local path, so the run log goes to $fallback instead." +
                s" Set $LogDirEnv to put it somewhere durable."
              )
            )
        }
    }

  private def localPathOf(root: String): Option[java.nio.file.Path] =
    if (root.startsWith("file:"))
      scala.util.Try(java.nio.file.Paths.get(java.net.URI.create(root))).toOption
    else if (root.matches(RemoteUri)) None
    else Some(java.nio.file.Paths.get(root))

  /** The store this invocation reads and writes. Whether writing is enabled is a separate decision,
    * made by RunCmd: `--dry-run` and `--no-run-log` still need to *read* the store to resume.
    */
  def forRun(
    root: String,
    env: Map[String, String] = sys.env,
    notify: String => Unit = System.err.println
  ): RunStore =
    env.get(UrlEnv).filter(_.trim.nonEmpty) match {
      case Some(url) =>
        JdbcRunStore.open(url.trim, env.get(UserEnv), env.get(PasswordEnv), env.get(CorrelationEnv))
      case None =>
        val resolved = fileLogRoot(root, env)
        resolved.notice.foreach(notify)
        new FileRunStore(resolved.path)
    }
}
