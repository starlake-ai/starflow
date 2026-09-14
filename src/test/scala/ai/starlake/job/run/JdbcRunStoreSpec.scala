package ai.starlake.job.run

import ai.starlake.PgContainerHelper

import java.sql.{Connection, DriverManager}

class JdbcRunStoreSpec extends RunStoreContractSpec with PgContainerHelper {

  private def connect(): Connection =
    DriverManager.getConnection(pgContainer.jdbcUrl, "test", "test")

  private def resetSchema(): Unit = {
    val connection = connect()
    try {
      val statement = connection.createStatement()
      statement.execute("DROP TABLE IF EXISTS sl_run_event")
      statement.execute("DROP TABLE IF EXISTS sl_run")
      // The api owns this schema in production; the test creates it from the same resource the
      // api is told to implement, so the two cannot drift. Strip lines that are pure `--` comments
      // before splitting on ';': the resource is free to use a semicolon on a comment line, as
      // prose does. Only whole-comment lines are dropped, so a `--` inside a string literal on a
      // real statement line, should this resource ever grow one, is left alone.
      val statements = JdbcRunStore
        .schemaDdl()
        .linesIterator
        .map(line => if (line.trim.startsWith("--")) "" else line)
        .mkString("\n")
        .split(";")
        .map(_.trim)
        .filter(_.nonEmpty)
      statements.foreach(statement.execute)
    } finally connection.close()
  }

  override def withStore(test: RunStore => Unit): Unit = {
    resetSchema()
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    try test(store)
    finally store.close()
  }

  /** What a new process sees: another connection to the same database. */
  override def reopenStore(previous: RunStore): RunStore =
    JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)

  "JdbcRunStore" should "expose the run row the api reads, without folding events" in {
    resetSchema()
    val store =
      JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), Some("airflow-42"))
    try {
      store.start(header("r1"))
      val connection = connect()
      try {
        val rs = connection
          .createStatement()
          .executeQuery(
            "SELECT status, fingerprint, correlation_id FROM sl_run WHERE run_id = 'r1'"
          )
        rs.next() shouldBe true
        rs.getString("status") shouldBe "RUNNING"
        rs.getString("fingerprint") shouldBe "fp1"
        rs.getString("correlation_id") shouldBe "airflow-42"
      } finally connection.close()

      store.append(
        event("r1", 1, 2, RunLogEventType.RunFinished).copy(exitCode = Some(0))
      )
      val connection2 = connect()
      try {
        val rs = connection2
          .createStatement()
          .executeQuery("SELECT status, exit_code, finished_at FROM sl_run WHERE run_id = 'r1'")
        rs.next() shouldBe true
        rs.getString("status") shouldBe "FINISHED"
        rs.getInt("exit_code") shouldBe 0
        rs.getTimestamp("finished_at") should not be null
      } finally connection2.close()
    } finally store.close()
  }

  it should "skip a run row with no events when finding the latest run" in {
    resetSchema()
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    try {
      // The complete, older run.
      store.start(header("r1"))
      store.append(event("r1", 1, 2, RunLogEventType.RunFinished).copy(exitCode = Some(0)))

      // A crash between sl_run's insert and sl_run_event's insert of start() leaves exactly this: a
      // newer sl_run row with no sl_run_event rows at all. latest() must not fold that orphan to
      // nothing and report no run to resume.
      val connection = connect()
      try
        connection
          .createStatement()
          .execute(
            "INSERT INTO sl_run(run_id, created_at, schema_version, fingerprint, status)" +
            " VALUES ('r2', now() + interval '1 minute', 1, 'fp2', 'RUNNING')"
          )
      finally connection.close()

      store.latest().map(_.runId) shouldBe Some("r1")
    } finally store.close()
  }

  it should "reject a second insert of the same event instead of duplicating it" in {
    resetSchema()
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    try {
      store.start(header("r1"))
      // (run_id, attempt, seq) is the primary key: a retry that re-sends an event cannot create a
      // second copy of it, which is what keeps the fold honest.
      a[RunStoreException] should be thrownBy store.append(header("r1"))
    } finally store.close()
  }

  it should "say what is missing when the schema was never created" in {
    val connection = connect()
    try {
      val statement = connection.createStatement()
      statement.execute("DROP TABLE IF EXISTS sl_run_event")
      statement.execute("DROP TABLE IF EXISTS sl_run")
    } finally connection.close()

    val thrown = the[RunStoreException] thrownBy
      JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    thrown.getMessage should include("sl_run")
    thrown.getMessage should include(JdbcRunStore.SchemaResource)
  }

  it should "say what is wrong when no driver can serve the url" in {
    val thrown = the[RunStoreException] thrownBy
      JdbcRunStore.open("jdbc:nosuchdriver://localhost/db", None, None, None)
    thrown.getMessage should include("jdbc:nosuchdriver")
    thrown.getMessage should include("driver")
  }

  it should "not let a stale RunFinished from an older attempt mark a run finished" in {
    resetSchema()
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    try {
      store.start(header("r1"))
      store.append(event("r1", 1, 2, RunLogEventType.TaskSucceeded, Some("a")))

      store.reopen("r1") shouldBe 2
      store.append(header("r1").copy(attempt = 2))
      store.append(event("r1", 2, 2, RunLogEventType.TaskSucceeded, Some("a")))

      // A replay (or a slow retry) of attempt 1's RunFinished arrives after attempt 2 is already
      // under way and has rows of its own in sl_run_event. It must still be recorded as an event,
      // but it must not move sl_run.status: the api trusts that column to reflect the newest
      // attempt, not whichever attempt's terminal event happened to land last.
      store.append(
        event("r1", 1, 3, RunLogEventType.RunFinished).copy(exitCode = Some(99))
      )

      val connection = connect()
      try {
        val rs = connection
          .createStatement()
          .executeQuery("SELECT status, exit_code FROM sl_run WHERE run_id = 'r1'")
        rs.next() shouldBe true
        rs.getString("status") shouldBe "RUNNING"
        rs.getObject("exit_code") shouldBe null
      } finally connection.close()
    } finally store.close()
  }

  it should "write an absent selection, options and exit code as SQL NULL, not a fabricated default" in {
    resetSchema()
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), None)
    try {
      store.start(header("r2").copy(options = None))
      store.append(event("r2", 1, 2, RunLogEventType.RunFinished))

      // getObject plus a null check, not getInt/getString: those getters coerce a SQL NULL into 0
      // or "", which would make "no selection was given" indistinguishable from "an empty selection
      // was resolved", and a run that never reported an exit code look like it exited 0.
      val connection = connect()
      try {
        val rs = connection
          .createStatement()
          .executeQuery("SELECT selection, options, exit_code FROM sl_run WHERE run_id = 'r2'")
        rs.next() shouldBe true
        rs.getObject("selection") shouldBe null
        rs.getObject("options") shouldBe null
        rs.getObject("exit_code") shouldBe null
      } finally connection.close()
    } finally store.close()
  }
}
