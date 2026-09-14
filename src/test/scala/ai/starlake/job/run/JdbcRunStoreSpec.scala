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
      // api is told to implement, so the two cannot drift.
      JdbcRunStore.schemaDdl().split(";").map(_.trim).filter(_.nonEmpty).foreach(statement.execute)
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
    val store = JdbcRunStore.open(pgContainer.jdbcUrl, Some("test"), Some("test"), Some("airflow-42"))
    try {
      store.start(header("r1"))
      val connection = connect()
      try {
        val rs = connection
          .createStatement()
          .executeQuery("SELECT status, fingerprint, correlation_id FROM sl_run WHERE run_id = 'r1'")
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
}
