package ai.starlake.integration.transform

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

import java.sql.DriverManager
import scala.util.Try

/** The whole path through Main on real DuckDB tables: a task reading a table a previous task did
  * not create fails, so the existence of the tables proves both what ran and in which order.
  */
class TransformBatchIntegrationSpec extends IntegrationTestBase {
  override def beforeEach(): Unit = ()
  override def afterEach(): Unit = ()

  private def withProject(test: File => Unit): Unit = withProject(audit = false)(test)

  /** @param auditSink
    *   the connection of the audit table, instead of the connection of the project
    */
  private def withProject(audit: Boolean, auditSink: Option[String] = None)(
    test: File => Unit
  ): Unit = {
    val root = File.newTemporaryDirectory("transform-batch-")
    try {
      (root / "datasets").createDirectories()
      (root / "metadata" / "application.sl.yml")
        .createIfNotExists(createParents = true)
        .overwrite(
          s"""version: 1
             |application:
             |  connectionRef: duckdb
             |  audit:
             |    active: $audit
             |    ${auditSink.fold("")(sink => s"sink: { connectionRef: $sink }")}
             |  connections:
             |    duckdb:
             |      type: jdbc
             |      options:
             |        url: "jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db"
             |        driver: "org.duckdb.DuckDBDriver"
             |    unreachable:
             |      type: jdbc
             |      options:
             |        url: "jdbc:duckdb:${root.pathAsString}/no/such/folder/audit.db"
             |        driver: "org.duckdb.DuckDBDriver"
             |""".stripMargin
        )
      val job = root / "metadata" / "transform" / "batch"
      job.createDirectories()
      (job / "_config.sl.yml").overwrite(
        """version: 1
          |transform:
          |  default:
          |    writeStrategy:
          |      type: OVERWRITE
          |""".stripMargin
      )
      (job / "up.sql").overwrite("SELECT 1 AS id")
      (job / "down.sql").overwrite("SELECT id FROM batch.up")
      (job / "other.sql").overwrite("SELECT 2 AS id")
      (job / "broken.sql").overwrite("SELECT id FROM batch.does_not_exist")
      (job / "after_broken.sql").overwrite("SELECT id FROM batch.broken")
      (job / "appended.sql").overwrite("SELECT 1 AS id")
      (job / "appended.sl.yml").overwrite(
        """version: 1
          |task:
          |  writeStrategy:
          |    type: APPEND
          |""".stripMargin
      )
      List("up", "down").foreach { name =>
        (job / s"$name.sl.yml").overwrite(
          """version: 1
            |task:
            |  tags: ["chain"]
            |""".stripMargin
        )
      }
      withEnvs("SL_ROOT" -> root.pathAsString) { test(root) }
    } finally {
      Try(root.delete())
    }
  }

  private def transform(args: String*): Boolean =
    Try(new Main().run(("transform" +: args).toArray)).getOrElse(false)

  private def tables(root: File): Set[String] = {
    val conn = DriverManager.getConnection(s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db")
    try {
      val rs = conn
        .createStatement()
        .executeQuery("SELECT table_name FROM information_schema.tables")
      Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1).toLowerCase).toSet
    } finally conn.close()
  }

  private def rowCount(root: File, table: String): Long = {
    val conn = DriverManager.getConnection(s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db")
    try {
      val rs = conn.createStatement().executeQuery(s"SELECT count(*) FROM batch.$table")
      rs.next()
      rs.getLong(1)
    } finally conn.close()
  }

  private def manyTasks(root: File, count: Int): List[String] = {
    val job = root / "metadata" / "transform" / "batch"
    (0 until count).toList.map { i =>
      (job / s"wide$i.sql").overwrite(s"SELECT $i AS id")
      s"batch.wide$i"
    }
  }

  private def ids(root: File, table: String): List[Int] = {
    val conn = DriverManager.getConnection(s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db")
    try {
      val rs = conn.createStatement().executeQuery(s"SELECT id FROM batch.$table")
      Iterator.continually(rs).takeWhile(_.next()).map(_.getInt(1)).toList
    } finally conn.close()
  }

  "transform --parallelism" should "write each task to its own table on a new database" in {
    withProject { root =>
      val names = manyTasks(root, 8)
      transform("--name", names.mkString(","), "--parallelism", "8") shouldBe true
      (0 until 8).foreach(i => ids(root, s"wide$i") shouldBe List(i))
    }
  }

  it should "replace the content of existing tables" in {
    withProject { root =>
      val names = manyTasks(root, 8)
      transform("--name", names.mkString(","), "--parallelism", "8") shouldBe true
      transform("--name", names.mkString(","), "--parallelism", "8") shouldBe true
      (0 until 8).foreach(i => ids(root, s"wide$i") shouldBe List(i))
    }
  }

  it should "log every task to the audit table on a new database" in {
    withProject(audit = true) { root =>
      val names = manyTasks(root, 8)
      transform("--name", names.mkString(","), "--parallelism", "8") shouldBe true
      (0 until 8).foreach(i => ids(root, s"wide$i") shouldBe List(i))
      val conn = DriverManager.getConnection(s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db")
      try {
        val rs = conn.createStatement().executeQuery("SELECT count(*) FROM audit.audit")
        rs.next()
        rs.getLong(1) shouldBe 8
      } finally conn.close()
    }
  }

  it should "run upstream tasks first" in {
    withProject { root =>
      transform("--name", "batch.down,batch.up,batch.other", "--parallelism", "3") shouldBe true
      tables(root) should contain allOf ("up", "down", "other")
    }
  }

  it should "skip what reads a failed task, run the rest and fail" in {
    withProject { root =>
      transform(
        "--name",
        "batch.broken,batch.after_broken,batch.other",
        "--parallelism",
        "3"
      ) shouldBe false
      val created = tables(root)
      created should contain("other")
      created should not contain "after_broken"
    }
  }

  it should "run all the tags on several threads with --recursive" in {
    withProject { root =>
      transform("--tags", "chain", "--recursive", "--parallelism", "2") shouldBe true
      tables(root) should contain allOf ("up", "down")
    }
  }

  /** The connection works, and inserting in this audit table fails */
  private def breakAuditTable(root: File): Unit = {
    val conn = DriverManager.getConnection(s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db")
    try {
      conn.createStatement().execute("CREATE SCHEMA audit")
      conn.createStatement().execute("CREATE TABLE audit.audit (not_an_audit_column INT)")
    } finally conn.close()
  }

  "transform with an audit table it cannot write" should "write the table and fail" in {
    withProject(audit = true) { root =>
      breakAuditTable(root)
      transform("--name", "batch.other") shouldBe false
      ids(root, "other") shouldBe List(2)
    }
  }

  it should "still run what reads a task whose audit row is lost" in {
    withProject(audit = true) { root =>
      breakAuditTable(root)
      transform("--name", "batch.up,batch.down") shouldBe false
      tables(root) should contain allOf ("up", "down")
    }
  }

  "transform with an audit connection it cannot open" should "write the table and fail" in {
    withProject(audit = true, auditSink = Some("unreachable")) { root =>
      transform("--name", "batch.other") shouldBe false
      ids(root, "other") shouldBe List(2)
    }
  }

  it should "still run what reads a task whose audit row is lost" in {
    withProject(audit = true, auditSink = Some("unreachable")) { root =>
      transform("--name", "batch.up,batch.down") shouldBe false
      tables(root) should contain allOf ("up", "down")
    }
  }

  "transform --name a,a" should "run the task twice" in {
    withProject { root =>
      transform("--name", "batch.appended,batch.appended") shouldBe true
      rowCount(root, "appended") shouldBe 2
    }
  }

  it should "run the task as many times as listed, whatever the case" in {
    withProject { root =>
      transform("--name", "batch.appended,BATCH.APPENDED,batch.appended") shouldBe true
      rowCount(root, "appended") shouldBe 3
    }
  }

  "transform --name a,b" should "run upstream first whatever the order of the list" in {
    withProject { root =>
      transform("--name", "batch.down,batch.up") shouldBe true
      tables(root) should contain allOf ("up", "down")
    }
  }

  it should "not run an unlisted upstream" in {
    withProject { root =>
      transform("--name", "batch.up,batch.other") shouldBe true
      val created = tables(root)
      created should contain allOf ("up", "other")
      created should not contain "down"
    }
  }

  it should "fail, skip what reads the failed task, and run the rest" in {
    withProject { root =>
      transform("--name", "batch.broken,batch.after_broken,batch.other") shouldBe false
      val created = tables(root)
      created should contain("other")
      created should not contain "after_broken"
    }
  }

  it should "run nothing when a name is unknown" in {
    withProject { root =>
      transform("--name", "batch.other,batch.typo") shouldBe false
      tables(root) should not contain "other"
    }
  }

  "transform --tags" should "run the tagged tasks in dependency order" in {
    withProject { root =>
      transform("--tags", "chain") shouldBe true
      val created = tables(root)
      created should contain allOf ("up", "down")
      created should not contain "other"
    }
  }

  it should "fail when no task has the tag" in {
    withProject { _ =>
      transform("--tags", "nothing_has_this") shouldBe false
    }
  }

  "transform --dry-run" should "refuse to run on an engine that would really write" in {
    withProject { root =>
      transform("--name", "batch.up", "--dry-run") shouldBe false
      tables(root) should not contain "up"
    }
  }

  "transform --recursive" should "refuse --interactive rather than write the tables" in {
    withProject { root =>
      transform("--recursive", "--name", "batch.down", "--interactive", "csv") shouldBe false
      tables(root) should not contain "down"
    }
  }

  it should "run the upstream of a task" in {
    withProject { root =>
      transform("--recursive", "--name", "batch.down") shouldBe true
      tables(root) should contain allOf ("up", "down")
    }
  }

  it should "run the upstream of each listed task" in {
    withProject { root =>
      transform("--recursive", "--name", "batch.down,batch.other") shouldBe true
      tables(root) should contain allOf ("up", "down", "other")
    }
  }

  it should "fail and not run the task when its upstream failed" in {
    withProject { root =>
      transform("--recursive", "--name", "batch.after_broken") shouldBe false
      tables(root) should not contain "after_broken"
    }
  }
}
