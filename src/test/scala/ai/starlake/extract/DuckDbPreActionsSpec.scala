package ai.starlake.extract

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.sql.{Connection, DriverManager}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/** All the connections of a pooled DuckDB database share one instance, and so its attached
  * databases and its secrets. Only what belongs to a connection has to run on each of them.
  */
class DuckDbPreActionsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = StarlakeConnectionPool.clearDuckdbPool()
  override def afterEach(): Unit = StarlakeConnectionPool.clearDuckdbPool()

  private def options(preActions: String): Map[String, String] =
    Map(
      "url"        -> "jdbc:duckdb:",
      "driver"     -> "org.duckdb.DuckDBDriver",
      "preActions" -> preActions
    )

  private def withConn[T](preActions: String)(f: Connection => T): T =
    JdbcDbUtils.withJDBCConnection(None, options(preActions))(f)

  private def queryString(conn: Connection, sql: String): String =
    Using.resource(conn.createStatement()) { statement =>
      Using.resource(statement.executeQuery(sql)) { rs =>
        rs.next()
        rs.getString(1)
      }
    }

  private def isAttached(conn: Connection, name: String): Boolean =
    queryString(
      conn,
      s"SELECT count(*) FROM duckdb_databases() WHERE database_name = '$name'"
    ) == "1"

  private def execute(conn: Connection, sql: String): Unit =
    Using.resource(conn.createStatement())(_.execute(sql))

  private def inParallel[T](threads: Int, runs: Int)(f: () => T): List[Try[T]] = {
    val pool = Executors.newFixedThreadPool(threads)
    try {
      val tasks = (1 to runs).map(_ => (() => Try(f())): Callable[Try[T]]).asJava
      pool.invokeAll(tasks, 120, TimeUnit.SECONDS).asScala.toList.map(_.get())
    } finally pool.shutdownNow()
  }

  "preActions on a pooled DuckDB" should "attach a database once per instance" in {
    val preActions = "ATTACH ':memory:' AS once_db"
    withConn(preActions) { conn =>
      isAttached(conn, "once_db") shouldBe true
      execute(conn, "DETACH once_db")
    }
    withConn(preActions)(isAttached(_, "once_db")) shouldBe false
  }

  it should "recognize a statement whatever its case and the blanks around it" in {
    val preActions = "\n   attach ':memory:' as blanks_db  "
    withConn(preActions)(execute(_, "DETACH blanks_db"))
    withConn(preActions)(isAttached(_, "blanks_db")) shouldBe false
  }

  it should "create a secret once per instance" in {
    val preActions = "CREATE OR REPLACE SECRET once_secret (TYPE s3, KEY_ID 'k', SECRET 's')"
    withConn(preActions)(execute(_, "DROP SECRET once_secret"))
    withConn(preActions) { conn =>
      queryString(
        conn,
        "SELECT count(*) FROM duckdb_secrets() WHERE name = 'once_secret'"
      ) shouldBe "0"
    }
  }

  it should "not detach again what later connections are using" in {
    val preActions = "DETACH DATABASE IF EXISTS kept_db; ATTACH ':memory:' AS kept_db"
    withConn(preActions)(execute(_, "CREATE TABLE kept_db.main.t AS SELECT 7 AS v"))
    withConn(preActions)(queryString(_, "SELECT v FROM kept_db.main.t")) shouldBe "7"
  }

  it should "not read again a variable that only serves to set up the instance" in {
    val file = Files.createTempFile("preaction", ".txt")
    Files.writeString(file, "key")
    val preActions =
      "SET VARIABLE setup_key = (SELECT CASE WHEN trim(content) = 'read again' " +
      s"THEN error('read again') ELSE trim(content) END FROM read_text('$file'));" +
      "CREATE OR REPLACE SECRET setup_secret (TYPE s3, KEY_ID getvariable('setup_key'), SECRET 's');" +
      "RESET VARIABLE setup_key"
    withConn(preActions)(_ => ())
    Files.writeString(file, "read again")
    try withConn(preActions)(queryString(_, "SELECT 1")) shouldBe "1"
    finally Files.delete(file)
  }

  it should "keep setting a variable that stays set" in {
    val preActions = "SET VARIABLE kept = 42; ATTACH ':memory:' AS kept_var_db"
    (1 to 2).foreach { _ =>
      withConn(preActions)(queryString(_, "SELECT getvariable('kept')")) shouldBe "42"
    }
  }

  it should "keep setting a variable that a statement of each connection reads" in {
    val preActions =
      "ATTACH ':memory:' AS read_var_db; CREATE TABLE IF NOT EXISTS read_var_db.main.seen (v INT);" +
      "SET VARIABLE read_var = 5;" +
      "INSERT INTO read_var_db.main.seen VALUES (getvariable('read_var'));" +
      "RESET VARIABLE read_var"
    withConn(preActions)(_ => ())
    withConn(preActions) { conn =>
      queryString(conn, "SELECT count(*) FROM read_var_db.main.seen WHERE v = 5") shouldBe "2"
    }
  }

  it should "set the current database of each connection" in {
    val preActions = "ATTACH ':memory:' AS used_db; USE used_db"
    (1 to 2).foreach { _ =>
      withConn(preActions)(queryString(_, "SELECT current_database()")) shouldBe "used_db"
    }
  }

  it should "run the statements of another connection definition on the same instance" in {
    withConn("ATTACH ':memory:' AS first_db")(_ => ())
    withConn("ATTACH ':memory:' AS second_db") { conn =>
      isAttached(conn, "first_db") shouldBe true
      isAttached(conn, "second_db") shouldBe true
    }
  }

  it should "run them again on the next instance" in {
    val preActions = "ATTACH ':memory:' AS next_db"
    withConn(preActions)(_ => ())
    StarlakeConnectionPool.clearDuckdbPool()
    withConn(preActions)(isAttached(_, "next_db")) shouldBe true
  }

  it should "try them again after a failure" in {
    val preActions = "ATTACH '/nonexistent/directory/x.duckdb' AS failing_db"
    an[Exception] should be thrownBy withConn(preActions)(_ => ())
    an[Exception] should be thrownBy withConn(preActions)(_ => ())
  }

  it should "run them on every connection of an unpooled database" in {
    val preActions = "ATTACH ':memory:' AS unpooled_db"
    Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { own =>
      JdbcDbUtils.withJDBCConnection(None, options(preActions), Some(own)) { conn =>
        execute(conn, "DETACH unpooled_db")
      }
      JdbcDbUtils.withJDBCConnection(None, options(preActions), Some(own)) { conn =>
        isAttached(conn, "unpooled_db") shouldBe true
      }
    }
  }

  it should "not conflict when connections open at the same time" in {
    val preActions =
      "CREATE OR REPLACE SECRET busy_a (TYPE s3, KEY_ID 'k', SECRET 's');" +
      "CREATE OR REPLACE SECRET busy_b (TYPE s3, KEY_ID 'k', SECRET 's');" +
      "CREATE OR REPLACE SECRET busy_c (TYPE s3, KEY_ID 'k', SECRET 's')"
    val results = inParallel(threads = 8, runs = 400) { () =>
      withConn(preActions)(queryString(_, "SELECT 1"))
    }
    results.filter(_.isFailure).map(_.failed.get.getMessage).distinct shouldBe Nil
  }

  it should "make a connection wait until the instance is ready" in {
    // The statement before ATTACH takes long enough for the other connections to reach this block
    val preActions =
      "SET VARIABLE slow = (SELECT sum(i) FROM range(30000000) t(i));" +
      "ATTACH ':memory:' AS ready_db"
    val results = inParallel(threads = 8, runs = 8) { () =>
      withConn(preActions)(isAttached(_, "ready_db"))
    }
    results.map(_.get) shouldBe List.fill(8)(true)
  }
}
