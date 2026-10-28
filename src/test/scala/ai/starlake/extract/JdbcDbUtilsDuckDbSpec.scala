package ai.starlake.extract

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, DriverManager}
import scala.util.Using

class JdbcDbUtilsDuckDbSpec extends AnyFlatSpec with Matchers {

  private val url = "jdbc:duckdb:"

  private def withConnection[T](f: Connection => T): T =
    Using.resource(DriverManager.getConnection(url))(f)

  private def exists(conn: Connection, name: String): Boolean =
    JdbcDbUtils.tableExists(conn, url, name, "duckdb")

  "tableExists on DuckDB" should "not abort the open transaction when the table is missing" in {
    withConnection { conn =>
      conn.setAutoCommit(false)
      exists(conn, "nope.missing") shouldBe false
      // DuckDB 2.0 aborts a transaction on any failing statement: a probe on the missing table
      // would make this one fail with "Current transaction is aborted"
      JdbcDbUtils.createSchema(conn, "nope")
      conn.commit()
    }
  }

  it should "resolve one, two and three part names, quoted or not, case insensitively" in {
    withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute("CREATE SCHEMA \"My.Domain\"")
      stmt.execute("CREATE TABLE \"My.Domain\".Account (id INT)")
      stmt.execute("CREATE TABLE main.top (id INT)")
      stmt.execute("CREATE VIEW main.top_view AS SELECT * FROM main.top")
      stmt.execute("CREATE TEMP TABLE scratch (id INT)")
      stmt.close()

      exists(conn, "\"My.Domain\".Account") shouldBe true
      exists(conn, "\"my.domain\".account") shouldBe true
      exists(conn, "memory.\"My.Domain\".Account") shouldBe true
      exists(conn, "top") shouldBe true
      exists(conn, "memory.top") shouldBe true
      exists(conn, "main.top_view") shouldBe true
      exists(conn, "scratch") shouldBe true

      exists(conn, "\"My.Domain\".missing") shouldBe false
      exists(conn, "other.top") shouldBe false
      exists(conn, "memory.other.top") shouldBe false
    }
  }

  it should "see a table created earlier in the same transaction" in {
    withConnection { conn =>
      conn.setAutoCommit(false)
      conn.createStatement().execute("CREATE TABLE pending (id INT)")
      exists(conn, "main.pending") shouldBe true
      conn.rollback()
      exists(conn, "main.pending") shouldBe false
    }
  }

  "rollbackQuietly" should "be a no-op on an autocommit connection" in {
    withConnection { conn =>
      conn.getAutoCommit shouldBe true
      JdbcDbUtils.rollbackQuietly(conn)
    }
  }

  "splitQualifiedName" should "honor double quotes and escaped quotes" in {
    JdbcDbUtils.splitQualifiedName("a.b") shouldBe List("a", "b")
    JdbcDbUtils.splitQualifiedName("\"a.b\".c") shouldBe List("a.b", "c")
    JdbcDbUtils.splitQualifiedName("\"a\"\"b\".c.d") shouldBe List("a\"b", "c", "d")
  }
}
