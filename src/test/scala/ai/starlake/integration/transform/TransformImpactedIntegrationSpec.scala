package ai.starlake.integration.transform

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.impact.ImpactFixtures
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

import java.io.ByteArrayOutputStream

import scala.sys.process.Process
import scala.util.Try

/** `transform --impacted` through Main on real DuckDB tables, in a git repository holding the
  * impact fixture project: raw.orders (load) -> stg.orders -> kpi.revenue -> kpi.dashboard, all
  * OVERWRITE.
  *
  * Every task runs once first, then stg.orders gets a row with id 999 that no run of stg.orders
  * writes: a rerun of stg.orders removes it, a rerun of kpi.revenue copies it.
  */
class TransformImpactedIntegrationSpec extends IntegrationTestBase {
  override def beforeEach(): Unit = ()
  override def afterEach(): Unit = ()

  private def git(repo: File, args: String*): String =
    Process(Seq("git", "-C", repo.pathAsString) ++ args).!!

  private def url(root: File): String = s"jdbc:duckdb:${root.pathAsString}/datasets/duckdb.db"

  private def options(root: File): Map[String, String] =
    Map("url" -> url(root), "driver" -> "org.duckdb.DuckDBDriver")

  // through the connection pool the transforms use, so that one DuckDB instance holds the file
  private def sql(root: File, statements: String*): Unit =
    JdbcDbUtils.withJDBCConnection(None, options(root)) { conn =>
      statements.foreach(conn.createStatement().execute(_))
    }

  private def ids(root: File, table: String, branch: Option[String] = None): List[Long] =
    JdbcDbUtils.withJDBCConnection(branch, options(root)) { conn =>
      val rs = conn.createStatement().executeQuery(s"SELECT id FROM $table ORDER BY id")
      Iterator.continually(rs).takeWhile(_.next()).map(_.getLong(1)).toList
    }

  private def dashboardRev(root: File): BigDecimal =
    JdbcDbUtils.withJDBCConnection(None, options(root)) { conn =>
      val rs = conn.createStatement().executeQuery("SELECT rev FROM kpi.dashboard")
      rs.next()
      BigDecimal(rs.getBigDecimal(1))
    }

  private def transform(args: String*): Boolean =
    Try(new Main().run(("transform" +: args).toArray)).getOrElse(false)

  /** The transform result with what it printed. */
  private def transformOut(args: String*): (Boolean, String) = {
    val stdout = new ByteArrayOutputStream()
    val ok = Console.withOut(stdout)(transform(args: _*))
    (ok, stdout.toString)
  }

  private val revenueTimesThree =
    "SELECT s.id, s.amount * 3 AS total FROM stg.orders s\n"

  private def withProject(test: File => Unit): Unit = {
    val root = File.newTemporaryDirectory("transform-impacted-")
    try {
      ImpactFixtures.writeProject(root)
      git(root, "init", "-q", "-b", "main")
      git(root, "config", "user.email", "test@example.com")
      git(root, "config", "user.name", "test")
      git(root, "config", "commit.gpgsign", "false")
      git(root, "add", "-A")
      git(root, "commit", "-q", "-m", "base")
      (root / "datasets").createDirectories()
      withEnvs("SL_ROOT" -> root.pathAsString) {
        sql(
          root,
          "CREATE SCHEMA raw",
          "CREATE TABLE raw.orders (id BIGINT, amount DECIMAL(10, 2), status VARCHAR)",
          "INSERT INTO raw.orders VALUES (1, 10, 'ok'), (2, 20, 'ok')"
        )
        transform("--name", "stg.orders,kpi.revenue,kpi.dashboard") shouldBe true
        sql(root, "INSERT INTO stg.orders VALUES (999, 1)")
        test(root)
      }
    } finally Try(root.delete())
  }

  "transform --impacted" should "run the changed task and what reads it, not what it reads" in {
    withProject { root =>
      dashboardRev(root) shouldBe BigDecimal(60) // 10 * 2 + 20 * 2, from the first run
      (root / "metadata/transform/kpi/revenue.sql").overwrite(revenueTimesThree)
      transform("--impacted", "--base", "HEAD") shouldBe true
      ids(root, "stg.orders") shouldBe List(1, 2, 999) // not run again
      ids(root, "kpi.revenue") shouldBe List(1, 2, 999) // run again, after the sentinel
      // run again after kpi.revenue: 10 * 3 + 20 * 3 + 1 * 3 (the sentinel)
      dashboardRev(root) shouldBe BigDecimal(93)
    }
  }

  it should "run nothing when only a load table changed" in {
    withProject { root =>
      (root / "metadata/load/raw/orders.sl.yml").overwrite(
        """version: 1
          |table:
          |  name: orders
          |  pattern: "orders.*.csv"
          |  attributes:
          |  - name: id
          |    type: long
          |  - name: amount
          |    type: double
          |  - name: status
          |    type: string
          |  metadata:
          |    format: DSV
          |    writeStrategy:
          |      type: APPEND
          |""".stripMargin
      )
      val (ok, out) = transformOut("--impacted", "--base", "HEAD")
      ok shouldBe true
      ids(root, "stg.orders") shouldBe List(1, 2, 999)
      ids(root, "kpi.revenue") shouldBe List(1, 2)
      val notRun = out.linesIterator.filter(_.startsWith("Not run: ")).toList
      notRun should contain("Not run: raw.orders: reload required")
      notRun should contain("Not run: stg.orders: blocked by reload of raw.orders")
    }
  }

  it should "run nothing without changes" in {
    withProject { root =>
      transform("--impacted", "--base", "HEAD") shouldBe true
      ids(root, "stg.orders") shouldBe List(1, 2, 999)
      ids(root, "kpi.revenue") shouldBe List(1, 2)
    }
  }

  it should "fail before running anything on an unknown ref" in {
    withProject { root =>
      (root / "metadata/transform/kpi/revenue.sql").overwrite(revenueTimesThree)
      transform("--impacted", "--base", "no-such-ref") shouldBe false
      ids(root, "kpi.revenue") shouldBe List(1, 2)
    }
  }

  it should "write the impacted tasks to the data branch only" in {
    withProject { root =>
      (root / "metadata/transform/kpi/revenue.sql").overwrite(revenueTimesThree)
      withEnvs("SL_DATA_BRANCH" -> "br_impacted") {
        transform("--impacted", "--base", "HEAD") shouldBe true
      }
      // production untouched
      ids(root, "kpi.revenue") shouldBe List(1, 2)
      // the branch read the production stg.orders, sentinel included
      ids(root, "kpi.revenue", Some("br_impacted")) shouldBe List(1, 2, 999)
    }
  }
}
