package ai.starlake.job.site.stats

import ai.starlake.TestHelper
import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
import better.files.File

import java.sql.DriverManager
import scala.util.Using

class DuckDbStatsSpec extends TestHelper {
  "JdbcStatsCollector on DuckDB" should "read tables, views, estimates and columns" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("sl-site-duckdb-")
      val url = s"jdbc:duckdb:${(dir / "stats.db").pathAsString}"
      Using.resource(DriverManager.getConnection(url)) { c =>
        Using.resource(c.createStatement()) { st =>
          st.execute("CREATE SCHEMA sales")
          st.execute(
            "CREATE TABLE sales.orders (id BIGINT NOT NULL, amount DECIMAL(18,2), note VARCHAR)"
          )
          st.execute("INSERT INTO sales.orders VALUES (1, 10.5, 'a'), (2, 3.25, 'b'), (3, 1, NULL)")
          st.execute("CREATE VIEW sales.v_orders AS SELECT id FROM sales.orders")
        }
      }
      val conn = ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> url, "driver" -> "org.duckdb.DuckDBDriver")
      )
      val orders = PhysicalTable(None, "SALES", "Orders")
      val view = PhysicalTable(None, "sales", "v_orders")
      val missing = PhysicalTable(None, "sales", "nope")

      val res = new JdbcStatsCollector(JdbcDialectStats.DuckDb)
        .collect("duckdb", conn, List(orders, view, missing))
        .tables

      res(orders).exists shouldBe true
      res(orders).tableType shouldBe Some("TABLE")
      res(orders).rowCountEstimated shouldBe true
      res(orders).rowCount.exists(_ >= 0) shouldBe true
      res(orders).columns shouldBe List(
        ColumnStat("id", "BIGINT", nullable = false),
        ColumnStat("amount", "DECIMAL(18,2)", nullable = true),
        ColumnStat("note", "VARCHAR", nullable = true)
      )
      res(view).tableType shouldBe Some("VIEW")
      res(missing) shouldBe RawTableStats.Missing
      dir.delete(swallowIOExceptions = true)
    }
  }

  "GenericJdbcStatsCollector on DuckDB" should "read columns from DatabaseMetaData only" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("sl-site-duckdb-generic-")
      val url = s"jdbc:duckdb:${(dir / "stats.db").pathAsString}"
      Using.resource(DriverManager.getConnection(url)) { c =>
        Using.resource(c.createStatement()) { st =>
          st.execute("CREATE SCHEMA sales")
          st.execute("CREATE TABLE sales.orders (id BIGINT NOT NULL, amount DECIMAL(18,2))")
          st.execute("INSERT INTO sales.orders VALUES (1, 10.5)")
          st.execute("CREATE VIEW sales.v_orders AS SELECT id FROM sales.orders")
          st.execute("CREATE SCHEMA salesxstats")
          st.execute("CREATE TABLE salesxstats.orders (other INT)")
        }
      }
      val conn = ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> url, "driver" -> "org.duckdb.DuckDBDriver")
      )
      val orders = PhysicalTable(None, "sales", "ORDERS")
      val view = PhysicalTable(None, "sales", "v_orders")
      val missing = PhysicalTable(None, "sales", "nope")
      val noSchema = PhysicalTable(None, "sales_stats", "orders")

      val collector = new GenericJdbcStatsCollector("duckdb")
      collector.connectionStatus shouldBe "unsupported"
      val res = collector.collect("duckdb", conn, List(orders, view, missing, noSchema)).tables

      res(orders).exists shouldBe true
      res(orders).tableType shouldBe Some("TABLE")
      res(orders).rowCount shouldBe None
      res(orders).rowCountEstimated shouldBe false
      res(orders).bytes shouldBe None
      res(orders).columns shouldBe List(
        ColumnStat("id", "BIGINT", nullable = false),
        ColumnStat("amount", "DECIMAL(18,2)", nullable = true)
      )
      res(view).tableType shouldBe Some("VIEW")
      res(missing) shouldBe RawTableStats.Missing
      // "_" is a LIKE wildcard in DatabaseMetaData patterns: it must not match salesxstats
      res(noSchema) shouldBe RawTableStats.Missing
      dir.delete(swallowIOExceptions = true)
    }
  }

  "JdbcStatsCollector on DuckDB" should "isolate a failing schema and a failing stats query" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("sl-site-duckdb-isolation-")
      val url = s"jdbc:duckdb:${(dir / "stats.db").pathAsString}"
      Using.resource(DriverManager.getConnection(url)) { c =>
        Using.resource(c.createStatement()) { st =>
          st.execute("CREATE SCHEMA sales")
          st.execute("CREATE TABLE sales.orders (id BIGINT NOT NULL, amount DECIMAL(18,2))")
          st.execute("CREATE SCHEMA hr")
          st.execute("CREATE TABLE hr.staff (name VARCHAR)")
        }
      }
      val conn = ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> url, "driver" -> "org.duckdb.DuckDBDriver")
      )
      // real DuckDB SQL, except: every query on schema "broken" and the stats query on "hr" hit a
      // table function that does not exist, so DuckDB itself raises the errors
      val failing = new JdbcDialectStats {
        private val d = JdbcDialectStats.DuckDb
        private val bad = "SELECT * FROM no_such_catalog_function()"
        val engine = d.engine
        val rowCountEstimated = d.rowCountEstimated
        def tablesSql(db: Option[String], schema: String) =
          if (schema == "broken") bad else d.tablesSql(db, schema)
        def statsSql(db: Option[String], schema: String) =
          if (schema == "hr") Some(bad) else d.statsSql(db, schema)
        def columnsSql(db: Option[String], schema: String) = d.columnsSql(db, schema)
      }
      val orders = PhysicalTable(None, "sales", "orders")
      val staff = PhysicalTable(None, "hr", "staff")
      val broken = PhysicalTable(None, "broken", "t")
      val ghost = PhysicalTable(None, "ghost", "t") // schema does not exist: Missing, no failure

      val res = new JdbcStatsCollector(failing)
        .collect("duckdb", conn, List(orders, staff, broken, ghost))

      res.tables(orders).exists shouldBe true
      res.tables(orders).rowCount.isDefined shouldBe true
      res.tables(staff).exists shouldBe true
      res.tables(staff).rowCount shouldBe None
      res.tables(staff).columns shouldBe List(ColumnStat("name", "VARCHAR", nullable = true))
      res.tables(ghost) shouldBe RawTableStats.Missing
      res.tables.contains(broken) shouldBe false
      res.failures.size shouldBe 2
      res.failures.exists(f =>
        f.startsWith("broken: ") && f.contains("no_such_catalog_function")
      ) shouldBe true
      res.failures.exists(f =>
        f.startsWith("hr stats: ") && f.contains("no_such_catalog_function")
      ) shouldBe true
      ConnectionStatus.from("duckdb", res, 1L).status shouldBe "ok"
      dir.delete(swallowIOExceptions = true)
    }
  }
}
