package ai.starlake.job.site.stats

import Rows.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StatsIsolationSpec extends AnyFlatSpec with Matchers {
  private val orders = PhysicalTable(None, "sales", "orders")
  private val staff = PhysicalTable(None, "hr", "staff")
  private val found =
    RawTableStats(true, Some("TABLE"), None, Some(5L), true, None, None, Nil)

  "StatsCollector.isolate" should "keep the tables of healthy groups when one group throws" in {
    val groups = StatsCollector.bySchema(List(orders, staff))
    val res = StatsCollector.isolate(groups) { g =>
      if (g.schema == "hr") throw new IllegalStateException("permission denied for schema hr")
      GroupResult(g.tables.map(_ -> found).toMap)
    }
    res.tables shouldBe Map(orders -> found)
    res.failures shouldBe List("hr: permission denied for schema hr")
    res.allFailed shouldBe false
  }

  it should "keep the non-fatal failures a group reports and qualify labels with the database" in {
    val t = PhysicalTable(Some("Proj"), "Sales", "orders")
    val res = StatsCollector.isolate(StatsCollector.bySchema(List(t))) { g =>
      GroupResult(Map(t -> found), List(s"${g.label} stats: boom"))
    }
    res.tables shouldBe Map(t -> found)
    res.failures shouldBe List("Proj.Sales stats: boom")
  }

  it should "report every group failing" in {
    val res = StatsCollector.isolate(StatsCollector.bySchema(List(orders, staff))) { g =>
      throw new RuntimeException(null: String)
    }
    res.tables shouldBe empty
    res.failures.sorted shouldBe List(
      "hr: java.lang.RuntimeException",
      "sales: java.lang.RuntimeException"
    )
    res.allFailed shouldBe true
  }

  private val tablesRows: List[Row] = List(
    Map("table_name" -> "orders", "table_type" -> "BASE TABLE")
  )
  private val columnRows: List[Row] = List(
    Map(
      "table_name"  -> "orders",
      "column_name" -> "id",
      "data_type"   -> "BIGINT",
      "is_nullable" -> "NO"
    )
  )

  /** A fake JDBC backend: answers the catalog queries, fails the stats query. */
  private def fakeQuery(failStats: Boolean)(sql: String): List[Row] =
    if (sql.contains("duckdb_tables()")) {
      if (failStats) throw new java.sql.SQLException("Catalog Error: duckdb_tables denied")
      List(Map("table_name" -> "orders", "row_count" -> 42L))
    } else if (sql.contains("information_schema.columns")) columnRows
    else if (sql.contains("information_schema.tables")) tablesRows
    else sys.error(s"unexpected $sql")

  "JdbcStatsCollector.collectGroup" should "degrade a failing stats query to no counts" in {
    val group = StatsCollector.bySchema(List(orders)).head
    val res = JdbcStatsCollector.collectGroup(
      JdbcDialectStats.DuckDb,
      fakeQuery(failStats = true),
      group,
      None
    )
    res.tables(orders).exists shouldBe true
    res.tables(orders).rowCount shouldBe None
    res.tables(orders).rowCountEstimated shouldBe false
    res.tables(orders).columns shouldBe List(ColumnStat("id", "BIGINT", nullable = false))
    res.failures shouldBe List("sales stats: Catalog Error: duckdb_tables denied")
  }

  it should "use the stats query when it succeeds" in {
    val group = StatsCollector.bySchema(List(orders)).head
    val res = JdbcStatsCollector.collectGroup(
      JdbcDialectStats.DuckDb,
      fakeQuery(failStats = false),
      group,
      None
    )
    res.tables(orders).rowCount shouldBe Some(42L)
    res.failures shouldBe Nil
  }

  "BigQueryStatsCollector.collectDataset" should "fall back to the API when the SQL fails" in {
    val t = PhysicalTable(Some("p"), "ds", "orders")
    val group = StatsCollector.bySchema(List(t), caseSensitive = true).head
    val res = BigQueryStatsCollector.collectDataset(
      "p",
      group,
      _ => throw new RuntimeException("Access Denied: __TABLES__"),
      _ => found
    )
    res shouldBe GroupResult(
      Map(t -> found),
      List("p.ds: used the API (__TABLES__ failed: Access Denied: __TABLES__)")
    )
  }

  it should "leave out only the tables the API fails on" in {
    val ok = PhysicalTable(Some("p"), "ds", "orders")
    val ko = PhysicalTable(Some("p"), "ds", "locked")
    val group = StatsCollector.bySchema(List(ok, ko), caseSensitive = true).head
    val res = BigQueryStatsCollector.collectDataset(
      "p",
      group,
      _ => throw new RuntimeException("Access Denied: __TABLES__"),
      name =>
        if (name == "locked") throw new RuntimeException("Access Denied: Table p:ds.locked")
        else found
    )
    res.tables shouldBe Map(ok -> found)
    res.failures shouldBe List(
      "p.ds: used the API (__TABLES__ failed: Access Denied: __TABLES__)",
      "p.ds.locked: Access Denied: Table p:ds.locked"
    )
  }

  it should "report every table when both the SQL and the API fail" in {
    val ok = PhysicalTable(Some("p"), "ds", "orders")
    val ko = PhysicalTable(Some("p"), "secret", "orders")
    val res = StatsCollector.isolate(StatsCollector.bySchema(List(ok, ko), caseSensitive = true)) {
      g =>
        BigQueryStatsCollector.collectDataset(
          "p",
          g,
          _ => throw new RuntimeException("Access Denied: __TABLES__"),
          name =>
            if (g.schema == "secret") throw new RuntimeException("Access Denied: Dataset p:secret")
            else found
        )
    }
    res.tables shouldBe Map(ok -> found)
    res.failures shouldBe List(
      "p.ds: used the API (__TABLES__ failed: Access Denied: __TABLES__)",
      "p.secret: used the API (__TABLES__ failed: Access Denied: __TABLES__)",
      "p.secret.orders: Access Denied: Dataset p:secret"
    )
    res.allFailed shouldBe false
    ConnectionStatus.from("bigquery", res, 1L).status shouldBe "ok"
  }

  it should "make the connection an error when the API fails on every table" in {
    val t = PhysicalTable(Some("p"), "ds", "orders")
    val res = StatsCollector.isolate(StatsCollector.bySchema(List(t), caseSensitive = true)) { g =>
      BigQueryStatsCollector.collectDataset(
        "p",
        g,
        _ => throw new RuntimeException("Access Denied: __TABLES__"),
        _ => throw new RuntimeException("Access Denied: Table p:ds.orders")
      )
    }
    res.tables shouldBe empty
    ConnectionStatus.from("bigquery", res, 1L).status shouldBe "error"
  }

  it should "fail its group visibly, without the API fallback, on an unsafe dataset name" in {
    val t = PhysicalTable(Some("p"), "bad`ds", "orders")
    var apiCalls = 0
    val res = StatsCollector.isolate(StatsCollector.bySchema(List(t), caseSensitive = true)) { g =>
      BigQueryStatsCollector.collectDataset(
        "p",
        g,
        _ => Nil,
        _ => { apiCalls += 1; found }
      )
    }
    apiCalls shouldBe 0
    res.tables shouldBe Map.empty
    res.failures.size shouldBe 1
    res.failures.head should include("Unsafe or empty BigQuery dataset name")
  }

  "ConnectionStatus.from" should "be ok with no error when nothing failed" in {
    ConnectionStatus.from("duckdb", CollectResult(Map(orders -> found), Nil), 12L) shouldBe
    ConnectionStats("duckdb", "ok", None, 12L)
  }

  it should "be ok with the first failure and a count of the others when some groups failed" in {
    val r = CollectResult(Map(orders -> found), List("hr: denied", "sales stats: no svv"))
    ConnectionStatus.from("redshift", r, 7L) shouldBe
    ConnectionStats("redshift", "ok", Some("hr: denied (+1 more)"), 7L)
  }

  it should "keep the collector success status (unsupported) on partial results" in {
    val r = CollectResult(Map(orders -> found), List("hr: denied"))
    ConnectionStatus.from("mysql", r, 3L, successStatus = "unsupported") shouldBe
    ConnectionStats("mysql", "unsupported", Some("hr: denied"), 3L)
  }

  it should "be error when every group failed" in {
    val r = CollectResult(Map.empty, List("hr: denied", "sales: denied"))
    ConnectionStatus.from("postgresql", r, 5L) shouldBe
    ConnectionStats("postgresql", "error", Some("hr: denied (+1 more)"), 5L)
    ConnectionStatus.from("mysql", r, 5L, successStatus = "unsupported").status shouldBe "error"
  }

  it should "be ok when there was nothing to collect" in {
    ConnectionStatus.from("duckdb", CollectResult(Map.empty, Nil), 0L) shouldBe
    ConnectionStats("duckdb", "ok", None, 0L)
  }
}
