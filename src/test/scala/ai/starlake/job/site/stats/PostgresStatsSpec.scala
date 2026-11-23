package ai.starlake.job.site.stats

import ai.starlake.TestHelper

import java.sql.DriverManager
import scala.util.Using

class PostgresStatsSpec extends TestHelper {
  "JdbcStatsCollector on PostgreSQL" should "read reltuples, sizes and columns" in {
    new WithSettings() {
      Using.resource(DriverManager.getConnection(pgContainer.jdbcUrl, "test", "test")) { c =>
        Using.resource(c.createStatement()) { st =>
          st.execute("DROP SCHEMA IF EXISTS site_stats CASCADE")
          st.execute("CREATE SCHEMA site_stats")
          st.execute(
            "CREATE TABLE site_stats.t1 (id BIGINT NOT NULL, amount NUMERIC(18,2), name VARCHAR(20))"
          )
          st.execute("INSERT INTO site_stats.t1 VALUES (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')")
          st.execute("ANALYZE site_stats.t1")
          st.execute("CREATE VIEW site_stats.v1 AS SELECT id FROM site_stats.t1")
          st.execute(
            "CREATE MATERIALIZED VIEW site_stats.mv1 AS SELECT id, amount FROM site_stats.t1"
          )
          st.execute("ANALYZE site_stats.mv1")
          st.execute(
            "CREATE TABLE site_stats.p1 (id BIGINT NOT NULL, day DATE) PARTITION BY RANGE (day)"
          )
          st.execute(
            "CREATE TABLE site_stats.p1_2026 PARTITION OF site_stats.p1 " +
            "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
          )
          st.execute("INSERT INTO site_stats.p1 VALUES (1, '2026-03-01'), (2, '2026-04-01')")
          st.execute("ANALYZE site_stats.p1")
        }
      }
      val conn = settings.appConfig.connections("test-pg")
      val t1 = PhysicalTable(None, "site_stats", "t1")
      val v1 = PhysicalTable(None, "site_stats", "v1")
      val mv1 = PhysicalTable(None, "site_stats", "mv1")
      val p1 = PhysicalTable(None, "site_stats", "p1")
      val missing = PhysicalTable(None, "site_stats", "missing")

      val res = new JdbcStatsCollector(JdbcDialectStats.Postgres)
        .collect("test-pg", conn, List(t1, v1, mv1, p1, missing))
        .tables

      res(t1).exists shouldBe true
      res(t1).tableType shouldBe Some("TABLE")
      res(t1).rowCount shouldBe Some(3L)
      res(t1).rowCountEstimated shouldBe true
      res(t1).bytes.exists(_ > 0) shouldBe true
      res(t1).lastModified shouldBe None
      res(t1).columns shouldBe List(
        ColumnStat("id", "bigint", nullable = false),
        ColumnStat("amount", "numeric(18,2)", nullable = true),
        ColumnStat("name", "character varying(20)", nullable = true)
      )
      res(v1).tableType shouldBe Some("VIEW")
      res(v1).rowCount shouldBe None

      // information_schema does not list materialized views: they come from pg_matviews and
      // pg_attribute, and InfoSchemaMapping.tableType folds 'MATERIALIZED VIEW' into VIEW
      res(mv1).exists shouldBe true
      res(mv1).tableType shouldBe Some("VIEW")
      res(mv1).rowCount shouldBe Some(3L)
      res(mv1).bytes.exists(_ > 0) shouldBe true
      res(mv1).columns shouldBe List(
        ColumnStat("id", "bigint", nullable = true),
        ColumnStat("amount", "numeric(18,2)", nullable = true)
      )

      // partitioned parent: exists, no size of its own (partitions hold the data)
      res(p1).exists shouldBe true
      res(p1).tableType shouldBe Some("TABLE")
      res(p1).bytes shouldBe None
      res(p1).columns shouldBe List(
        ColumnStat("id", "bigint", nullable = false),
        ColumnStat("day", "date", nullable = true)
      )

      res(missing).exists shouldBe false
    }
  }
}
