package ai.starlake.job.site.stats

import ai.starlake.TestHelper
import ai.starlake.config.ConnectionInfo
import ai.starlake.job.site.{NodeId, SiteNode}
import ai.starlake.schema.model.ConnectionType

import scala.collection.immutable.TreeMap
import scala.concurrent.duration._

class SparkDeltaStatsSpec extends TestHelper {
  "SparkStatsCollector" should "read Delta details and catalog statistics" in {
    new WithSettings() {
      val spark = sparkSession
      spark.sql("CREATE DATABASE IF NOT EXISTS site_stats")
      spark.sql("DROP TABLE IF EXISTS site_stats.delta_t")
      spark.sql("CREATE TABLE site_stats.delta_t (id BIGINT, name STRING) USING delta")
      spark.sql("INSERT INTO site_stats.delta_t VALUES (1, 'a'), (2, 'b')")
      spark.sql("DROP TABLE IF EXISTS site_stats.parquet_t")
      spark.sql("CREATE TABLE site_stats.parquet_t (id BIGINT) USING parquet")
      spark.sql("INSERT INTO site_stats.parquet_t VALUES (1), (2), (3)")
      spark.sql("ANALYZE TABLE site_stats.parquet_t COMPUTE STATISTICS")

      val conn = ConnectionInfo(ConnectionType.FS)
      val delta = PhysicalTable(None, "site_stats", "delta_t")
      val parquet = PhysicalTable(None, "site_stats", "parquet_t")
      val missing = PhysicalTable(None, "site_stats", "nope")

      val res = new SparkStatsCollector(_ => spark)
        .collect("spark", conn, List(delta, parquet, missing))
        .tables

      res(delta).exists shouldBe true
      res(delta).format shouldBe Some("delta")
      res(delta).bytes.exists(_ > 0) shouldBe true
      res(delta).lastModified.isDefined shouldBe true
      res(delta).rowCount shouldBe None
      res(delta).columns.map(c => c.name -> c.`type`) shouldBe List(
        "id"   -> "bigint",
        "name" -> "string"
      )

      res(parquet).format shouldBe Some("parquet")
      res(parquet).rowCount shouldBe Some(3L)
      res(parquet).rowCountEstimated shouldBe true
      res(parquet).bytes.exists(_ > 0) shouldBe true

      res(missing) shouldBe RawTableStats.Missing
    }
  }

  it should "isolate a corrupt Delta table and a missing schema, keep full type spellings" in {
    new WithSettings() {
      val spark = sparkSession
      spark.sql("CREATE DATABASE IF NOT EXISTS site_iso")
      spark.sql("DROP TABLE IF EXISTS site_iso.good")
      spark.sql(
        "CREATE TABLE site_iso.good (id BIGINT NOT NULL, amount DECIMAL(18,2), code VARCHAR(10), " +
        "type STRING, provider STRING) USING delta"
      )
      spark.sql("INSERT INTO site_iso.good VALUES (1, 1.5, 'a', 'x', 'y')")
      spark.sql("DROP TABLE IF EXISTS site_iso.broken")
      spark.sql("CREATE TABLE site_iso.broken (id BIGINT) USING delta")
      spark.sql("INSERT INTO site_iso.broken VALUES (1)")
      val location =
        spark.sql("DESCRIBE DETAIL site_iso.broken").select("location").head().getString(0)
      better.files.File(new java.net.URI(location)).delete()
      org.apache.spark.sql.delta.DeltaLog.clearCache()

      val good = PhysicalTable(None, "site_iso", "good")
      val broken = PhysicalTable(None, "site_iso", "broken")
      val ghost = PhysicalTable(None, "site_ghost", "t")

      val result = new SparkStatsCollector(_ => spark)
        .collect("spark", ConnectionInfo(ConnectionType.FS), List(good, broken, ghost))

      result.tables(good).exists shouldBe true
      result.tables(good).tableType shouldBe Some("TABLE")
      result.tables(good).format shouldBe Some("delta")
      result.tables(good).columns shouldBe List(
        ColumnStat("id", "bigint", nullable = false),
        ColumnStat("amount", "decimal(18,2)", nullable = true),
        ColumnStat("code", "varchar(10)", nullable = true),
        ColumnStat("type", "string", nullable = true),
        ColumnStat("provider", "string", nullable = true)
      )
      result.tables.contains(broken) shouldBe false
      result.tables(ghost) shouldBe RawTableStats.Missing
      result.failures should have size 1
      result.failures.head should startWith("site_iso.broken: ")
      ConnectionStatus.from("spark", result, 1L).status shouldBe "ok"
    }
  }

  it should "never create the Spark session when there is nothing to collect" in {
    new WithSettings() {
      var created = false
      val result = new SparkStatsCollector(_ => { created = true; sparkSession })
        .collect("spark", ConnectionInfo(ConnectionType.FS), Nil)
      created shouldBe false
      result shouldBe CollectResult(Map.empty, Nil)
    }
  }

  it should "collect two FS connections concurrently through the runner" in {
    new WithSettings() {
      val spark = sparkSession
      spark.sql("CREATE DATABASE IF NOT EXISTS site_conc")
      List("a", "b").foreach { n =>
        spark.sql(s"DROP TABLE IF EXISTS site_conc.t_$n")
        spark.sql(s"CREATE TABLE site_conc.t_$n (id BIGINT) USING parquet")
      }
      val conn = ConnectionInfo(ConnectionType.FS)
      def node(n: String) =
        SiteNode(
          NodeId.load("site_conc", s"t_$n"),
          "load",
          s"t_$n",
          "site_conc",
          s"t_$n",
          connectionRef = Some(s"fs_$n")
        )
      val nodes = TreeMap(List("a", "b").map(node).map(n => n.id -> n): _*)
      val runner = new StatsRunner(
        _ => Some(conn),
        _ => Some(new SparkStatsCollector()),
        timeout = 120.seconds
      )
      val stats = runner.run(nodes)
      stats.connections.map { case (k, v) => k -> v.status } shouldBe
      Map("fs_a" -> "ok", "fs_b" -> "ok")
      stats.tables.values.forall(_.exists) shouldBe true
    }
  }
}
