package ai.starlake.job.site.stats

import ai.starlake.TestHelper
import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.job.site.{NodeId, SiteColumn, SiteNode}
import ai.starlake.schema.model.ConnectionType

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.TreeMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class StatsRunnerSpec extends TestHelper {
  private val jdbc =
    ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:", "driver" -> "x"))
  private val kafka = ConnectionInfo(ConnectionType.KAFKA)

  private def node(
    id: String,
    kind: String,
    d: String,
    t: String,
    ref: String,
    cols: List[SiteColumn] = Nil
  ) =
    SiteNode(id, kind, t, d, t, connectionRef = Some(ref), columns = cols)

  private def treeOf(ns: SiteNode*) = TreeMap(ns.map(n => n.id -> n): _*)

  private val nodes = treeOf(
    node(
      NodeId.load("sales", "orders"),
      "load",
      "sales",
      "orders",
      "ok",
      List(SiteColumn("id", sqlType = Some("BIGINT")))
    ),
    node(NodeId.transform("sales.orders"), "transform", "sales", "orders", "ok"),
    node(NodeId.load("sales", "gone"), "load", "sales", "gone", "ok"),
    node(NodeId.load("hr", "staff"), "load", "hr", "staff", "boom"),
    node(NodeId.load("hr", "slow"), "load", "hr", "slow", "slow"),
    node(NodeId.load("ev", "clicks"), "load", "ev", "clicks", "kafka"),
    node(NodeId.load("x", "y"), "load", "x", "y", "unknown"),
    node(NodeId.load("g", "t"), "load", "g", "t", "generic")
  )

  private val calls = new AtomicInteger(0)
  private val requested = new ConcurrentHashMap[String, List[PhysicalTable]]()

  private class Fake(
    behavior: List[PhysicalTable] => Map[PhysicalTable, RawTableStats],
    status: String = "ok",
    failures: List[String] = Nil
  ) extends StatsCollector {
    val engine = "duckdb"
    override val connectionStatus: String = status
    def collect(ref: String, c: ConnectionInfo, tables: List[PhysicalTable])(implicit
      settings: Settings
    ): CollectResult = {
      calls.incrementAndGet()
      requested.put(ref, tables)
      CollectResult(behavior(tables), failures)
    }
  }

  private val found = RawTableStats(
    true,
    Some("TABLE"),
    None,
    Some(5L),
    false,
    Some(10L),
    None,
    List(
      ColumnStat("id", "BIGINT", nullable = false),
      ColumnStat("extra", "VARCHAR", nullable = true)
    )
  )

  private def collectorFor(
    refOf: ConnectionInfo => String
  )(c: ConnectionInfo): Option[StatsCollector] =
    refOf(c) match {
      case "ok" =>
        Some(
          new Fake(ts =>
            ts.map(t => t -> (if (t.table == "orders") found else RawTableStats.Missing)).toMap
          )
        )
      case "boom"    => Some(new Fake(_ => throw new IllegalStateException("access denied")))
      case "slow"    => Some(new Fake(_ => { Thread.sleep(5000); Map.empty }))
      case "generic" => Some(new Fake(ts => ts.map(_ -> found).toMap, status = "unsupported"))
      case _         => None
    }

  private def refOption(c: ConnectionInfo) = c.options.getOrElse("ref", "kafka")

  private def withRef(ref: String) = jdbc.copy(options = jdbc.options + ("ref" -> ref))

  "StatsRunner" should "isolate failures, time out, and fan results out to every node" in {
    new WithSettings() {
      val byRef = Map(
        "ok"      -> withRef("ok"),
        "boom"    -> withRef("boom"),
        "slow"    -> withRef("slow"),
        "generic" -> withRef("generic"),
        "kafka"   -> kafka
      )
      val runner = new StatsRunner(byRef.get, collectorFor(refOption), timeout = 300.millis)
      val stats = runner.run(nodes)

      stats.schemaVersion shouldBe 1
      stats.connections("ok").status shouldBe "ok"
      stats.connections("ok").engine shouldBe "duckdb"
      stats.connections("ok").error shouldBe None
      stats.connections("boom").status shouldBe "error"
      stats.connections("boom").error.get should include("access denied")
      stats.connections("slow").status shouldBe "error"
      stats.connections("slow").error.get should include("timed out")
      stats.connections("kafka").status shouldBe "unsupported"
      stats.connections("unknown").status shouldBe "error"
      stats.connections("unknown").error shouldBe Some("Connection not found: unknown")
      stats.connections("generic").status shouldBe "unsupported"
      stats.connections.keys.toList shouldBe List(
        "boom",
        "generic",
        "kafka",
        "ok",
        "slow",
        "unknown"
      )

      // one physical table shared by two nodes, collected once by the "ok" collector call
      requested.get("ok") shouldBe List(
        PhysicalTable(None, "sales", "gone"),
        PhysicalTable(None, "sales", "orders")
      )
      calls.get() shouldBe 4 // ok, boom, slow, generic; kafka and unknown never reach a collector
      stats.tables("load:sales.orders").rowCount shouldBe Some(5L)
      stats.tables("transform:sales.orders").rowCount shouldBe Some(5L)
      stats.tables("load:sales.orders").connectionRef shouldBe "ok"
      stats.tables("load:sales.orders").columnDiff shouldBe List(
        ColumnDiff("id", Some("BIGINT"), Some("BIGINT"), "match"),
        ColumnDiff("extra", None, Some("VARCHAR"), "extraInWarehouse")
      )
      stats.tables("transform:sales.orders").columnDiff shouldBe Nil
      stats.tables("load:sales.gone").exists shouldBe false
      stats.tables("load:sales.gone").columnDiff shouldBe Nil
      stats.tables("load:g.t").exists shouldBe true

      stats.tables.contains("load:hr.staff") shouldBe false
      stats.tables.contains("load:hr.slow") shouldBe false
      stats.tables.contains("load:ev.clicks") shouldBe false
      stats.tables.keys.toList shouldBe stats.tables.keys.toList.sorted
    }
  }

  it should "abandon a slow connection after the timeout and keep the others" in {
    new WithSettings() {
      val ns = treeOf(
        node(NodeId.load("hr", "slow"), "load", "hr", "slow", "slow"),
        node(NodeId.load("sales", "orders"), "load", "sales", "orders", "ok")
      )
      val runner = new StatsRunner(
        Map("slow" -> withRef("slow"), "ok" -> withRef("ok")).get,
        collectorFor(refOption),
        timeout = 200.millis
      )
      val start = System.nanoTime()
      val stats = runner.run(ns)
      (System.nanoTime() - start).nanos should be < 3.seconds
      stats.connections("slow") shouldBe ConnectionStats(
        "duckdb",
        "error",
        Some("timed out after 0.2s"),
        stats.connections("slow").durationMs
      )
      stats.connections("slow").durationMs should be >= 200L
      stats.connections("ok").status shouldBe "ok"
      stats.tables("load:sales.orders").exists shouldBe true
      stats.tables.contains("load:hr.slow") shouldBe false
    }
  }

  it should "leave the stats of other connections intact when one connection throws" in {
    new WithSettings() {
      val ns = treeOf(
        node(NodeId.load("hr", "staff"), "load", "hr", "staff", "boom"),
        node(NodeId.load("sales", "orders"), "load", "sales", "orders", "ok")
      )
      val runner = new StatsRunner(
        Map("boom" -> withRef("boom"), "ok" -> withRef("ok")).get,
        collectorFor(refOption),
        timeout = 10.seconds
      )
      val stats = runner.run(ns)
      stats.connections("boom").status shouldBe "error"
      stats.connections("boom").error shouldBe Some("access denied")
      stats.connections("ok").status shouldBe "ok"
      stats.tables.keySet shouldBe Set("load:sales.orders")
      stats.tables("load:sales.orders").rowCount shouldBe Some(5L)
    }
  }

  it should "collect a transform on its sink connection and report that connection" in {
    new WithSettings() {
      val t = node(NodeId.transform("sales.orders"), "transform", "sales", "orders", "run")
        .copy(sink = Some(ai.starlake.schema.model.AllSinks(connectionRef = Some("ok"))))
      val stats = new StatsRunner(
        Map("run" -> withRef("boom"), "ok" -> withRef("ok")).get,
        collectorFor(refOption),
        timeout = 10.seconds
      ).run(treeOf(t))
      stats.connections.keySet shouldBe Set("ok")
      stats.tables("transform:sales.orders").connectionRef shouldBe "ok"
      stats.tables("transform:sales.orders").exists shouldBe true
    }
  }

  it should "report error when the connection lookup or collector selection throws" in {
    new WithSettings() {
      val ns = treeOf(
        node(NodeId.load("a", "t"), "load", "a", "t", "badLookup"),
        node(NodeId.load("b", "t"), "load", "b", "t", "badCollector"),
        node(NodeId.load("sales", "orders"), "load", "sales", "orders", "ok")
      )
      val runner = new StatsRunner(
        {
          case "badLookup" => throw new IllegalStateException("cannot read /etc/app/conf.yml")
          case ref         => Some(withRef(ref))
        },
        c =>
          if (refOption(c) == "badCollector") throw new IllegalArgumentException("no engine")
          else collectorFor(refOption)(c),
        timeout = 10.seconds
      )
      val stats = runner.run(ns)
      stats.connections("badLookup").status shouldBe "error"
      stats.connections("badLookup").error shouldBe Some("cannot read <path>")
      stats.connections("badCollector").status shouldBe "error"
      stats.connections("badCollector").error shouldBe Some("no engine")
      stats.connections("ok").status shouldBe "ok"
      stats.tables.keySet shouldBe Set("load:sales.orders")
    }
  }

  it should "report an unsupported connection type without collecting" in {
    new WithSettings() {
      val ns = treeOf(node(NodeId.load("ev", "clicks"), "load", "ev", "clicks", "kafka"))
      val before = calls.get()
      val stats = new StatsRunner(Map("kafka" -> kafka).get, StatsRunner.collectorFor, 10.seconds)
        .run(ns)
      stats.connections("kafka").status shouldBe "unsupported"
      stats.connections("kafka").error.get should include("KAFKA")
      stats.tables shouldBe empty
      calls.get() shouldBe before
    }
  }

  it should "sanitize collector failures and connection errors" in {
    new WithSettings() {
      val ns = treeOf(
        node(NodeId.load("sales", "orders"), "load", "sales", "orders", "partial"),
        node(NodeId.load("hr", "t"), "load", "hr", "t", "crash")
      )
      val collectors: ConnectionInfo => Option[StatsCollector] = c =>
        refOption(c) match {
          case "partial" =>
            Some(
              new Fake(
                ts => ts.map(_ -> found).toMap,
                failures = List("hr: corrupt log at /home/jane/w/_delta_log\n== Plan ==\nScan")
              )
            )
          case _ =>
            Some(
              new Fake(_ =>
                throw new RuntimeException("open file:/Users/jane/x.db failed\n" + ("z" * 400))
              )
            )
        }
      val stats = new StatsRunner(
        Map("partial" -> withRef("partial"), "crash" -> withRef("crash")).get,
        collectors,
        10.seconds
      ).run(ns)
      stats.connections("partial").status shouldBe "ok"
      stats.connections("partial").error shouldBe Some("hr: corrupt log at <path>")
      stats.connections("crash").error shouldBe Some("open <path> failed")
    }
  }

  it should "skip externals and nodes without a connection, and always shut the pool down" in {
    new WithSettings() {
      val ns = treeOf(
        SiteNode(NodeId.external("ref.fx"), "external", "fx", "ref", "fx"),
        SiteNode(NodeId.load("a", "b"), "load", "b", "a", "b"),
        node(NodeId.load("hr", "slow"), "load", "hr", "slow", "slow")
      )
      val stats = new StatsRunner(
        Map("slow" -> withRef("slow")).get,
        collectorFor(refOption),
        timeout = 100.millis
      ).run(ns)
      stats.connections.keySet shouldBe Set("slow")
      stats.tables shouldBe empty
      def statsThreads =
        Thread.getAllStackTraces.keySet.asScala.filter(t =>
          t.getName.startsWith("sl-stats") && t.isAlive
        )
      val deadline = System.nanoTime() + 3.seconds.toNanos
      while (statsThreads.nonEmpty && System.nanoTime() < deadline) Thread.sleep(20)
      statsThreads shouldBe empty
    }
  }

  "collectorFor" should "pick collectors from the connection type" in {
    StatsRunner.collectorFor(ConnectionInfo(ConnectionType.BQ)).map(_.engine) shouldBe Some(
      "bigquery"
    )
    StatsRunner.collectorFor(ConnectionInfo(ConnectionType.FS)).map(_.engine) shouldBe Some(
      "spark"
    )
    StatsRunner
      .collectorFor(
        ConnectionInfo(
          ConnectionType.JDBC,
          options = Map("url" -> "jdbc:postgresql://h/db", "driver" -> "d")
        )
      )
      .map(_.engine) shouldBe Some("postgresql")
    StatsRunner
      .collectorFor(
        ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:", "driver" -> "d"))
      )
      .map(c => (c.engine, c.connectionStatus)) shouldBe Some(("duckdb", "ok"))
    val generic = StatsRunner
      .collectorFor(
        ConnectionInfo(
          ConnectionType.JDBC,
          options = Map("url" -> "jdbc:mysql://h/db", "driver" -> "d")
        )
      )
      .get
    generic.engine shouldBe "mysql"
    generic.connectionStatus shouldBe "unsupported"
    StatsRunner.collectorFor(ConnectionInfo(ConnectionType.KAFKA)) shouldBe None
    StatsRunner.collectorFor(
      ConnectionInfo(ConnectionType.JDBC, options = Map("sfUrl" -> "x.snowflakecomputing.com"))
    ) shouldBe None
  }
}
