package ai.starlake.job.site.stats

import ai.starlake.config.ConnectionInfo
import ai.starlake.job.site.{NodeId, SiteNode}
import ai.starlake.schema.model.{AllSinks, ConnectionType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PhysicalTablesSpec extends AnyFlatSpec with Matchers {
  private def node(
    kind: String,
    d: String,
    t: String,
    ref: Option[String],
    db: Option[String] = None
  ) =
    SiteNode(
      id = if (kind == "load") NodeId.load(d, t) else NodeId.transform(s"$d.$t"),
      kind = kind,
      name = t,
      domain = d,
      table = t,
      database = db,
      connectionRef = ref
    )

  "group" should "collect a shared physical table once and keep every node id" in {
    val nodes = List(
      node("load", "sales", "orders", Some("pg")),
      node("transform", "SALES", "Orders", Some("pg")),
      node("load", "sales", "customers", Some("pg")),
      node("load", "hr", "staff", Some("bq"), Some("my-project")),
      node("load", "nowhere", "x", None),
      SiteNode("external:a.b", "external", "b", "a", "b")
    )
    val groups = PhysicalTables.group(nodes)
    groups.keySet shouldBe Set("pg", "bq")
    groups("pg") shouldBe List(
      PhysicalTable(None, "sales", "customers") -> List("load:sales.customers"),
      PhysicalTable(None, "sales", "orders") -> List("load:sales.orders", "transform:sales.orders")
    )
    groups("bq") shouldBe List(
      PhysicalTable(Some("my-project"), "hr", "staff") -> List("load:hr.staff")
    )
  }

  it should "skip an undeclared external, collect a declared one like a load" in {
    val bare = SiteNode(
      "external:a.b",
      "external",
      "b",
      "a",
      "b",
      connectionRef = Some("pg"),
      undeclared = Some(true)
    )
    val declared =
      SiteNode("external:ref.fx", "external", "fx", "ref", "fx", connectionRef = Some("pg"))
    PhysicalTables.group(List(bare)) shouldBe empty
    PhysicalTables.group(List(bare, declared)) shouldBe Map(
      "pg" -> List(PhysicalTable(None, "ref", "fx") -> List("external:ref.fx"))
    )
  }

  it should "locate a declared external where a load of the same domain would live" in {
    val ext = SiteNode("external:ref.fx", "external", "fx", "ref", "fx")
    def at(ref: String, db: Option[String], defaultDb: Option[String]) =
      locate(ext.copy(connectionRef = Some(ref), database = db), defaultDb).map(_.table.database)
    at("duck", None, Some("lake")) shouldBe Some(Some("lake"))
    at("duck", Some("own"), Some("lake")) shouldBe Some(Some("own"))
    at("bq", None, Some("default-db")) shouldBe Some(Some("default-db"))
    at("bq", None, None) shouldBe Some(Some("conn-project"))
    at("spark", Some("catalog"), Some("lake")) shouldBe Some(None)
    locate(ext, Some("lake")) shouldBe None
  }

  it should "collect a transform on its sink connection, not its run connection" in {
    val t = node("transform", "kpi", "revenue", Some("run")).copy(
      sink = Some(AllSinks(connectionRef = Some("warehouse")))
    )
    val load = node("load", "sales", "orders", Some("pg")).copy(
      sink = Some(AllSinks(connectionRef = Some("pg")))
    )
    val groups = PhysicalTables.group(List(t, load))
    groups.keySet shouldBe Set("warehouse", "pg")
    groups("warehouse") shouldBe List(
      PhysicalTable(None, "kpi", "revenue") -> List("transform:kpi.revenue")
    )
    PhysicalTables.connectionRefOf(t) shouldBe Some("warehouse")
    PhysicalTables.connectionRefOf(node("transform", "kpi", "x", Some("run"))) shouldBe Some("run")
  }

  private val bq = ConnectionInfo(ConnectionType.BQ, options = Map("projectId" -> "conn-project"))
  private val spark = ConnectionInfo(ConnectionType.FS)
  private val duck =
    ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:", "driver" -> "d"))
  private val connections = Map("bq" -> bq, "spark" -> spark, "duck" -> duck)

  private def locate(n: SiteNode, defaultDb: Option[String]) =
    PhysicalTables.locate(n, connections.get, defaultDb)

  "locate" should "resolve the BigQuery project like core: database, SL_DATABASE, projectId" in {
    val withDb = node("load", "hr", "staff", Some("bq"), Some("own"))
    val noDb = node("load", "hr", "staff", Some("bq"))
    val task = node("transform", "kpi", "rev", Some("bq"))
    locate(withDb, Some("default-db")).map(_.table.database) shouldBe Some(Some("own"))
    locate(noDb, Some("default-db")).map(_.table.database) shouldBe Some(Some("default-db"))
    locate(noDb, None).map(_.table.database) shouldBe Some(Some("conn-project"))
    locate(task, Some("default-db")).map(_.table.database) shouldBe Some(Some("default-db"))
    locate(task, None).map(_.table.database) shouldBe Some(Some("conn-project"))
  }

  it should "drop the database on Spark, which writes domain.table" in {
    val n = node("load", "sales", "orders", Some("spark"), Some("catalog"))
    locate(n, Some("default-db")) shouldBe Some(
      PhysicalLocation("spark", PhysicalTable(None, "sales", "orders"))
    )
  }

  it should "use the default database for JDBC loads only, as the native loaders do" in {
    val load = node("load", "sales", "orders", Some("duck"))
    val task = node("transform", "kpi", "rev", Some("duck"))
    locate(load, Some("lake")).map(_.table.database) shouldBe Some(Some("lake"))
    locate(load.copy(database = Some("own")), Some("lake")).map(_.table.database) shouldBe
    Some(Some("own"))
    locate(task, Some("lake")).map(_.table.database) shouldBe Some(None)
    locate(task.copy(database = Some("own")), Some("lake")).map(_.table.database) shouldBe
    Some(Some("own"))
  }

  it should "resolve the database without the connection when it cannot be read" in {
    val n = node("load", "a", "b", Some("missing"), Some("db"))
    val unreadable: String => Option[ConnectionInfo] = _ => throw new IllegalStateException("boom")
    PhysicalTables.locate(n, unreadable, None) shouldBe Some(
      PhysicalLocation("missing", PhysicalTable(Some("db"), "a", "b"))
    )
    // a load without a declared database takes the default one, a transform none
    PhysicalTables.locate(n.copy(database = None), unreadable, Some("dflt")) shouldBe Some(
      PhysicalLocation("missing", PhysicalTable(Some("dflt"), "a", "b"))
    )
    PhysicalTables.locate(
      n.copy(kind = "transform", database = None),
      unreadable,
      Some("dflt")
    ) shouldBe Some(PhysicalLocation("missing", PhysicalTable(None, "a", "b")))
    PhysicalTables.locate(n.copy(connectionRef = None), connections.get, None) shouldBe None
    PhysicalTables.locate(
      n.copy(kind = "external", undeclared = Some(true)),
      connections.get,
      None
    ) shouldBe None
    PhysicalTables.locate(
      n.copy(kind = "external", database = None),
      unreadable,
      Some("dflt")
    ) shouldBe Some(PhysicalLocation("missing", PhysicalTable(Some("dflt"), "a", "b")))
  }

  it should "group with the resolved databases" in {
    val nodes = List(
      node("load", "sales", "orders", Some("spark"), Some("c1")),
      node("transform", "sales", "orders", Some("spark"))
    )
    PhysicalTables.group(nodes, connections.get, None) shouldBe Map(
      "spark" -> List(
        PhysicalTable(None, "sales", "orders") -> List(
          "load:sales.orders",
          "transform:sales.orders"
        )
      )
    )
  }
}
