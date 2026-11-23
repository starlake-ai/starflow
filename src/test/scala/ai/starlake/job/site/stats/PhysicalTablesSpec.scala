package ai.starlake.job.site.stats

import ai.starlake.job.site.{NodeId, SiteNode}
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

  it should "skip an external node even when it carries a connection" in {
    val ext = SiteNode("external:a.b", "external", "b", "a", "b", connectionRef = Some("pg"))
    PhysicalTables.group(List(ext)) shouldBe empty
  }
}
