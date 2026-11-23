package ai.starlake.job.site

import ai.starlake.lineage.ColLineage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class LineageBuilderSpec extends AnyFlatSpec with Matchers {
  private def load(d: String, t: String, cols: List[SiteColumn] = Nil) =
    SiteNode(NodeId.load(d, t), "load", t, d, t, columns = cols)
  private def task(d: String, t: String) =
    SiteNode(NodeId.transform(s"$d.$t"), "transform", t, d, t)

  private val customers = load("sales", "customers", List(SiteColumn("id")))
  private val orders =
    load("sales", "orders", List(SiteColumn("customer_id", foreignKey = Some("customers.id"))))
  private val revenue = task("kpi", "revenue")
  private val nodes = TreeMap(Seq(customers, orders, revenue).map(n => n.id -> n): _*)

  "build" should "link producers to tasks and create external nodes" in {
    val res = LineageBuilder.build(
      nodes,
      Map(revenue.id -> List("sales.orders", "db.sales.customers", "refdata.fx_rates", "kpi.revenue")),
      Map.empty
    )
    res.edges should contain allOf (
      SiteEdge("load:sales.orders", "transform:kpi.revenue", "lineage"),
      SiteEdge("load:sales.customers", "transform:kpi.revenue", "lineage"),
      SiteEdge("external:refdata.fx_rates", "transform:kpi.revenue", "lineage")
    )
    res.edges.exists(e => e.from == e.to) shouldBe false
    res.nodes("external:refdata.fx_rates").kind shouldBe "external"
    res.nodes("external:refdata.fx_rates").domain shouldBe "refdata"
  }

  it should "resolve single-part references by table name" in {
    val res = LineageBuilder.build(nodes, Map(revenue.id -> List("ORDERS")), Map.empty)
    res.edges.filter(_.kind == "lineage") shouldBe
      List(SiteEdge("load:sales.orders", "transform:kpi.revenue", "lineage"))
  }

  it should "resolve foreign keys in the same domain first" in {
    val res = LineageBuilder.build(nodes, Map.empty, Map.empty)
    res.edges shouldBe List(SiteEdge("load:sales.orders", "load:sales.customers", "fk"))
    res.nodes("load:sales.orders").columns.head.foreignKeyNode shouldBe Some("load:sales.customers")
  }

  it should "map column lineage onto node ids" in {
    val lineage = ColLineage.Lineage(
      Nil,
      List(
        ColLineage.Relation(
          ColLineage.Column("sales", "customers", "id"),
          ColLineage.Column("kpi", "revenue", "customer_id"),
          None
        )
      )
    )
    val res = LineageBuilder.build(nodes, Map.empty, Map(revenue.id -> lineage))
    res.columnEdges shouldBe List(
      SiteColumnEdge("load:sales.customers#id", "transform:kpi.revenue#customer_id", revenue.id)
    )
  }
}
