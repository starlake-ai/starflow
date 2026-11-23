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
      Map(
        revenue.id -> List("sales.orders", "db.sales.customers", "refdata.fx_rates", "kpi.revenue")
      ),
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

  private def fkNodes(fk: String, extra: SiteNode*) = {
    val src = load("sales", "invoices", List(SiteColumn("ref", foreignKey = Some(fk))))
    TreeMap((Seq(customers, src) ++ extra).map(n => n.id -> n): _*)
  }

  private def fkResult(fk: String, extra: SiteNode*) = {
    val res = LineageBuilder.build(fkNodes(fk, extra: _*), Map.empty, Map.empty)
    (res.edges, res.nodes("load:sales.invoices").columns.head.foreignKeyNode)
  }

  it should "resolve a one-part foreign key as a table name" in {
    fkResult("customers") shouldBe
    (List(SiteEdge("load:sales.invoices", "load:sales.customers", "fk")), Some(customers.id))
  }

  it should "resolve a two-part foreign key as domain.table when such a table exists" in {
    val crm = load("crm", "accounts")
    fkResult("crm.accounts", crm) shouldBe
    (List(SiteEdge("load:sales.invoices", "load:crm.accounts", "fk")), Some(crm.id))
  }

  it should "resolve a two-part foreign key as table.column otherwise" in {
    fkResult("customers.id") shouldBe
    (List(SiteEdge("load:sales.invoices", "load:sales.customers", "fk")), Some(customers.id))
  }

  it should "resolve a three-part foreign key as domain.table.column" in {
    val crm = load("crm", "customers")
    fkResult("crm.customers.id", crm) shouldBe
    (List(SiteEdge("load:sales.invoices", "load:crm.customers", "fk")), Some(crm.id))
  }

  it should "resolve foreign keys declared on nested columns" in {
    val nested = load(
      "sales",
      "carts",
      List(
        SiteColumn(
          "buyer",
          `type` = "struct",
          columns = List(SiteColumn("customer_id", foreignKey = Some("customers.id")))
        )
      )
    )
    val res =
      LineageBuilder.build(
        TreeMap(Seq(customers, nested).map(n => n.id -> n): _*),
        Map.empty,
        Map.empty
      )
    res.edges shouldBe List(SiteEdge("load:sales.carts", "load:sales.customers", "fk"))
    res.nodes("load:sales.carts").columns.head.columns.head.foreignKeyNode shouldBe
    Some(customers.id)
  }

  it should "not emit an fk edge for a self reference" in {
    val tree = load("sales", "tree", List(SiteColumn("parent_id", foreignKey = Some("tree.id"))))
    val res = LineageBuilder.build(TreeMap(tree.id -> tree), Map.empty, Map.empty)
    res.edges shouldBe Nil
    res.nodes(tree.id).columns.head.foreignKeyNode shouldBe Some(tree.id)
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

  it should "register external nodes for unresolved column sources" in {
    val lineage = ColLineage.Lineage(
      Nil,
      List(
        ColLineage.Relation(
          ColLineage.Column("refdata", "fx_rates", "rate"),
          ColLineage.Column("kpi", "revenue", "amount"),
          None
        )
      )
    )
    val res = LineageBuilder.build(nodes, Map.empty, Map(revenue.id -> lineage))
    res.columnEdges shouldBe List(
      SiteColumnEdge("external:refdata.fx_rates#rate", "transform:kpi.revenue#amount", revenue.id)
    )
    res.nodes.contains("external:refdata.fx_rates") shouldBe true
    res.columnEdges.foreach(e => res.nodes.contains(e.from.takeWhile(_ != '#')) shouldBe true)
  }

  it should "drop column relations whose target is not the task itself" in {
    val lineage = ColLineage.Lineage(
      Nil,
      List(
        ColLineage.Relation(
          ColLineage.Column("sales", "customers", "id"),
          ColLineage.Column("kpi", "some_cte", "x"),
          None
        )
      )
    )
    val res = LineageBuilder.build(nodes, Map.empty, Map(revenue.id -> lineage))
    res.columnEdges shouldBe Nil
  }

  private def aliasLineage(table: String) = ColLineage.Lineage(
    Nil,
    List(
      ColLineage.Relation(
        ColLineage.Column("", table, "rate"),
        ColLineage.Column("kpi", "revenue", "amount"),
        None
      )
    )
  )

  it should "drop relations from an unresolved alias with an empty domain" in {
    val res = LineageBuilder.build(nodes, Map.empty, Map(revenue.id -> aliasLineage("fx")))
    res.columnEdges shouldBe Nil
    res.nodes.contains("external:fx") shouldBe false
  }

  it should "resolve a column source with a null domain by table name" in {
    val lineage = ColLineage.Lineage(
      Nil,
      List(
        ColLineage.Relation(
          ColLineage.Column(null, "customers", "id"),
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

  it should "map an empty-domain source to an external node registered by the table pass" in {
    val res = LineageBuilder.build(
      nodes,
      Map(revenue.id -> List("refdata.fx_rates")),
      Map(revenue.id -> aliasLineage("fx_rates"))
    )
    res.columnEdges shouldBe List(
      SiteColumnEdge("external:refdata.fx_rates#rate", "transform:kpi.revenue#amount", revenue.id)
    )
    res.nodes.contains("external:fx_rates") shouldBe false
  }
}
