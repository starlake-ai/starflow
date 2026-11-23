package ai.starlake.impact

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TaskShapeBuilderSpec extends AnyFlatSpec with Matchers {

  private val orders = NodeRef("starbake", "orders")
  private val products = NodeRef("starbake", "products")

  private val known: Map[NodeRef, List[String]] = Map(
    orders   -> List("order_id", "product_id", "quantity", "order_date"),
    products -> List("product_id", "price", "name")
  )

  private def resolve(name: String): NodeRef = {
    val parts = name.toLowerCase.split('.').toList
    parts.takeRight(2) match {
      case d :: t :: Nil => NodeRef(d, t)
      case t :: Nil      => NodeRef("", t)
      case _             => NodeRef("", name.toLowerCase)
    }
  }

  private def build(sql: String) = TaskShapeBuilder.build(sql, resolve, known.get)

  private val cteSql =
    """WITH d AS (
      |  SELECT o.order_id, Sum(o.quantity * p.price) AS total
      |  FROM starbake.orders o JOIN starbake.products p ON o.product_id = p.product_id
      |  GROUP BY o.order_id)
      |SELECT order_id, total, total * 2 AS dbl FROM d WHERE order_id > 0""".stripMargin

  "build" should "list inputs without CTE names" in {
    build(cteSql).inputs shouldBe Set(orders, products)
  }

  it should "trace output columns to input columns through CTEs" in {
    val shape = build(cteSql)
    shape.columns.map(_.name) shouldBe List("order_id", "total", "dbl")
    shape.column("order_id").get.sources shouldBe Set(SourceCol(orders, "order_id"))
    shape.column("total").get.sources shouldBe
      Set(SourceCol(orders, "quantity"), SourceCol(products, "price"))
    shape.column("dbl").get.sources shouldBe shape.column("total").get.sources
  }

  it should "change only the fingerprint of the column whose CTE expression changed" in {
    val changed = cteSql.replace("Sum(o.quantity * p.price)", "Sum(o.quantity * p.price * 1.2)")
    val a = build(cteSql)
    val b = build(changed)
    b.column("order_id").get.fingerprint shouldBe a.column("order_id").get.fingerprint
    b.column("total").get.fingerprint should not be a.column("total").get.fingerprint
    b.column("dbl").get.fingerprint should not be a.column("dbl").get.fingerprint
    b.rowFingerprint shouldBe a.rowFingerprint
  }

  it should "ignore whitespace and case in fingerprints" in {
    val reformatted = cteSql.replace("\n", "\n    ").replace("Sum(", "SUM(")
    build(reformatted).columns.map(_.fingerprint) shouldBe build(cteSql).columns.map(_.fingerprint)
    build(reformatted).rowFingerprint shouldBe build(cteSql).rowFingerprint
  }

  it should "change the row fingerprint on WHERE, JOIN and GROUP BY changes" in {
    val base = build(cteSql).rowFingerprint
    build(cteSql.replace("order_id > 0", "order_id > 10")).rowFingerprint should not be base
    build(cteSql.replace("JOIN", "LEFT JOIN")).rowFingerprint should not be base
    build(cteSql.replace("GROUP BY o.order_id", "GROUP BY o.order_id, o.order_date")).rowFingerprint should not be base
  }

  it should "collect row-level identifiers" in {
    build(cteSql).rowIdentifiers should contain allOf ("order_id", "product_id")
    build(cteSql).rowIdentifiers should not contain "quantity"
  }

  it should "detect star inputs" in {
    val shape = build("SELECT * FROM starbake.orders")
    shape.starInputs shouldBe Set(orders)
    shape.columns.map(_.name) shouldBe List("order_id", "product_id", "quantity", "order_date")
  }

  it should "record inputs with unknown columns as unresolved" in {
    val shape = build("SELECT x.a FROM other.mystery x")
    shape.unresolvedInputs.keySet shouldBe Set(NodeRef("other", "mystery"))
    shape.column("a").get.sources shouldBe empty
    shape.unresolved shouldBe None
  }

  it should "fall back to an unresolved shape when SQL does not parse" in {
    val shape = build("SELECT FROM WHERE starbake.orders")
    shape.unresolved.isDefined shouldBe true
    shape.columns shouldBe empty
  }
}
