package ai.starlake.impact

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ChangeDetectorSpec extends AnyFlatSpec with Matchers {
  import ChangeClass._

  private val orders = NodeRef("starbake", "orders")
  private val kpi = NodeRef("kpi", "revenue")

  private def load(
    attrs: List[AttributeView],
    structural: String = "APPEND",
    metadata: String = ""
  ): NodeView =
    NodeView(orders, NodeKind.LoadTable, attrs, None, structural, metadata)

  private val baseAttrs = List(
    AttributeView("order_id", "order_id", "long"),
    AttributeView("amount", "amount", "decimal")
  )

  private val known: NodeRef => Option[List[String]] =
    r => if (r == orders) Some(List("order_id", "amount", "status")) else None

  private def resolve(n: String): NodeRef = {
    val p = n.toLowerCase.split('.')
    NodeRef(if (p.length >= 2) p(p.length - 2) else "", p.last)
  }

  private def task(sql: String, structural: String = "OVERWRITE", metadata: String = ""): NodeView =
    NodeView(
      kpi,
      NodeKind.Task,
      Nil,
      Some(TaskShapeBuilder.build(sql, resolve, known)),
      structural,
      metadata
    )

  private def view(nodes: NodeView*) =
    SnapshotView("v", nodes.map(n => n.ref -> n).toMap, Map.empty)

  private def detect(a: NodeView, b: NodeView) = ChangeDetector.detect(view(a), view(b))

  private val sql =
    "SELECT o.order_id, o.amount * 2 AS total FROM starbake.orders o WHERE o.status = 'ok'"

  "detect" should "report nothing when nothing changed" in {
    detect(task(sql), task(sql)) shouldBe empty
  }

  it should "classify an added load attribute as NON_BREAKING" in {
    val c = detect(load(baseAttrs), load(baseAttrs :+ AttributeView("note", "note", "string"))).head
    c.changeClass shouldBe NonBreaking
    c.columns shouldBe Cols(Set("note"))
    c.details shouldBe List(ColumnAdded("note"))
  }

  it should "classify a removed load attribute as BREAKING with a gone column" in {
    val c = detect(load(baseAttrs), load(baseAttrs.take(1))).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe Cols(Set("amount"))
    c.goneColumns shouldBe Set("amount")
  }

  it should "classify a load rename as BREAKING on the old name" in {
    val renamed = List(baseAttrs.head, AttributeView("amount", "amount_eur", "decimal"))
    val c = detect(load(baseAttrs), load(renamed)).head
    c.changeClass shouldBe Breaking
    c.details shouldBe List(ColumnRenamed("amount", "amount_eur"))
  }

  it should "classify a load type change as BREAKING" in {
    val retyped = List(baseAttrs.head, AttributeView("amount", "amount", "double"))
    detect(load(baseAttrs), load(retyped)).head.details shouldBe List(
      TypeChanged("amount", "decimal", "double")
    )
  }

  it should "classify a write strategy change as row-level BREAKING" in {
    val c = detect(load(baseAttrs), load(baseAttrs, structural = "UPSERT_BY_KEY(order_id)")).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
  }

  it should "classify a metadata-only change as METADATA" in {
    val c = detect(load(baseAttrs), load(baseAttrs, metadata = "comment=x")).head
    c.changeClass shouldBe Metadata
  }

  it should "classify an expression change as BREAKING for that column only" in {
    val c = detect(task(sql), task(sql.replace("o.amount * 2", "o.amount * 3"))).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe Cols(Set("total"))
    c.details shouldBe List(ExpressionChanged("total"))
  }

  it should "ignore whitespace-only SQL edits" in {
    detect(task(sql), task(sql.replace(" FROM ", "\n   FROM "))) shouldBe empty
  }

  it should "classify a WHERE change as row-level BREAKING" in {
    val c = detect(task(sql), task(sql.replace("'ok'", "'done'"))).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
  }

  it should "classify an added task column as NON_BREAKING" in {
    val c = detect(task(sql), task(sql.replace(" FROM", ", o.status FROM"))).head
    c.changeClass shouldBe NonBreaking
    c.columns shouldBe Cols(Set("status"))
  }

  it should "detect a task column rename through its fingerprint" in {
    val c = detect(task(sql), task(sql.replace("AS total", "AS total_amount"))).head
    c.details shouldBe List(ColumnRenamed("total", "total_amount"))
    c.changeClass shouldBe Breaking
  }

  it should "classify an unparseable new SQL as BREAKING unresolved" in {
    val c = detect(task(sql), task("SELECT FROM WHERE")).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
    c.unresolved.isDefined shouldBe true
  }

  it should "classify a numeric-only change of unparseable SQL as BREAKING unresolved" in {
    val unparseable =
      "SELECT TRANSFORM(o.order_id) USING 'cat' AS (a) FROM starbake.orders o WHERE o.amount > 1"
    detect(task(unparseable), task(unparseable)) shouldBe empty
    val c = detect(task(unparseable), task(unparseable.replace("> 1", "> 2"))).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
    c.unresolved.isDefined shouldBe true
  }

  it should "classify a numeric-only change of an unresolved UNION ALL as BREAKING" in {
    val union =
      "SELECT order_id FROM starbake.orders WHERE amount > 1 " +
      "UNION ALL SELECT order_id FROM starbake.orders"
    val c = detect(task(union), task(union.replace("> 1", "> 2"))).head
    c.changeClass shouldBe Breaking
    c.unresolved.isDefined shouldBe true
  }

  it should "classify a load table replaced by a task of the same name as BREAKING" in {
    val asTask = NodeView(
      orders,
      NodeKind.Task,
      Nil,
      Some(TaskShapeBuilder.build(sql, resolve, known)),
      "APPEND",
      ""
    )
    val c = ChangeDetector.detect(view(load(baseAttrs)), view(asTask)).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
  }

  it should "classify a task whose shape appears as row-level BREAKING" in {
    val bare = NodeView(kpi, NodeKind.Task, Nil, None, "OVERWRITE", "")
    val c = detect(bare, task(sql)).head
    c.changeClass shouldBe Breaking
    c.columns shouldBe AllColumns
  }

  it should "pair each renamed task column with a distinct new name" in {
    val twice = "SELECT o.amount * 2 AS a, o.amount * 2 AS b FROM starbake.orders o"
    val c = detect(task(twice), task(twice.replace("AS a", "AS x").replace("AS b", "AS y"))).head
    c.details.toSet shouldBe Set(ColumnRenamed("a", "x"), ColumnRenamed("b", "y"))
  }

  it should "report added and deleted nodes" in {
    val added = ChangeDetector.detect(view(), view(task(sql))).head
    added.changeClass shouldBe Added
    val deleted = ChangeDetector.detect(view(task(sql)), view()).head
    deleted.changeClass shouldBe Breaking
    deleted.deleted shouldBe true
  }

  "otherChanges" should "list changed, added and removed untraced files" in {
    val a = SnapshotView("a", Map.empty, Map("dags/x.sl.yml" -> "1", "application.sl.yml" -> "1"))
    val b = SnapshotView("b", Map.empty, Map("dags/x.sl.yml" -> "2", "dags/y.sl.yml" -> "1"))
    ChangeDetector.otherChanges(a, b) shouldBe List(
      "application.sl.yml (deleted)",
      "dags/x.sl.yml (changed)",
      "dags/y.sl.yml (added)"
    )
  }
}
