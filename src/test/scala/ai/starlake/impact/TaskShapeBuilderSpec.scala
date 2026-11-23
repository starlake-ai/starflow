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
    build(
      cteSql.replace("GROUP BY o.order_id", "GROUP BY o.order_id, o.order_date")
    ).rowFingerprint should not be base
  }

  it should "collect row-level identifiers" in {
    build(cteSql).rowIdentifiers should contain allOf ("order_id", "product_id")
    build(cteSql).rowIdentifiers should not contain "quantity"
  }

  it should "detect star inputs" in {
    val shape = build("SELECT * FROM starbake.orders")
    shape.starInputs shouldBe Set(orders)
    shape.columns.map(_.name) shouldBe List("order_id", "product_id", "quantity", "order_date")
    shape.unresolvedInputs shouldBe empty
  }

  it should "record inputs with unknown columns as unresolved" in {
    val shape = build("SELECT x.a FROM other.mystery x")
    shape.unresolvedInputs.keySet shouldBe Set(NodeRef("other", "mystery"))
    shape.column("a").get.sources shouldBe empty
    shape.unresolved shouldBe None
  }

  it should "fall back to an unresolved shape when SQL does not parse" in {
    // Hive / Spark TRANSFORM ... USING is not supported by JSqlParser: the SELECT list does not parse
    // but the FROM clause is valid, so the inputs are still found.
    val sql =
      "SELECT TRANSFORM(o.order_id, o.quantity) USING 'cat' AS (a, b) FROM starbake.orders o"
    val shape = build(sql)
    shape.unresolved.isDefined shouldBe true
    shape.columns shouldBe empty
    shape.inputs shouldBe Set(orders)
  }

  it should "subtract CTE names from the inputs when SQL does not parse" in {
    val sql =
      "WITH d AS (SELECT o.order_id FROM starbake.orders o) " +
      "SELECT TRANSFORM(d.order_id) USING 'cat' AS (a) FROM d"
    val shape = build(sql)
    shape.unresolved.isDefined shouldBe true
    shape.inputs shouldBe Set(orders)
  }

  it should "trace columns of an input written with a three-part name" in {
    val shape = build("SELECT o.quantity FROM proj.starbake.orders o")
    shape.unresolved shouldBe None
    shape.column("quantity").get.sources shouldBe Set(SourceCol(orders, "quantity"))
    shape.unresolvedInputs shouldBe empty
  }

  it should "fall back to table level when a qualified column cannot be attributed" in {
    val shape = build("SELECT o.nonexistent FROM starbake.orders o")
    shape.unresolved shouldBe None
    shape.unresolvedInputs shouldBe
    Map(orders -> "column nonexistent could not be attributed to an input")
  }

  it should "fall back to table level when an unqualified column cannot be attributed" in {
    val shape = build("SELECT nonexistent, quantity FROM starbake.orders")
    shape.unresolvedInputs shouldBe
    Map(orders -> "column nonexistent could not be attributed to an input")
    shape.column("quantity").get.sources shouldBe Set(SourceCol(orders, "quantity"))
  }

  it should "put every input at table level when a column cannot be attributed" in {
    val shape = build(
      "SELECT o.nonexistent, p.price FROM starbake.orders o " +
      "JOIN starbake.products p ON o.product_id = p.product_id"
    )
    shape.unresolvedInputs.keySet shouldBe Set(orders, products)
    shape.column("price").get.sources shouldBe Set(SourceCol(products, "price"))
  }

  it should "keep the existing reason of an input already unresolved" in {
    val shape = build(
      "SELECT o.nonexistent, x.a FROM starbake.orders o JOIN other.mystery x ON o.order_id = x.id"
    )
    shape.unresolvedInputs shouldBe Map(
      orders                      -> "column nonexistent could not be attributed to an input",
      NodeRef("other", "mystery") -> "columns unknown offline"
    )
  }

  it should "attribute every column of the CTE query to an input" in {
    build(cteSql).unresolvedInputs shouldBe empty
  }

  private val cteFilterSql =
    "WITH d AS (SELECT o.quantity * 2 AS q FROM starbake.orders o) SELECT q FROM d WHERE q > 5"

  it should "count a CTE column used in WHERE as row level" in {
    val a = build(cteFilterSql)
    val b = build(cteFilterSql.replace("* 2", "* 3"))
    b.rowFingerprint should not be a.rowFingerprint
    a.rowIdentifiers should contain("quantity")
  }

  it should "count a FROM subquery column used in WHERE as row level" in {
    val sql = "SELECT q FROM (SELECT o.quantity * 2 AS q FROM starbake.orders o) s WHERE q > 5"
    build(sql.replace("* 2", "* 3")).rowFingerprint should not be build(sql).rowFingerprint
  }

  it should "follow row-level CTE columns through other CTEs" in {
    val sql =
      "WITH a AS (SELECT o.quantity * 2 AS q FROM starbake.orders o), " +
      "b AS (SELECT a.q + 1 AS r FROM a) SELECT r FROM b WHERE r > 5"
    build(sql.replace("* 2", "* 3")).rowFingerprint should not be build(sql).rowFingerprint
  }

  it should "count a CTE column used in JOIN, GROUP BY or HAVING as row level" in {
    val join =
      "WITH d AS (SELECT o.product_id + 0 AS pid FROM starbake.orders o) " +
      "SELECT p.name FROM d JOIN starbake.products p ON d.pid = p.product_id"
    build(join.replace("+ 0", "+ 1")).rowFingerprint should not be build(join).rowFingerprint
    val groupBy =
      "WITH d AS (SELECT o.quantity * 2 AS q, o.order_id FROM starbake.orders o) " +
      "SELECT q, Count(order_id) AS n FROM d GROUP BY q"
    build(groupBy.replace("* 2", "* 3")).rowFingerprint should not be build(groupBy).rowFingerprint
    val having =
      "WITH d AS (SELECT o.quantity * 2 AS q, o.order_id FROM starbake.orders o) " +
      "SELECT order_id FROM d GROUP BY order_id HAVING Sum(q) > 5"
    build(having.replace("* 2", "* 3")).rowFingerprint should not be build(having).rowFingerprint
  }

  it should "keep string literals case sensitive in normalize" in {
    TaskShapeBuilder.normalize("SELECT  A \n FROM t WHERE c = 'It''s  FR'  ") shouldBe
    "select a from t where c = 'It''s  FR'"
    TaskShapeBuilder.normalize("WHERE c = 'FR'") should not be TaskShapeBuilder.normalize(
      "WHERE c = 'fr'"
    )
  }

  it should "change fingerprints when only the case of a string literal changes" in {
    val where = "SELECT o.order_id FROM starbake.orders o WHERE o.country = 'FR'"
    build(where.replace("'FR'", "'fr'")).rowFingerprint should not be build(where).rowFingerprint
    val concat = "SELECT Concat(p.name, 'X') AS label FROM starbake.products p"
    build(concat.replace("'X'", "'x'")).column("label").get.fingerprint should not be
    build(concat).column("label").get.fingerprint
  }

  it should "change the row fingerprint on a DISTINCT change" in {
    val sql = "SELECT order_id FROM starbake.orders"
    build(sql.replace("SELECT", "SELECT DISTINCT")).rowFingerprint should not be
    build(sql).rowFingerprint
  }

  it should "change inputs and the row fingerprint when the FROM source changes" in {
    val a = build("SELECT product_id FROM starbake.orders")
    val b = build("SELECT product_id FROM starbake.products")
    a.inputs shouldBe Set(orders)
    b.inputs shouldBe Set(products)
    b.rowFingerprint should not be a.rowFingerprint
  }

  it should "trace the known side of a join with an unknown input" in {
    val shape = build(
      "SELECT o.order_id, x.a FROM starbake.orders o JOIN other.mystery x ON o.order_id = x.id"
    )
    shape.unresolved shouldBe None
    shape.column("order_id").get.sources shouldBe Set(SourceCol(orders, "order_id"))
    shape.column("a").get.sources shouldBe empty
    shape.unresolvedInputs.keySet shouldBe Set(NodeRef("other", "mystery"))
  }

  it should "treat a top-level UNION ALL as unresolved (the resolver does not support it)" in {
    val shape = build(
      "SELECT order_id FROM starbake.orders UNION ALL SELECT product_id FROM starbake.products"
    )
    shape.unresolved.isDefined shouldBe true
    shape.columns shouldBe empty
    shape.inputs shouldBe Set(orders, products)
  }

  it should "fall back to table level for a set operation inside a CTE" in {
    val sql =
      "WITH d AS (SELECT o.quantity AS q FROM starbake.orders o " +
      "UNION ALL SELECT p.price FROM starbake.products p) SELECT q FROM d"
    val shape = build(sql)
    shape.unresolved shouldBe None
    // the resolver follows the first branch only: products must still be impacted
    shape.unresolvedInputs.keySet should contain(products)
    build(sql.replace("p.price", "p.price * 2")).rowFingerprint should not be shape.rowFingerprint
  }

  it should "treat SELECT * from an input with unknown columns as unresolved" in {
    val shape = build("SELECT * FROM other.mystery")
    shape.unresolved.isDefined shouldBe true
    shape.columns shouldBe empty
    shape.inputs shouldBe Set(NodeRef("other", "mystery"))
  }

  private val castSql =
    "SELECT CAST(o.order_date AS DATE) AS d, o.quantity AS q, o.order_id FROM starbake.orders o"

  private def fp(sql: String, column: String): String = build(sql).column(column).get.fingerprint

  it should "change the fingerprint when a CAST target changes" in {
    val changed = castSql.replace("AS DATE", "AS TIMESTAMP")
    fp(changed, "d") should not be fp(castSql, "d")
    fp(changed, "q") shouldBe fp(castSql, "q")
    fp(changed, "order_id") shouldBe fp(castSql, "order_id")
  }

  it should "change the fingerprint when a CAST is removed" in {
    val changed = castSql.replace("CAST(o.order_date AS DATE)", "o.order_date")
    fp(changed, "d") should not be fp(castSql, "d")
    fp(changed, "q") shouldBe fp(castSql, "q")
  }

  it should "change the fingerprint when an EXTRACT field changes" in {
    val sql = "SELECT EXTRACT(YEAR FROM o.order_date) AS y, o.quantity AS q FROM starbake.orders o"
    val changed = sql.replace("YEAR", "MONTH")
    fp(changed, "y") should not be fp(sql, "y")
    fp(changed, "q") shouldBe fp(sql, "q")
  }

  it should "change the fingerprint when a :: cast target changes" in {
    val sql = "SELECT p.price::INT AS pr, p.name FROM starbake.products p"
    val changed = sql.replace("::INT", "::BIGINT")
    fp(changed, "pr") should not be fp(sql, "pr")
    fp(changed, "name") shouldBe fp(sql, "name")
  }

  it should "change the outer fingerprint when a CAST inside a CTE changes" in {
    val sql =
      "WITH c AS (SELECT CAST(o.order_date AS DATE) AS d, o.quantity AS q FROM starbake.orders o) " +
      "SELECT d, q FROM c"
    val changed = sql.replace("AS DATE", "AS TIMESTAMP")
    fp(changed, "d") should not be fp(sql, "d")
    fp(changed, "q") shouldBe fp(sql, "q")
  }

  it should "change the outer fingerprint when a CAST inside a FROM subquery changes" in {
    val sql =
      "SELECT s.d, s.q FROM (SELECT CAST(o.order_date AS DATE) AS d, o.quantity AS q " +
      "FROM starbake.orders o) s"
    val changed = sql.replace("AS DATE", "AS TIMESTAMP")
    fp(changed, "d") should not be fp(sql, "d")
    fp(changed, "q") shouldBe fp(sql, "q")
  }
}
