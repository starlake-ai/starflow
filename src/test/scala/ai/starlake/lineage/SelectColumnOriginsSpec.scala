package ai.starlake.lineage

import ai.starlake.lineage.SelectColumnOrigins.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The origin of the columns of a SELECT, from the YAML columns of the tables of the project
  * (#1948)
  */
class SelectColumnOriginsSpec extends AnyFlatSpec with Matchers {

  private val orders = TableRef("sales", "orders")
  private val customers = TableRef("sales", "customers")
  private val undeclared = TableRef("sales", "undeclared")

  private val catalog = new Catalog {
    private val tables = Map(
      orders     -> Some(List("id", "amount", "customer_id", "tags")),
      customers  -> Some(List("id", "name")),
      undeclared -> None
    )
    // A table named alone must be the only one of that name, as in the catalog of the project
    def resolve(nameParts: List[String]): Option[TableRef] = nameParts match {
      case List(table) =>
        tables.keys.filter(_.table == table).toList match {
          case List(ref) => Some(ref)
          case _         => None
        }
      case parts => Some(TableRef(parts(parts.size - 2), parts.last)).filter(tables.contains)
    }
    def columns(table: TableRef): Option[List[String]] = tables.getOrElse(table, None)
  }

  private def origins(sql: String): List[(String, Origin)] =
    SelectColumnOrigins.of(sql, catalog).get

  private def copied(columns: ColumnRef*): Origin = Copied(columns.toList)
  private def order(column: String) = ColumnRef(orders, column)
  private def customer(column: String) = ColumnRef(customers, column)
  private def branches(branches: (Origin, Set[TableRef])*): Origin =
    Branches(branches.toList.map { case (origin, reads) => Branch(origin, reads) })

  "SelectColumnOrigins" should "read a column copied as it is, renamed or cast" in {
    origins(
      """SELECT id, amount AS total, CAST(customer_id AS STRING) AS customer,
        |  SAFE_CAST(amount AS NUMERIC) AS safe_amount, (amount) AS parenthesed
        |FROM sales.orders""".stripMargin
    ) shouldBe List(
      "id"          -> copied(order("id")),
      "total"       -> copied(order("amount")),
      "customer"    -> copied(order("customer_id")),
      "safe_amount" -> copied(order("amount")),
      "parenthesed" -> copied(order("amount"))
    )
    origins("SELECT amount::INTEGER AS amount FROM sales.orders") shouldBe List(
      "amount" -> copied(order("amount"))
    )
  }

  it should "read a computed column as computed, an element of an array or a map included" in {
    origins(
      """SELECT amount + 1 AS plus_one, UPPER(id) AS upper_id, SUM(amount) AS total,
        |  COALESCE(amount, 0) AS amount, CASE WHEN id = '1' THEN amount END AS first,
        |  CAST(amount + 1 AS STRING) AS cast_sum, 'a' AS literal, tags[1] AS first_tag
        |FROM sales.orders
        |GROUP BY 1, 2, 4, 5, 6, 7, 8""".stripMargin
    ).map(_._2).distinct shouldBe List(Computed)
  }

  it should "give no name of a column to an expression without an alias, nor to a name given twice" in {
    // The engine names CAST(amount AS INTEGER) itself
    origins("SELECT CAST(amount AS INTEGER), id FROM sales.orders").map(_._1) should not contain
    "amount"
    origins(
      "SELECT o.id AS id, c.id AS id, amount FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"
    ) shouldBe List("id" -> Computed, "id" -> Computed, "amount" -> copied(order("amount")))
    origins(
      "SELECT * FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"
    ).filter(_._1 == "id") shouldBe List("id" -> Computed, "id" -> Computed)
  }

  it should "name a column in parentheses after the column, and a cast column on the engines that do" in {
    origins("SELECT (amount) FROM sales.orders") shouldBe List("amount" -> copied(order("amount")))
    SelectColumnOrigins
      .of(
        "SELECT CAST(amount AS INTEGER), amount::TEXT FROM sales.orders",
        catalog,
        castKeepsName = true
      )
      .get shouldBe List("amount" -> Computed, "amount" -> Computed)
    SelectColumnOrigins
      .of("SELECT CAST(amount AS INTEGER) FROM sales.orders", catalog, castKeepsName = true)
      .get shouldBe List("amount" -> copied(order("amount")))
  }

  it should "follow the columns through common table expressions, subqueries and joins" in {
    origins(
      """WITH first AS (SELECT id AS order_id, amount FROM sales.orders),
        |  second AS (SELECT order_id AS ref, amount + 1 AS plus_one FROM first)
        |SELECT ref, plus_one FROM second""".stripMargin
    ) shouldBe List("ref" -> copied(order("id")), "plus_one" -> Computed)
    origins(
      """SELECT o.id, c.name, sub.name AS other_name, amount, name AS unqualified
        |FROM sales.orders o
        |JOIN sales.customers c ON o.customer_id = c.id
        |JOIN (SELECT id, name FROM sales.customers) sub ON sub.id = c.id""".stripMargin
    ) shouldBe List(
      "id"         -> copied(order("id")),
      "name"       -> copied(customer("name")),
      "other_name" -> copied(customer("name")),
      "amount"     -> copied(order("amount")),
      // In two tables of the FROM clause
      "unqualified" -> Computed
    )
    // Column lists rename the columns of a common table expression or of an alias
    origins(
      "WITH c (ident, total) AS (SELECT id, amount FROM sales.orders) SELECT total FROM c"
    ) shouldBe List("total" -> copied(order("amount")))
    origins("SELECT b FROM (SELECT id, amount FROM sales.orders) AS t (a, b)") shouldBe List(
      "b" -> copied(order("amount"))
    )
  }

  it should "read a column of a known table that its YAML does not declare" in {
    origins("SELECT o.status, status AS same FROM sales.orders o") shouldBe List(
      "status" -> copied(order("status")),
      "same"   -> copied(order("status"))
    )
  }

  it should "expand * and t.*, without the EXCEPT columns, and with the REPLACE ones computed" in {
    origins("SELECT * FROM sales.orders") shouldBe List(
      "id"          -> copied(order("id")),
      "amount"      -> copied(order("amount")),
      "customer_id" -> copied(order("customer_id")),
      "tags"        -> copied(order("tags"))
    )
    origins(
      "SELECT c.*, o.amount FROM sales.orders o JOIN sales.customers c ON o.customer_id = c.id"
    ) shouldBe List(
      "id"     -> copied(customer("id")),
      "name"   -> copied(customer("name")),
      "amount" -> copied(order("amount"))
    )
    origins(
      "SELECT * EXCEPT (customer_id, tags) REPLACE (amount * 2 AS amount) FROM sales.orders"
    ) shouldBe List("id" -> copied(order("id")), "amount" -> Computed)
  }

  it should "read every branch of a UNION, with the tables that it reads" in {
    origins(
      "SELECT id, amount FROM sales.orders UNION ALL SELECT id, 1 AS amount FROM sales.customers"
    ) shouldBe List(
      "id" -> branches(
        copied(order("id"))    -> Set(orders),
        copied(customer("id")) -> Set(customers)
      ),
      "amount" -> branches(
        copied(order("amount")) -> Set(orders),
        Computed                -> Set(customers)
      )
    )
    origins(
      """WITH u AS (SELECT id FROM sales.orders UNION ALL SELECT id FROM sales.customers)
        |SELECT id FROM u""".stripMargin
    ) shouldBe List(
      "id" -> branches(copied(order("id")) -> Set(orders), copied(customer("id")) -> Set(customers))
    )
  }

  it should "read the first branch of an EXCEPT, and nothing of an INTERSECT or of branches whose columns are not all known" in {
    origins("SELECT id FROM sales.orders EXCEPT DISTINCT SELECT id FROM sales.customers") shouldBe
    List("id" -> copied(order("id")))
    origins(
      "SELECT id FROM sales.orders INTERSECT DISTINCT SELECT id FROM sales.customers"
    ) shouldBe
    List("id" -> Computed)
    // The columns of sales.undeclared are not known
    origins("SELECT * FROM sales.undeclared UNION ALL SELECT id FROM sales.orders") shouldBe Nil
    origins("SELECT id FROM sales.orders UNION ALL SELECT * FROM sales.undeclared") shouldBe List(
      "id" -> Computed
    )
  }

  it should "not read the reference of a recursive common table expression to itself as a table" in {
    // WITH RECURSIVE marks its first item only
    origins(
      """WITH RECURSIVE first AS (SELECT id FROM sales.customers),
        |  orders AS (SELECT id FROM sales.orders UNION ALL SELECT id FROM orders WHERE id < '9')
        |SELECT id FROM orders""".stripMargin
    ) shouldBe List(
      "id" -> branches(copied(order("id")) -> Set(orders), Computed -> Set.empty[TableRef])
    )
    origins(
      """WITH RECURSIVE orders AS (
        |  SELECT id FROM sales.orders UNION ALL SELECT id FROM orders WHERE id < '9'
        |)
        |SELECT id FROM orders""".stripMargin
    ) shouldBe List(
      "id" -> branches(copied(order("id")) -> Set(orders), Computed -> Set.empty[TableRef])
    )
  }

  it should "read the names of BigQuery and of a database" in {
    List(
      "SELECT o.amount FROM `sales`.`orders` o",
      "SELECT amount FROM `myproject.sales.orders`",
      "SELECT orders.amount FROM myproject.sales.orders",
      "SELECT `amount` FROM `myproject`.`sales`.`orders`",
      // A table named alone
      "SELECT amount FROM orders"
    ).foreach { sql =>
      withClue(sql) {
        origins(sql) shouldBe List("amount" -> copied(order("amount")))
      }
    }
  }

  it should "read the columns of a table whose columns are not declared, and nothing of an unknown one" in {
    origins("SELECT code, u.label FROM sales.undeclared u") shouldBe List(
      "code"  -> copied(ColumnRef(undeclared, "code")),
      "label" -> copied(ColumnRef(undeclared, "label"))
    )
    origins(
      "SELECT id, unknown_column FROM other.unknown_table JOIN sales.orders USING (id)"
    ) shouldBe List("id" -> copied(order("id")), "unknown_column" -> Computed)
  }

  it should "fail on a statement that is not a SELECT" in {
    SelectColumnOrigins.of("DELETE FROM sales.orders", catalog).isFailure shouldBe true
    SelectColumnOrigins.of("SELECT FROM WHERE", catalog).isFailure shouldBe true
  }
}
