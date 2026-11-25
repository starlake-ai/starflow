package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.warehouse._
import ai.starlake.extract.JdbcDbUtils

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class DbtWarehouseSpec extends TestHelper {
  private def duckdb(): Map[String, String] = Map(
    "url"    -> s"jdbc:duckdb:$starlakeTestRoot/dbt_warehouse.db",
    "driver" -> "org.duckdb.DuckDBDriver"
  )

  private def setUp(options: Map[String, String]): Unit =
    JdbcDbUtils.withJDBCConnection(None, options) { conn =>
      val st = conn.createStatement()
      st.execute("CREATE SCHEMA IF NOT EXISTS staging")
      st.execute(
        "CREATE OR REPLACE TABLE staging.stg_orders(order_id INTEGER, status VARCHAR, amount DOUBLE, order_date DATE)"
      )
      st.execute(
        "INSERT INTO staging.stg_orders VALUES (1, 'completed', 12.5, DATE '2024-01-05'), (2, 'returned', 8.0, DATE '2024-01-20')"
      )
    }

  /** Counts the calls that reach the warehouse */
  private final class Counting(underlying: DbtWarehouse) extends DbtWarehouse {
    var queries = 0
    var columnCalls = 0
    def name: String = underlying.name
    def query(sql: String): Try[DbtTable] = { queries += 1; underlying.query(sql) }
    def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] = {
      columnCalls += 1; underlying.columns(r)
    }
    def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] = underlying.relation(r)
  }

  new WithSettings() {
    "JdbcWarehouse" should "answer a query as a dbt table" in {
      setUp(duckdb())
      val t = new JdbcWarehouse("test", duckdb())
        .query(
          "select status, amount, order_date from staging.stg_orders order by order_id"
        )
        .get
      t.size shouldBe 2
      t.getColumn_names.asScala shouldBe List("status", "amount", "order_date")
      t.getColumns.get(0).values().asScala shouldBe List("completed", "returned")
      t.getColumns.byName("amount").values().asScala shouldBe List(12.5, 8.0)
      t.get(0).asScala shouldBe List("completed", 12.5, "2024-01-05")
    }

    it should "read the columns of a relation, and say whether it exists" in {
      setUp(duckdb())
      val w = new JdbcWarehouse("test", duckdb())
      val cols = w.columns(WarehouseRelation(None, "staging", "stg_orders")).get
      cols.map(_.getName) shouldBe List("order_id", "status", "amount", "order_date")
      cols.find(_.getName == "status").get.is_string() shouldBe true
      cols.find(_.getName == "amount").get.is_number() shouldBe true
      w.relation(WarehouseRelation(None, "staging", "stg_orders")).get shouldBe
      Some(WarehouseRelation(None, "staging", "stg_orders"))
      w.relation(WarehouseRelation(None, "staging", "no_such")).get shouldBe None
      w.columns(WarehouseRelation(None, "staging", "no_such")).get shouldBe Nil
    }

    "CachingWarehouse" should "query once per statement and once per relation" in {
      setUp(duckdb())
      val counting = new Counting(new JdbcWarehouse("test", duckdb()))
      val cached = new CachingWarehouse(counting, maxRows = 10000)
      (1 to 3).foreach(_ => cached.query(" select 42 as answer ").get)
      (1 to 2).foreach(_ => cached.columns(WarehouseRelation(None, "staging", "stg_orders")).get)
      counting.queries shouldBe 1
      counting.columnCalls shouldBe 1
    }

    it should "refuse a statement that writes, without running it" in {
      val counting = new Counting(new JdbcWarehouse("test", duckdb()))
      new CachingWarehouse(counting, 10000).query("create table staging.x as select 1") match {
        case Failure(DbtSkip(reason)) =>
          reason should startWith("statement is not read-only: create table")
        case other => fail(s"expected a skip, got $other")
      }
      counting.queries shouldBe 0
    }

    it should "skip past the row limit" in {
      setUp(duckdb())
      new CachingWarehouse(new JdbcWarehouse("test", duckdb()), maxRows = 1)
        .query("select * from staging.stg_orders") match {
        case Failure(DbtSkip(reason)) =>
          reason shouldBe "run_query returned more than 1 rows (--introspect-max-rows)"
        case other => fail(s"expected a skip, got $other")
      }
    }

    it should "skip with the database message when a statement fails" in {
      new CachingWarehouse(new JdbcWarehouse("test", duckdb()), 10000)
        .query("select * from staging.no_such_table") match {
        case Failure(DbtSkip(reason)) => reason should include("no_such_table")
        case other                    => fail(s"expected a skip, got $other")
      }
    }

    "OfflineWarehouse" should "skip every call with the MVP's reasons" in {
      val w = new OfflineWarehouse()
      w.offline shouldBe true
      w.query("select 1") match {
        case Failure(DbtSkip(reason)) => reason shouldBe "needs the warehouse at import (run_query)"
        case other                    => fail(s"expected a skip, got $other")
      }
    }
  }
}
