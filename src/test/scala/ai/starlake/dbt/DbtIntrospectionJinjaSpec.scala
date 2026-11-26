package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.jinja.{DbtJinja, DbtJinjaContext, DbtRendered}
import ai.starlake.dbt.manifest._
import ai.starlake.dbt.warehouse._

import java.time.ZonedDateTime
import java.util
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** The calls of dbt macros that read the warehouse, answered by a fake one */
class DbtIntrospectionJinjaSpec extends TestHelper {
  private lazy val manifest = DbtFixtures.shopManifest
  private lazy val relations = new DbtRelations(manifest)
  private def node(name: String) = manifest.nodes.find(_.name == name).get

  private final class Fake extends DbtWarehouse {
    val queries = scala.collection.mutable.ListBuffer.empty[String]
    def name = "fake"
    def query(sql: String, maxRows: Int): Try[DbtTable] = {
      queries += sql.trim
      val s = sql.toLowerCase
      if (s.contains("select 42")) Success(table(List("answer"), List(List(42L))))
      else if (s.contains("datediff") || s.contains("date_diff"))
        Success(table(List("n"), List(List(3L))))
      else if (s.contains("group by status"))
        Success(
          table(List("value"), List(List("completed"), List("placed"), List("returned")))
        )
      else Success(table(List("n"), List(List(4L))))
    }
    def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] = Success(
      if (r.identifier == "customers") Nil // not built yet
      else if (r.identifier == "raw_customers")
        List(
          new DbtWarehouseColumn("id", "INTEGER", "\""),
          new DbtWarehouseColumn("first_name", "VARCHAR", "\"")
        )
      else
        List(
          new DbtWarehouseColumn("customer_id", "INTEGER", "\""),
          new DbtWarehouseColumn("first_name", "VARCHAR", "\""),
          new DbtWarehouseColumn("last_name", "VARCHAR", "\"")
        )
    )
    def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] =
      Success(Some(r).filterNot(_.identifier == "no_such"))
  }

  /** A warehouse whose every call fails, as a connection that breaks */
  private final class Broken extends DbtWarehouse {
    def name = "broken"
    private def boom[T]: Try[T] = Failure(new RuntimeException("connection reset\nat line 2"))
    def query(sql: String, maxRows: Int): Try[DbtTable] = boom
    def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] = boom
    def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] = boom
  }

  private def table(cols: List[String], rows: List[List[Any]]): DbtTable =
    new DbtTable(
      cols,
      rows.map(r =>
        new util.ArrayList[Object](r.map(_.asInstanceOf[Object]).asJava): util.List[Object]
      )
    )

  private def jinja(w: DbtWarehouse) = new DbtJinja(
    manifest,
    relations,
    DbtJinjaContext(
      DbtTarget(
        "dev",
        manifest.metadata.adapterType,
        Some("main"),
        relations.defaultDatabase,
        None
      ),
      DbtVars(Map.empty, Set(manifest.metadata.projectName), Map.empty),
      Map.empty,
      _ => None,
      ZonedDateTime.parse("2024-03-01T00:00:00Z")
    ),
    new CachingWarehouse(w, 10000)
  )

  private def render(template: String, w: DbtWarehouse = new Fake, incremental: Boolean = false) =
    jinja(w).render(node("stg_orders"), template, incremental)

  private def skipped(rendered: DbtRendered): String = rendered.sql match {
    case Failure(DbtSkip(reason)) => reason
    case other                    => fail(s"expected a skip, got $other")
  }

  "run_query" should "give a table whose columns and rows the macros read" in {
    val r = render(
      """{% set t = run_query("select 42 as answer") %}{{ t.columns[0].values()[0] }} {{ t[0][0] }} {{ t | length }} {{ t.columns['answer'].values() | list }}"""
    )
    r.sql.get.trim shouldBe "42 42 1 [42]"
    r.warehouseReads shouldBe List(WarehouseRead("query: select 42 as answer", "1 row"))
  }

  "statement and load_result" should "run the call body and give its data" in {
    val r = render(
      """{% call statement('n', fetch_result=True) %}select count(*) as n from {{ ref('stg_orders') }}{% endcall %}{% set res = load_result('n') %}{{ res['data'][0][0] }} {{ res['data'] | map(attribute=0) | list }} {{ res['table'] | length }}"""
    )
    r.sql.get.trim shouldBe "4 [4] 1"
    r.warehouseReads shouldBe List(
      WarehouseRead("query: select count(*) as n from staging.stg_orders", "1 row")
    )
  }

  "a row of run_query" should "be read by index and by column name" in {
    val r = render(
      """{% for row in run_query("select 42 as answer") %}{{ row['answer'] }} {{ row.answer }} {{ row[0] }} {{ row | length }} {{ row.get('answer') }} {{ row.keys() | list }}{% endfor %}"""
    )
    r.sql.get.trim shouldBe "42 42 42 1 42 ['answer']"
  }

  "statement" should "take its name as a keyword argument" in {
    render(
      """{% call statement(name='k', fetch_result=True) %}select 42 as answer{% endcall %}{{ load_result('k')['data'][0]['answer'] }}"""
    ).sql.get.trim shouldBe "42"
  }

  "a warehouse that fails" should "skip the model with its message, and record no read" in {
    List(
      """{% set t = run_query("select 42 as answer") %}""",
      """{% set c = adapter.get_columns_in_relation(this) %}""",
      """{% set r = adapter.get_relation(this.database, this.schema, this.identifier) %}""",
      """{% set r = load_relation(this) %}"""
    ).foreach { template =>
      val r = render(template, new Broken)
      skipped(r) shouldBe "connection reset"
      r.warehouseReads shouldBe empty
    }
  }

  "load_result" should "give none for a statement that did not run" in {
    render("""{{ load_result('nothing') is none }}""").sql.get.trim shouldBe "true"
  }

  "adapter.get_columns_in_relation" should "give dbt columns, by name, type and quoted name" in {
    val r = render(
      """{% for c in adapter.get_columns_in_relation(source('shop_raw', 'raw_customers')) %}{{ c.name }}:{{ c.data_type }}:{{ c.quoted }}:{{ c.is_string() }} {% endfor %}"""
    )
    r.sql.get.trim shouldBe "id:INTEGER:\"id\":false first_name:VARCHAR:\"first_name\":true"
    r.warehouseReads shouldBe List(WarehouseRead("columns: raw.raw_customers", "2 columns"))
  }

  it should "give no column for a relation that does not exist" in {
    val r = render("""{{ adapter.get_columns_in_relation(ref('customers')) | length }}""")
    r.sql.get.trim shouldBe "0"
    r.warehouseReads shouldBe List(WarehouseRead("columns: marts.customers", "0 columns"))
  }

  it should "note on the model that the relation was not found, its columns read as empty" in {
    val r = render("""{{ adapter.get_columns_in_relation(ref('customers')) | length }}""")
    r.notes shouldBe List(
      "`marts.customers` not found in the warehouse at import: its columns were read as empty"
    )
    render(
      """{{ adapter.get_columns_in_relation(ref('stg_customers')) | length }}"""
    ).notes shouldBe empty
  }

  it should "skip the model when its argument is not a relation" in {
    skipped(
      render("""{{ adapter.get_columns_in_relation('staging.stg_orders') }}""")
    ) shouldBe "adapter.get_columns_in_relation: the argument is not a relation ('staging.stg_orders')"
    skipped(
      render("""{{ adapter.get_columns_in_relation(none) }}""")
    ) shouldBe "adapter.get_columns_in_relation: the argument is not a relation (none)"
  }

  "adapter.get_missing_columns" should "give the columns of the first relation missing in the second" in {
    render(
      """{% for c in adapter.get_missing_columns(ref('stg_customers'), source('shop_raw', 'raw_customers')) %}{{ c.name }} {% endfor %}"""
    ).sql.get.trim shouldBe "customer_id last_name"
  }

  "adapter.get_relation" should "take keyword arguments and give none for a missing relation" in {
    val r = render(
      """{{ adapter.get_relation(database=this.database, schema='staging', identifier='stg_orders') }}|{{ adapter.get_relation(database=this.database, schema='staging', identifier='no_such') is none }}"""
    )
    r.sql.get.trim shouldBe "staging.stg_orders|true"
    r.warehouseReads shouldBe List(
      WarehouseRead("relation: staging.stg_orders", "found"),
      WarehouseRead("relation: staging.no_such", "missing")
    )
  }

  it should "note on the model a relation that was not found" in {
    render(
      """{{ adapter.get_relation(database=this.database, schema='staging', identifier='no_such') is none }}"""
    ).notes shouldBe List(
      "`staging.no_such` not found in the warehouse at import: the SQL is converted as if it does not exist"
    )
  }

  "load_relation" should "give the relation the warehouse has" in {
    render("""{{ load_relation(ref('stg_orders')) }}""").sql.get.trim shouldBe "staging.stg_orders"
  }

  "a relation" should "be a mapping with dbt's metadata, read with or without a default" in {
    render(
      """{{ this is mapping }} {{ this.get('metadata')['type'] }} {{ this.get('metadata', {})['type'] }} {{ this.get('other', 'd') }}"""
    ).sql.get.trim shouldBe "true BaseRelation BaseRelation d"
  }

  "dbt_utils.get_column_values and star" should "render from the warehouse's answers" in {
    val r = render(
      """{{ dbt_utils.get_column_values(ref('stg_orders'), 'status') | list }}|{{ dbt_utils.star(from=ref('stg_customers'), except=['last_name']) }}"""
    )
    r.sql.get shouldBe "['completed', 'placed', 'returned']|\"customer_id\",\n  \"first_name\""
    r.warehouseReads shouldBe List(
      WarehouseRead("relation: staging.stg_orders", "found"),
      WarehouseRead(
        "query: select status as value from staging.stg_orders group by status order by count(*) desc",
        "3 rows"
      ),
      WarehouseRead("columns: staging.stg_customers", "3 columns")
    )
  }

  "the two renders of an incremental model" should "send a statement to the warehouse once" in {
    val fake = new Fake
    val j = jinja(fake)
    val template = """{% set t = run_query("select 42 as answer") %}select {{ t[0][0] }} as a"""
    j.render(node("stg_orders"), template, incremental = false).sql.get
    j.render(node("stg_orders"), template, incremental = true).sql.get
    fake.queries.count(_ == "select 42 as answer") shouldBe 1
  }

  "a statement that writes" should "skip the model without running" in {
    val fake = new Fake
    skipped(
      render("""{% do run_query("create table marts.x as select 1") %}select 1""", fake)
    ) should startWith("statement is not read-only: create table marts.x")
    fake.queries shouldBe empty
  }

  "an adapter method that writes" should "skip the model as a write under --introspect" in {
    skipped(render("""{% do adapter.drop_relation(this) %}select 1""")) shouldBe
    "writes to the warehouse at import (adapter.drop_relation)"
    skipped(render("""{% do adapter.create_schema(this) %}select 1""")) shouldBe
    "writes to the warehouse at import (adapter.create_schema)"
    skipped(
      jinja(new OfflineWarehouse).render(
        node("stg_orders"),
        """{% do adapter.drop_relation(this) %}select 1""",
        false
      )
    ) shouldBe "needs the warehouse at import (adapter.drop_relation)"
  }

  "without --introspect" should "skip as before introspection" in {
    def offline(template: String) =
      skipped(jinja(new OfflineWarehouse).render(node("stg_orders"), template, false))
    offline("""{{ run_query("select 1") }}""") shouldBe "needs the warehouse at import (run_query)"
    offline(
      """{% do run_query("create table marts.x as select 1") %}"""
    ) shouldBe "needs the warehouse at import (run_query)"
    offline(
      """{% call statement('x', fetch_result=True) %}select 1{% endcall %}"""
    ) shouldBe "needs the warehouse at import (statement)"
    offline("""{{ load_result('x') }}""") shouldBe "needs the warehouse at import (load_result)"
    offline(
      """{{ load_relation(this) }}"""
    ) shouldBe "needs the warehouse at import (load_relation)"
    offline(
      """{{ adapter.get_missing_columns(this, this) }}"""
    ) shouldBe "needs the warehouse at import (adapter.get_missing_columns)"
    offline(
      """{{ dbt_utils.get_column_values(ref('stg_orders'), 'status') }}"""
    ) shouldBe "needs the warehouse at import (load_relation)"
  }
}
