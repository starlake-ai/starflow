package ai.starlake.dbt

import ai.starlake.dbt.warehouse.ReadOnlyGuard
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReadOnlyGuardSpec extends AnyFlatSpec with Matchers {
  private val reads = List(
    "select 42 as answer",
    "with t as (select 1 as a) select a from t",
    "select 1 union all select 2",
    "values (1), (2)",
    "show tables",
    "describe raw.raw_orders",
    "explain select 1",
    "  select status as value from staging.stg_orders group by status order by status  ;",
    "select 1; select 2"
  )
  private val writes = List(
    "create table marts.x as select 1",
    "insert into marts.x values (1)",
    "update marts.x set a = 1",
    "delete from marts.x",
    "drop table marts.x",
    "call my_proc()",
    "select 1; drop table marts.x",
    "select a into marts.x from raw.y",
    "select 1 union select a into marts.x from raw.y",
    "with d as (delete from marts.x returning a) select a from d",
    "explain analyze delete from marts.x",
    "explain analyze insert into marts.x values (1)",
    "select * from (with d as (delete from marts.x returning a) select a from d) s",
    "select * from (select 1 into t) s",
    "with a as (select 1) select * from (with d as (delete from marts.x returning a) select a from d) s",
    "select 1 union (with d as (delete from marts.x returning a) select a from d)",
    "select * from t where a in (with d as (delete from marts.x returning a) select a from d)",
    "this is not sql"
  )

  "ReadOnlyGuard" should "accept the statements that only read" in {
    reads.foreach(sql => withClue(sql)(ReadOnlyGuard.check(sql) shouldBe Right(())))
  }

  it should "refuse the statements that write or that it cannot parse" in {
    writes.foreach { sql =>
      withClue(sql)(ReadOnlyGuard.check(sql).isLeft shouldBe true)
    }
  }

  it should "name the statement in its reason, cut to 80 characters" in {
    val sql = "create table marts.x as select " + ("a, " * 40) + "1"
    val Left(reason) = ReadOnlyGuard.check(sql)
    reason shouldBe s"statement is not read-only: ${sql.take(80)}"
  }

  it should "give the reason for unparseable input and for a multi-statement input" in {
    ReadOnlyGuard.check("this is not sql") shouldBe
    Left("statement is not read-only: this is not sql")
    ReadOnlyGuard.check("select 1; drop table marts.x") shouldBe
    Left("statement is not read-only: select 1; drop table marts.x")
  }

  it should "turn each run of whitespace holding a newline into one space in its reason" in {
    val sql = "drop table\n   marts.x\n\n  cascade"
    ReadOnlyGuard.check(sql) shouldBe
    Left("statement is not read-only: drop table marts.x cascade")
  }
}
