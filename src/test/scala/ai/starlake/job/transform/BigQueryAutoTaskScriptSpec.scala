package ai.starlake.job.transform

import ai.starlake.schema.model.Materialization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Native BigQuery script assembly (#1792, #1803). Pure string building: no BigQuery access. */
class BigQueryAutoTaskScriptSpec extends AnyFlatSpec with Matchers {

  private def createsTarget(presql: String*): Boolean =
    BigQueryAutoTask.presqlCreatesTable(presql.toList, Some("prj"), "my_domain", "my_table")

  "presqlCreatesTable" should "recognise the presql creating the target (#1803)" in {
    val positives = List(
      "CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64, dt DATE) PARTITION BY dt OPTIONS(description='x')",
      "create table if not exists my_domain.my_table (ts TIMESTAMP) PARTITION BY DATE(ts)",
      "CREATE TABLE `my_domain.my_table` (id INT64)",
      "CREATE TABLE `prj.my_domain.my_table` (id INT64)",
      "CREATE TABLE `prj`.`my_domain`.`my_table` (id INT64)",
      "CREATE TABLE `my_domain`.`my_table` (id INT64)",
      "CREATE TABLE my_domain . my_table (id INT64)",
      "create or replace table my_domain.my_table AS SELECT 1 AS id",
      "CREATE TABLE IF NOT EXISTS my_domain.my_table CLONE other_ds.src",
      "CREATE TABLE IF NOT EXISTS my_domain.my_table COPY other_ds.src",
      "CREATE TABLE IF NOT EXISTS my_domain.my_table (ts TIMESTAMP) PARTITION BY DATE(ts) OPTIONS(require_partition_filter=true)",
      "CREATE TABLE IF NOT EXISTS my_domain.my_table LIKE other_ds.template",
      "CREATE TABLE IF NOT EXISTS my_domain.my_table(id INT64)",
      "-- bootstrap\n/* block */ CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)",
      "# bootstrap\nCREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)",
      "BEGIN TRANSACTION; CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64); COMMIT TRANSACTION",
      "\n  CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)"
    )
    positives.foreach { sql =>
      withClue(sql) { createsTarget(sql) shouldBe true }
    }
  }

  it should "find the CREATE among several statements and entries" in {
    createsTarget(
      "DELETE FROM other_ds.log WHERE TRUE",
      "CREATE TEMP FUNCTION f(x INT64) AS (x + 1); CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64); DELETE FROM my_domain.my_table WHERE id = 1"
    ) shouldBe true
  }

  it should "ignore everything that does not create the target itself" in {
    val negatives = List(
      "DELETE FROM my_domain.my_table WHERE dt = CURRENT_DATE()",
      "CREATE TEMP TABLE my_table AS SELECT 1 AS id",
      "CREATE TEMPORARY TABLE my_domain.my_table (id INT64)",
      "CREATE OR REPLACE TEMP TABLE my_table AS SELECT 1 AS id",
      "CREATE EXTERNAL TABLE my_domain.my_table OPTIONS(format='CSV', uris=['gs://b/*'])",
      "CREATE SNAPSHOT TABLE my_domain.my_table CLONE my_domain.src",
      "CREATE VIEW my_domain.my_table AS SELECT 1 AS id",
      "CREATE TABLE my_domain.my_table_hist (id INT64)",
      "CREATE TABLE other_domain.my_table (id INT64)",
      "CREATE TABLE other_prj.my_domain.my_table (id INT64)",
      "CREATE TABLE my_table (id INT64)",
      "CREATE TABLE My_Domain.MY_TABLE (id INT64)",
      "CREATE TABLE my_domain.my_table_é (id INT64)",
      "CREATE TABLE \"my_domain\".\"my_table\" (id INT64)",
      "INSERT INTO my_domain.log VALUES ('done; CREATE TABLE my_domain.my_table (id INT64)')",
      "INSERT INTO my_domain.log VALUES ('it''s; CREATE TABLE my_domain.my_table (id INT64)')",
      "SELECT \"x; CREATE TABLE my_domain.my_table (id INT64)\"",
      "# CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)",
      "IF NOT EXISTS (SELECT 1 FROM my_domain.INFORMATION_SCHEMA.TABLES WHERE table_name = 'my_table') THEN SELECT 1; CREATE TABLE my_domain.my_table (id INT64); END IF",
      "BEGIN SELECT 1; CREATE TABLE my_domain.my_table (id INT64); EXCEPTION WHEN ERROR THEN SELECT 2; END",
      "-- CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)",
      "INSERT INTO my_domain.log SELECT 'CREATE TABLE my_domain.my_table'",
      ""
    )
    negatives.foreach { sql =>
      withClue(sql) { createsTarget(sql) shouldBe false }
    }
    createsTarget() shouldBe false
  }

  it should "match a fully qualified name only in the target's project" in {
    val presql = List("CREATE TABLE IF NOT EXISTS `my-prj.my_domain.my_table` (id INT64)")
    BigQueryAutoTask.presqlCreatesTable(
      presql,
      Some("MY-PRJ"),
      "my_domain",
      "my_table"
    ) shouldBe true
    BigQueryAutoTask.presqlCreatesTable(
      presql,
      Some("other-prj"),
      "my_domain",
      "my_table"
    ) shouldBe false
    BigQueryAutoTask.presqlCreatesTable(presql, None, "my_domain", "my_table") shouldBe false
  }

  it should "find a top-level CREATE after a completed scripting block" in {
    createsTarget(
      "IF TRUE THEN SELECT 1; END IF; CREATE TABLE IF NOT EXISTS my_domain.my_table (id INT64)"
    ) shouldBe true
  }

  it should "recognise a unicode table name" in {
    BigQueryAutoTask.presqlCreatesTable(
      List("CREATE TABLE IF NOT EXISTS my_domain.ventes_été (id INT64)"),
      None,
      "my_domain",
      "ventes_été"
    ) shouldBe true
  }

  "plannedFromPresql" should "apply to tables Starlake writes, never to views nor audit tables" in {
    BigQueryAutoTask.plannedFromPresql(true, false, Materialization.TABLE) shouldBe true
    BigQueryAutoTask.plannedFromPresql(true, false, Materialization.HYBRID) shouldBe true
    BigQueryAutoTask.plannedFromPresql(true, false, Materialization.VIEW) shouldBe false
    BigQueryAutoTask.plannedFromPresql(
      true,
      false,
      Materialization.MATERIALIZED_VIEW
    ) shouldBe false
    BigQueryAutoTask.plannedFromPresql(true, true, Materialization.TABLE) shouldBe false
    BigQueryAutoTask.plannedFromPresql(false, false, Materialization.TABLE) shouldBe false
  }

  "nativeScript" should "terminate every presql statement before the main SQL and the postsql" in {
    BigQueryAutoTask.nativeScript(
      preSql = List("CREATE TABLE IF NOT EXISTS d.t (a INT64)", "DELETE FROM d.t WHERE a = 1"),
      mainSql = "INSERT INTO d.t(a) SELECT 1 AS a",
      postSql = List("SELECT 1", "SELECT 2")
    ) shouldBe
    "CREATE TABLE IF NOT EXISTS d.t (a INT64);\n" +
    "DELETE FROM d.t WHERE a = 1;\n" +
    "INSERT INTO d.t(a) SELECT 1 AS a;\n" +
    "SELECT 1;\n" +
    "SELECT 2"
  }

  it should "equal the 1.7.5 script expression" in {
    val preSql = List("DELETE FROM d.t WHERE TRUE")
    val mainSql = "CREATE TABLE d.t AS SELECT 1 AS a"
    val postSql = List("SELECT 1")
    BigQueryAutoTask.nativeScript(preSql, mainSql, postSql) shouldBe
    preSql.map(_ + ";\n").mkString + mainSql + ";\n" + postSql.mkString(";\n")
    BigQueryAutoTask.nativeScript(Nil, mainSql, Nil) shouldBe mainSql + ";\n"
  }
}
