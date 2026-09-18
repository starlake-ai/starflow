package ai.starlake.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Table-level lineage feeds DAG generation and `transform --recursive`, so the set of input tables
  * extracted from a task SQL must be exact: a missing entry schedules a task before its upstream,
  * an extra entry adds a phantom node to the generated DAG.
  */
class SqlParserInputTablesSpec extends AnyFlatSpec with Matchers {

  "extractInputTableNames" should "not report CTE names as input tables" in {
    SqlParser.extractInputTableNames(
      "WITH recent AS (SELECT * FROM sales.orders) SELECT * FROM recent"
    ) should contain theSameElementsAs List("sales.orders")
  }

  it should "not report CTE names that reference other CTEs" in {
    SqlParser.extractInputTableNames(
      """WITH a AS (SELECT * FROM d.t1),
        |     b AS (SELECT * FROM a JOIN d.t2 ON a.id = d.t2.id)
        |SELECT * FROM b""".stripMargin
    ) should contain theSameElementsAs List("d.t1", "d.t2")
  }

  it should "report tables nested inside a subquery in the FROM clause" in {
    SqlParser.extractInputTableNames(
      "SELECT * FROM (SELECT a FROM db.t1) x JOIN db.t2 ON x.a = db.t2.a"
    ) should contain theSameElementsAs List("db.t1", "db.t2")
  }

  it should "ignore table names appearing in comments" in {
    SqlParser.extractInputTableNames(
      "-- SELECT * FROM ghost.tbl\nSELECT * FROM real.tbl"
    ) should contain theSameElementsAs List("real.tbl")
  }

  it should "ignore table names appearing in string literals" in {
    SqlParser.extractInputTableNames(
      "SELECT 'from fake.tbl' AS c FROM real.tbl"
    ) should contain theSameElementsAs List("real.tbl")
  }

  it should "unquote double quoted and backquoted identifiers" in {
    SqlParser.extractInputTableNames(
      """SELECT * FROM "MyDomain"."MyTable""""
    ) should contain theSameElementsAs List("MyDomain.MyTable")

    SqlParser.extractInputTableNames(
      "SELECT * FROM `proj.ds.tbl` WHERE x = 1"
    ) should contain theSameElementsAs List("proj.ds.tbl")
  }

  it should "exclude the write target of an INSERT" in {
    SqlParser.extractInputTableNames(
      "INSERT INTO tgt.t SELECT * FROM src.s"
    ) should contain theSameElementsAs List("src.s")
  }

  it should "exclude the write target of a MERGE but keep the source" in {
    SqlParser.extractInputTableNames(
      "MERGE INTO tgt.t USING src.s ON (tgt.t.id = src.s.id) WHEN MATCHED THEN UPDATE SET x = 1"
    ) should contain theSameElementsAs List("src.s")
  }

  it should "exclude the write target of an UPDATE and a DELETE" in {
    SqlParser.extractInputTableNames(
      "UPDATE tgt.t SET x = 1 FROM src.s WHERE tgt.t.id = src.s.id"
    ) should contain theSameElementsAs List("src.s")

    SqlParser.extractInputTableNames(
      "DELETE FROM tgt.t WHERE id IN (SELECT id FROM src.s)"
    ) should contain theSameElementsAs List("src.s")
  }

  it should "exclude the write target of CREATE TABLE AS SELECT" in {
    SqlParser.extractInputTableNames(
      "CREATE TABLE tgt.t AS SELECT * FROM src.s"
    ) should contain theSameElementsAs List("src.s")
  }

  it should "report a self reference only once and not as the write target" in {
    // Incremental reload: the task reads its own sink. The read must survive.
    SqlParser.extractInputTableNames(
      "SELECT * FROM kpi.revenue WHERE dt > '2026-01-01'"
    ) should contain theSameElementsAs List("kpi.revenue")
  }

  it should "not report table functions as tables" in {
    SqlParser.extractInputTableNames(
      "SELECT * FROM read_parquet('s3://bucket/path')"
    ) shouldBe empty
  }

  "extractInputTableNamesWithFallback" should "fall back to the regex extractor on unparseable SQL" in {
    // Un-substituted Jinja makes the SQL unparseable; we still want a best effort answer.
    SQLUtils.extractInputTableNamesWithFallback(
      "SELECT * FROM sales.orders WHERE x = {{ unresolved"
    ) should contain("sales.orders")
  }

  it should "drop CTE names in the regex fallback path too" in {
    val unparseable =
      "WITH recent AS (SELECT * FROM sales.orders) SELECT * FROM recent WHERE x = {{ unresolved"
    val refs = SQLUtils.extractInputTableNamesWithFallback(unparseable)
    refs should contain("sales.orders")
    refs should not contain "recent"
  }
}
