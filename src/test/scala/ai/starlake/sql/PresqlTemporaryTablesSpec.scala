package ai.starlake.sql

import ai.starlake.sql.PresqlTemporaryTables.{Lexing, TemporaryTable}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The temporary tables of the presql of a task are read as common table expressions of its SELECT
  * by the sync of its attributes (#1950)
  */
class PresqlTemporaryTablesSpec extends AnyFlatSpec with Matchers {

  "PresqlTemporaryTables.of" should "read the temporary tables that the presql creates from a query" in {
    PresqlTemporaryTables.of(
      List(
        """-- Previous state of the table
          |CREATE TEMP TABLE previous_state AS SELECT * FROM mydb.orders;
          |create or replace temporary table `latest` as (select id from mydb.orders where label = 'a;b');
          |DECLARE x INT64 DEFAULT 1;
          |CREATE TEMPORARY TABLE IF NOT EXISTS "quoted" AS WITH t AS (SELECT 1 AS id) SELECT id FROM t""".stripMargin,
        "CREATE LOCAL TEMP TABLE local_one AS\nSELECT 'it\\'s' AS label;"
      ),
      lexing = Lexing.of("bigquery")
    ) shouldBe List(
      TemporaryTable("previous_state", "SELECT * FROM mydb.orders"),
      // Without the parentheses that enclose the query
      TemporaryTable("`latest`", "select id from mydb.orders where label = 'a;b'"),
      TemporaryTable("\"quoted\"", "WITH t AS (SELECT 1 AS id) SELECT id FROM t"),
      TemporaryTable("local_one", "SELECT 'it\\'s' AS label")
    )
  }

  it should "read a temporary table named like a column or an alias of its query" in {
    PresqlTemporaryTables.of(
      List(
        "CREATE TEMP TABLE max_date AS SELECT MAX(d) AS max_date FROM mydb.t WHERE k = 'max_date'"
      )
    ) shouldBe List(
      TemporaryTable("max_date", "SELECT MAX(d) AS max_date FROM mydb.t WHERE k = 'max_date'")
    )
  }

  it should "read a temporary table dropped before it is created, or named like a column after a comma" in {
    PresqlTemporaryTables.of(
      List(
        "DROP TABLE IF EXISTS prev; CREATE TEMP TABLE prev AS SELECT * FROM mydb.orders",
        "CREATE TEMP TABLE max_date AS SELECT k, max_date FROM mydb.t"
      )
    ) shouldBe List(
      TemporaryTable("prev", "SELECT * FROM mydb.orders"),
      TemporaryTable("max_date", "SELECT k, max_date FROM mydb.t")
    )
  }

  it should "strip the comments of the engine, outside the strings and the quoted names" in {
    // An apostrophe in a # comment, a semicolon in a -- comment
    PresqlTemporaryTables
      .of(
        List(
          "# rows that didn't change\nCREATE TEMP TABLE prev AS\nSELECT id -- key; first\nFROM mydb.t"
        ),
        lexing = Lexing.of("bigquery")
      )
      .map(table => table.name -> table.query.replaceAll("\\s+", " ")) shouldBe List(
      "prev" -> "SELECT id FROM mydb.t"
    )
    // A backslash escapes a quote on Snowflake too: the INSERT is a statement of its own
    PresqlTemporaryTables.of(
      List("CREATE TEMP TABLE a AS SELECT 'O\\'Brien' AS n; INSERT INTO a SELECT 'x'"),
      lexing = Lexing.of("snowflake")
    ) shouldBe Nil
  }

  it should "strip the WITH [NO] DATA clause of a query" in {
    PresqlTemporaryTables.of(
      List("CREATE TEMP TABLE empty_copy AS SELECT * FROM mydb.t WITH NO DATA")
    ) shouldBe List(TemporaryTable("empty_copy", "SELECT * FROM mydb.t"))
  }

  it should "read the backslashes and the # comments as BigQuery does, on BigQuery only" in {
    // A backslash ends a DuckDB string
    PresqlTemporaryTables.of(
      List("CREATE TEMP TABLE a AS SELECT 'C:\\' AS p; CREATE TEMP TABLE b AS SELECT 1 AS x")
    ) shouldBe List(TemporaryTable("a", "SELECT 'C:\\' AS p"), TemporaryTable("b", "SELECT 1 AS x"))
    PresqlTemporaryTables.of(
      List("# the previous state, it's kept\nCREATE TEMP TABLE a AS SELECT 1 AS x; # done"),
      lexing = Lexing.of("bigquery")
    ) shouldBe List(TemporaryTable("a", "SELECT 1 AS x"))
  }

  it should "leave out the temporary tables that are not common table expressions" in {
    PresqlTemporaryTables.of(
      List(
        // A column list, options, a qualified name, no query
        "CREATE TEMP TABLE with_columns (id INT64) AS SELECT 1",
        "CREATE TEMP TABLE with_options OPTIONS(expiration_timestamp = CURRENT_TIMESTAMP()) AS SELECT 1",
        "CREATE TEMP TABLE mydb.qualified AS SELECT 1",
        "CREATE TEMP TABLE empty_one (id INT64); INSERT INTO empty_one SELECT 1",
        // Created twice, or from itself
        "CREATE TEMP TABLE twice AS SELECT 1 AS id; CREATE OR REPLACE TEMP TABLE twice AS SELECT 2 AS id",
        "CREATE TEMP TABLE itself AS SELECT * FROM mydb.itself_source UNION ALL SELECT * FROM itself",
        // Not temporary
        "CREATE TABLE permanent AS SELECT 1",
        // Changed after it is created, or also created in another form
        "CREATE TEMP TABLE altered AS SELECT 1 AS id; ALTER TABLE altered ADD COLUMN b INT64",
        "CREATE TEMP TABLE filled AS SELECT 1 AS id; INSERT INTO filled SELECT 2",
        "CREATE TEMP TABLE `both` (id INT64); CREATE OR REPLACE TEMP TABLE `both` AS SELECT 1 AS id"
      )
    ) shouldBe Nil
    // A script that runs statements conditionally or repeatedly
    PresqlTemporaryTables.of(
      List(
        "CREATE TEMP TABLE first_one AS SELECT 1 AS id",
        "IF x > 0 THEN CREATE TEMP TABLE second_one AS SELECT 1 AS id; END IF"
      ),
      lexing = Lexing.of("bigquery")
    ) shouldBe Nil
    List(
      "outer_loop: LOOP SELECT 1; CREATE TEMP TABLE looped AS SELECT 1 AS id; END LOOP",
      "EXECUTE IMMEDIATE 'CREATE TEMP TABLE dynamic AS SELECT 1 AS id'; CREATE TEMP TABLE after_dynamic AS SELECT 1 AS id",
      "CALL mydb.prepare(); CREATE TEMP TABLE after_call AS SELECT 1 AS id"
    ).foreach { presql =>
      PresqlTemporaryTables.of(List(presql), lexing = Lexing.of("bigquery")) shouldBe Nil
    }
    // A transaction is not one
    List(
      "BEGIN TRANSACTION; CREATE TEMP TABLE in_transaction AS SELECT 1 AS id; COMMIT TRANSACTION",
      "BEGIN WORK; CREATE TEMP TABLE in_transaction AS SELECT 1 AS id; COMMIT",
      "BEGIN; CREATE TEMP TABLE in_transaction AS SELECT 1 AS id; COMMIT"
    ).foreach { presql =>
      PresqlTemporaryTables.of(List(presql)) shouldBe
      List(TemporaryTable("in_transaction", "SELECT 1 AS id"))
    }
  }

  "PresqlTemporaryTables.withTemporaryTables" should "put the temporary tables that the SELECT reads in front of it" in {
    val presql = List(
      "CREATE TEMP TABLE base AS SELECT * FROM mydb.orders",
      "CREATE TEMP TABLE unused AS SELECT * FROM mydb.missing",
      "CREATE TEMP TABLE derived AS SELECT id FROM base"
    )
    // Directly and through another one, in the order of the presql, unused ones left out
    PresqlTemporaryTables.withTemporaryTables(
      "-- the rule\nSELECT id FROM derived;",
      presql
    ) shouldBe (
      """WITH base AS (
        |SELECT * FROM mydb.orders
        |),
        |derived AS (
        |SELECT id FROM base
        |)
        |SELECT id FROM derived""".stripMargin,
      List(
        TemporaryTable("base", "SELECT * FROM mydb.orders"),
        TemporaryTable("derived", "SELECT id FROM base")
      )
    )
    // In front of the common table expressions of the SELECT, which may read them
    PresqlTemporaryTables
      .withTemporaryTables(
        "with recursive t AS (SELECT id FROM base) SELECT `base`.id FROM t, `base`",
        presql
      )
      ._1 shouldBe
    """with recursive base AS (
      |SELECT * FROM mydb.orders
      |),
      |t AS (SELECT id FROM base) SELECT `base`.id FROM t, `base`""".stripMargin
  }

  it should "leave the SELECT as it is when it reads none of them" in {
    val presql = List("CREATE TEMP TABLE base AS SELECT * FROM mydb.orders")
    List(
      "SELECT id FROM mydb.orders",
      // A table of a dataset, or a column, an alias or a string, of the same name
      "SELECT id FROM mydb.base",
      "SELECT id FROM mydb.`base`",
      "SELECT base_id, t.base FROM mydb.orders t",
      "SELECT id AS base FROM mydb.orders WHERE kind = 'base'",
      "SELECT id, base FROM mydb.orders",
      "SELECT id FROM mydb.orders WHERE x IN (1, base)",
      "SELECT id FROM mydb.orders ORDER BY id, base",
      // A SELECT that defines it itself, with a column list or MATERIALIZED
      "WITH base AS (SELECT 1 AS id) SELECT id FROM base",
      "WITH base (id) AS (SELECT 1) SELECT id FROM base",
      "WITH base AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM base"
    ).foreach { select =>
      PresqlTemporaryTables.withTemporaryTables(select, presql) shouldBe (select, Nil)
    }
  }

  it should "put them in front of a WITH clause that follows a comment" in {
    PresqlTemporaryTables.withTemporaryTables(
      "# the rule\nWITH a AS (SELECT id FROM prev) SELECT id FROM a",
      List("CREATE TEMP TABLE prev AS SELECT 1 AS id"),
      lexing = Lexing.of("bigquery")
    ) shouldBe (
      """WITH prev AS (
        |SELECT 1 AS id
        |),
        |a AS (SELECT id FROM prev) SELECT id FROM a""".stripMargin,
      List(TemporaryTable("prev", "SELECT 1 AS id"))
    )
  }

  it should "leave out the tables that read a table that the SELECT defines, or in a recursive WITH clause, a name that it defines" in {
    val presql = List(
      "CREATE TEMP TABLE base AS SELECT * FROM mydb.orders",
      "CREATE TEMP TABLE derived AS SELECT id FROM base",
      "CREATE TEMP TABLE other AS SELECT id FROM t"
    )
    // derived would read the permanent base
    PresqlTemporaryTables
      .withTemporaryTables(
        "WITH base AS (SELECT 1 AS id) SELECT d.id FROM derived d JOIN base USING (id)",
        presql
      )
      ._2 shouldBe Nil
    // In a recursive WITH clause, other would read the t of the SELECT
    PresqlTemporaryTables
      .withTemporaryTables(
        "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) SELECT id FROM other",
        presql
      )
      ._2 shouldBe Nil
    // but not in a WITH clause that is not recursive: other reads the permanent t
    PresqlTemporaryTables
      .withTemporaryTables("WITH t AS (SELECT 1 AS id) SELECT id FROM other", presql)
      ._2
      .map(_.name) shouldBe List("other")
  }

  "TemporaryTable.readsTable" should "find the table of a task, alone or qualified by its dataset, in its query" in {
    val table = TemporaryTable("previous_state", "SELECT * FROM `project.mydb.orders` WHERE x > 0")
    table.readsTable("mydb", "orders") shouldBe true
    TemporaryTable("t", "SELECT * FROM orders").readsTable("mydb", "orders") shouldBe true
    TemporaryTable("t", "SELECT a FROM x JOIN mydb.orders USING (a)").readsTable(
      "mydb",
      "orders"
    ) shouldBe true
    // Another table, of another dataset, or a column
    TemporaryTable("t", "SELECT * FROM mydb.orders_history").readsTable(
      "mydb",
      "orders"
    ) shouldBe false
    TemporaryTable("t", "SELECT * FROM otherdb.orders").readsTable("mydb", "orders") shouldBe false
    TemporaryTable("t", "SELECT orders FROM mydb.t").readsTable("mydb", "orders") shouldBe false
    TemporaryTable("t", "SELECT id, orders FROM mydb.t").readsTable("mydb", "orders") shouldBe false
    TemporaryTable("t", "SELECT * FROM mydb.t, orders").readsTable("mydb", "orders") shouldBe true
  }

  "PresqlTemporaryTables.statements" should "split a presql section at the semicolons outside the quotes and comments" in {
    PresqlTemporaryTables.statements(
      """SELECT 'a;b' AS x; -- c;d
        |SELECT "e;f", `g;h`;
        |/* i;j */ SELECT 1;""".stripMargin
    ) shouldBe List("SELECT 'a;b' AS x", "SELECT \"e;f\", `g;h`", "SELECT 1")
    // A backslash escapes a quote on BigQuery only
    PresqlTemporaryTables.statements(
      "SELECT 'it\\'s;'; SELECT 2",
      lexing = Lexing.of("bigquery")
    ) shouldBe
    List("SELECT 'it\\'s;'", "SELECT 2")
  }

  "PresqlTemporaryTables.commonTableExpressionNames" should "find the names that a WITH clause defines, and only them" in {
    PresqlTemporaryTables.commonTableExpressionNames(
      "WITH a AS (SELECT 1), b (x) AS (SELECT 2), c AS MATERIALIZED (SELECT 3) SELECT 1"
    ) shouldBe List("a", "b", "c")
    PresqlTemporaryTables.commonTableExpressionNames("SELECT f(a, areas (b))") shouldBe Nil
  }
}
