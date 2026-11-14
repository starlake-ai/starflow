package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{
  BigQuerySink,
  Materialization,
  Sink,
  WriteStrategy,
  WriteStrategyType
}

import java.util.regex.Pattern

/** The BigQuery OVERWRITE template evaluates the SELECT first, then replaces the rows of an
  * existing table in a transaction, so that a failure leaves the table as it was and the SELECT may
  * read the table it replaces (#1938)
  */
class BigQueryOverwriteTemplateSpec extends TestHelper {
  new WithSettings() {
    private val select = "SELECT p, k, v + 1 AS v FROM my_domain.my_table"

    private def render(
      columns: List[String] = List("p", "k", "v"),
      sink: => Sink = BigQuerySink(partition = Some(List("p"))),
      tableFilter: Option[Boolean] = None,
      materializedView: Materialization = Materialization.TABLE,
      query: String = select
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)),
        query,
        TransformStrategiesBuilder.TableComponents("", "my_domain", "my_table", columns),
        targetTableExists = true,
        truncate = false,
        materializedView = materializedView,
        settings.appConfig.jdbcEngines("bigquery"),
        sink,
        tableFilter.map(filter => "tableRequirePartitionFilter" -> filter.toString).toMap
      )

    private def occurrences(sql: String, text: String): Int =
      sql.split(Pattern.quote(text), -1).length - 1

    /** The texts, each found once in the SQL, in this order */
    private def inOrder(sql: String, texts: String*): Unit = {
      texts.foreach(text => withClue(text)(occurrences(sql, text) shouldBe 1))
      val positions = texts.map(sql.indexOf).toList
      positions shouldBe positions.sorted
    }

    private val off =
      "ALTER TABLE my_domain.my_table SET OPTIONS (require_partition_filter = false);"
    private val on = "ALTER TABLE my_domain.my_table SET OPTIONS (require_partition_filter = true);"

    private def alters(sql: String): List[String] = "ALTER TABLE .*?;".r.findAllIn(sql).toList

    "The BigQuery OVERWRITE template" should "evaluate the SELECT before the transaction that replaces the rows" in {
      val sql = render()
      occurrences(sql, select) shouldBe 1
      inOrder(
        sql,
        // A table of the column types of the target, into which the SELECT is inserted as into the
        // target, outside the transaction
        "CREATE TEMP TABLE SL_INCOMING AS SELECT `p`,`k`,`v` FROM my_domain.my_table WHERE FALSE;",
        s"INSERT INTO SL_INCOMING(`p`,`k`,`v`) $select\n;",
        "BEGIN TRANSACTION;",
        "TRUNCATE TABLE my_domain.my_table;",
        "INSERT INTO my_domain.my_table(`p`,`k`,`v`) SELECT `p`,`k`,`v` FROM SL_INCOMING;",
        "COMMIT TRANSACTION;",
        // On the error path of the transaction, whatever the statement that failed
        "ROLLBACK TRANSACTION;",
        // The task fails with the error of the run
        "RAISE;",
        "DROP TABLE SL_INCOMING;"
      )
      alters(sql) shouldBe Nil
      // The handlers of the transaction and of its rollback
      occurrences(sql, "EXCEPTION WHEN ERROR THEN") shouldBe 2
    }

    it should "end the SELECT of the task before the next statement, after a line comment" in {
      val commented = s"$select -- the latest rows"
      render(query = commented) should include(s"$commented\n;")
      render(columns = List("_PARTITIONTIME", "k", "v"), query = commented) should include(
        s"$commented\n;"
      )
    }

    it should "turn a partition filter that the table requires off around the statement that types the temporary table only" in {
      val sql = render(tableFilter = Some(true))
      alters(sql) shouldBe List(off, on, on)
      // The statement in a block of its own, whose handler turns the filter on again
      occurrences(sql, "EXCEPTION WHEN ERROR THEN") shouldBe 3
      occurrences(sql, "RAISE;") shouldBe 2
      sql should include regex "(?s)require_partition_filter = false\\);\\s+BEGIN\\s+CREATE TEMP TABLE SL_INCOMING[^;]*;\\s+EXCEPTION WHEN ERROR THEN\\s+ALTER[^;]*;\\s+RAISE;\\s+END;"
      val positions = List(
        sql.indexOf(off),
        sql.indexOf("CREATE TEMP TABLE SL_INCOMING"),
        // On again when it fails, before its error is raised again
        sql.indexOf(on),
        sql.indexOf("RAISE;"),
        // and once it succeeded, before the SELECT of the task, which the filter checks as before
        sql.lastIndexOf(on),
        sql.indexOf("INSERT INTO SL_INCOMING"),
        sql.indexOf("BEGIN TRANSACTION;")
      )
      positions.foreach(_ should be >= 0)
      positions shouldBe positions.sorted
      // The table keeps its option, whatever its sink declares
      alters(
        render(
          sink = BigQuerySink(partition = Some(List("p")), requirePartitionFilter = Some(false)),
          tableFilter = Some(true)
        )
      ) shouldBe List(off, on, on)
      alters(
        render(
          sink = BigQuerySink(partition = Some(List("p")), requirePartitionFilter = Some(true)),
          tableFilter = Some(false)
        )
      ) shouldBe Nil
      // Without the state of the table, the sink tells
      alters(
        render(sink =
          BigQuerySink(partition = Some(List("p")), requirePartitionFilter = Some(true))
        )
      ) shouldBe List(off, on, on)
    }

    it should "insert a SELECT that returns a pseudo-column of ingestion time after the TRUNCATE, in the transaction" in {
      // A table cannot have a column named after these pseudo-columns
      List(List("_PARTITIONTIME", "k", "v"), List("k", "`_partitiondate`", "v")).foreach {
        columns =>
          withClue(columns) {
            val sql = render(columns = columns, tableFilter = Some(true))
            sql should not include "SL_INCOMING"
            occurrences(sql, select) shouldBe 1
            inOrder(
              sql,
              "BEGIN TRANSACTION;",
              "TRUNCATE TABLE my_domain.my_table;",
              s"$select\n;",
              "COMMIT TRANSACTION;",
              "ROLLBACK TRANSACTION;",
              "RAISE;"
            )
            // The SELECT of the task filters the partitions it reads
            alters(sql) shouldBe Nil
          }
      }
      // A column whose name merely holds one keeps the temporary table
      render(columns = List("p_partitiontime", "k")) should include("CREATE TEMP TABLE SL_INCOMING")
    }

    it should "keep replacing a materialized view" in {
      val sql = render(materializedView = Materialization.MATERIALIZED_VIEW)
      sql should include("DROP MATERIALIZED VIEW IF EXISTS my_domain.my_table;")
      sql should include(s"CREATE MATERIALIZED VIEW my_domain.my_table AS $select")
      sql should not include "TRANSACTION"
    }
  }
}
