package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.job.strategies.TransformStrategiesBuilder
import ai.starlake.schema.model.{AllSinks, Materialization, WriteStrategy, WriteStrategyType}

import java.util.regex.Pattern

/** The script of a native BigQuery task checks the expectations declared `failOnError` before the
  * commit of its write, under `expectations.beforeCommit` (#1976). BigQuery runs these scripts in
  * the integration tests only: these tests check what the scripts hold.
  */
class BigQueryExpectationsCheckSpec extends TestHelper {
  private val queries = List(
    "SELECT COUNT(*) FROM dom.t WHERE id IS NULL;",
    "SELECT CASE WHEN COUNT(*) >= 2 THEN 0 ELSE 1 END FROM dom.t -- a comment",
    "SELECT COUNT(*) FROM dom.t WHERE id < 0; -- a; comment\n"
  )
  private val check = BigQueryAutoTask.expectationsCheck(queries)
  private val table = "`project.dom.t`"

  private def occurrences(sql: String, text: String): Int =
    sql.split(Pattern.quote(text), -1).length - 1

  private def indexOf(sql: String, text: String): Int = {
    val index = sql.indexOf(text)
    withClue(s"$text in $sql") { index should be >= 0 }
    index
  }

  "The check of the expectations" should "run each query once and raise their values when one is not 0" in {
    // Without its terminator, and closed on a line of its own after a line comment
    check should include(
      "IFNULL(CAST((\nSELECT COUNT(*) FROM dom.t WHERE id IS NULL\n  ) AS STRING)"
    )
    check should include("-- a comment\n  ) AS STRING)")
    // Without a terminator followed by a comment
    check should include("IFNULL(CAST((\nSELECT COUNT(*) FROM dom.t WHERE id < 0\n  ) AS STRING)")
    // A query that fails gets its error as its value
    occurrences(check, "EXCEPTION WHEN ERROR THEN") shouldBe queries.size
    check should include(
      s"[CONCAT('${BigQueryAutoTask.ExpectationQueryError}', SUBSTR(@@error.message"
    )
    check should include(
      "RAISE USING MESSAGE = CONCAT('" + BigQueryAutoTask.ExpectationsFailedBeforeCommit
    )
    check should include("WHERE IFNULL(SAFE_CAST(sl_result AS BIGNUMERIC) != 0, TRUE)")
    check.trim should startWith("-- The expectations")
    check.trim should endWith("END;")
  }

  "The script of a write in a template that renders the check" should "be left as it is" in {
    val main =
      s"BEGIN\nBEGIN TRANSACTION;\nTRUNCATE TABLE $table;\n$check\nCOMMIT TRANSACTION;\nEND"
    BigQueryAutoTask.withExpectationsCheck(main, check, table, createsTable = false) shouldBe Some(
      main
    )
  }

  "The script of a write in a template that runs a transaction without the check" should
  "not check the expectations before the commit" in {
    val main = s"BEGIN\nbegin transaction;\nTRUNCATE TABLE $table;\nCOMMIT TRANSACTION;\nEND"
    BigQueryAutoTask.withExpectationsCheck(main, check, table, createsTable = false) shouldBe None
  }

  "The script of a write without a transaction" should "run in one, which the check ends" in {
    val main =
      s"DECLARE incomingPartitionKeys DEFAULT (SELECT 1);\nMERGE INTO $table USING (SELECT 1) ON FALSE WHEN NOT MATCHED THEN INSERT ROW"
    val script =
      BigQueryAutoTask.withExpectationsCheck(main, check, table, createsTable = false).get
    occurrences(script, "BEGIN TRANSACTION;") shouldBe 1
    occurrences(script, check) shouldBe 1
    // The statements in a block of their own, where their DECLARE comes first
    indexOf(script, "BEGIN TRANSACTION;") should be < indexOf(script, s"BEGIN\n$main\n;\nEND;")
    indexOf(script, main) should be < indexOf(script, check)
    indexOf(script, check) should be < indexOf(script, "COMMIT TRANSACTION;")
    indexOf(script, "COMMIT TRANSACTION;") should be < indexOf(script, "ROLLBACK TRANSACTION;")
    indexOf(script, "ROLLBACK TRANSACTION;") should be < script.lastIndexOf("RAISE;")
    // A rollback that fails is told
    indexOf(script, "ROLLBACK TRANSACTION;") should be < indexOf(
      script,
      s"RAISE USING MESSAGE = CONCAT('${BigQueryAutoTask.WriteNotUndone}', sl_error);"
    )
    // nativeScript adds the terminator
    script should endWith("END")
  }

  "The script of a write that creates the table" should "drop it when the check fails" in {
    val main = s"CREATE TABLE $table AS SELECT 1 AS id"
    val script = BigQueryAutoTask.withExpectationsCheck(main, check, table, createsTable = true).get
    script should not include "TRANSACTION"
    indexOf(script, main) should be < indexOf(script, check)
    indexOf(script, check) should be < indexOf(script, "EXCEPTION WHEN ERROR THEN")
    script should include(s"DROP TABLE IF EXISTS $table;")
    script.lastIndexOf("RAISE;") should be > indexOf(script, s"DROP TABLE IF EXISTS $table;")
    // A drop that fails is told
    indexOf(script, s"DROP TABLE IF EXISTS $table;") should be < indexOf(
      script,
      s"RAISE USING MESSAGE = CONCAT('${BigQueryAutoTask.WriteNotUndone}', sl_error);"
    )
    script should endWith("END")
  }

  "The values of a failed check" should "be read from the error of the script" in {
    val marker = BigQueryAutoTask.ExpectationsFailedBeforeCommit
    BigQueryAutoTask.expectationResults(
      new Exception(s"""Query error: ${marker}["0","1","NULL"] at [42:3]""")
    ) shouldBe Some(List("0", "1", "NULL"))
    BigQueryAutoTask.expectationResults(
      new RuntimeException("Job failed", new Exception(s"""${marker}["3"]"""))
    ) shouldBe Some(List("3"))
    // Quoted in the JSON of an error
    BigQueryAutoTask.expectationResults(
      new Exception(s"""{"message": "${marker}[\\"0\\",\\"2\\"] at [1:1]"}""")
    ) shouldBe Some(List("0", "2"))
    // After a failed rollback
    val notUndone = new Exception(s"""${BigQueryAutoTask.WriteNotUndone}${marker}["1"] at [9:9]""")
    BigQueryAutoTask.expectationResults(notUndone) shouldBe Some(List("1"))
    BigQueryAutoTask.writeNotUndone(notUndone) shouldBe true
    BigQueryAutoTask.writeNotUndone(new Exception(s"""${marker}["1"]""")) shouldBe false
    // Unreadable values, or a message that quotes the script, are not those of the check
    BigQueryAutoTask.expectationResults(
      new Exception(s"""${marker}truncated ["0""")
    ) shouldBe None
    BigQueryAutoTask.expectationResults(new Exception(s"Syntax error near: $check")) shouldBe None
    BigQueryAutoTask.expectationResults(new Exception("Syntax error")) shouldBe None
  }

  new WithSettings() {
    private val select = "SELECT p, k, v FROM dom.source"

    private def render(
      strategy: WriteStrategyType,
      partition: String = "p",
      templateVariables: Map[String, Any] = Map(BigQueryAutoTask.ExpectationsCheckVariable -> check)
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(`type` = Some(strategy), key = List("k"), timestamp = Some("v")),
        select,
        TransformStrategiesBuilder.TableComponents("", "dom", "t", List(partition, "k", "v")),
        targetTableExists = true,
        truncate = false,
        materializedView = Materialization.TABLE,
        settings.appConfig.jdbcEngines("bigquery"),
        AllSinks(partition = Some(List(partition))).getSink(),
        templateVariables
      )

    "The OVERWRITE templates of BigQuery" should "render the check in their transaction, before its commit" in {
      List(
        WriteStrategyType.OVERWRITE              -> "p",
        WriteStrategyType.OVERWRITE              -> "_PARTITIONTIME",
        WriteStrategyType.OVERWRITE_BY_PARTITION -> "p",
        WriteStrategyType.OVERWRITE_BY_PARTITION -> "_PARTITIONTIME"
      ).foreach { case (strategy, partition) =>
        withClue(s"$strategy $partition") {
          val sql = render(strategy, partition)
          occurrences(sql, check) shouldBe 1
          indexOf(sql, "BEGIN TRANSACTION;") should be < indexOf(sql, check)
          indexOf(sql, check) should be < indexOf(sql, "COMMIT TRANSACTION;")
          BigQueryAutoTask.withExpectationsCheck(sql, check, "dom.t", createsTable = false) shouldBe
          Some(sql)
          // Without the check, as before
          render(strategy, partition, Map.empty) should not include "sl_expectation_results"
        }
      }
    }

    "The other templates of BigQuery" should "run in the transaction that ends with the check" in {
      List(
        WriteStrategyType.APPEND,
        WriteStrategyType.UPSERT_BY_KEY,
        WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP,
        WriteStrategyType.DELETE_THEN_INSERT,
        WriteStrategyType.SCD2
      ).foreach { strategy =>
        withClue(strategy) {
          val sql = render(strategy)
          sql should not include "sl_expectation_results"
          val script =
            BigQueryAutoTask.withExpectationsCheck(sql, check, "dom.t", createsTable = false).get
          occurrences(script, "BEGIN TRANSACTION;") shouldBe 1
          indexOf(script, check) should be < indexOf(script, "COMMIT TRANSACTION;")
        }
      }
    }
  }
}
