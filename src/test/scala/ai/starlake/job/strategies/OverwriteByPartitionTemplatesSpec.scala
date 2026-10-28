package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.schema.model.{
  AllSinks,
  BigQuerySink,
  Materialization,
  Sink,
  WriteStrategy,
  WriteStrategyType
}

import java.util.regex.Pattern

/** The OVERWRITE_BY_PARTITION templates evaluate the SELECT of the task once, and replace the NULL
  * partition (#1875)
  */
class OverwriteByPartitionTemplatesSpec extends TestHelper {
  new WithSettings() {
    private val select =
      "SELECT p, k, v + 1 AS v FROM my_domain.my_table WHERE p = DATE '2026-10-01'"

    private def render(
      strategyBuilder: String,
      engine: String,
      database: String = "",
      domain: String = "my_domain",
      columns: List[String] = List("p", "k", "v"),
      sink: => Sink = AllSinks(partition = Some(List("p"))).getSink(),
      templateVariables: Map[String, Any] = Map.empty
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE_BY_PARTITION)),
        select,
        TransformStrategiesBuilder.TableComponents(database, domain, "my_table", columns),
        targetTableExists = true,
        truncate = false,
        materializedView = Materialization.TABLE,
        settings.appConfig.jdbcEngines(engine).copy(strategyBuilder = strategyBuilder),
        sink,
        templateVariables
      )

    private def occurrences(sql: String, text: String): Int =
      sql.split(Pattern.quote(text), -1).length - 1

    "The OVERWRITE_BY_PARTITION templates" should "evaluate the SELECT once and replace the NULL partition" in {
      List(
        "bigquery"   -> "bigquery",
        "duckdb"     -> "duckdb",
        "jdbc"       -> "h2",
        "postgresql" -> "postgresql",
        "mysql"      -> "mysql",
        "redshift"   -> "redshift",
        "snowflake"  -> "snowflake",
        "sqlserver"  -> "sqlserver"
      ).foreach { case (strategyBuilder, engine) =>
        withClue(strategyBuilder) {
          val sql = render(strategyBuilder, engine)
          occurrences(sql, select) shouldBe 1
          // The SELECT is materialized: a view would evaluate it again after the DELETE
          sql should not include "VIEW"
          if (strategyBuilder == "mysql") sql should include("<=>")
          else sql should include("WHERE p IS NULL AND EXISTS")
        }
      }
      // Spark overwrites the partitions with one INSERT
      occurrences(render("spark", "spark"), select) shouldBe 1
    }

    it should "create the temporary tables that each engine allows" in {
      val bigquery = render("bigquery", "bigquery")
      // A table of the column types of the target, into which the SELECT is inserted as into the
      // target
      bigquery should include(
        "CREATE TEMP TABLE SL_INCOMING AS SELECT `p`,`k`,`v` FROM my_domain.my_table WHERE FALSE;"
      )
      bigquery should include(s"INSERT INTO SL_INCOMING(`p`,`k`,`v`) $select;")
      // Inside the transaction, which sees one snapshot of the target
      bigquery.indexOf("BEGIN TRANSACTION") should be < bigquery.indexOf("CREATE TEMP TABLE")
      render("redshift", "redshift") should include(s"CREATE TABLE #SL_INCOMING AS $select;")
      render("mysql", "mysql") should include(
        s"CREATE TEMPORARY TABLE my_domain.SL_INCOMING_BY_PARTITION AS $select;"
      )
      render("jdbc", "h2") should include(
        s"CREATE TEMPORARY TABLE SL_INCOMING_BY_PARTITION AS $select;"
      )
      val snowflake = render("snowflake", "snowflake")
      snowflake should include(
        "CREATE OR REPLACE TEMPORARY TABLE my_domain.SL_INCOMING AS SELECT p,k,v FROM my_domain.my_table WHERE FALSE;"
      )
      // Dropping it would commit before the postsql
      snowflake should not include "DROP"
      render("snowflake", "snowflake", database = "my_db") should include(
        "CREATE OR REPLACE TEMPORARY TABLE my_db.my_domain.SL_INCOMING AS"
      )
      render("snowflake", "snowflake", domain = "") should include(
        "CREATE OR REPLACE TEMPORARY TABLE SL_INCOMING AS"
      )
    }

    it should "disable the partition filter of a BigQuery table around the transaction" in {
      val sql = render(
        "bigquery",
        "bigquery",
        sink = BigQuerySink(partition = Some(List("p")), requirePartitionFilter = Some(true))
      )
      val positions = List(
        "require_partition_filter = false",
        "BEGIN TRANSACTION",
        "CREATE TEMP TABLE",
        "COMMIT TRANSACTION",
        "require_partition_filter = true"
      ).map(sql.indexOf)
      positions.foreach(_ should be >= 0)
      positions shouldBe positions.sorted
    }

    it should "turn the partition filter of a BigQuery table on again on every path (#1880)" in {

      /** The script, for a table that requires a partition filter or not, read before the run */
      def bigquery(
        tableFilter: Option[Boolean],
        sinkFilter: Option[Boolean],
        partition: String = "p"
      ): String =
        render(
          "bigquery",
          "bigquery",
          columns = List(partition, "k", "v"),
          sink = BigQuerySink(
            partition = Some(List(partition)),
            requirePartitionFilter = sinkFilter
          ),
          templateVariables =
            tableFilter.map(filter => "tableRequirePartitionFilter" -> filter.toString).toMap
        )
      val off = "ALTER TABLE my_domain.my_table SET OPTIONS (require_partition_filter = false);"
      val on = "ALTER TABLE my_domain.my_table SET OPTIONS (require_partition_filter = true);"
      def alters(sql: String): List[String] =
        "ALTER TABLE .*?;".r.findAllIn(sql).toList

      // A table that requires one, whose sink does not declare it: turned off, and on again on the
      // error path, after the rollback, and on success
      val undeclared = bigquery(Some(true), None)
      val positions = List(
        off,
        "BEGIN TRANSACTION",
        "COMMIT TRANSACTION",
        "EXCEPTION WHEN ERROR THEN",
        "ROLLBACK TRANSACTION",
        on,
        "RAISE;"
      ).map(undeclared.indexOf)
      positions.foreach(_ should be >= 0)
      positions shouldBe positions.sorted
      alters(undeclared) shouldBe List(off, on, on)
      // A table that does not require one is left as it is
      alters(bigquery(Some(false), None)) shouldBe Nil
      // A sink that declares the option sets the state of the table after a successful run
      alters(bigquery(Some(false), Some(true))) shouldBe List(on)
      // on the error path only
      alters(bigquery(Some(true), Some(false))) shouldBe List(off, on)
      // Without the state of the table, the sink tells
      alters(bigquery(None, Some(true))) shouldBe List(off, on, on)
      alters(bigquery(None, None)) shouldBe Nil
      // Tables partitioned by ingestion time too
      val ingestionTime = bigquery(Some(true), None, partition = "_PARTITIONTIME")
      ingestionTime should include("EXCEPTION WHEN ERROR THEN")
      alters(ingestionTime) shouldBe List(off, on, on)
    }

    it should "keep the SELECT inline for a BigQuery table partitioned by ingestion time" in {
      // A table cannot have a column named after these pseudo-columns
      val sql = render(
        "bigquery",
        "bigquery",
        columns = List("_PARTITIONTIME", "k", "v"),
        sink = AllSinks(partition = Some(List("_PARTITIONTIME"))).getSink()
      )
      sql should not include "SL_INCOMING"
      sql should include(
        s"WHERE _PARTITIONTIME IN (SELECT DISTINCT _PARTITIONTIME FROM ($select));"
      )
    }

    it should "refuse a SELECT that does not return the partition column" in {
      // The DELETE would read the partition of each row of the target, and delete them all
      the[IllegalArgumentException] thrownBy render(
        "duckdb",
        "duckdb",
        columns = List("k", "v")
      ) should have message
      "OVERWRITE_BY_PARTITION on my_domain.my_table: the SELECT does not return the partition column p"
      // Quoted or in another case, a partition expression, or unknown columns
      render("duckdb", "duckdb", columns = List("\"P\"", "k", "v")) should include("DELETE")
      render(
        "duckdb",
        "duckdb",
        columns = List("k", "v"),
        sink = AllSinks(partition = Some(List("DATE(ts)"))).getSink()
      ) should include("WHERE DATE(ts) IN")
      render("duckdb", "duckdb", columns = List("*")) should include("DELETE")
    }
  }
}
