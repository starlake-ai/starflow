package ai.starlake.job.strategies

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig}
import ai.starlake.schema.model.{
  BigQuerySink,
  Materialization,
  MergeOn,
  WriteStrategy,
  WriteStrategyType
}

import java.util.regex.Pattern

/** The BigQuery table that a transform task creates gets the partition expiration of its sink, as
  * the table that a load creates (#1883)
  */
class BigQueryPartitionExpirationSpec extends TestHelper {
  new WithSettings() {
    private val select = "SELECT p, k, v FROM my_domain.my_source"

    private def render(
      sink: BigQuerySink,
      strategyType: WriteStrategyType = WriteStrategyType.OVERWRITE_BY_PARTITION,
      on: MergeOn = MergeOn.TARGET,
      timestamp: Option[String] = None,
      materializedView: Materialization = Materialization.TABLE
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(
          `type` = Some(strategyType),
          on = Some(on),
          key = List("k"),
          timestamp = timestamp
        ),
        select,
        TransformStrategiesBuilder
          .TableComponents("", "my_domain", "my_table", List("p", "k", "v")),
        targetTableExists = false,
        truncate = false,
        materializedView = materializedView,
        settings.appConfig.jdbcEngines("bigquery"),
        sink
      )

    private def occurrences(sql: String, text: String): Int =
      sql.split(Pattern.quote(text), -1).length - 1

    private val expiration = "OPTIONS (partition_expiration_days = 1827)"

    private val partitioned =
      BigQuerySink(partition = Some(List("p")), clustering = Some(List("k")), days = Some(1827))

    "The BigQuery table created by a transform task" should "get the partition expiration of its sink" in {
      List[(MergeOn, Option[String])](
        MergeOn.TARGET            -> None,
        MergeOn.SOURCE_AND_TARGET -> None,
        MergeOn.SOURCE_AND_TARGET -> Some("v")
      ).foreach { case (on, timestamp) =>
        withClue(s"$on $timestamp") {
          val sql = render(partitioned, on = on, timestamp = timestamp)
          // In the order of the CREATE TABLE statement of BigQuery, the query last
          val positions =
            List(
              "CREATE TABLE my_domain.my_table",
              "PARTITION BY (p)",
              "CLUSTER BY (k)",
              s"$expiration AS"
            )
              .map(sql.indexOf)
          positions.foreach(_ should be >= 0)
          positions shouldBe positions.sorted
          occurrences(sql, "OPTIONS") shouldBe 1
        }
      }
    }

    it should "get it whatever the strategy of the task, which creates the table the same way" in {
      WriteStrategyType.strategies.foreach { strategyType =>
        withClue(strategyType) {
          val sql = render(partitioned, strategyType = strategyType)
          sql should include(s"CLUSTER BY (k) $expiration AS")
          occurrences(sql, "OPTIONS") shouldBe 1
        }
      }
    }

    it should "get none when its sink has no partition expiration" in {
      render(
        BigQuerySink(partition = Some(List("p")))
      ) should not include "partition_expiration_days"
    }

    it should "get none when its sink sets 0 days or less" in {
      List(0, -1).foreach { days =>
        withClue(days) {
          render(
            BigQuerySink(partition = Some(List("p")), days = Some(days))
          ) should not include "partition_expiration_days"
        }
      }
    }

    it should "get none when it is not partitioned, as a load ignores the expiration then" in {
      render(BigQuerySink(days = Some(1827))) should not include "partition_expiration_days"
    }

    it should "get none when it is partitioned by an integer range, which BigQuery does not expire" in {
      val sql = render(
        BigQuerySink(
          partition = Some(List("range_bucket(k, GENERATE_ARRAY(0, 100, 10))")),
          days = Some(1827)
        )
      )
      sql should include("PARTITION BY (range_bucket(k, GENERATE_ARRAY(0, 100, 10)))")
      sql should not include "partition_expiration_days"
    }

    it should "get none when it is a materialized view, which takes the partition expiration of its tables" in {
      val sql = render(partitioned, materializedView = Materialization.MATERIALIZED_VIEW)
      sql should include("CREATE MATERIALIZED VIEW my_domain.my_table")
      sql should not include "partition_expiration_days"
    }

    "The BigQuery table created by a load" should "get the partition expiration of its sink, whatever its number of days" in {
      val testSettings = settings
      val job = new BigQueryJobBase {
        val settings: Settings = testSettings
        def cliConfig: BigQueryLoadConfig = BigQueryLoadConfig(connectionRef = None)
      }
      List(1827, 36500).foreach { days =>
        withClue(days) {
          job
            .timePartitioning("p", Some(days), requirePartitionFilter = false)
            .build()
            .getExpirationMs
            .longValue() shouldBe days * 24L * 3600 * 1000
        }
      }
    }
  }
}
