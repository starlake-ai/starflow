package ai.starlake.job.transform

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The SQL of native BigQuery sharding (#2012). Pure string building: no BigQuery access. */
class BigQueryShardingSqlSpec extends AnyFlatSpec with Matchers {

  private val select = "SELECT region, year, amount FROM sales.orders"

  "shardValuesQuery" should "read the distinct shard values of the task's SELECT as strings" in {
    BigQueryAutoTask.shardValuesQuery(List("region", "year"), select) shouldBe
    "SELECT DISTINCT CAST(region AS STRING) AS region, CAST(year AS STRING) AS year" +
    s" FROM ($select)"
  }

  it should "drop the trailing semicolon of the SELECT" in {
    BigQueryAutoTask.shardValuesQuery(List("region"), s"  $select ;\n") shouldBe
    s"SELECT DISTINCT CAST(region AS STRING) AS region FROM ($select)"
  }

  "shardQuery" should "select the rows of one shard, matching the values as read" in {
    BigQueryAutoTask.shardQuery(
      List("region", "year"),
      List(Some("eu-west"), Some("2024")),
      s"$select;"
    ) shouldBe
    s"SELECT * FROM ($select)" +
    " WHERE CAST(region AS STRING) = 'eu-west' AND CAST(year AS STRING) = '2024'"
  }

  it should "match a null shard value with IS NULL" in {
    BigQueryAutoTask.shardQuery(List("region", "year"), List(Some("eu"), None), select) shouldBe
    s"SELECT * FROM ($select) WHERE CAST(region AS STRING) = 'eu' AND year IS NULL"
  }

  it should "escape quotes and backslashes in a shard value" in {
    BigQueryAutoTask.shardQuery(List("name"), List(Some("""o'k\x""")), select) shouldBe
    s"""SELECT * FROM ($select) WHERE CAST(name AS STRING) = 'o\\'k\\\\x'"""
  }

  "shardSuffix" should "name the shard table from its values, as the Spark path does" in {
    BigQueryAutoTask.shardSuffix(List(Some("eu-west"), None, Some("2024"))) shouldBe
    "eu_west_null_2024"
  }
}
