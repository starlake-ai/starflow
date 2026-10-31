package ai.starlake.integration.transform

import com.google.cloud.bigquery._

/** The BigQuery table that a transform task creates gets the partition expiration of its sink, as
  * the table that a load creates (#1883)
  */
class PartitionExpirationBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val table = newTable("expiration")

  private def transformWith(days: Option[Int]): Boolean =
    transform(
      table,
      s"""version: 1
         |task:
         |  name: $table
         |  writeStrategy:
         |    type: OVERWRITE_BY_PARTITION
         |    on: TARGET
         |  sink:
         |    partition:
         |      - p
         |    clustering:
         |      - k
         |${days.map(d => s"    days: $d").getOrElse("")}
         |""".stripMargin,
      "SELECT DATE '2026-10-01' AS p, 1 AS k, 100 AS v"
    )

  /** The partition expiration of the table, in milliseconds */
  private def expirationMs(): Option[Long] = {
    val created = Option(bigquery.getTable(TableId.of(dataset, table)))
    created shouldBe defined
    Option(created.get.getDefinition[StandardTableDefinition].getTimePartitioning)
      .flatMap(partitioning => Option(partitioning.getExpirationMs))
      .map(_.longValue)
  }

  if (remote) {
    "A BigQuery transform task" should "create its table with the partition expiration of its sink" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        createDataset()
        bigquery.delete(TableId.of(dataset, table))
        assert(transformWith(Some(1827)))
        expirationMs() shouldBe Some(1827L * 24 * 3600 * 1000)
        // The next runs write into the table, whose expiration is set at creation only
        assert(transformWith(None))
        expirationMs() shouldBe Some(1827L * 24 * 3600 * 1000)
      }
    }

    it should "create its table without a partition expiration when its sink has none" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        createDataset()
        bigquery.delete(TableId.of(dataset, table))
        assert(transformWith(None))
        // Unless the dataset has a default one
        expirationMs() shouldBe Option(
          bigquery.getDataset(dataset).getDefaultPartitionExpirationMs
        ).map(_.longValue)
      }
    }
  }
}
