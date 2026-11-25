package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path

/** A DELETE_THEN_INSERT or UPSERT_BY_KEY Spark task whose SELECT reads its own Delta table inserts
  * the rows that SELECT returned before the delete (#2013)
  */
class SparkSelfReadingStrategiesSpec extends TestHelper {

  /** Rows from the last day already loaded on: a SELECT that reads its target */
  private val sql =
    """SELECT d, k, count(*) AS n FROM selfspark.src
      |{% if sl_incremental %}
      |WHERE d >= (SELECT max(d) FROM selfspark.target)
      |{% endif %}
      |GROUP BY d, k""".stripMargin

  new WithSettings() {
    private def reset(): Unit = {
      val session = sparkSession
      session.sql("CREATE SCHEMA IF NOT EXISTS selfspark")
      session.sql("DROP TABLE IF EXISTS selfspark.target")
      session.sql("DROP TABLE IF EXISTS selfspark.src")
      session.sql("CREATE TABLE selfspark.src (d INT, k STRING) USING delta")
      session.sql("INSERT INTO selfspark.src VALUES (1, 'a'), (1, 'a'), (2, 'b')")
    }

    private def target(): List[(Int, String, Long)] =
      sparkSession
        .sql("SELECT d, k, n FROM selfspark.target ORDER BY d, k")
        .collect()
        .map(r => (r.getInt(0), r.getString(1), r.getLong(2)))
        .toList

    private def run(strategy: WriteStrategyType, on: MergeOn): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "selfspark",
        table = "target",
        python = None,
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(strategy),
            key = List("d", "k"),
            on = Some(on),
            timestamp = Some("n")
          )
        )
      )
      val folder = new Path(starlakeMetadataPath + "/transform/selfspark")
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(folder, "target.sl.yml")
      )
      storageHandler.write(sql, new Path(folder, "target.sql"))
      val result = new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = "selfspark.target"))
      result.failed.foreach(e => logger.error("selfspark.target failed", e))
      result.isSuccess
    }

    for {
      strategy <- List(WriteStrategyType.DELETE_THEN_INSERT, WriteStrategyType.UPSERT_BY_KEY)
      on       <- List(MergeOn.TARGET, MergeOn.SOURCE_AND_TARGET)
    } {
      s"$strategy on $on on Spark" should "insert the rows its SELECT returned before the delete" in {
        reset()
        run(strategy, on) shouldBe true
        target() shouldBe List((1, "a", 2L), (2, "b", 1L))
        sparkSession.sql("INSERT INTO selfspark.src VALUES (2, 'b'), (3, 'c')")
        run(strategy, on) shouldBe true
        // the second run reads d >= 2: (2, b) is replaced, (3, c) added, (1, a) left alone
        target() shouldBe List((1, "a", 2L), (2, "b", 2L), (3, "c", 1L))
      }
    }
  }
}
