package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path

/** An SCD2 Spark task with `checkColumns` opens a version of a key only when one of the checked
  * columns changes, a null on one side included
  */
class SparkSCD2CheckColumnsSpec extends TestHelper {
  private val sql = "SELECT id, label, note FROM scd2spark.src"

  new WithSettings() {
    private def reset(): Unit = {
      val session = sparkSession
      session.sql("CREATE SCHEMA IF NOT EXISTS scd2spark")
      session.sql("DROP TABLE IF EXISTS scd2spark.target")
      session.sql("DROP TABLE IF EXISTS scd2spark.src")
      session.sql("CREATE TABLE scd2spark.src (id INT, label STRING, note STRING) USING delta")
      session.sql("INSERT INTO scd2spark.src VALUES (1, 'a', NULL)")
    }

    private def versions(): List[(String, String, Boolean)] =
      sparkSession
        .sql("SELECT label, note, valid_to FROM scd2spark.target")
        .collect()
        .map(r => (r.getString(0), r.getString(1), r.isNullAt(2)))
        .toList
        .sortBy { case (label, note, current) =>
          (current, String.valueOf(label), String.valueOf(note))
        }

    private def run(checkColumns: List[String]): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "scd2spark",
        table = "target",
        python = None,
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(WriteStrategyType.SCD2),
            key = List("id"),
            checkColumns = Some(checkColumns),
            startTs = Some("valid_from"),
            endTs = Some("valid_to")
          )
        )
      )
      val folder = new Path(starlakeMetadataPath + "/transform/scd2spark")
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(folder, "target.sl.yml")
      )
      storageHandler.write(sql, new Path(folder, "target.sql"))
      val result = new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = "scd2spark.target"))
      result.failed.foreach(e => logger.error("scd2spark.target failed", e))
      result.isSuccess
    }

    List("the listed columns" -> List("label", "note"), "'*'" -> List("*")).foreach {
      case (name, checkColumns) =>
        s"SCD2 with checkColumns $name on Spark" should "open a version only when a checked column changes, a null included" in {
          reset()
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, true))
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, true))
          sparkSession.sql("UPDATE scd2spark.src SET note = 'x'")
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, false), ("a", "x", true))
          sparkSession.sql("UPDATE scd2spark.src SET label = 'b'")
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, false), ("a", "x", false), ("b", "x", true))
          // a value to null
          sparkSession.sql("UPDATE scd2spark.src SET note = NULL")
          run(checkColumns) shouldBe true
          versions() shouldBe List(
            ("a", null, false),
            ("a", "x", false),
            ("b", "x", false),
            ("b", null, true)
          )
        }
    }

    "SCD2 with checkColumns on Spark" should "not open a version when only an unchecked column changes" in {
      reset()
      run(List("label")) shouldBe true
      sparkSession.sql("UPDATE scd2spark.src SET note = 'x'")
      run(List("label")) shouldBe true
      versions() shouldBe List(("a", null, true))
    }
  }
}
