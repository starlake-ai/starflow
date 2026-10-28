package ai.starlake.integration.transform

/** The write strategy templates of a BigQuery project receive the options of the sink of the task,
  * as `sinkOptions`, and never those of its connection (#1877)
  */
class SinkOptionsBQIntegrationSpec extends BigQueryTransformIntegrationBase {
  private val table = newTable("opts")
  private val seenTable = dropAfter(s"${table}_seen")

  /** A template of the project, which records what it receives in a table of its own */
  private def writeTemplate(): Unit = {
    val folder =
      (project / "metadata" / "write-strategies" / "templates" / "bigquery").createDirectories()
    (folder / "overwrite.j2").overwrite(
      """CREATE OR REPLACE TABLE {{ tableDomain }}.{{ tableName }}_seen AS
          |SELECT '{% if sinkOptions is defined %}{{ sinkOptions }}{% else %}undefined{% endif %}' AS sink_options;
          |
          |DELETE FROM {{ tableFullName }} WHERE TRUE;
          |
          |INSERT INTO {{ tableFullName }}({{ tableColumnsCsv }}) {{ selectStatement }};
          |""".stripMargin
    )
  }

  /** Runs the task, whose table exists so that the template of its strategy runs */
  private def seen(options: Option[String]): List[List[Option[String]]] = {
    createDataset()
    run(s"CREATE OR REPLACE TABLE `$dataset.$table` (k INT64)")
    writeTemplate()
    assert(
      transform(
        table,
        s"""version: 1
           |task:
           |  name: $table
           |  writeStrategy:
           |    type: OVERWRITE
           |${options.map(o => s"  sink:\n    options:\n      $o").getOrElse("")}
           |""".stripMargin,
        "SELECT 1 AS k"
      )
    )
    values(s"SELECT sink_options FROM `$dataset.$seenTable`")
  }

  if (remote) {
    "The write strategy templates of BigQuery" should "receive the options of the sink of the task" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        seen(Some("my_option: hello")) shouldBe List(List(Some("{my_option=hello}")))
      }
    }

    it should "not receive the options of its connection" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        seen(None) shouldBe List(List(Some("undefined")))
      }
    }
  }
}
