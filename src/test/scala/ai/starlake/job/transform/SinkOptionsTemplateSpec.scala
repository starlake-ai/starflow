package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import scala.util.Using

/** The write strategy templates of a project receive the options of the sink of the task, as
  * `sinkOptions`, and never those of its connection (#1877)
  */
class SinkOptionsTemplateSpec extends TestHelper {
  private val connection = "test-duckdb"

  private def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "$connection"
           |connections.$connection {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/sink_options.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(engineConfiguration) {
    private def withConnection[T](f: java.sql.Connection => T): T =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(connection).options
      )(f)

    /** A template of the project, which records what it receives */
    private def writeTemplate(): Unit =
      storageHandler.write(
        """CREATE OR REPLACE TABLE dom.seen AS
          |SELECT '{% if sinkOptions is defined %}{{ sinkOptions }}{% else %}undefined{% endif %}' AS sink_options;
          |
          |DELETE FROM {{ tableFullName }};
          |
          |INSERT INTO {{ tableFullName }}({{ tableColumnsCsv }}) {{ selectStatement }};
          |""".stripMargin,
        new Path(DatasetArea.writeStrategies, "templates/duckdb/overwrite.j2")
      )

    /** Runs the task, whose table exists so that the template of its strategy runs */
    private def seen(options: Option[Map[String, String]]): String = {
      withConnection { conn =>
        Using.resource(conn.createStatement()) { statement =>
          statement.execute("CREATE SCHEMA IF NOT EXISTS dom")
          statement.execute("CREATE OR REPLACE TABLE dom.t_opts (k INT)")
        }
      }
      val task = AutoTaskInfo(
        name = "",
        sql = Some("SELECT 1 AS k"),
        database = None,
        domain = "dom",
        table = "t_opts",
        sink = Some(AllSinks(connectionRef = Some(connection), options = options)),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
      )
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(s"$starlakeMetadataPath/transform/dom/t_opts.sl.yml")
      )
      storageHandler.write(
        task.getSql(),
        new Path(s"$starlakeMetadataPath/transform/dom/t_opts.sql")
      )
      val workflow = new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
      workflow.autoJob(TransformConfig(name = "dom.t_opts")).isSuccess shouldBe true
      withConnection { conn =>
        Using.resource(conn.createStatement()) { statement =>
          Using.resource(statement.executeQuery("SELECT sink_options FROM dom.seen")) { resultSet =>
            resultSet.next()
            resultSet.getString(1)
          }
        }
      }
    }

    "The write strategy templates" should "receive the options of the sink of the task" in {
      writeTemplate()
      seen(Some(Map("my_option" -> "hello"))) shouldBe "{my_option=hello}"
    }

    it should "not receive the options of its connection" in {
      writeTemplate()
      seen(None) shouldBe "undefined"
    }
  }
}
