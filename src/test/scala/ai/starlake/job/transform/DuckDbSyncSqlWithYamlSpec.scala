package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

class DuckDbSyncSqlWithYamlSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_sync_sql_yaml.db"
  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sl.yml")
  lazy val pathSqlBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sql")

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |syncSqlWithYaml = true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:$duckDbPath"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbConfiguration) {

    private def withDuckDbConnection[T](f: java.sql.Connection => T): T = {
      val connection = "test-duckdb"
      val options = settings.appConfig.connections(connection).options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        f(conn)
      }
    }

    private def setupInitialData(): Unit = {
      withDuckDbConnection { conn =>
        val stmt = conn.createStatement()
        stmt.execute("CREATE SCHEMA IF NOT EXISTS mydb")
        stmt.execute("DROP TABLE IF EXISTS mydb.mytable")
        stmt.execute(
          """CREATE TABLE mydb.mytable(
            |  id VARCHAR,
            |  name VARCHAR,
            |  amount INTEGER
            |)""".stripMargin
        )
        stmt.execute(
          "INSERT INTO mydb.mytable VALUES ('1', 'Alice', 100), ('2', 'Bob', 200)"
        )
      }
    }

    private def runOverwrite(sql: String): scala.util.Try[String] = {
      // SQL-only task: no attributes declared, exactly like a hand-written
      // transform whose .sl.yml carries only the write strategy
      val businessTask = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "mydb",
        table = "mytable",
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
      )
      val businessTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(businessTask)
      storageHandler.write(businessTaskDef, pathBusiness)
      storageHandler.write(businessTask.getSql(), pathSqlBusiness)

      val schemaHandler = settings.schemaHandler()
      val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
      workflow.autoJob(TransformConfig(name = "mydb.mytable"))
    }

    "OVERWRITE with a new mid-SELECT column, no declared attributes, syncSqlWithYaml on" should
    "sync the YAML from the SQL, evolve the table, and land values by name" in {
      setupInitialData()

      val result = runOverwrite(
        """SELECT * FROM (VALUES
          |  ('10', 'x@x.com', 'Xavier', 1000),
          |  ('20', 'y@y.com', 'Yolanda', 2000)
          |) AS t(id, email, name, amount)""".stripMargin
      )
      result.isSuccess shouldBe true

      val results = withDuckDbConnection { conn =>
        val rs = conn
          .createStatement()
          .executeQuery("SELECT id, name, amount, email FROM mydb.mytable ORDER BY id")
        val buf = scala.collection.mutable.ListBuffer[(String, String, Int, String)]()
        while (rs.next()) {
          buf += (
            (
              rs.getString("id"),
              rs.getString("name"),
              rs.getInt("amount"),
              rs.getString("email")
            )
          )
        }
        buf.toList
      }
      results should have size 2
      results should contain(("10", "Xavier", 1000, "x@x.com"))
      results should contain(("20", "Yolanda", 2000, "y@y.com"))

      // the sync must have written the resolved attributes back to the YAML
      val yaml = storageHandler.read(pathBusiness)
      yaml should include("email")
    }
  }
}
