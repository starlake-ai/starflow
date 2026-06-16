package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

class DuckDbMergeStrategySpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_merge.db"
  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sl.yml")
  lazy val pathSqlBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sql")

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_merge.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    val result = config.withFallback(super.testConfiguration)
    result
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
        stmt.execute(
          """CREATE TABLE IF NOT EXISTS mydb.mytable(
            |  id VARCHAR,
            |  name VARCHAR,
            |  amount INTEGER
            |)""".stripMargin
        )
        stmt.execute("DELETE FROM mydb.mytable")
        stmt.execute(
          "INSERT INTO mydb.mytable VALUES ('1', 'Alice', 100), ('2', 'Bob', 200), ('3', 'Charlie', 300)"
        )
      }
    }

    private def readTable(): Seq[(String, String, Int)] = {
      withDuckDbConnection { conn =>
        val rs = conn
          .createStatement()
          .executeQuery("SELECT id, name, amount FROM mydb.mytable ORDER BY id")
        val buf = scala.collection.mutable.ListBuffer[(String, String, Int)]()
        while (rs.next()) {
          buf += ((rs.getString("id"), rs.getString("name"), rs.getInt("amount")))
        }
        buf.toList
      }
    }

    "UPSERT_BY_KEY on DuckDB via parquet intermediate" should "merge data correctly" in {
      setupInitialData()

      val businessTask = AutoTaskInfo(
        name = "",
        sql = Some(
          """SELECT * FROM (VALUES
            |  ('1', 'Alice_Updated', 150),
            |  ('4', 'Diana', 400)
            |) AS t(id, name, amount)""".stripMargin
        ),
        database = None,
        domain = "mydb",
        table = "mytable",
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(WriteStrategyType.UPSERT_BY_KEY),
            key = List("id"),
            on = Some(MergeOn.TARGET)
          )
        )
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

      val results = readTable()
      results should have size 4
      results should contain(("1", "Alice_Updated", 150)) // upserted
      results should contain(("2", "Bob", 200)) // unchanged
      results should contain(("3", "Charlie", 300)) // unchanged
      results should contain(("4", "Diana", 400)) // inserted
    }

    "OVERWRITE on DuckDB via parquet intermediate" should "replace all data" in {
      setupInitialData()

      val businessTask = AutoTaskInfo(
        name = "",
        sql = Some(
          """SELECT * FROM (VALUES
            |  ('10', 'Xavier', 1000),
            |  ('20', 'Yolanda', 2000)
            |) AS t(id, name, amount)""".stripMargin
        ),
        database = None,
        domain = "mydb",
        table = "mytable",
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(WriteStrategyType.OVERWRITE)
          )
        )
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

      val results = readTable()
      results should have size 2
      results should contain(("10", "Xavier", 1000))
      results should contain(("20", "Yolanda", 2000))
    }

    "APPEND with SELECT * on DuckDB" should "resolve column names via LIMIT 0 and append data" in {
      setupInitialData()

      val businessTask = AutoTaskInfo(
        name = "",
        sql = Some(
          """SELECT * FROM (VALUES
            |  ('1', 'Alice_Dup', 999),
            |  ('4', 'Diana', 400)
            |) AS t(id, name, amount)""".stripMargin
        ),
        database = None,
        domain = "mydb",
        table = "mytable",
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(WriteStrategyType.APPEND)
          )
        )
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

      val results = readTable()
      results should have size 5 // 3 original + 2 appended
      results should contain(("1", "Alice", 100)) // original
      results should contain(("1", "Alice_Dup", 999)) // appended duplicate
      results should contain(("4", "Diana", 400)) // appended new
    }

  }
}
