package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

class DuckDbOverwriteSchemaSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_overwrite_schema.db"
  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sl.yml")
  lazy val pathSqlBusiness = new Path(starlakeMetadataPath + "/transform/mydb/mytable.sql")

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
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

    private def runOverwrite(
      sql: String,
      attributes: List[TableAttribute] = Nil
    ): scala.util.Try[String] = {
      val businessTask = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "mydb",
        table = "mytable",
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE))),
        attributes = attributes
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

    "OVERWRITE into an existing table with reordered SELECT columns" should
    "write every value into the column of the same name" in {
      setupInitialData()

      // The SELECT emits the same columns as the target but in a different
      // order (name first, id second; both VARCHAR, so arity and types line
      // up positionally). A positional INSERT would succeed and silently
      // write names into id and ids into name.
      runOverwrite(
        """SELECT * FROM (VALUES
            |  ('Xavier', '10', 1000),
            |  ('Yolanda', '20', 2000)
            |) AS t(name, id, amount)""".stripMargin
      )

      val results = readTable()
      results should have size 2
      results should contain(("10", "Xavier", 1000))
      results should contain(("20", "Yolanda", 2000))
    }

    "OVERWRITE with a column the table does not have and no declared attributes" should
    "fail loudly and leave the existing data untouched" in {
      setupInitialData()

      val result = runOverwrite(
        """SELECT * FROM (VALUES
            |  ('10', 'Xavier', 1000, 'x@x.com'),
            |  ('20', 'Yolanda', 2000, 'y@y.com')
            |) AS t(id, name, amount, email)""".stripMargin
      )

      result.isFailure shouldBe true
      // the error must name the offending column and tell the user exactly
      // how to evolve the table schema
      val message = result.failed.get.getMessage
      message should include("email")
      message should include("starlake transform --name mydb.mytable --sync-apply")
      // the pre-existing rows must survive the failed run: TRUNCATE and the
      // failed INSERT share a transaction and roll back together
      val results = readTable()
      results should have size 2
      results should contain(("1", "Alice", 100))
      results should contain(("2", "Bob", 200))
    }

    "OVERWRITE with a new mid-SELECT column and declared typed attributes" should
    "sync the schema and land every value in the column of the same name" in {
      setupInitialData()

      // email is added in the MIDDLE of the SELECT. The schema sync appends
      // it at the END of the table; only a name-based INSERT can line the
      // two up.
      val result = runOverwrite(
        """SELECT * FROM (VALUES
            |  ('10', 'x@x.com', 'Xavier', 1000),
            |  ('20', 'y@y.com', 'Yolanda', 2000)
            |) AS t(id, email, name, amount)""".stripMargin,
        attributes = List(
          TableAttribute(name = "id", `type` = "string"),
          TableAttribute(name = "email", `type` = "string"),
          TableAttribute(name = "name", `type` = "string"),
          TableAttribute(name = "amount", `type` = "int")
        )
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
    }
  }
}
