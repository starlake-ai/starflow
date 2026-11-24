package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

class DuckDbIncrementalVariableSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_incremental_variable.db"
  lazy val pathTask = new Path(starlakeMetadataPath + "/transform/mydb/target.sl.yml")
  lazy val pathSql = new Path(starlakeMetadataPath + "/transform/mydb/target.sql")

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

  val incrementalSql: String =
    """SELECT id, updated_at FROM mydb.src
      |{% if sl_incremental %}
      |WHERE updated_at > (SELECT max(updated_at) FROM mydb.target)
      |{% endif %}""".stripMargin

  new WithSettings(duckDbConfiguration) {

    private def withDuckDb[T](f: java.sql.Connection => T): T = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options)(f)
    }

    private def resetTables(): Unit = withDuckDb { conn =>
      val stmt = conn.createStatement()
      stmt.execute("CREATE SCHEMA IF NOT EXISTS mydb")
      stmt.execute("DROP TABLE IF EXISTS mydb.target")
      stmt.execute("DROP TABLE IF EXISTS mydb.presql_marker")
      stmt.execute("CREATE OR REPLACE TABLE mydb.src(id INTEGER, updated_at INTEGER)")
      stmt.execute("INSERT INTO mydb.src VALUES (1, 10), (2, 20)")
    }

    private def addSourceRow(id: Int, updatedAt: Int): Unit = withDuckDb { conn =>
      conn.createStatement().execute(s"INSERT INTO mydb.src VALUES ($id, $updatedAt)")
    }

    private def targetIds(): List[Int] = withDuckDb { conn =>
      val rs = conn.createStatement().executeQuery("SELECT id FROM mydb.target ORDER BY id")
      val buf = scala.collection.mutable.ListBuffer[Int]()
      while (rs.next()) buf += rs.getInt("id")
      buf.toList
    }

    private def writeTask(sql: String, presql: List[String] = Nil): Unit = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "mydb",
        table = "target",
        presql = presql,
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
      )
      val yml = mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task)
      storageHandler.write(yml, pathTask)
      storageHandler.write(task.getSql(), pathSql)
    }

    private def workflow() = new IngestionWorkflow(storageHandler, settings.schemaHandler())

    private def run(
      truncate: Boolean = false,
      options: Map[String, String] = Map.empty
    ): scala.util.Try[String] =
      workflow().autoJob(
        TransformConfig(name = "mydb.target", truncate = truncate, options = options)
      )

    "sl_incremental" should "be false on the first run and true on the next ones" in {
      resetTables()
      writeTask(incrementalSql)
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2)

      addSourceRow(3, 30)
      run().isSuccess shouldBe true
      // APPEND of the full SELECT would duplicate 1 and 2: only the new row was added
      targetIds() shouldBe List(1, 2, 3)
    }

    it should "be false under --truncate, which reloads the full SELECT" in {
      resetTables()
      writeTask(incrementalSql)
      run().isSuccess shouldBe true
      addSourceRow(3, 30)
      run(truncate = true).isSuccess shouldBe true
      targetIds() shouldBe List(1, 2, 3)
    }

    it should "follow the forced table state in both outputs of --compile" in {
      resetTables()
      writeTask(incrementalSql)
      val (whenMissing, whenExists) =
        workflow().compileAutoJob(TransformConfig(name = "mydb.target")).get
      whenMissing.toLowerCase should not include "max(updated_at)"
      whenExists.toLowerCase should include("max(updated_at)")
    }

    it should "ignore a value passed in --options" in {
      resetTables()
      writeTask(incrementalSql)
      run(options = Map("sl_incremental" -> "true")).isSuccess shouldBe true
      targetIds() shouldBe List(1, 2)
    }

    it should "stay false on the first run of a task with presql, which creates the target first" in {
      resetTables()
      writeTask(
        incrementalSql,
        presql = List("CREATE TABLE IF NOT EXISTS mydb.presql_marker AS SELECT 1 AS x")
      )
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2)
    }

    it should "not be computed for a task that does not use it" in {
      resetTables()
      writeTask("SELECT id, updated_at FROM mydb.src")
      val task = workflow().buildTask(TransformConfig(name = "mydb.target"))
      task.usesIncremental shouldBe false
      task.incrementalVars(None) shouldBe Map.empty
    }

    "AutoTask.mentionsIncremental" should "match the variable as a whole word only" in {
      AutoTask.mentionsIncremental(List("{% if sl_incremental %}x{% endif %}")) shouldBe true
      AutoTask.mentionsIncremental(List("select 1", "{{ sl_incremental }}")) shouldBe true
      AutoTask.mentionsIncremental(List("select sl_incremental_count from t")) shouldBe false
      AutoTask.mentionsIncremental(Nil) shouldBe false
    }
  }
}
