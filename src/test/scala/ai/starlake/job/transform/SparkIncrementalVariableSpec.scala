package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

/** `sl_incremental` in a Spark task whose write is delegated to a JDBC task (DuckDB sink) */
class SparkIncrementalVariableSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_spark_incremental_variable.db"
  lazy val pathTask = new Path(starlakeMetadataPath + "/transform/mydb/target.sl.yml")
  lazy val pathSql = new Path(starlakeMetadataPath + "/transform/mydb/target.sql")

  lazy val configuration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "spark"
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

  new WithSettings(configuration) {

    private def withDuckDb[T](f: java.sql.Connection => T): T = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options)(f)
    }

    private def resetTables(): Unit = {
      withDuckDb { conn =>
        val stmt = conn.createStatement()
        stmt.execute("CREATE SCHEMA IF NOT EXISTS mydb")
        stmt.execute("DROP TABLE IF EXISTS mydb.target")
        stmt.execute("CREATE OR REPLACE TABLE mydb.run_log(n INTEGER)")
      }
      val session = sparkSession
      session.sql("CREATE SCHEMA IF NOT EXISTS mydb")
      session.sql("DROP TABLE IF EXISTS mydb.src")
      session.sql("CREATE TABLE mydb.src (id INT, updated_at INT) USING parquet")
      session.sql("INSERT INTO mydb.src VALUES (1, 10), (2, 20)")
    }

    private def addSourceRow(id: Int, updatedAt: Int): Unit =
      sparkSession.sql(s"INSERT INTO mydb.src VALUES ($id, $updatedAt)")

    private def targetIds(): List[Int] = withDuckDb { conn =>
      val rs = conn.createStatement().executeQuery("SELECT id FROM mydb.target ORDER BY id")
      val buf = scala.collection.mutable.ListBuffer[Int]()
      while (rs.next()) buf += rs.getInt("id")
      buf.toList
    }

    private def runLogCount(): Int = withDuckDb { conn =>
      val rs = conn.createStatement().executeQuery("SELECT count(*) AS c FROM mydb.run_log")
      rs.next()
      rs.getInt("c")
    }

    /** Writes the task, with its postsql in the YAML or in the .sql file */
    private def writeTask(sql: String, postsql: List[String], postsqlInSqlFile: Boolean): Unit = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "mydb",
        table = "target",
        connectionRef = Some("spark"),
        postsql = if (postsqlInSqlFile) Nil else postsql,
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
      )
      val yml = mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task)
      storageHandler.write(yml, pathTask)
      val sqlFile =
        if (postsqlInSqlFile)
          ("-- SL_SQL" :: task.getSql() :: "-- SL_POSTSQL" :: postsql).mkString("\n")
        else task.getSql()
      storageHandler.write(sqlFile, pathSql)
    }

    private def run(truncate: Boolean = false): scala.util.Try[String] =
      new IngestionWorkflow(storageHandler, settings.schemaHandler())
        .autoJob(TransformConfig(name = "mydb.target", truncate = truncate))

    behavior of "sl_incremental"

    List(false, true).foreach { postsqlInSqlFile =>
      val where = if (postsqlInSqlFile) "the .sql file" else "the YAML"
      it should s"follow the Spark task in its SQL and in the JDBC task it delegates to, postsql in $where" in {
        resetTables()
        writeTask(
          """SELECT id, updated_at FROM mydb.src
            |{% if sl_incremental %}
            |WHERE id > 2
            |{% endif %}""".stripMargin,
          // run by the JDBC task, after the Spark task created the target
          postsql = List("{% if not sl_incremental %}INSERT INTO mydb.run_log VALUES (1){% endif %}"),
          postsqlInSqlFile = postsqlInSqlFile
        )
        val first = run()
        first.failed.foreach(e => fail(e))
        targetIds() shouldBe List(1, 2)
        runLogCount() shouldBe 1

        addSourceRow(3, 30)
        run().isSuccess shouldBe true
        // APPEND of the full SELECT would duplicate 1 and 2: only the new row was added
        targetIds() shouldBe List(1, 2, 3)
        runLogCount() shouldBe 1

        // the JDBC task is not --truncate, the Spark task is
        run(truncate = true).isSuccess shouldBe true
        runLogCount() shouldBe 2
      }
    }
  }
}
