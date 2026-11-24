package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.utils.IncrementalVariable
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
      stmt.execute("CREATE OR REPLACE TABLE mydb.run_log(n INTEGER)")
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

    private def runLogCount(): Int = withDuckDb { conn =>
      val rs = conn.createStatement().executeQuery("SELECT count(*) AS c FROM mydb.run_log")
      rs.next()
      rs.getInt("c")
    }

    private def targetMaxUpdatedAt(): Int = withDuckDb { conn =>
      val rs = conn.createStatement().executeQuery("SELECT max(updated_at) AS m FROM mydb.target")
      rs.next()
      rs.getInt("m")
    }

    /** Writes the task YAML and, unless `yamlOnly`, its .sql file */
    private def writeTask(
      sql: String,
      presql: List[String] = Nil,
      postsql: List[String] = Nil,
      sqlFile: Option[String] = None,
      yamlOnly: Boolean = false
    ): Unit = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "mydb",
        table = "target",
        presql = presql,
        postsql = postsql,
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
      )
      val yml = mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task)
      storageHandler.write(yml, pathTask)
      if (yamlOnly) storageHandler.delete(pathSql)
      else storageHandler.write(sqlFile.getOrElse(task.getSql()), pathSql)
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

    it should "keep its tags in the loaded task, and the full load out of its dependencies" in {
      resetTables()
      writeTask(incrementalSql)
      val loaded = settings
        .schemaHandler()
        .tasks(reload = true)
        .find(t => t.domain == "mydb" && t.table == "target")
        .get
      loaded.getSql() should include("sl_incremental")
      loaded.getSql().toLowerCase should include("max(updated_at)")
      loaded.fullLoadSql() should not include "sl_incremental"
      loaded.fullLoadSql().toLowerCase should not include "max(updated_at)"

      val task = workflow().buildTask(TransformConfig(name = "mydb.target"))
      val noStreams =
        org.apache.spark.sql.catalyst.util.CaseInsensitiveMap(Map.empty[String, String])
      task.dependencies(noStreams).map(_.toLowerCase) should not contain "mydb.target"
      val forced = workflow().buildTask(
        TransformConfig(name = "mydb.target", options = Map("sl_incremental" -> "true"))
      )
      forced.dependencies(noStreams).map(_.toLowerCase) should not contain "mydb.target"
    }

    it should "render sl_incremental in the postsql of the .sql file" in {
      resetTables()
      writeTask(
        "SELECT 1",
        sqlFile = Some("""-- SL_SQL
            |SELECT id, updated_at FROM mydb.src
            |-- SL_POSTSQL
            |{% if not sl_incremental %}INSERT INTO mydb.run_log VALUES (1){% endif %}
            |""".stripMargin)
      )
      run().isSuccess shouldBe true
      runLogCount() shouldBe 1
      run().isSuccess shouldBe true
      runLogCount() shouldBe 1
      run(truncate = true).isSuccess shouldBe true
      runLogCount() shouldBe 2
    }

    it should "render the env vars of a .sql file that uses it at run time" in {
      resetTables()
      storageHandler.write(
        "version: 1\nenv:\n  mult: \"10\"\n",
        new Path(starlakeMetadataPath + "/env.sl.yml")
      )
      settings.schemaHandler().activeEnvVars(reload = true)
      writeTask(
        "SELECT 1",
        sqlFile = Some("""SELECT id, updated_at * {{mult}} AS updated_at FROM mydb.src
            |{% if sl_incremental %}
            |WHERE id > (SELECT max(id) FROM mydb.target)
            |{% endif %}""".stripMargin)
      )
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2)
      targetMaxUpdatedAt() shouldBe 200
      addSourceRow(3, 30)
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2, 3)
      targetMaxUpdatedAt() shouldBe 300
    }

    it should "render sl_incremental in the presql of the YAML" in {
      resetTables()
      writeTask(
        "SELECT id, updated_at FROM mydb.src",
        presql = List("{% if not sl_incremental %}INSERT INTO mydb.run_log VALUES (1){% endif %}")
      )
      run().isSuccess shouldBe true
      runLogCount() shouldBe 1
      run().isSuccess shouldBe true
      runLogCount() shouldBe 1
      run(truncate = true).isSuccess shouldBe true
      runLogCount() shouldBe 2
    }

    it should "render sl_incremental in the SQL of a task declared in YAML only" in {
      resetTables()
      writeTask(incrementalSql, yamlOnly = true)
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2)
      addSourceRow(3, 30)
      run().isSuccess shouldBe true
      targetIds() shouldBe List(1, 2, 3)
      run(truncate = true).isSuccess shouldBe true
      targetIds() shouldBe List(1, 2, 3)
    }

    it should "run the SQL given with --query instead of the one of the task" in {
      resetTables()
      writeTask(incrementalSql)
      workflow()
        .autoJob(
          TransformConfig(
            name = "mydb.target",
            query = Some("SELECT 100 AS id, 1000 AS updated_at")
          )
        )
        .isSuccess shouldBe true
      targetIds() shouldBe List(100)
    }

    it should "render presql and postsql with the table state of each output of --compile" in {
      resetTables()
      writeTask(
        incrementalSql,
        presql =
          List("{% if sl_incremental %}SELECT 'pre_delta'{% else %}SELECT 'pre_full'{% endif %}"),
        postsql =
          List("{% if sl_incremental %}SELECT 'post_delta'{% else %}SELECT 'post_full'{% endif %}")
      )
      // the target does not exist: the output for an existing target must not depend on it
      val (whenMissing, whenExists) =
        workflow().compileAutoJob(TransformConfig(name = "mydb.target")).get
      whenMissing should include("pre_full")
      whenMissing should include("post_full")
      whenMissing should not include "_delta"
      whenExists should include("pre_delta")
      whenExists should include("post_delta")
      whenExists should not include "_full"
      whenExists.toLowerCase should include("max(updated_at)")
    }

    it should "render each variant of the orchestrator statements with its table state" in {
      resetTables()
      // the statements are generated before any run: the target must not be checked
      withDuckDb(
        _.createStatement().execute("CREATE TABLE mydb.target(id INTEGER, updated_at INTEGER)")
      )
      writeTask(
        "SELECT 1",
        sqlFile = Some(
          s"""-- SL_PRESQL
             |{% if sl_incremental %}SELECT 'pre_delta'{% else %}SELECT 'pre_full'{% endif %}
             |-- SL_SQL
             |$incrementalSql
             |""".stripMargin
        )
      )
      val statements = workflow()
        .buildTask(TransformConfig(name = "mydb.target"))
        .buildListOfSQLStatements()
      statements.mainSqlIfExists.mkString("\n").toLowerCase should include("max(updated_at)")
      statements.mainSqlIfNotExists.mkString("\n").toLowerCase should not include "max(updated_at)"
      // a single presql for both variants: that of a first run
      statements.preSqls.mkString("\n") should include("pre_full")
    }

    /** Writes the task YAML as given, without .sql file */
    private def writeYaml(yaml: String): Unit = {
      storageHandler.write(yaml, pathTask)
      storageHandler.delete(pathSql)
    }

    private def taskYaml(head: String, sqlLine: String): String =
      s"""${head}version: 1
         |task:
         |  domain: mydb
         |  table: target
         |  writeStrategy:
         |    type: APPEND
         |  sink:
         |    connectionRef: test-duckdb
         |$sqlLine
         |""".stripMargin

    it should "keep working in a double-quoted and in a block scalar of the YAML" in {
      List(
        """  sql: "SELECT id, updated_at FROM mydb.src {% if sl_incremental %}WHERE updated_at > (SELECT max(updated_at) FROM mydb.target){% endif %}"""",
        """  sql: |
          |    SELECT id, updated_at FROM mydb.src
          |    {% if sl_incremental %}
          |    WHERE updated_at > (SELECT max(updated_at) FROM mydb.target)
          |    {% endif %}""".stripMargin
      ).foreach { sqlLine =>
        withClue(sqlLine) {
          resetTables()
          writeYaml(taskYaml("", sqlLine))
          run().isSuccess shouldBe true
          targetIds() shouldBe List(1, 2)
          addSourceRow(3, 30)
          run().isSuccess shouldBe true
          targetIds() shouldBe List(1, 2, 3)
        }
      }
    }

    it should "fail with a clear error on YAML that does not read once the variable is deferred" in {
      List(
        // the deferred expression is written back with single-quoted literals
        taskYaml(
          "",
          """  sql: 'SELECT id, updated_at FROM mydb.src {{ "WHERE id > 1" if sl_incremental else "" }}'"""
        ),
        // a set at YAML level is written back as a tag
        taskYaml(
          "{% set filter = sl_incremental %}\n",
          """  sql: "SELECT id, updated_at FROM mydb.src {% if filter %}WHERE id > 1{% endif %}""""
        )
      ).foreach { yaml =>
        withClue(yaml) {
          resetTables()
          writeYaml(yaml)
          val result = scala.util.Try(run()).flatten
          result.isFailure shouldBe true
          val message = result.failed.get.getMessage
          message should include("target.sl.yml")
          message should include("sl_incremental")
          message should include("double-quoted or block scalars")
        }
      }
    }

    it should "not derive sl_incremental from a forced table state on a run path" in {
      resetTables()
      writeTask(incrementalSql)
      // the target is missing: planning against an existing table must not make the run incremental
      val task = workflow().buildTask(TransformConfig(name = "mydb.target"))
      task.buildAllSQLQueriesMerged(None, Some(true)).toLowerCase should not include
      "max(updated_at)"
      val (whenMissing, whenExists) =
        workflow().compileAutoJob(TransformConfig(name = "mydb.target")).get
      whenMissing.toLowerCase should not include "max(updated_at)"
      whenExists.toLowerCase should include("max(updated_at)")
    }

    it should "give the branch handler a main SQL whose tables can be read" in {
      resetTables()
      writeTask(incrementalSql)
      withDuckDb(
        _.createStatement().execute("CREATE TABLE mydb.target(id INTEGER, updated_at INTEGER)")
      )
      val task = workflow().buildTask(TransformConfig(name = "mydb.target"))
      // the loaded SQL keeps the tags, on which the parser fails
      scala.util
        .Try(ai.starlake.sql.SQLUtils.extractTableNames(task.taskDesc.getSql()))
        .isFailure shouldBe
      true
      ai.starlake.sql.SQLUtils
        .extractTableNames(task.renderedMainSql())
        .map(_.toLowerCase) should contain allOf ("mydb.src", "mydb.target")
    }

    "IncrementalVariable.mentionedIn" should "match the variable as a whole word only" in {
      IncrementalVariable.mentionedIn(List("{% if sl_incremental %}x{% endif %}")) shouldBe true
      IncrementalVariable.mentionedIn(List("select 1", "{{ sl_incremental }}")) shouldBe true
      IncrementalVariable.mentionedIn(List("select sl_incremental_count from t")) shouldBe false
      IncrementalVariable.mentionedIn(Nil) shouldBe false
    }
  }
}
