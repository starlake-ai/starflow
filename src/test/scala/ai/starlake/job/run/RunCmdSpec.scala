package ai.starlake.job.run

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.transform.AutoTask
import ai.starlake.lineage.TaskViewDependency
import ai.starlake.schema.model.{AutoTaskInfo, JdbcSink, WriteStrategy}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

class RunCmdSpec extends TestHelper {

  val pgConfiguration: Config = {
    val config = ConfigFactory.parseString("""
                                             |connectionRef: "test-pg"
                                             |""".stripMargin)
    config.withFallback(super.testConfiguration)
  }

  private def writeTask(domain: String, table: String, sql: String)(implicit
    settings: Settings,
    withSettings: WithSettings
  ): Unit = {
    val taskInfo = AutoTaskInfo(
      name = "",
      sql = Some(sql),
      database = None,
      domain = domain,
      table = table,
      sink = Some(JdbcSink(connectionRef = Some("test-pg")).toAllSinks()),
      python = None,
      writeStrategy = Some(WriteStrategy.Overwrite)
    )
    val yamlPath = new Path(starlakeMetadataPath + s"/transform/$domain/$table.sl.yml")
    val sqlPath = new Path(starlakeMetadataPath + s"/transform/$domain/$table.sql")
    val taskDef = withSettings.mapper
      .writer()
      .withAttribute(classOf[Settings], settings)
      .writeValueAsString(taskInfo)
    withSettings.storageHandler.write(taskDef, yamlPath)
    withSettings.storageHandler.write(taskInfo.getSql(), sqlPath)
  }

  new WithSettings(pgConfiguration) {
    "starlake run" should "execute a two-task chain in dependency order" in {
      val session = sparkSession
      import session.implicits._

      val usersDF = Seq(
        ("John", "Doe", 10),
        ("Sam", "Hal", 20)
      ).toDF("firstname", "lastname", "age")
      val usersOptions =
        settings.appConfig.connections("test-pg").options + ("dbtable" -> "myusers.myusers")
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        usersOptions
      ) { conn =>
        val statement = conn.createStatement()
        statement.execute("CREATE SCHEMA IF NOT EXISTS myusers")
        statement.execute(
          "CREATE TABLE IF NOT EXISTS myusers.myusers(firstname TEXT, lastname TEXT, age INT)"
        )
      }
      usersDF.write.format("jdbc").options(usersOptions).mode(SaveMode.Overwrite).save()

      writeTask(
        "myusers",
        "userout",
        "select firstname, lastname from myusers.myusers where age <= 20"
      )
      writeTask(
        "myusers",
        "userout2",
        "select firstname from myusers.userout"
      )

      // reload: the cached handler may already hold the job list read before the task files existed
      val schemaHandler = settings.schemaHandler(reload = true)

      // The ordering assertion below cannot distinguish a real edge from two independent nodes
      // that happen to finish in submission order, so assert the edge itself.
      val tasks =
        AutoTask.unauthenticatedTasks(reload = false)(
          settings,
          settings.storageHandler(),
          schemaHandler
        )
      val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)
      val dag = DagBuilder.build(deps, loadTables = Set.empty) match {
        case Right(d)    => d
        case Left(cycle) => fail(s"unexpected cycle: ${cycle.mkString(" -> ")}")
      }
      dag.parents("myusers.userout2") should contain("myusers.userout")
      dag.parents("myusers.userout") should not contain "myusers.userout2"

      val result = RunCmd.runProject(RunConfig(), schemaHandler)

      result.exitCode shouldBe 0
      val summary = result.summary.getOrElse(fail("expected a summary"))
      val taskIds = summary.results
        .filter(_.node.typ == RunNodeType.Task)
        .map(_.node.id)
      taskIds should contain allOf ("myusers.userout", "myusers.userout2")
      taskIds.indexOf("myusers.userout") should be < taskIds.indexOf("myusers.userout2")
      summary.byId("myusers.userout").status shouldBe NodeStatus.Succeeded
      summary.byId("myusers.userout2").status shouldBe NodeStatus.Succeeded

      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-pg").options
      ) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery("select count(*) from myusers.userout2")
        rs.next() shouldBe true
        rs.getInt(1) shouldBe 2
      }
    }

    it should "exit with code 2 on a cyclic graph" in {
      writeTask("cyclic", "alpha", "select * from cyclic.beta")
      writeTask("cyclic", "beta", "select * from cyclic.alpha")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result = RunCmd.runProject(RunConfig(), schemaHandler)

      result.exitCode shouldBe 2
      result.errorMessage.getOrElse("") should include("Cycle detected")
    }
  }
}
