package ai.starlake.job.run

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.Main
import ai.starlake.job.transform.AutoTask
import ai.starlake.lineage.TaskViewDependency
import ai.starlake.schema.model.{AutoTaskInfo, JdbcSink, WriteStrategy}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode

import java.io.ByteArrayOutputStream
import scala.util.{Success, Try}

class RunCmdSpec extends TestHelper {

  val pgConfiguration: Config = {
    val config = ConfigFactory.parseString("""
                                             |connectionRef: "test-pg"
                                             |""".stripMargin)
    config.withFallback(super.testConfiguration)
  }

  /** Writes a one-table load domain whose files are picked up from the stage area. */
  private def writeLoadTable(domain: String, table: String, rename: Option[String] = None)(implicit
    withSettings: WithSettings
  ): Unit = {
    val domainYaml =
      s"""version: 1
         |load:
         |  name: "$domain"
         |  metadata:
         |    format: "DSV"
         |    separator: ","
         |    withHeader: true
         |    writeStrategy:
         |      type: "OVERWRITE"
         |""".stripMargin
    // triple-quoted strings do not process escapes, hence the explicit newline concatenation
    val renameLine = rename.map("  rename: \"" + _ + "\"\n").getOrElse("")
    val tableYaml =
      s"""version: 1
         |table:
         |  name: "$table"
         |$renameLine  pattern: "$table-.*.csv"
         |  attributes:
         |    - name: "id"
         |      type: "string"
         |    - name: "name"
         |      type: "string"
         |""".stripMargin
    withSettings.storageHandler
      .write(domainYaml, new Path(starlakeLoadPath + s"/$domain/_config.sl.yml"))
    withSettings.storageHandler
      .write(tableYaml, new Path(starlakeLoadPath + s"/$domain/$table.sl.yml"))
  }

  private def stageFile(domain: String, fileName: String, content: String)(implicit
    settings: Settings,
    withSettings: WithSettings
  ): Unit = {
    val stagePath = DatasetArea.stage(domain)
    withSettings.storageHandler.mkdirs(stagePath)
    withSettings.storageHandler.write(content, new Path(stagePath, fileName))
  }

  private def fileNamesIn(path: Path): List[String] =
    Option(new java.io.File(path.toUri.getPath).listFiles())
      .map(_.toList.map(_.getName).sorted)
      .getOrElse(Nil)

  private def rowCount(table: String)(implicit settings: Settings): Try[Int] =
    Try {
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-pg").options
      ) { conn =>
        val rs = conn.createStatement().executeQuery(s"select count(*) from $table")
        rs.next()
        rs.getInt(1)
      }
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

    // PINS KNOWN-DEFERRED BEHAVIOR, NOT DESIRED BEHAVIOR.
    // SchemaHandler.domains returns its cached, unfiltered list once _domains is populated, which
    // runProject does when it builds the load-table map. IngestionWorkflow.domainsToWatch therefore
    // ignores the domains filter that a load node passes, and the node scans every domain's stage
    // area filtered only by table name. See the scaladoc on RunCmd.executeNode: the defect is
    // pre-existing platform behavior shared with LoadCmd and is deferred until both can be fixed
    // together. When someone fixes the caching, this test must fail so they flip it deliberately.
    it should "(pinned, deferred) let one load node ingest another domain's staged file" in {
      writeLoadTable("dom1", "things")
      writeLoadTable("dom2", "things")
      stageFile("dom1", "things-1.csv", "id,name\n1,one\n")
      stageFile("dom2", "things-2.csv", "id,name\n2,two\n3,three\n")
      // Only dom1.things is referenced by a transform, so it is the only load node in the DAG.
      writeTask("agg", "fromdom1", "select id, name from dom1.things")

      val schemaHandler = settings.schemaHandler(reload = true)

      // Capture stdout to check that only the summary table lands there. Utils.printOut, which
      // ingest uses for its human-readable block, is gated on Main.cliMode: off under the test
      // runner, on under the CLI, so turn it on to reproduce what a user would see.
      val capturedStdout = new ByteArrayOutputStream()
      val previousCliMode = Main.cliMode
      Main.cliMode = true
      // parallelism 1: with a shared stage scan, concurrent load nodes would race for the files
      val result =
        try {
          Console.withOut(capturedStdout) {
            RunCmd.runProject(RunConfig(parallelism = Some(1)), schemaHandler)
          }
        } finally Main.cliMode = previousCliMode
      val stdout = capturedStdout.toString("UTF-8")

      val summary = result.summary.getOrElse(fail("expected a summary"))
      val dom1Stage = fileNamesIn(DatasetArea.stage("dom1"))
      val dom2Stage = fileNamesIn(DatasetArea.stage("dom2"))

      // The summary table is the machine-consumable output and must be on stdout ...
      stdout should include("SUCCEEDED")
      stdout should include("dom1.things")
      // ... while ingest's "Loading / Format / File(s) / Table" block must not be
      stdout should not include "Table: dom1.things"
      stdout should not include "Format: DSV"

      result.exitCode shouldBe 0
      summary.byId("dom1.things").node.typ shouldBe RunNodeType.LoadTable
      summary.byId("dom1.things").status shouldBe NodeStatus.Succeeded
      // dom2.things is referenced by no task, so it is not a node at all
      summary.byId.keySet should not contain "dom2.things"

      dom1Stage shouldBe empty
      // THE PINNED DEFECT: dom2's file was consumed even though no node targeted dom2
      dom2Stage shouldBe empty
      rowCount("dom2.things") shouldBe Success(2)
      rowCount("dom1.things") shouldBe Success(1)
    }

    it should "load a renamed table using its declared name" in {
      writeLoadTable("dom3", "raw", rename = Some("renamed"))
      stageFile("dom3", "raw-1.csv", "id,name\n1,one\n")
      // Lineage names the table by its final name; the load path filters on the declared one
      writeTask("agg", "fromdom3", "select id, name from dom3.renamed")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result = RunCmd.runProject(RunConfig(parallelism = Some(1)), schemaHandler)

      val summary = result.summary.getOrElse(fail("expected a summary"))
      result.exitCode shouldBe 0
      summary.byId("dom3.renamed").node.typ shouldBe RunNodeType.LoadTable
      summary.byId("dom3.renamed").status shouldBe NodeStatus.Succeeded
      // Before the fix the node passed the final name "renamed" to a filter matching declared
      // names, matched nothing, and reported success having ingested nothing.
      rowCount("dom3.renamed") shouldBe Success(1)
    }

    it should "execute only the selected task and its upstreams" in {
      // Domain "pick", not "sel": jsqlparser reads a leading SEL as Teradata's SELECT abbreviation,
      // so "from sel.first" fails to parse and the task would fail for a reason unrelated to
      // selection.
      writeTask("pick", "first", "select 1 as n")
      writeTask("pick", "second", "select n from pick.first")
      writeTask("pick", "third", "select n from pick.second")
      writeTask("pick", "unrelated", "select 2 as n")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result =
        RunCmd.runProject(
          RunConfig(parallelism = Some(1), select = Seq("+pick.second")),
          schemaHandler
        )

      result.exitCode shouldBe 0
      val executed = result.summary
        .getOrElse(fail("expected a summary"))
        .results
        .filter(_.node.typ == RunNodeType.Task)
        .map(_.node.id)
        .toSet
      executed shouldBe Set("pick.first", "pick.second")
    }

    it should "agree with the dry-run plan for the same selector" in {
      // Spec section 13, acceptance criterion 2: --select +some.table executes exactly that task
      // and its upstreams, verified against --dry-run output.
      writeTask("acc", "first", "select 1 as n")
      writeTask("acc", "second", "select n from acc.first")
      writeTask("acc", "other", "select 3 as n")

      val schemaHandler = settings.schemaHandler(reload = true)
      val config = RunConfig(parallelism = Some(1), select = Seq("+acc.second"))

      val planned = new ByteArrayOutputStream()
      val dryResult =
        Console.withOut(planned) {
          RunCmd.runProject(config.copy(dryRun = true), schemaHandler)
        }
      val plan = planned.toString("UTF-8")

      dryResult.exitCode shouldBe 0
      dryResult.summary shouldBe None
      plan should include("acc.first")
      plan should include("acc.second")
      (plan should not).include("acc.other")

      val runResult = RunCmd.runProject(config, schemaHandler)
      val executed = runResult.summary
        .getOrElse(fail("expected a summary"))
        .results
        .filter(_.node.typ == RunNodeType.Task)
        .map(_.node.displayName)
        .toSet
      executed shouldBe Set("acc.first", "acc.second")
      executed.foreach(name => plan should include(name))
    }

    it should "not execute an excluded task" in {
      // Independent tasks on purpose. The ordering property of exclusion -- that excluding an
      // interior node keeps its neighbours ordered -- is asserted as a DAG edge in RunDagSpec,
      // which is the only place it can fail for the right reason: with the scheduler's
      // alphabetical tie-break, an execution-order assertion here would pass either way.
      writeTask("exc", "a", "select 1 as n")
      writeTask("exc", "b", "select 2 as n")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result =
        RunCmd.runProject(
          RunConfig(parallelism = Some(1), select = Seq("exc.*"), exclude = Seq("exc.a")),
          schemaHandler
        )

      result.exitCode shouldBe 0
      val executed = result.summary
        .getOrElse(fail("expected a summary"))
        .results
        .filter(_.node.typ == RunNodeType.Task)
        .map(_.node.id)
      executed should contain("exc.b")
      executed should not contain "exc.a"
    }

    it should "exit with code 3 when the selection matches nothing" in {
      writeTask("empty", "task", "select 1 as n")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result =
        RunCmd.runProject(RunConfig(select = Seq("nope.nothing")), schemaHandler)

      result.exitCode shouldBe 3
      result.summary shouldBe None
      result.errorMessage.getOrElse("") should include("nope.nothing")
    }

    it should "exit with code 2 on a malformed selector" in {
      writeTask("bad", "task", "select 1 as n")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result =
        RunCmd.runProject(RunConfig(select = Seq("a.b.c")), schemaHandler)

      result.exitCode shouldBe 2
      result.errorMessage.getOrElse("") should include("a.b.c")
    }

    it should "execute nothing under --dry-run" in {
      writeLoadTable("dry", "things")
      stageFile("dry", "things-1.csv", "id,name\n1,one\n")
      writeTask("dry", "fromthings", "select id, name from dry.things")

      val schemaHandler = settings.schemaHandler(reload = true)
      val result =
        Console.withOut(new ByteArrayOutputStream()) {
          RunCmd.runProject(RunConfig(parallelism = Some(1), dryRun = true), schemaHandler)
        }

      result.exitCode shouldBe 0
      // The staged file is still waiting: no load node ran.
      fileNamesIn(DatasetArea.stage("dry")) shouldBe List("things-1.csv")
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
