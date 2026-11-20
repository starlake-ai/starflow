package ai.starlake.lineage

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.generator.DagGenerateCmd
import ai.starlake.schema.model.*
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.Path

class TaskViewDependencySpec extends TestHelper {

  "TaskViewDependency.dependencies" should "emit table name in sink and schedule in cron for TABLE-type entries" in {
    new WithSettings() {
      // Deploy a LOAD domain 'sales' with two tables:
      // - 'orders' carries a table-level schedule
      // - 'customers' has no schedule at all
      val domainConfigPath = new Path(DatasetArea.load, "sales/_config.sl.yml")
      storageHandler.mkdirs(domainConfigPath.getParent)
      storageHandler.write(
        s"""---
           |version: 1
           |load:
           |  name: "sales"
           |  metadata:
           |    directory: "$starlakeTestRoot/incoming/sales"
           |""".stripMargin,
        domainConfigPath
      )
      storageHandler.write(
        """---
          |version: 1
          |table:
          |  name: "orders"
          |  pattern: "orders.*.csv"
          |  metadata:
          |    format: "DSV"
          |    withHeader: true
          |    separator: ","
          |    schedule: "0 2 * * *"
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      required: false
          |""".stripMargin,
        new Path(DatasetArea.load, "sales/orders.sl.yml")
      )
      storageHandler.write(
        """---
          |version: 1
          |table:
          |  name: "customers"
          |  pattern: "customers.*.csv"
          |  metadata:
          |    format: "DSV"
          |    withHeader: true
          |    separator: ","
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      required: false
          |""".stripMargin,
        new Path(DatasetArea.load, "sales/customers.sl.yml")
      )

      // Transform task selecting from both load tables so its parents resolve
      // as TABLE-type dependencies (they exist as domain tables, not as tasks).
      val revenueTask = AutoTaskInfo(
        name = "revenue",
        sql = Some(
          "SELECT o.id FROM sales.orders o JOIN sales.customers c ON o.id = c.id"
        ),
        database = None,
        domain = "kpi",
        table = "revenue",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val revenueTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, revenueTask))
      val revenueTaskPath = new Path(starlakeMetadataPath + "/transform/kpi/revenue.sl.yml")
      storageHandler.mkdirs(revenueTaskPath.getParent)
      storageHandler.write(revenueTaskDef, revenueTaskPath)

      val transformConfigDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, AutoJobInfo("", Nil)))
      storageHandler.write(
        transformConfigDef,
        new Path(starlakeMetadataPath + "/transform/kpi/_config.sl.yml")
      )

      val schemaHandler = settings.schemaHandler()
      val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
      val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)

      val tableDeps = deps.filter(_.typ == TaskViewDependency.TABLE_TYPE)
      tableDeps.map(_.name) should contain theSameElementsAs List(
        "sales.orders",
        "sales.customers"
      )

      // Scenario 1: table with a schedule -> sink = table name, cron = schedule
      val ordersDep = tableDeps
        .find(_.name == "sales.orders")
        .getOrElse(fail("expected a TABLE-type dependency named 'sales.orders'"))
      ordersDep.sink shouldBe Some("sales.orders")
      ordersDep.cron shouldBe Some("0 2 * * *")

      // Scenario 2: table without a schedule -> sink = table name, cron = "None"
      val customersDep = tableDeps
        .find(_.name == "sales.customers")
        .getOrElse(fail("expected a TABLE-type dependency named 'sales.customers'"))
      customersDep.sink shouldBe Some("sales.customers")
      customersDep.cron shouldBe Some("None")

      // Scenario 4 (non-regression): TASK-type entries keep their own sink/cron
      val taskDeps = deps.filter(_.typ == TaskViewDependency.TASK_TYPE)
      taskDeps should not be empty
      taskDeps.foreach { dep =>
        dep.name shouldBe "kpi.revenue"
        dep.sink shouldBe Some("kpi.revenue")
        dep.cron shouldBe Some("None")
      }
    }
  }

  /** A project that reads tables declared in metadata/external, such as the tables that another
    * project loads (#1984)
    */
  private abstract class ExternalTablesProject extends WithSettings() {
    def write(path: Path, content: String): Unit = {
      storageHandler.mkdirs(path.getParent)
      storageHandler.write(content, path)
    }

    def domain(area: Path, name: String, metadata: String = ""): Unit =
      write(
        new Path(area, s"$name/_config.sl.yml"),
        s"""---
           |version: 1
           |load:
           |  name: "$name"
           |  metadata:
           |    directory: "$starlakeTestRoot/incoming/$name"
           |$metadata""".stripMargin
      )

    def table(area: Path, domain: String, name: String, metadata: String = ""): Unit =
      write(
        new Path(area, s"$domain/$name.sl.yml"),
        s"""---
           |version: 1
           |table:
           |  name: "$name"
           |  pattern: "$name.*.csv"
           |  metadata:
           |    format: "DSV"
           |$metadata
           |  attributes:
           |    - name: "id"
           |      type: "string"
           |""".stripMargin
      )

    private def yaml(value: AnyRef): String =
      mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(value)

    private def task(name: String, sql: String, dagRef: Option[String]): Unit = {
      val info = AutoTaskInfo(
        name = name,
        sql = Some(sql),
        database = None,
        domain = "kpi",
        table = name,
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        dagRef = dagRef,
        attributes = List(TableAttribute("total", "long"))
      )
      write(
        new Path(starlakeMetadataPath + s"/transform/kpi/$name.sl.yml"),
        yaml(TaskDesc(latestSchemaVersion, info))
      )
    }

    /** The task kpi.report, that reads the tables, and kpi.summary, that reads kpi.report */
    def tasks(dagRef: Option[String] = None): Unit = {
      task(
        "report",
        """SELECT a.id FROM other_db.neighbour.addresses a
          |JOIN clients c ON a.id = c.id
          |JOIN partner.accounts p ON a.id = p.id
          |JOIN partner.contacts k ON a.id = k.id
          |JOIN elsewhere.missing m ON a.id = m.id""".stripMargin,
        dagRef
      )
      task("summary", "SELECT COUNT(*) AS total FROM kpi.report", None)
      write(
        new Path(starlakeMetadataPath + "/transform/kpi/_config.sl.yml"),
        yaml(TransformDesc(latestSchemaVersion, AutoJobInfo("", Nil)))
      )
    }

    /** Generates the DAG of kpi.report with a template that writes its dependencies, as the DAG
      * templates read them, and returns the parents of kpi.report
      */
    def dagParents(dagName: String, options: String = ""): Map[String, JsonNode] = {
      val template = new Path(starlakeTestRoot, "dependencies.py.j2")
      write(template, "{{ context.dependencies }}")
      write(
        new Path(DatasetArea.dags, s"$dagName.sl.yml"),
        s"""---
           |version: 1
           |dag:
           |  comment: "dependencies"
           |  template: "$template"
           |  filename: "{{domain}}_$dagName.py"
           |$options""".stripMargin
      )
      tasks(dagRef = Some(dagName))
      val schemaHandler = settings.schemaHandler()
      // get: a failure gives its cause
      DagGenerateCmd.run(Array("--tasks").toIndexedSeq, schemaHandler).get

      val generated = new Path(new Path(DatasetArea.build, "dags"), s"kpi_$dagName.py")
      val nodes = new ObjectMapper().readTree(storageHandler.read(generated))
      nodes.size() shouldBe 1
      val report = nodes.get(0)
      report.at("/data/name").asText() shouldBe "kpi.report"
      val children = report.get("children")
      (0 until children.size())
        .map(children.get(_).get("data"))
        .map(data => data.get("name").asText() -> data)
        .toMap
    }

    // Another project loads the domain 'neighbour', with the schedule and the freshness of its
    // domain, and a table that has its own
    domain(
      DatasetArea.external,
      "neighbour",
      """    schedule: "0 5 * * *"
        |    freshness:
        |      error: "2 hours"
        |""".stripMargin
    )
    table(
      DatasetArea.external,
      "neighbour",
      "addresses",
      """    schedule: "0 6 * * *"
        |    freshness:
        |      error: "1 hour"""".stripMargin
    )
    table(DatasetArea.external, "neighbour", "clients")
    // This project loads the domain 'partner', which has a schedule and a freshness. Another
    // table of a domain of that name, without them, is declared in metadata/external, and so is
    // the loaded table, with another schedule
    domain(
      DatasetArea.load,
      "partner",
      """    schedule: "0 3 * * *"
        |    freshness:
        |      error: "3 hours"
        |""".stripMargin
    )
    table(DatasetArea.load, "partner", "contacts")
    domain(DatasetArea.external, "partner")
    table(DatasetArea.external, "partner", "accounts")
    table(DatasetArea.external, "partner", "contacts", """    schedule: "0 9 * * *"""")
    // autoExportSchema writes the table of a task into metadata/external, with other columns
    domain(DatasetArea.external, "kpi")
    table(DatasetArea.external, "kpi", "report")
  }

  "TaskViewDependency.dependencies" should "resolve the tables declared in metadata/external like the loaded ones (#1984)" in {
    new ExternalTablesProject() {
      tasks()
      val schemaHandler = settings.schemaHandler()
      val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
      val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)

      // The task depends on each table, under the name <domain>.<table>, whatever the number of
      // parts of the reference. The table that no project declares stays unknown.
      deps
        .filter(_.name == "kpi.report")
        .map(dep =>
          (dep.parentRef, dep.parent, dep.parentTyp)
        ) should contain theSameElementsAs List(
        ("other_db.neighbour.addresses", "neighbour.addresses", TaskViewDependency.TABLE_TYPE),
        ("clients", "neighbour.clients", TaskViewDependency.TABLE_TYPE),
        ("partner.accounts", "partner.accounts", TaskViewDependency.TABLE_TYPE),
        ("partner.contacts", "partner.contacts", TaskViewDependency.TABLE_TYPE),
        ("elsewhere.missing", "", TaskViewDependency.UNKNOWN_TYPE)
      )
      // A task comes before the table that metadata/external declares under its name
      deps
        .filter(_.name == "kpi.summary")
        .map(dep => (dep.parentRef, dep.parent, dep.parentTyp)) shouldBe List(
        ("kpi.report", "kpi.report", TaskViewDependency.TASK_TYPE)
      )

      // Each table gets the schedule and the freshness of its declaration, or of its domain. The
      // tables of the other tests of this spec are in the same project.
      val parents = deps.filter(_.name == "kpi.report").map(_.parent).toSet
      val tables = deps
        .filter(dep => dep.typ == TaskViewDependency.TABLE_TYPE && parents.contains(dep.name))
        .map(dep => dep.name -> (dep.sink, dep.cron, dep.freshness, dep.external))
        .toMap
      tables shouldBe Map(
        "neighbour.addresses" -> (Some("neighbour.addresses"), Some("0 6 * * *"), 3600L, true),
        "neighbour.clients"   -> (Some("neighbour.clients"), Some("0 5 * * *"), 7200L, true),
        // Neither the schedule nor the freshness of the loaded domain of the same name
        "partner.accounts" -> (Some("partner.accounts"), Some("None"), 0L, true),
        // The loaded table comes before its declaration in metadata/external
        "partner.contacts" -> (Some("partner.contacts"), Some("0 3 * * *"), 10800L, false)
      )
    }
  }

  it should "show the columns of a task rather than those that metadata/external declares for its table (#1984)" in {
    new ExternalTablesProject() {
      tasks()
      val schemaHandler = settings.schemaHandler()
      val diagram = new AutoTaskDependencies(settings, schemaHandler, storageHandler)
        .jobAsDiagram(AutoTaskDependenciesConfig(tasks = Some(List("kpi.summary", "kpi.report"))))
      def columns(id: String): List[String] =
        diagram.items
          .find(_.id == id)
          .getOrElse(fail(s"expected an item '$id'"))
          .columns
          .map(_.name)
      columns("kpi.report") shouldBe List("total")
      columns("neighbour.addresses") shouldBe List("id")
    }
  }

  "dag-generate" should "make a transform depend on the tables declared in metadata/external (#1984)" in {
    new ExternalTablesProject() {
      val children = dagParents("dependencies").map { case (name, data) =>
        name -> (
          data.get("typ").asText(),
          data.get("sink").asText(),
          data.get("cron").asText(),
          data.get("freshness").asLong(),
          data.get("external").asBoolean()
        )
      }
      // The table that no project declares is not a dataset of the DAG
      children shouldBe Map(
        "neighbour.addresses" -> ("table", "neighbour.addresses", "0 6 * * *", 3600L, true),
        "neighbour.clients"   -> ("table", "neighbour.clients", "0 5 * * *", 7200L, true),
        "partner.accounts"    -> ("table", "partner.accounts", "None", 0L, true),
        "partner.contacts"    -> ("table", "partner.contacts", "0 3 * * *", 10800L, false)
      )
    }
  }

  it should "leave the tables declared in metadata/external out of a DAG that runs its dependencies first (#1984)" in {
    new ExternalTablesProject() {
      // Such a DAG runs its table dependencies as loads, and this project does not load them
      dagParents(
        "dependencies_first",
        """  options:
          |    run_dependencies_first: "True"
          |""".stripMargin
      ).keySet shouldBe Set("partner.contacts")
    }
  }

  "TaskViewDependency.dependencies" should "leave out the tables of metadata/external when a file there cannot be read (#1984)" in {
    new ExternalTablesProject() {
      tasks()
      val broken = new Path(DatasetArea.external, "neighbour/broken.sl.yml")
      write(broken, "table: [")
      try {
        val schemaHandler = settings.schemaHandler()
        val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
        val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)
        // As before #1984: the loaded tables only
        deps
          .filter(_.name == "kpi.report")
          .map(dep => dep.parentRef -> dep.parentTyp)
          .toMap shouldBe Map(
          "other_db.neighbour.addresses" -> TaskViewDependency.UNKNOWN_TYPE,
          "clients"                      -> TaskViewDependency.UNKNOWN_TYPE,
          "partner.accounts"             -> TaskViewDependency.UNKNOWN_TYPE,
          "partner.contacts"             -> TaskViewDependency.TABLE_TYPE,
          "elsewhere.missing"            -> TaskViewDependency.UNKNOWN_TYPE
        )
      } finally {
        // The tests of this spec share their project
        storageHandler.delete(broken)
      }
    }
  }
}
