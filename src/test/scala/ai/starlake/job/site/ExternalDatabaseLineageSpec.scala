package ai.starlake.job.site

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.lineage.ColLineage
import better.files.File

/** Tables cited with their database, `database.schema.table`, in the SQL that the column lineage
  * reads (#2039), and tables of metadata/external read from the database that the external domain
  * declares (#2038), which writes such names.
  *
  * The fixture copy declares an external domain `shared_ds` whose `database` is the variable
  * `producer_project_id` (`proj-b`), and a job `exports` whose tasks run on a BigQuery connection
  * of the project `proj-a`: they read `shared_ds.orders` with a two-part name, with a three-part
  * name, and through a `SELECT *` over a three-part name, and the load table `sales.customers`. The
  * site is built offline: the credentials point to a missing file.
  */
class ExternalDatabaseLineageSpec extends IntegrationTestBase with SiteFixture {
  import ExternalDatabaseLineageSpec.*

  override lazy val fixtureRoot: File = {
    val tmp = File.newTemporaryDirectory("sl-site-external-database-")
    File("src/test/resources/site/fixture").copyTo(tmp)(File.CopyOptions(overwrite = true))
    val app = tmp / "metadata" / "application.sl.yml"
    app.overwrite(
      app.contentAsString + "    bq:\n      type: bigquery\n      options:\n" +
      "        projectId: \"proj-a\"\n        location: \"EU\"\n" +
      "        authType: \"APPLICATION_DEFAULT\"\n"
    )
    (tmp / "metadata" / "env.sl.yml").overwrite(
      "version: 1\nenv:\n  producer_project_id: \"proj-b\"\n"
    )
    val shared = (tmp / "metadata" / "external" / "shared_ds").createDirectories()
    (shared / "_config.sl.yml").overwrite(
      "version: 1\nload:\n  name: \"shared_ds\"\n  database: \"{{producer_project_id}}\"\n"
    )
    (shared / "orders.sl.yml").overwrite(
      """version: 1
        |table:
        |  name: "orders"
        |  pattern: "orders.*"
        |  attributes:
        |    - name: "id"
        |      type: "long"
        |    - name: "amount"
        |      type: "decimal"
        |""".stripMargin
    )
    val jobDir = (tmp / "metadata" / "transform" / "exports").createDirectories()
    for ((name, sql) <- tasks) {
      (jobDir / s"$name.sql").overwrite(sql)
      (jobDir / s"$name.sl.yml").overwrite(
        "version: 1\ntask:\n  connectionRef: \"bq\"\n  writeStrategy:\n    type: \"OVERWRITE\"\n"
      )
    }
    tmp
  }

  override protected def afterAll(): Unit = {
    fixtureRoot.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  private def offline[T](envs: (String, String)*)(fun: => T): T =
    withEnvs(
      Seq(
        "SL_ROOT"                        -> fixtureRoot.pathAsString,
        "SL_ENV"                         -> "",
        "GOOGLE_APPLICATION_CREDENTIALS" -> (fixtureRoot / "no-credentials.json").pathAsString
      ) ++ envs: _*
    )(fun)

  // The tasks of the job exports, which run on BigQuery. The other tasks of the fixture run on
  // DuckDB, where an application database with a dash, unquoted, would not parse.
  private def lineageWarnings(model: SiteModel): Map[String, List[String]] =
    model.nodes.collect {
      case (id, node) if id.startsWith("transform:exports.") =>
        id -> node.warnings.filter(_.startsWith("Column lineage failed"))
    }

  private def edgesOf(model: SiteModel, task: String): List[SiteColumnEdge] =
    model.columnEdges.filter(_.task == s"transform:exports.$task")

  private def edges(from: String, task: String, columns: String*): List[SiteColumnEdge] =
    columns.toList.map { column =>
      SiteColumnEdge(
        s"$from#$column",
        s"transform:exports.$task#$column",
        s"transform:exports.$task"
      )
    }

  private def checkColumnLineage(model: SiteModel): Unit = {
    lineageWarnings(model).filter(_._2.nonEmpty) shouldBe empty
    for (task <- List("orders_two_part", "orders_three_part"))
      edgesOf(model, task) should contain theSameElementsAs
      edges("external:shared_ds.orders", task, "id", "amount")
    edgesOf(model, "customers_copy") should contain theSameElementsAs
    edges("load:sales.customers", "customers_copy", "id", "email")
  }

  "The SQL of a task" should "read a table of metadata/external from the database it declares" in
  offline() {
    val handler = settings.schemaHandler()
    val task = handler.tasks().find(_.fullName() == "exports.orders_two_part").get
    handler.substituteRefTaskMainSQL(task.sql.get, task.getRunConnection()) shouldBe
    "SELECT src.id, src.amount FROM `proj-b`.`shared_ds`.`orders` AS src"
  }

  "getTablesWithColumnNames" should "keep the database a table is cited with" in offline() {
    settings
      .schemaHandler()
      .getTablesWithColumnNames(
        List("proj-b.shared_ds.orders", "shared_ds.orders"),
        None,
        declaredOnly = true
      )
      .map { case (database, domain, table) => (database, domain, table.name) } shouldBe
    List(("proj-b", "shared_ds", "orders"), ("", "shared_ds", "orders"))
  }

  "The site" should "draw the column lineage of tables cited with their database" in offline() {
    val model = ModelBuilder(settings.schemaHandler())(settings).build()
    checkColumnLineage(model)
    lineageWarnings(model).keySet shouldBe tasks.map { case (task, _) =>
      s"transform:exports.$task"
    }.toSet
  }

  it should "draw the same column lineage with an application database" in
  offline("SL_DATABASE" -> "proj-a") {
    val handler = settings.schemaHandler()
    val task = handler.tasks().find(_.fullName() == "exports.customers_copy").get
    handler.substituteRefTaskMainSQL(task.sql.get, task.getRunConnection()) should include(
      "`proj-a`.`sales`.`customers`"
    )
    checkColumnLineage(ModelBuilder(handler)(settings).build())
  }

  "col-lineage" should "find the columns of a table cited with its database" in offline() {
    val handler = settings.schemaHandler()
    val task = handler.tasks().find(_.fullName() == "exports.orders_three_part").get
    val lineage = new ColLineage(settings, handler)
      .sqlColLineage(
        None,
        task.sql.get,
        "exports",
        "orders_three_part",
        task.getRunConnection(),
        None
      )
      .get
    lineage.relations.map(r => (r.from.domain, r.from.table, r.from.column, r.to.column)) should
    contain theSameElementsAs List(
      ("shared_ds", "orders", "id", "id"),
      ("shared_ds", "orders", "amount", "amount")
    )
  }
}

object ExternalDatabaseLineageSpec {
  val tasks: List[(String, String)] = List(
    "orders_two_part"   -> "SELECT src.id, src.amount FROM shared_ds.orders AS src",
    "orders_three_part" -> "SELECT src.id, src.amount FROM `proj-b.shared_ds.orders` AS src",
    "orders_star" -> "WITH o AS (SELECT * FROM `proj-b`.shared_ds.orders) SELECT o.id, o.amount FROM o",
    "customers_copy" -> "SELECT c.id, c.email FROM sales.customers AS c"
  )
}
