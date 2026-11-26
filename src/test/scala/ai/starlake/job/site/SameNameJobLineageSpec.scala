package ai.starlake.job.site

import ai.starlake.integration.IntegrationTestBase
import better.files.File

/** A transform job that bears the name of the load domain whose tables its tasks read, a common
  * layout (`metadata/load/sales` and `metadata/transform/sales`). The fixture copy gets a job
  * `sales` with two tasks: `daily_orders` reads the load table `sales.orders`, and `top_orders`
  * reads the task `sales.daily_orders`. Whichever of the two `sales` entries comes first in
  * `objectNames`, one of these lookups used to miss (#2029). The shared fixture and its golden model
  * are left unchanged.
  */
class SameNameJobLineageSpec extends IntegrationTestBase with SiteFixture {

  override lazy val fixtureRoot: File = {
    val tmp = File.newTemporaryDirectory("sl-site-same-name-")
    File("src/test/resources/site/fixture").copyTo(tmp)(File.CopyOptions(overwrite = true))
    val jobDir = tmp / "metadata" / "transform" / "sales"
    jobDir.createDirectories()
    (jobDir / "daily_orders.sql").overwrite("SELECT o.id, o.amount FROM sales.orders o")
    (jobDir / "daily_orders.sl.yml").overwrite(
      """version: 1
        |task:
        |  writeStrategy:
        |    type: "OVERWRITE"
        |  attributes:
        |    - name: "id"
        |      type: "long"
        |    - name: "amount"
        |      type: "decimal"
        |""".stripMargin
    )
    (jobDir / "top_orders.sql").overwrite("SELECT d.id FROM sales.daily_orders d")
    (jobDir / "top_orders.sl.yml").overwrite(
      """version: 1
        |task:
        |  writeStrategy:
        |    type: "OVERWRITE"
        |""".stripMargin
    )
    tmp
  }

  override protected def afterAll(): Unit = {
    fixtureRoot.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  "getTablesWithColumnNames" should "find the tables of a domain and of a job of the same name" in
  withFixture { implicit settings =>
    val tables = settings
      .schemaHandler()
      .getTablesWithColumnNames(
        List("sales.orders", "sales.daily_orders"),
        None,
        declaredOnly = true
      )
    tables.map { case (domain, t) => s"$domain.${t.name}" } shouldBe
    List("sales.orders", "sales.daily_orders")
    tables.head._2.attrs.map(_._1) shouldBe List("amount", "customer_id", "id")
  }

  "The site" should "draw the column lineage of tasks that read a table of their job's name" in
  withFixture { implicit settings =>
    val model = ModelBuilder(settings.schemaHandler()).build()
    val tasks = List("transform:sales.daily_orders", "transform:sales.top_orders")
    for (task <- tasks) model.nodes(task).warnings shouldBe empty
    model.columnEdges.filter(edge => tasks.contains(edge.task)) should contain theSameElementsAs
    List(
      SiteColumnEdge(
        "load:sales.orders#amount",
        "transform:sales.daily_orders#amount",
        "transform:sales.daily_orders"
      ),
      SiteColumnEdge(
        "load:sales.orders#id",
        "transform:sales.daily_orders#id",
        "transform:sales.daily_orders"
      ),
      SiteColumnEdge(
        "transform:sales.daily_orders#id",
        "transform:sales.top_orders#id",
        "transform:sales.top_orders"
      )
    )
  }
}
