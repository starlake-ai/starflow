package ai.starlake.job.site

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.transform.BigQueryAutoTask
import better.files.File

import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

/** The site compiles the SQL of a task without any connection to the warehouse (#2030). Three steps
  * of `transform` would otherwise reach it: aligning the schema of a table whose task declares
  * typed columns (`syncYamlWithDb`, on by default), which on BigQuery reads and may create the
  * dataset; reading the BigQuery table of an `OVERWRITE` task, to know whether it requires a
  * partition filter; running a `SELECT *` on a JDBC sink to resolve its columns. The credentials
  * point to a missing file, so no BigQuery client can be created, and the DuckDB file of the
  * default connection does not exist, so any connection to it would create it.
  */
class SiteOfflineCompileSpec extends IntegrationTestBase with SiteFixture {

  override lazy val fixtureRoot: File = {
    val tmp = File.newTemporaryDirectory("sl-site-offline-")
    File("src/test/resources/site/fixture").copyTo(tmp)(File.CopyOptions(overwrite = true))
    val app = tmp / "metadata" / "application.sl.yml"
    app.overwrite(
      app.contentAsString + "    bigquery:\n      type: bigquery\n      options:\n" +
      "        projectId: \"sl-site-offline-test\"\n        location: \"EU\"\n" +
      "        authType: \"APPLICATION_DEFAULT\"\n"
    )
    val jobDir = (tmp / "metadata" / "transform" / "offline").createDirectories()
    val amounts = "SELECT o.customer_id, o.amount FROM sales.orders o"
    val typedColumns =
      """  attributes:
        |    - name: "customer_id"
        |      type: "long"
        |    - name: "amount"
        |      type: "decimal"
        |""".stripMargin
    (jobDir / "customer_amounts.sql").overwrite(amounts)
    (jobDir / "customer_amounts.sl.yml").overwrite(
      "version: 1\ntask:\n  connectionRef: \"bigquery\"\n  writeStrategy:\n    type: \"OVERWRITE\"\n" +
      typedColumns
    )
    (jobDir / "local_amounts.sql").overwrite(amounts)
    (jobDir / "local_amounts.sl.yml").overwrite(
      "version: 1\ntask:\n  writeStrategy:\n    type: \"OVERWRITE\"\n" + typedColumns
    )
    (jobDir / "all_orders.sql").overwrite("SELECT * FROM sales.orders")
    (jobDir / "all_orders.sl.yml").overwrite(
      "version: 1\ntask:\n  writeStrategy:\n    type: \"OVERWRITE\"\n"
    )
    tmp
  }

  override protected def afterAll(): Unit = {
    fixtureRoot.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  private def offline[T](fun: => T): T =
    withEnvs(
      "SL_ROOT"                        -> fixtureRoot.pathAsString,
      "SL_ENV"                         -> "",
      "GOOGLE_APPLICATION_CREDENTIALS" -> (fixtureRoot / "no-credentials.json").pathAsString
    )(fun)

  "The site" should "compile the SQL of every task without connecting to the warehouse" in offline {
    val model = ModelBuilder(settings.schemaHandler())(settings).build()
    for (task <- List("customer_amounts", "local_amounts", "all_orders")) {
      val node = model.nodes(s"transform:offline.$task")
      node.warnings.filter(_.startsWith("SQL compilation failed")) shouldBe empty
      val compiled = node.sql.flatMap(_.compiled).getOrElse("")
      compiled should include(task)
      compiled should not include "ALTER"
    }
    (fixtureRoot / "datasets" / "duckdb.db").exists shouldBe false
  }

  "An offline compilation" should "not read the BigQuery table of an OVERWRITE task" in offline {
    val current = settings
    val handler = current.schemaHandler()
    val taskDesc = handler.tasks().find(_.fullName() == "offline.customer_amounts").get
    val reads = new AtomicInteger(0)
    val task = new BigQueryAutoTask(
      appId = None,
      taskDesc = taskDesc,
      commandParameters = Map.empty,
      interactive = None,
      truncate = false,
      test = false,
      logExecution = false,
      accessToken = None,
      resultPageSize = 200,
      resultPageNumber = 1,
      dryRun = true,
      scheduledDate = None,
      syncSchema = false
    )(current, current.storageHandler(), handler) {
      override protected def strategyTemplateVariables(): Map[String, Any] = {
        reads.incrementAndGet()
        super.strategyTemplateVariables()
      }
    }
    task.buildAllSQLQueries(None, Some(true), offline = true)
    reads.get shouldBe 0
    // the counter itself: online, the table is read, then the schema alignment fails on the
    // missing credentials
    Try(task.buildAllSQLQueries(None, Some(true)))
    reads.get shouldBe 1
  }
}
