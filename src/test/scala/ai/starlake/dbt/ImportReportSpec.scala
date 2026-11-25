package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.dbt.manifest.{DbtProjectFile, DbtRelations}
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.EntryStatus
import ai.starlake.dbt.write.ProjectWriter
import ai.starlake.schema.model.TableAttribute
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.util.Success

class ImportReportSpec extends TestHelper {
  new WithSettings() {
    private val manifest = DbtFixtures.shopManifest
    private val stubInference = (_: java.nio.file.Path, _: String) =>
      Success(List(TableAttribute("id", "long")))
    private lazy val plan = DbtMapper.plan(
      DbtMappingContext(
        manifest,
        new DbtRelations(manifest),
        DbtFixtures.jinja(),
        None,
        stubInference
      ),
      DbtProjectFile.read(manifest.projectRoot).get
    )
    private lazy val report = plan.report

    "The report" should "list the skipped model that calls run_query and the snapshot" in {
      report.skipped.map(e => e.dbtId -> e.status) should contain(
        "model.shop.needs_warehouse" -> EntryStatus.Skipped(
          "needs the warehouse at import (run_query)"
        )
      )
      report.reportedOnly.map(_.dbtId) should contain("snapshot.shop.customers_snapshot")
    }

    it should "list the vars and env vars baked into the SQL, with the tasks that use them" in {
      report.vars
        .map(v => (v.name, v.value)) should contain allOf (("currency", "EUR"), ("min_amount", "0"))
      report.vars.find(_.name == "currency").get.usedBy shouldBe List("staging.stg_orders")
      report.envVars.map(v => (v.name, v.value, v.usedBy)) shouldBe List(
        ("SHOP_CHANNEL", "web", List("marts.orders"))
      )
    }

    it should "list the tasks that read a skipped model" in {
      report.dependentsOfSkipped shouldBe List(
        "model.shop.needs_warehouse" -> List("marts.warehouse_consumer")
      )
    }

    it should "render all of it as Markdown" in {
      val md = report.toMarkdown
      md should startWith("# dbt import report")
      md should include("## Skipped")
      md should include(
        "model `model.shop.needs_warehouse`: needs the warehouse at import (run_query)"
      )
      md should include("## Downstream of skipped objects")
      md should include("`model.shop.needs_warehouse` is read by `marts.warehouse_consumer`")
      md should include("## Baked env vars")
      md should include("`SHOP_CHANNEL` = `web` in `marts.orders`")
    }

    "ProjectWriter" should "refuse to overwrite existing files without --force, and never delete" in {
      val unrelated = new Path(DatasetArea.transform, "marts/handwritten.sql")
      storageHandler.write("select 1", unrelated)
      ProjectWriter.conflicts(plan) shouldBe Nil
      ProjectWriter.write(plan, force = false).isSuccess shouldBe true
      val task = new Path(DatasetArea.transform, "marts/orders.sql")
      storageHandler.write("edited", task)

      ProjectWriter.conflicts(plan) should contain(task)
      ProjectWriter.write(plan, force = false).isFailure shouldBe true
      storageHandler.read(task) shouldBe "edited"

      ProjectWriter.write(plan, force = true).isSuccess shouldBe true
      storageHandler.read(task) should include("from staging.stg_orders")
      storageHandler.read(unrelated) shouldBe "select 1"
      storageHandler.exists(ProjectWriter.reportPath) shouldBe true
    }

    it should "add the DuckDB connection to application.sl.yml, default only when there is none" in {
      ProjectWriter.editApplication(ConnectionEdit("dbt_dev", "jdbc:duckdb:/data/shop.duckdb"))
      val app = YamlSerde.mapper
        .readTree(storageHandler.read(ProjectWriter.applicationPath))
        .path("application")
      app
        .path("connections")
        .path("dbt_dev")
        .path("options")
        .path("url")
        .asText() shouldBe "jdbc:duckdb:/data/shop.duckdb"
      app.path("connections").path("dbt_dev").path("loader").asText() shouldBe "native"
      app.path("connectionRef").asText() shouldBe "dbt_dev"
      ProjectWriter.conflicts(
        plan.copy(connection = Some(ConnectionEdit("dbt_dev", "x")))
      ) should contain(ProjectWriter.applicationPath)
    }
  }
}
