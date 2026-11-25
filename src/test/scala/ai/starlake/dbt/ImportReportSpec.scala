package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.dbt.manifest.{DbtProjectFile, DbtRelations}
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.{BakedValue, EntryStatus, ImportReport, ReportEntry}
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
      report.downstreamOfSkipped shouldBe List(
        "marts.warehouse_consumer" -> List("model.shop.needs_warehouse")
      )
    }

    it should "list every task downstream of a skipped object, through any number of levels" in {
      import EntryStatus._
      val chain = ImportReport(
        List(
          ReportEntry("model", "model.p.a", Some("s.a"), Skipped("run_query")),
          ReportEntry("model", "model.p.x", Some("s.x"), NotImported("enabled: false")),
          ReportEntry("model", "model.p.b", Some("s.b"), Converted, dependsOn = List("model.p.a")),
          ReportEntry("model", "model.p.c", Some("s.c"), Converted, dependsOn = List("model.p.b")),
          ReportEntry(
            "model",
            "model.p.d",
            Some("s.d"),
            Converted,
            dependsOn = List("model.p.c", "model.p.x")
          ),
          ReportEntry("model", "model.p.ok", Some("s.ok"), Converted)
        ),
        Nil,
        Nil,
        Nil
      )
      val md = chain.toMarkdown
      md should include("- `s.b` depends on skipped `model.p.a`\n")
      md should include("- `s.c` depends on skipped `model.p.a`\n")
      md should include("- `s.d` depends on skipped `model.p.a`, `model.p.x`\n")
      md should not include "`s.ok` depends"
    }

    it should "list the tasks that read a skipped seed through its source" in {
      import EntryStatus._
      val viaSource = ImportReport(
        List(
          ReportEntry("seed", "seed.p.raw", Some("raw.raw"), Skipped("seed file not found")),
          ReportEntry(
            "source",
            "source.p.raw.raw",
            Some("raw.raw"),
            Converted,
            dependsOn = List("seed.p.raw")
          ),
          ReportEntry(
            "model",
            "model.p.stg",
            Some("s.stg"),
            Converted,
            dependsOn = List("source.p.raw.raw")
          )
        ),
        Nil,
        Nil,
        Nil
      )
      viaSource.downstreamOfSkipped should contain("s.stg" -> List("seed.p.raw"))
      viaSource.runnableTasks shouldBe Nil
    }

    it should "render all of it as Markdown" in {
      val md = report.toMarkdown
      md should startWith("# dbt import report")
      md should include("## Skipped")
      md should include(
        "model `model.shop.needs_warehouse`: needs the warehouse at import (run_query)"
      )
      md should include("## Downstream of skipped objects")
      md should include(
        "`marts.warehouse_consumer` depends on skipped `model.shop.needs_warehouse`"
      )
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

    it should "list the warehouse answers per task" in {
      val r = report.copy(warehouseReads =
        List(BakedValue("query: select 42 as answer", "1 rows", List("marts.needs_warehouse")))
      )
      r.toMarkdown should include(
        "## Warehouse answers\n\n- `marts.needs_warehouse`: query: select 42 as answer -> 1 rows\n"
      )
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
