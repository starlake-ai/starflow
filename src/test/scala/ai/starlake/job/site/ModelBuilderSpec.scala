package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase

import scala.util.{Failure, Success}

class ModelBuilderSpec extends IntegrationTestBase with SiteFixture {

  private def build(compile: Boolean = true)(implicit settings: Settings): SiteModel =
    new ModelBuilder(
      settings.schemaHandler(),
      _ => if (compile) Success("SELECT 1") else Failure(new Exception("boom")),
      _ => Success(None)
    ).build()

  "ModelBuilder" should "build load and transform nodes" in withFixture { implicit settings =>
    val model = build()
    model.schemaVersion shouldBe 1
    model.nodes.keySet should contain allOf (
      "load:sales.customers",
      "load:sales.orders",
      "transform:kpi.revenue",
      "external:refdata.fx_rates"
    )
    val customers = model.nodes("load:sales.customers")
    customers.description shouldBe Some("**Customers** master data.")
    customers.tags shouldBe List("pii")
    customers.columns.map(_.name) shouldBe List("id", "email")
    customers.columns.head.primaryKey shouldBe true
    customers.columns.head.required shouldBe true
    customers.acl shouldBe List(SiteAcl("roles/viewer", List("user:a@example.com")))
    customers.rls.map(_.name) shouldBe List("fr_only")
    customers.expectations shouldBe List(SiteExpectation("is_unique('id')", true))
    customers.pattern shouldBe Some("customers.*.csv")
    customers.dag shouldBe Some("airflow_load")
    customers.connectionRef shouldBe Some("duckdb")
    customers.engine shouldBe Some("duckdb")
    customers.filePath shouldBe Some("load/sales/customers.sl.yml")
  }

  it should "warn on unknown doc blocks and resolve foreign keys" in withFixture {
    implicit settings =>
      val orders = build().nodes("load:sales.orders")
      orders.warnings should contain("Unknown doc block 'missing_block'")
      orders.columns.find(_.name == "customer_id").flatMap(_.foreignKeyNode) shouldBe
      Some("load:sales.customers")
  }

  it should "describe transforms with raw and compiled SQL" in withFixture { implicit settings =>
    val model = build()
    val revenue = model.nodes("transform:kpi.revenue")
    revenue.description shouldBe Some("Revenue per customer")
    revenue.sql.flatMap(_.raw).get should include("FROM sales.orders")
    revenue.sql.flatMap(_.compiled) shouldBe Some("SELECT 1")
    revenue.dag shouldBe Some("airflow_transform")
    revenue.filePath shouldBe Some("transform/kpi/revenue.sl.yml")
    model.edges should contain(SiteEdge("load:sales.orders", "transform:kpi.revenue", "lineage"))
    model.edges should contain(SiteEdge("load:sales.orders", "load:sales.customers", "fk"))
  }

  it should "record a warning when SQL compilation fails" in withFixture { implicit settings =>
    val revenue = build(compile = false).nodes("transform:kpi.revenue")
    revenue.sql.flatMap(_.compiled) shouldBe None
    revenue.warnings.exists(_.startsWith("SQL compilation failed")) shouldBe true
  }

  it should "build groups, docs and project overview" in withFixture { implicit settings =>
    val model = build()
    model.groups.domains shouldBe List(
      SiteGroup("sales", Some("Sales domain"), List("load:sales.customers", "load:sales.orders"))
    )
    model.groups.jobs.map(_.name) shouldBe List("kpi")
    model.groups.jobs.head.nodes shouldBe List("transform:kpi.revenue")
    model.groups.dags.map(d => d.name -> d.nodes) shouldBe List(
      "airflow_load"      -> List("load:sales.customers", "load:sales.orders"),
      "airflow_transform" -> List("transform:kpi.revenue")
    )
    model.docs.keySet shouldBe Set("customers_desc", "__overview__")
    model.project.overview shouldBe Some("# Fixture project")
    model.project.defaultConnection shouldBe "duckdb"
  }

  it should "turn failed table lineage into warnings" in {
    val (deps, warnings) = ModelBuilder.extractTaskDeps(
      List(
        "transform:a.ok"  -> Success(List("load:x.y")),
        "transform:a.bad" -> Failure(new Exception("kaboom"))
      )
    )
    deps shouldBe Map("transform:a.ok" -> List("load:x.y"))
    warnings shouldBe Map("transform:a.bad" -> List("Table lineage failed: kaboom"))
  }

  it should "build with the production defaults" in withFixture { implicit settings =>
    val model = ModelBuilder(settings.schemaHandler()).build()
    model.nodes.keySet should contain("transform:kpi.revenue")
    model.nodes.keySet should not contain "external:fx"
    model.nodes.keySet should contain("external:refdata.fx_rates")
    model.columnEdges.exists(_.to.startsWith("transform:kpi.revenue#")) shouldBe true
  }
}
