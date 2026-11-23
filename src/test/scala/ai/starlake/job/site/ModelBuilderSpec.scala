package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import better.files.File

import ai.starlake.schema.model.{AutoTaskInfo, TableAttribute}

import scala.util.{Failure, Success, Try}

class ModelBuilderSpec extends IntegrationTestBase with SiteFixture {

  private def build(
    compile: Boolean = true,
    taskDeps: Option[AutoTaskInfo => Try[List[String]]] = None
  )(implicit settings: Settings): SiteModel = {
    val schemaHandler = settings.schemaHandler()
    new ModelBuilder(
      schemaHandler,
      _ => if (compile) Success("SELECT 1") else Failure(new Exception("boom")),
      _ => Success(None),
      taskDeps.getOrElse(ModelBuilder.defaultTaskDeps(schemaHandler))
    ).build()
  }

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

  it should "locate task files in the job folder, python relative to metadata" in withFixture {
    implicit settings =>
      val report = build().nodes("transform:reporting.fx_report")
      report.filePath shouldBe Some("transform/finance/fx_report.sl.yml")
      report.python shouldBe Some("transform/finance/fx_report.py")
  }

  it should "record a warning when SQL compilation fails" in withFixture { implicit settings =>
    val revenue = build(compile = false).nodes("transform:kpi.revenue")
    revenue.sql.flatMap(_.compiled) shouldBe None
    revenue.warnings.exists(_.startsWith("SQL compilation failed")) shouldBe true
  }

  it should "build groups, docs and project overview" in withFixture { implicit settings =>
    val model = build()
    model.groups.domains shouldBe List(
      SiteGroup(
        "sales",
        Some("The **sales** domain."),
        List("load:sales.customers", "load:sales.orders")
      )
    )
    model.groups.jobs.map(j => j.name -> j.nodes) shouldBe List(
      "finance" -> List("transform:reporting.fx_report"),
      "kpi"     -> List("transform:kpi.revenue")
    )
    model.groups.jobs.map(j => j.name -> j.description) shouldBe List(
      "finance" -> None,
      "kpi"     -> Some("KPIs built from the sales domain")
    )
    settings.schemaHandler().jobs().find(_.name == "kpi").flatMap(_.comment) shouldBe
    Some("KPIs built from the sales domain")
    model.groups.dags.map(d => d.name -> d.comment) shouldBe List(
      "airflow_load"      -> "Scheduled **load** DAG.",
      "airflow_transform" -> "transform dag"
    )
    model.nodes("load:sales.customers").rls.map(_.description) shouldBe
    List("Only **French** rows.")
    model.groups.dags.map(d => d.name -> d.nodes) shouldBe List(
      "airflow_load"      -> List("load:sales.customers", "load:sales.orders"),
      "airflow_transform" -> List("transform:kpi.revenue", "transform:reporting.fx_report")
    )
    model.docs.keySet shouldBe Set(
      "customers_desc",
      "sales_desc",
      "fr_only_desc",
      "load_dag_desc",
      "__overview__"
    )
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

  it should "type the columns of a transform on its sink connection engine" in {
    val root = File.newTemporaryDirectory("sl-site-sink-")
    try {
      File("src/test/resources/site/fixture").copyTo(root)(File.CopyOptions(overwrite = true))
      val app = root / "metadata" / "application.sl.yml"
      app.overwrite(
        app.contentAsString + "    pg:\n      type: jdbc\n      options:\n" +
        "        url: \"jdbc:postgresql://localhost:5432/db\"\n" +
        "        driver: \"org.postgresql.Driver\"\n"
      )
      val dir = (root / "metadata" / "transform" / "sinkpg").createDirectories()
      (dir / "sunk.sl.yml").overwrite(
        """version: 1
          |task:
          |  domain: "sinkpg"
          |  sink:
          |    connectionRef: "pg"
          |  attributes:
          |    - name: "id"
          |      type: "long"
          |    - name: "label"
          |      type: "string"
          |""".stripMargin
      )
      (dir / "sunk.sql").overwrite("SELECT 1 AS id, 'a' AS label")
      withEnvs("SL_ROOT" -> root.pathAsString, "SL_ENV" -> "") {
        val node = build()(settings).nodes("transform:sinkpg.sunk")
        node.engine shouldBe Some("duckdb")
        node.connectionRef shouldBe Some("duckdb")
        node.columns.map(c => c.name -> c.sqlType) shouldBe
        List("id" -> Some("BIGINT"), "label" -> Some("TEXT"))
      }
    } finally root.delete(swallowIOExceptions = true)
  }

  it should "turn a task that cannot be built into a table lineage warning" in withFixture {
    implicit settings =>
      val model = build(taskDeps = Some(_ => Failure(new Exception("cannot build task"))))
      model.nodes("transform:kpi.revenue").warnings should contain(
        "Table lineage failed: cannot build task"
      )
      model.edges.filter(_.kind == "lineage") shouldBe Nil
  }

  it should "build each task in its own Try" in {
    val deps = ModelBuilder.taskDependencies(
      AutoTaskInfo(name = "bad", sql = None, database = None, domain = "kpi", table = "bad"),
      _ => throw new IllegalStateException("no engine"),
      throw new IllegalStateException("streams must not be read")
    )
    deps.failed.get.getMessage shouldBe "no engine"
  }

  it should "read column lineage inputs from declared tables only" in withFixture {
    implicit settings =>
      val tables = ModelBuilder.declaredTablesWithColumns(
        settings.schemaHandler(),
        List("sales.orders", "refdata.fx_rates", "customers")
      )
      tables.map { case (domain, t) => s"$domain.${t.name}" } shouldBe
      List("sales.orders", ".customers")
      tables.head._2.attrs.map(_._1) shouldBe List("amount", "customer_id", "id")
  }

  it should "build with the production defaults" in withFixture { implicit settings =>
    val model = ModelBuilder(settings.schemaHandler()).build()
    model.nodes.keySet should contain("transform:kpi.revenue")
    model.nodes.keySet should not contain "external:fx"
    model.nodes.keySet should contain("external:refdata.fx_rates")
    model.columnEdges.exists(_.to.startsWith("transform:kpi.revenue#")) shouldBe true
  }

  "declaredSqlType" should "use the Spark JDBC dialect type on postgresql and redshift" in withFixture {
    implicit settings =>
      val sh = settings.schemaHandler()
      def sqlType(tpe: String, engine: String) =
        ModelBuilder.declaredSqlType(TableAttribute("c", tpe), engine, sh)
      // The built-in types map `postgres`, not `postgresql`: core falls back to Spark's dialect
      sqlType("long", "postgresql") shouldBe Some("BIGINT")
      sqlType("int", "postgresql") shouldBe Some("BIGINT")
      sqlType("string", "postgresql") shouldBe Some("TEXT")
      sqlType("decimal", "postgresql") shouldBe Some("NUMERIC(38,9)")
      sqlType("double", "postgresql") shouldBe Some("FLOAT8")
      sqlType("boolean", "postgresql") shouldBe Some("BOOLEAN")
      sqlType("date", "postgresql") shouldBe Some("DATE")
      sqlType("timestamp", "postgresql") shouldBe Some("TIMESTAMP WITH TIME ZONE")
      sqlType("long", "redshift") shouldBe Some("BIGINT")
      sqlType("string", "redshift") shouldBe Some("TEXT")
  }

  it should "use the Spark SQL type on spark" in withFixture { implicit settings =>
    val sh = settings.schemaHandler()
    def sqlType(tpe: String) = ModelBuilder.declaredSqlType(TableAttribute("c", tpe), "spark", sh)
    sqlType("long") shouldBe Some("BIGINT")
    sqlType("string") shouldBe Some("STRING")
    sqlType("decimal") shouldBe Some("DECIMAL(38,9)")
    sqlType("timestamp") shouldBe Some("TIMESTAMP")
    sqlType("date") shouldBe Some("DATE")
  }

  it should "keep the ddlMapping of the engine on duckdb, bigquery and snowflake" in withFixture {
    implicit settings =>
      val sh = settings.schemaHandler()
      def sqlType(tpe: String, engine: String) =
        ModelBuilder.declaredSqlType(TableAttribute("c", tpe), engine, sh)
      sqlType("long", "duckdb") shouldBe Some("BIGINT")
      sqlType("decimal", "duckdb") shouldBe Some("DECIMAL")
      sqlType("long", "bigquery") shouldBe Some("INT64")
      sqlType("string", "snowflake") shouldBe Some("STRING")
  }

  "column" should "flag ignored attributes only" in withFixture { implicit settings =>
    val builder = ModelBuilder(settings.schemaHandler())
    def col(attr: TableAttribute) =
      builder.column(attr, Nil, Some("duckdb"), DocBlocks(Map.empty))._1
    col(TableAttribute("raw", "string", ignore = Some(true))).ignore shouldBe Some(true)
    col(TableAttribute("kept", "string", ignore = Some(false))).ignore shouldBe None
    col(TableAttribute("plain", "string")).ignore shouldBe None
  }

  it should "leave the SQL type of an array leaf unknown except on Spark" in withFixture {
    implicit settings =>
      val builder = ModelBuilder(settings.schemaHandler())
      def sqlType(attr: TableAttribute, engine: String) =
        builder.column(attr, Nil, Some(engine), DocBlocks(Map.empty))._1.sqlType
      val tags = TableAttribute("tags", "string", array = Some(true))
      List("duckdb", "bigquery", "snowflake", "postgresql", "redshift").foreach { engine =>
        withClue(engine) { sqlType(tags, engine) shouldBe None }
      }
      sqlType(tags, "spark") shouldBe Some("ARRAY<STRING>")
      sqlType(TableAttribute("name", "string"), "duckdb") shouldBe Some("VARCHAR")
  }
}
