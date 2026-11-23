package ai.starlake.job.site

import ai.starlake.schema.model.{AutoJobInfo, AutoTaskInfo, ObjectSchedule, TableAttribute}

/** Doc references reach warehouse descriptions (BigQuery, Spark, JDBC COMMENT ON) through the
  * loaded model, so the model must carry the block text rather than the load-time marker.
  */
class DocRefsAtLoadSpec extends SiteFixture {

  "Loaded domains, tables and externals" should "carry the doc block text" in withFixture {
    implicit settings =>
      val schemaHandler = settings.schemaHandler(reload = true)
      val sales = schemaHandler.domains(reload = true).find(_.name == "sales").get
      sales.comment shouldBe Some("The **sales** domain.")
      val customers = sales.tables.find(_.name == "customers").get
      customers.comment shouldBe Some("**Customers** master data.")
      customers.rls.map(_.description) shouldBe List("Only **French** rows.")
      // an unknown block is kept in its source form, never as the marker
      sales.tables.find(_.name == "orders").get.comment shouldBe
      Some("""Orders, see {{ doc("missing_block") }}""")

      val fx = schemaHandler.externals().flatMap(_.tables).find(_.name == "fx_rates").get
      fx.comment shouldBe Some("Daily **FX rates**, one row per currency.")

      schemaHandler.tableOnly("sales.customers").get.table.comment shouldBe
      Some("**Customers** master data.")

      val all =
        schemaHandler.domains().flatMap(d => d.comment.toList ++ d.tables.flatMap(_.comment))
      all.exists(_.contains("sl-doc")) shouldBe false
  }

  "The resolver" should "reach jobs, tasks, nested columns and the job default" in {
    val (blocks, _) = DocBlocks.parse(Seq("{% docs d %}text{% enddocs %}"))
    val marker = "<!--sl-doc:d-->"
    val nested =
      TableAttribute("s", "struct", attributes = List(TableAttribute("c", comment = Some(marker))))
    val task = AutoTaskInfo(
      name = "t",
      sql = None,
      database = None,
      domain = "x",
      table = "t",
      comment = Some(s"before $marker"),
      attributes = List(nested),
      taskTimeoutMs = None
    )
    val job = AutoJobInfo(
      name = "j",
      tasks = List(task),
      comment = Some(marker),
      default = Some(task)
    )
    var warnings = List.empty[String]
    val resolved = new DocBlocks.Resolver(blocks, w => warnings ::= w).job(job)
    resolved.comment shouldBe Some("text")
    resolved.tasks.head.comment shouldBe Some("before text")
    resolved.tasks.head.attributes.head.attributes.head.comment shouldBe Some("text")
    resolved.default.flatMap(_.comment) shouldBe Some("before text")
    warnings shouldBe Nil
  }

  it should "not read the blocks when no description holds a reference" in {
    val resolver = new DocBlocks.Resolver(throw new IllegalStateException("read"), _ => ())
    val job = AutoJobInfo(name = "j", comment = Some("plain"))
    resolver.job(job).comment shouldBe Some("plain")
  }

  "Schedule updates" should "change only the schedule of the file as written" in withFixture {
    implicit settings =>
      val taskFile = fixtureRoot / "metadata" / "transform" / "kpi" / "documented.sl.yml"
      taskFile.overwrite(
        """version: 1
          |task:
          |  comment: '{{ doc("sales_desc") }}'
          |""".stripMargin
      )
      val schemaHandler = settings.schemaHandler(reload = true)
      schemaHandler.domains(reload = true)
      schemaHandler.jobs(reload = true)

      schemaHandler.updateTableSchedule(
        ObjectSchedule("sales", "customers", Some("0 1 * * *"), "table")
      )
      val tableFile = fixtureRoot / "metadata" / "load" / "sales" / "customers.sl.yml"
      val table = tableFile.contentAsString
      table should include("doc(")
      table should include("customers_desc")
      table should not include "Customers** master data"
      table should include("0 1 * * *")
      table should include("separator")

      schemaHandler.updateTaskSchedule(
        ObjectSchedule("kpi", "documented", Some("0 2 * * *"), "task")
      )
      val task = taskFile.contentAsString
      task should include("sales_desc")
      task should not include "The **sales** domain."
      task should include("0 2 * * *")
  }
}
