package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import better.files.File

import java.io.ByteArrayOutputStream
import scala.sys.process.Process

class ImpactCmdSpec extends TestHelper {

  private def projects(
    newOverrides: Map[String, String],
    removed: Set[String] = Set.empty
  ): (File, File) = {
    val oldDir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-old-"))
    val newDir =
      ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-new-"), newOverrides, removed)
    (oldDir, newDir)
  }

  private val removeAmountFromStg = Map(
    "metadata/transform/stg/orders.sql" ->
    "SELECT o.id, o.amount * 100 AS amount FROM raw.orders o WHERE o.status = 'ok'\n"
  )

  private def result(r: scala.util.Try[ai.starlake.utils.JobResult]): ImpactJobResult =
    r.get.asInstanceOf[ImpactJobResult]

  "ImpactCmd" should "trace an expression change down the chain" in {
    new WithSettings() {
      val (o, n) = projects(removeAmountFromStg)
      val report = ImpactCmd.analyze(o, n, "old", "new", None).get
      report.direct.map(d => (d.node.name, d.changeClass)) shouldBe List(
        "stg.orders" -> ChangeClass.Breaking
      )
      report.impacted.map(_.node.name) shouldBe List("kpi.dashboard", "kpi.revenue")
      report.impacted.find(_.node.name == "kpi.revenue").get.columns shouldBe Cols(Set("total"))
      o.delete(); n.delete()
    }
  }

  it should "return exit code 1 with --fail-on breaking and 0 without" in {
    new WithSettings() {
      val (o, n) = projects(removeAmountFromStg)
      val out = File.newTemporaryFile(suffix = ".json")
      val base = Seq(
        "--path1",
        o.pathAsString,
        "--path2",
        n.pathAsString,
        "--format",
        "json",
        "--output",
        out.pathAsString
      )
      result(
        ImpactCmd.run(base :+ "--fail-on" :+ "breaking", settings.schemaHandler())
      ).exitCode shouldBe 1
      result(ImpactCmd.run(base, settings.schemaHandler())).exitCode shouldBe 0
      out.contentAsString should include("\"hasBreaking\" : true")
      o.delete(); n.delete(); out.delete()
    }
  }

  it should "treat a comment-only change as METADATA and pass --fail-on non-breaking" in {
    new WithSettings() {
      val (o, n) = projects(
        Map(
          "metadata/transform/kpi/revenue.sl.yml" ->
          "version: 1\ntask:\n  name: revenue\n  domain: kpi\n  table: revenue\n  comment: \"revenue per order\"\n  writeStrategy:\n    type: OVERWRITE\n"
        )
      )
      val args =
        Seq("--path1", o.pathAsString, "--path2", n.pathAsString, "--fail-on", "non-breaking")
      val stdout = new ByteArrayOutputStream()
      val r = Console.withOut(stdout)(result(ImpactCmd.run(args, settings.schemaHandler())))
      r.exitCode shouldBe 0
      r.report.get.direct.map(d => (d.node.name, d.changeClass)) shouldBe List(
        "kpi.revenue" -> ChangeClass.Metadata
      )
      // without --output the report goes to stdout, headed by the labels of both sides
      stdout.toString should include(s"Change impact: ${o.pathAsString} -> ${n.pathAsString}")
      o.delete(); n.delete()
    }
  }

  it should "flag consumers of a deleted load table as likely failures" in {
    new WithSettings() {
      val (o, n) = projects(Map.empty, removed = Set("metadata/load/raw/orders.sl.yml"))
      val report = ImpactCmd.analyze(o, n, "old", "new", None).get
      report.direct.find(_.node.name == "raw.orders").get.deleted shouldBe true
      report.impacted.find(_.node.name == "stg.orders").get.likelyFailure shouldBe true
      o.delete(); n.delete()
    }
  }

  it should "list an untraced DAG change" in {
    new WithSettings() {
      val (o, n) = projects(
        Map(
          "metadata/dags/default.sl.yml" -> "version: 1\ndag:\n  template: \"other.py.j2\"\n  filename: \"{{domain}}.py\"\n"
        )
      )
      ImpactCmd.analyze(o, n, "old", "new", None).get.otherChanges shouldBe List(
        "dags/default.sl.yml (changed)"
      )
      o.delete(); n.delete()
    }
  }

  it should "return exit code 2 on usage errors" in {
    new WithSettings() {
      val usageErrors = List(
        Seq("--base", "master", "--path1", "/tmp/x"),
        Seq("--head", "master", "--path2", "/tmp/x"),
        Seq("--path2", "/tmp/x"),
        Seq("--path1", "/tmp/x", "--format", "pdf"),
        Seq("--path1", "/tmp/x", "--fail-on", "always"),
        Seq("--no-such-option")
      )
      usageErrors.foreach { args =>
        withClue(args.mkString(" ")) {
          result(ImpactCmd.run(args, settings.schemaHandler())).exitCode shouldBe 2
        }
      }
    }
  }

  it should "return exit code 2 when a project cannot be loaded" in {
    new WithSettings() {
      val (o, n) =
        projects(Map("metadata/transform/kpi/revenue.sl.yml" -> "version: 1\ntask: [unclosed\n"))
      val r = result(
        ImpactCmd.run(
          Seq("--path1", o.pathAsString, "--path2", n.pathAsString),
          settings.schemaHandler()
        )
      )
      r.exitCode shouldBe 2
      r.report shouldBe None
      r.error.get should include("Could not load project")
      o.delete(); n.delete()
    }
  }

  it should "compare the working tree against --base in a git repository" in {
    new WithSettings() {
      val repo = gitRepo()
      val project = ImpactFixtures.writeProject(repo / "project")
      commitAll(repo)
      (project / "metadata/transform/stg/orders.sql").writeText(removeAmountFromStg.values.head)

      val checkoutsBefore = gitCheckouts()
      val out = File.newTemporaryFile(suffix = ".txt")
      val r = withSettingsRoot(project) { s =>
        ImpactCmd.run(
          Seq("--base", "HEAD", "--output", out.pathAsString, "--fail-on", "breaking"),
          s.schemaHandler()
        )(s)
      }
      result(r).exitCode shouldBe 1
      val text = out.contentAsString
      text should include("kpi.revenue")
      text should include regex "Change impact: merge-base of HEAD \\([0-9a-f]{7}\\) -> working tree"
      // the temporary checkout of the old side is deleted
      gitCheckouts() shouldBe checkoutsBefore
      // the working tree is left untouched
      Process(Seq("git", "-C", repo.pathAsString, "status", "--porcelain")).!!.trim shouldBe
      "M project/metadata/transform/stg/orders.sql"
      repo.delete(); out.delete()
    }
  }

  it should "compare --base against --head without looking at the working tree" in {
    new WithSettings() {
      val repo = gitRepo()
      val project = ImpactFixtures.writeProject(repo / "project")
      commitAll(repo)
      (project / "metadata/transform/stg/orders.sql").writeText(removeAmountFromStg.values.head)
      commitAll(repo)
      // an uncommitted edit that --head must not see
      (project / "metadata/transform/kpi/dashboard.sql").writeText("SELECT 1 AS rev\n")

      val checkoutsBefore = gitCheckouts()
      val r = withSettingsRoot(project) { s =>
        val stdout = new ByteArrayOutputStream()
        Console.withOut(stdout) {
          ImpactCmd.run(Seq("--base", "HEAD~1", "--head", "HEAD"), s.schemaHandler())(s)
        }
      }
      val report = result(r).report.get
      report.direct.map(_.node.name) shouldBe List("stg.orders")
      report.newSide should startWith("HEAD (")
      gitCheckouts() shouldBe checkoutsBefore
      repo.delete()
    }
  }

  it should "report an unknown git ref with exit code 2" in {
    new WithSettings() {
      val repo = gitRepo()
      val project = ImpactFixtures.writeProject(repo / "project")
      commitAll(repo)
      val checkoutsBefore = gitCheckouts()
      List(
        Seq("--base", "no-such-ref")                    -> "no-such-ref",
        Seq("--base", "HEAD", "--head", "no-such-head") -> "no-such-head"
      ).foreach { case (args, ref) =>
        withClue(args.mkString(" ")) {
          val r = result(withSettingsRoot(project) { s =>
            ImpactCmd.run(args, s.schemaHandler())(s)
          })
          r.exitCode shouldBe 2
          r.error.get should include(s"unknown git ref '$ref'")
        }
      }
      gitCheckouts() shouldBe checkoutsBefore
      repo.delete()
    }
  }

  it should "return exit code 2 with --base outside a git repository" in {
    new WithSettings() {
      val project = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-nogit-"))
      val r = result(withSettingsRoot(project) { s =>
        ImpactCmd.run(Seq("--base", "HEAD"), s.schemaHandler())(s)
      })
      r.exitCode shouldBe 2
      r.error should not be empty
      project.delete()
    }
  }

  it should "report the expression change of the lineage sample down to its KPIs" in {
    new WithSettings() {
      val sample = File("samples/lineage/metadata")
      withClue(s"cwd ${File(".").path.toAbsolutePath}: ") { sample.isDirectory shouldBe true }
      val repo = gitRepo()
      val project = repo / "lineage"
      sample.copyTo(project / "metadata")
      commitAll(repo)
      val sql = project / "metadata/transform/starbake_analytics/order_items_analysis.sql"
      val original = sql.contentAsString
      original should include("Sum( o.quantity * p.price )")
      sql.writeText(
        original.replace("Sum( o.quantity * p.price )", "Sum( o.quantity * p.price ) * 1.2")
      )

      val out = File.newTemporaryFile(suffix = ".md")
      val r = result(withSettingsRoot(project) { s =>
        ImpactCmd.run(
          Seq("--base", "HEAD", "--format", "markdown", "--output", out.pathAsString),
          s.schemaHandler()
        )(s)
      })
      val markdown = out.contentAsString
      info(markdown)
      r.exitCode shouldBe 0
      val report = r.report.get
      report.direct.map(d => (d.node.name, d.changeClass, d.columns)) shouldBe List(
        (
          "starbake_analytics.order_items_analysis",
          ChangeClass.Breaking,
          Cols(Set("total_order_value"))
        )
      )
      report.direct.head.details shouldBe List(ExpressionChanged("total_order_value"))
      // overall_kpis is the only task reading total_order_value, through total_revenue and
      // avg_order_value, and daily_revenue is computed from total_revenue
      report.impacted.map(i => (i.node.name, i.columns)) shouldBe List(
        "starbake_kpis.overall_kpis" -> Cols(
          Set("total_revenue", "avg_order_value", "daily_revenue")
        )
      )
      report.impacted.head.path should not be empty
      markdown should include("starbake_analytics.order_items_analysis")
      markdown should include("starbake_kpis.overall_kpis")
      repo.delete(); out.delete()
    }
  }

  it should "be registered and document its options" in {
    new WithSettings() {
      ai.starlake.job.Main.commands.map(_.command) should contain("impact")
      val page = ImpactCmd.markdown(1)
      List("--base", "--head", "--path1", "--path2", "--fail-on", "HTML-escaped").foreach { s =>
        page should include(s)
      }
    }
  }

  private def git(repo: File, args: String*): String =
    Process(Seq("git", "-C", repo.pathAsString) ++ args).!!

  private def gitRepo(): File = {
    val repo = File.newTemporaryDirectory("impact-git-")
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "test")
    git(repo, "config", "commit.gpgsign", "false")
    repo
  }

  private def commitAll(repo: File): Unit = {
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "commit")
  }

  /** Temporary directories GitSnapshot.archive creates, to check that every checkout is deleted. */
  private def gitCheckouts(): Set[String] =
    File(System.getProperty("java.io.tmpdir")).list
      .filter(_.name.startsWith("starlake-git-"))
      .map(_.name)
      .toSet

  private def withSettingsRoot[T](root: File)(f: Settings => T)(implicit settings: Settings): T =
    f(
      settings.copy(appConfig =
        settings.appConfig.copy(
          root = root.pathAsString,
          metadata = (root / "metadata").pathAsString
        )
      )
    )
}
