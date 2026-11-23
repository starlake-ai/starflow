package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import better.files.File

import java.io.ByteArrayOutputStream
import scala.sys.process.Process

class ImpactCmdSpec extends TestHelper {

  /** An old and a new fixture project, both deleted after `f` whatever happens. */
  private def withProjects[A](newOverrides: Map[String, String], removed: Set[String] = Set.empty)(
    f: (File, File) => A
  ): A = {
    val oldDir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-old-"))
    val newDir =
      ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-new-"), newOverrides, removed)
    deleting(oldDir, newDir)(f(oldDir, newDir))
  }

  /** Runs `f`, then deletes `files` whatever happens. */
  private def deleting[A](files: File*)(f: => A): A =
    try f
    finally files.foreach(_.delete(swallowIOExceptions = true))

  private val removeAmountFromStg = Map(
    "metadata/transform/stg/orders.sql" ->
    "SELECT o.id, o.amount * 100 AS amount FROM raw.orders o WHERE o.status = 'ok'\n"
  )

  /** raw.orders gets a new attribute nobody reads: a NON_BREAKING change only. */
  private val addNoteToRawOrders = Map(
    "metadata/load/raw/orders.sl.yml" ->
    """version: 1
      |table:
      |  name: orders
      |  pattern: "orders.*.csv"
      |  attributes:
      |  - name: id
      |    type: long
      |  - name: amount
      |    type: decimal
      |  - name: status
      |    type: string
      |  - name: note
      |    type: string
      |  metadata:
      |    format: DSV
      |    writeStrategy:
      |      type: APPEND
      |""".stripMargin
  )

  private def result(r: scala.util.Try[ai.starlake.utils.JobResult]): ImpactJobResult =
    r.get.asInstanceOf[ImpactJobResult]

  "ImpactCmd" should "trace an expression change down the chain" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        val report = ImpactCmd.analyze(o, n, "old", "new", None).get
        report.direct.map(d => (d.node.name, d.changeClass)) shouldBe List(
          "stg.orders" -> ChangeClass.Breaking
        )
        report.impacted.map(_.node.name) shouldBe List("kpi.dashboard", "kpi.revenue")
        report.impacted.find(_.node.name == "kpi.revenue").get.columns shouldBe Cols(Set("total"))
      }
    }
  }

  it should "return exit code 1 with --fail-on breaking and 0 without" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        val out = File.newTemporaryFile(suffix = ".json")
        deleting(out) {
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
        }
      }
    }
  }

  it should "return exit code 1 with --fail-on non-breaking on a NON_BREAKING change" in {
    new WithSettings() {
      withProjects(addNoteToRawOrders) { (o, n) =>
        val base = Seq("--path1", o.pathAsString, "--path2", n.pathAsString)
        val stdout = new ByteArrayOutputStream()
        Console.withOut(stdout) {
          val r =
            result(ImpactCmd.run(base :+ "--fail-on" :+ "non-breaking", settings.schemaHandler()))
          r.exitCode shouldBe 1
          r.report.get.direct.map(d => (d.node.name, d.changeClass)) shouldBe List(
            "raw.orders" -> ChangeClass.NonBreaking
          )
          result(
            ImpactCmd.run(base :+ "--fail-on" :+ "breaking", settings.schemaHandler())
          ).exitCode shouldBe 0
        }
      }
    }
  }

  it should "treat a comment-only change as METADATA and pass --fail-on non-breaking" in {
    new WithSettings() {
      withProjects(
        Map(
          "metadata/transform/kpi/revenue.sl.yml" ->
          "version: 1\ntask:\n  name: revenue\n  domain: kpi\n  table: revenue\n  comment: \"revenue per order\"\n  writeStrategy:\n    type: OVERWRITE\n"
        )
      ) { (o, n) =>
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
      }
    }
  }

  it should "compare --path1 with the working tree when --path2 is not given" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        val stdout = new ByteArrayOutputStream()
        val r = withSettingsRoot(n) { s =>
          Console.withOut(stdout)(
            ImpactCmd.run(Seq("--path1", o.pathAsString), s.schemaHandler())(s)
          )
        }
        val report = result(r).report.get
        report.newSide shouldBe "working tree"
        report.direct.map(_.node.name) shouldBe List("stg.orders")
        stdout.toString should include(s"Change impact: ${o.pathAsString} -> working tree")
      }
    }
  }

  it should "flag consumers of a deleted load table as likely failures" in {
    new WithSettings() {
      withProjects(Map.empty, removed = Set("metadata/load/raw/orders.sl.yml")) { (o, n) =>
        val report = ImpactCmd.analyze(o, n, "old", "new", None).get
        report.direct.find(_.node.name == "raw.orders").get.deleted shouldBe true
        report.impacted.find(_.node.name == "stg.orders").get.likelyFailure shouldBe true
      }
    }
  }

  it should "list an untraced DAG change" in {
    new WithSettings() {
      withProjects(
        Map(
          "metadata/dags/default.sl.yml" -> "version: 1\ndag:\n  template: \"other.py.j2\"\n  filename: \"{{domain}}.py\"\n"
        )
      ) { (o, n) =>
        ImpactCmd.analyze(o, n, "old", "new", None).get.otherChanges shouldBe List(
          "dags/default.sl.yml (changed)"
        )
      }
    }
  }

  it should "report a change of load postsql, load pattern or task connection" in {
    new WithSettings() {
      val orders = ImpactFixtures.baseFiles("metadata/load/raw/orders.sl.yml")
      val stgTask = ImpactFixtures.baseFiles("metadata/transform/stg/orders.sl.yml")
      List(
        Map(
          "metadata/load/raw/orders.sl.yml" ->
          orders.replace("  attributes:", "  postsql:\n  - \"DELETE FROM x\"\n  attributes:")
        ) -> "raw.orders",
        Map(
          "metadata/load/raw/orders.sl.yml" -> orders.replace("orders.*.csv", "orders2.*.csv")
        ) -> "raw.orders",
        Map(
          "metadata/transform/stg/orders.sl.yml" -> (stgTask + "  connectionRef: other\n")
        ) -> "stg.orders"
      ).foreach { case (overrides, node) =>
        withClue(overrides.values.head) {
          withProjects(overrides) { (o, n) =>
            val report = ImpactCmd.analyze(o, n, "old", "new", None).get
            report.direct.map(d => (d.node.name, d.changeClass)) shouldBe List(
              node -> ChangeClass.Breaking
            )
          }
        }
      }
    }
  }

  it should "list a load or transform file change that no node explains as not classified" in {
    new WithSettings() {
      val orders = ImpactFixtures.baseFiles("metadata/load/raw/orders.sl.yml")
      withProjects(
        Map(
          // a YAML comment changes the file, not what the snapshot reads from it
          "metadata/load/raw/orders.sl.yml"  -> (orders + "# reviewed\n"),
          "metadata/load/raw/_config.sl.yml" -> "version: 1\nload:\n  name: raw\n# owner: x\n",
          "metadata/transform/kpi/notes.txt" -> "a note\n",
          "metadata/extract/source.sl.yml"   -> "version: 1\n",
          "metadata/mapping/m.txt"           -> "m\n"
        )
      ) { (o, n) =>
        val report = ImpactCmd.analyze(o, n, "old", "new", None).get
        report.direct shouldBe empty
        report.otherChanges shouldBe List(
          "extract/source.sl.yml (added)",
          "load/raw/_config.sl.yml (changed, not classified)",
          "load/raw/orders.sl.yml (changed, not classified)",
          "mapping/m.txt (added)",
          "transform/kpi/notes.txt (added, not classified)"
        )
      }
      // a file whose node has a direct change is explained by it
      withProjects(
        Map("metadata/load/raw/orders.sl.yml" -> (orders.replace("APPEND", "OVERWRITE") + "# x\n"))
      ) { (o, n) =>
        val report = ImpactCmd.analyze(o, n, "old", "new", None).get
        report.direct.map(_.node.name) shouldBe List("raw.orders")
        report.otherChanges shouldBe empty
      }
    }
  }

  it should "return exit code 2 on usage errors" in {
    new WithSettings() {
      val usageErrors = List(
        Seq("--base", "master", "--path1", "/tmp/x"),
        Seq("--head", "master", "--path2", "/tmp/x"),
        Seq("--head", "master", "--path1", "/tmp/x"),
        Seq("--path2", "/tmp/x"),
        Seq("--path1", "/tmp/x", "--format", "pdf"),
        Seq("--path1", "/tmp/x", "--fail-on", "always"),
        Seq("--no-such-option")
      )
      usageErrors.foreach { args =>
        withClue(args.mkString(" ")) {
          val r = result(ImpactCmd.run(args, settings.schemaHandler()))
          r.exitCode shouldBe 2
          // rejected by the parser, not by a later git or loading error
          r.error shouldBe Some("invalid arguments")
        }
      }
    }
  }

  it should "return exit code 2 when a project cannot be loaded" in {
    new WithSettings() {
      withProjects(
        Map("metadata/transform/kpi/revenue.sl.yml" -> "version: 1\ntask: [unclosed\n")
      ) { (o, n) =>
        val r = result(
          ImpactCmd.run(
            Seq("--path1", o.pathAsString, "--path2", n.pathAsString),
            settings.schemaHandler()
          )
        )
        r.exitCode shouldBe 2
        r.report shouldBe None
        r.error.get should include("Could not load project")
      }
    }
  }

  it should "return exit code 2 when the html template is missing" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        val missing = (n / "no-such-template.html.j2").pathAsString
        val r = result(
          ImpactCmd.run(
            Seq(
              "--path1",
              o.pathAsString,
              "--path2",
              n.pathAsString,
              "--format",
              "html",
              "--template",
              missing
            ),
            settings.schemaHandler()
          )
        )
        r.exitCode shouldBe 2
        r.report shouldBe None
        r.error.get should include(s"Template not found: $missing")
      }
    }
  }

  it should "return exit code 2 when the output cannot be written" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        // a regular file where the output's parent directory should be
        val notADir = File.newTemporaryFile(suffix = ".txt")
        deleting(notADir) {
          val out = (notADir / "sub" / "report.txt").pathAsString
          val r = result(
            ImpactCmd.run(
              Seq("--path1", o.pathAsString, "--path2", n.pathAsString, "--output", out),
              settings.schemaHandler()
            )
          )
          r.exitCode shouldBe 2
          r.report shouldBe None
          r.error should not be empty
          notADir.isRegularFile shouldBe true
        }
      }
    }
  }

  it should "compare the working tree against --base in a git repository" in {
    new WithSettings() {
      val repo = gitRepo()
      val out = File.newTemporaryFile(suffix = ".txt")
      deleting(repo, out) {
        val project = ImpactFixtures.writeProject(repo / "project")
        commitAll(repo)
        (project / "metadata/transform/stg/orders.sql").writeText(removeAmountFromStg.values.head)

        val checkoutsBefore = gitCheckouts()
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
      }
    }
  }

  it should "compare --base against --head without looking at the working tree" in {
    new WithSettings() {
      val repo = gitRepo()
      deleting(repo) {
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
      }
    }
  }

  it should "report an unknown git ref with exit code 2" in {
    new WithSettings() {
      val repo = gitRepo()
      deleting(repo) {
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
      }
    }
  }

  it should "return exit code 2 when --path1 or --path2 is not a project directory" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, n) =>
        val missing = (o.parent / s"no-such-project-${java.util.UUID.randomUUID()}").pathAsString
        List(
          Seq("--path1", missing, "--path2", n.pathAsString) -> missing,
          Seq("--path1", o.pathAsString, "--path2", missing) -> missing,
          Seq("--path1", (o / "metadata").pathAsString, "--path2", n.pathAsString) ->
          (o / "metadata").pathAsString
        ).foreach { case (args, bad) =>
          withClue(args.mkString(" ")) {
            val r = result(
              ImpactCmd.run(args :+ "--fail-on" :+ "breaking", settings.schemaHandler())
            )
            r.exitCode shouldBe 2
            r.report shouldBe None
            r.error.get should include(bad)
            r.error.get should include("is not a project directory")
          }
        }
      }
    }
  }

  it should "return exit code 2 when the working tree is not a project directory" in {
    new WithSettings() {
      withProjects(removeAmountFromStg) { (o, _) =>
        val notAProject = File.newTemporaryDirectory("impact-notaproject-")
        deleting(notAProject) {
          val r = result(withSettingsRoot(notAProject) { s =>
            ImpactCmd.run(Seq("--path1", o.pathAsString), s.schemaHandler())(s)
          })
          r.exitCode shouldBe 2
          r.error.get should include(s"${notAProject.pathAsString} is not a project directory")
        }
      }
    }
  }

  it should "return exit code 2 in git mode from a directory that is not the project root" in {
    new WithSettings() {
      val repo = gitRepo()
      deleting(repo) {
        ImpactFixtures.writeProject(repo / "project")
        commitAll(repo)
        val checkoutsBefore = gitCheckouts()
        List(Seq("--base", "HEAD"), Seq("--base", "HEAD", "--head", "HEAD")).foreach { args =>
          withClue(args.mkString(" ")) {
            // the repository root, one level above the project
            val r = result(withSettingsRoot(repo) { s =>
              ImpactCmd.run(args, s.schemaHandler())(s)
            })
            r.exitCode shouldBe 2
            r.error.get should include("is not a project directory")
          }
        }
        gitCheckouts() shouldBe checkoutsBefore
      }
    }
  }

  it should "compare against an empty old side when the base commit predates the project" in {
    new WithSettings() {
      val repo = gitRepo()
      deleting(repo) {
        (repo / "README.md").writeText("before the project\n")
        commitAll(repo)
        val project = ImpactFixtures.writeProject(repo / "project")
        commitAll(repo)
        val r = result(withSettingsRoot(project) { s =>
          val stdout = new ByteArrayOutputStream()
          Console.withOut(stdout) {
            ImpactCmd.run(Seq("--base", "HEAD~1"), s.schemaHandler())(s)
          }
        })
        r.exitCode shouldBe 0
        val report = r.report.get
        report.oldSide should include regex "merge-base of HEAD~1 \\([0-9a-f]{7}, no project at this commit\\)"
        report.direct.map(_.changeClass).distinct shouldBe List(ChangeClass.Added)
      }
    }
  }

  it should "return exit code 2 with --base outside a git repository" in {
    new WithSettings() {
      val project = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-nogit-"))
      deleting(project) {
        val r = result(withSettingsRoot(project) { s =>
          ImpactCmd.run(Seq("--base", "HEAD"), s.schemaHandler())(s)
        })
        r.exitCode shouldBe 2
        r.error should not be empty
      }
    }
  }

  it should "report the expression change of the lineage sample down to its KPIs" in {
    new WithSettings() {
      val sample = File("samples/lineage/metadata")
      withClue(s"cwd ${File(".").path.toAbsolutePath}: ") { sample.isDirectory shouldBe true }
      val repo = gitRepo()
      val out = File.newTemporaryFile(suffix = ".md")
      deleting(repo, out) {
        val project = repo / "lineage"
        sample.copyTo(project / "metadata")
        commitAll(repo)
        val sql = project / "metadata/transform/starbake_analytics/order_items_analysis.sql"
        val original = sql.contentAsString
        original should include("Sum( o.quantity * p.price )")
        sql.writeText(
          original.replace("Sum( o.quantity * p.price )", "Sum( o.quantity * p.price ) * 1.2")
        )

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
      }
    }
  }

  it should "be registered, read-only and document its options" in {
    new WithSettings() {
      ai.starlake.job.Main.commands.map(_.command) should contain("impact")
      ImpactCmd.readOnly shouldBe true
      ai.starlake.job.Main.isReadOnly(List("impact", "--base", "main")) shouldBe true
      ai.starlake.job.Main.isReadOnly(List("transform", "--name", "x")) shouldBe false
      ai.starlake.job.Main.isReadOnly(Nil) shouldBe false
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
