package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import better.files.File

class ProjectSnapshotSpec extends TestHelper {

  "ProjectSnapshot.load" should "load load tables, tasks and lineage offline" in {
    new WithSettings() {
      val dir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-"))
      val view = ProjectSnapshot.load(dir, "base", None).get

      view.label shouldBe "base"
      view.nodes.keySet shouldBe Set(
        NodeRef("raw", "orders"),
        NodeRef("stg", "orders"),
        NodeRef("kpi", "revenue"),
        NodeRef("kpi", "dashboard")
      )
      view.nodes(NodeRef("raw", "orders")).kind shouldBe NodeKind.LoadTable
      view.nodes(NodeRef("raw", "orders")).attributes.map(_.finalName) shouldBe
      List("id", "amount", "status")
      view.nodes(NodeRef("raw", "orders")).structural should include("APPEND")
      view.nodes(NodeRef("stg", "orders")).kind shouldBe NodeKind.Task
      view.nodes(NodeRef("stg", "orders")).structural should include("OVERWRITE")

      val revenue = view.nodes(NodeRef("kpi", "revenue")).shape.get
      revenue.unresolved shouldBe None
      revenue.inputs shouldBe Set(NodeRef("stg", "orders"))
      revenue.column("total").get.sources shouldBe Set(
        SourceCol(NodeRef("stg", "orders"), "amount")
      )
      view.nodes(NodeRef("stg", "orders")).shape.get.rowIdentifiers should contain("status")
      val dashboard = view.nodes(NodeRef("kpi", "dashboard")).shape.get
      dashboard.unresolved shouldBe None
      dashboard.column("rev").get.sources shouldBe Set(
        SourceCol(NodeRef("kpi", "revenue"), "total")
      )
      view.otherFiles.keySet should contain allOf ("application.sl.yml", "dags/default.sl.yml")
      view.otherFiles("application.sl.yml") should fullyMatch regex "[0-9a-f]{64}"
      dir.delete()
    }
  }

  it should "not create anything in the current project" in {
    new WithSettings() {
      val stageOfRaw = File(DatasetArea.stage("raw").toUri.getPath)
      stageOfRaw.exists shouldBe false
      val dir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-"))
      ProjectSnapshot.load(dir, "base", None).get
      stageOfRaw.exists shouldBe false
      dir.delete()
    }
  }

  it should "lower-case the final names of load and task attributes" in {
    new WithSettings() {
      val dir = ImpactFixtures.writeProject(
        File.newTemporaryDirectory("impact-"),
        overrides = Map(
          "metadata/load/raw/orders.sl.yml" ->
          """version: 1
              |table:
              |  name: orders
              |  pattern: "orders.*.csv"
              |  attributes:
              |  - name: ID
              |    type: long
              |  - name: Amount
              |    type: decimal
              |  - name: status
              |    rename: STATUS
              |    type: string
              |""".stripMargin,
          "metadata/transform/stg/orders.sl.yml" ->
          """version: 1
              |task:
              |  name: orders
              |  domain: stg
              |  table: orders
              |  attributes:
              |  - name: ID
              |    type: long
              |  - name: Amount
              |    type: decimal
              |""".stripMargin
        )
      )
      val view = ProjectSnapshot.load(dir, "base", None).get
      val raw = view.nodes(NodeRef("raw", "orders")).attributes
      raw.map(_.finalName) shouldBe List("id", "amount", "status")
      raw.map(_.key) shouldBe List("ID", "Amount", "status")
      view.nodes(NodeRef("stg", "orders")).attributes.map(_.finalName) shouldBe
      List("id", "amount")
      val revenue = view.nodes(NodeRef("kpi", "revenue")).shape.get
      revenue.unresolved shouldBe None
      revenue.column("total").get.sources shouldBe Set(
        SourceCol(NodeRef("stg", "orders"), "amount")
      )
      view.nodes(NodeRef("stg", "orders")).shape.get.unresolvedInputs shouldBe empty
      dir.delete()
    }
  }

  it should "mark a python task as unresolved" in {
    new WithSettings() {
      val dir = ImpactFixtures.writeProject(
        File.newTemporaryDirectory("impact-"),
        overrides = Map("metadata/transform/kpi/dashboard.py" -> "print('x')\n"),
        removed = Set("metadata/transform/kpi/dashboard.sql")
      )
      val view = ProjectSnapshot.load(dir, "base", None).get
      view.nodes(NodeRef("kpi", "dashboard")).shape.get.unresolved shouldBe Some("python task")
      dir.delete()
    }
  }

  it should "fingerprint the content of a python task" in {
    new WithSettings() {
      def pythonFingerprint(code: String): String = {
        val dir = ImpactFixtures.writeProject(
          File.newTemporaryDirectory("impact-"),
          overrides = Map("metadata/transform/kpi/dashboard.py" -> code),
          removed = Set("metadata/transform/kpi/dashboard.sql")
        )
        val view = ProjectSnapshot.load(dir, "base", None).get
        dir.delete()
        view.nodes(NodeRef("kpi", "dashboard")).shape.get.rowFingerprint
      }
      val base = pythonFingerprint("print('x')\n")
      base should not be empty
      pythonFingerprint("print('x')\n") shouldBe base
      pythonFingerprint("print('y')\n") should not be base
    }
  }

  it should "fail with a migrate hint on an unreadable project" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("impact-")
      (dir / "metadata" / "load" / "raw").createDirectories()
      (dir / "metadata" / "load" / "raw" / "orders.sl.yml").writeText("table: [not: valid")
      val result = ProjectSnapshot.load(dir, "base", None)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("migrate")
      dir.delete()
    }
  }

  it should "fail instead of skipping an unreadable domain or transform config" in {
    new WithSettings() {
      List(
        "metadata/load/raw/_config.sl.yml",
        "metadata/transform/kpi/_config.sl.yml"
      ).foreach { config =>
        val dir = ImpactFixtures.writeProject(
          File.newTemporaryDirectory("impact-"),
          overrides = Map(config -> "version: 1\nload: [not: valid")
        )
        val result = ProjectSnapshot.load(dir, "base", None)
        withClue(config) {
          result.isFailure shouldBe true
          result.failed.get.getMessage should include("_config.sl.yml")
          result.failed.get.getMessage should include("migrate")
        }
        dir.delete()
      }
    }
  }

  /** Every path under `dir`, directories included, with the content of the files. */
  private def tree(dir: File): Map[String, Option[Seq[Byte]]] =
    dir.listRecursively.map { f =>
      dir.relativize(f).toString -> (if (f.isRegularFile) Some(f.byteArray.toSeq) else None)
    }.toMap

  it should "never write into the project directory" in {
    new WithSettings() {
      val dir = ImpactFixtures.writeProject(
        File.newTemporaryDirectory("impact-"),
        overrides = Map(
          // an empty domain name makes the schema handler rewrite the file with the folder name
          "metadata/load/raw/_config.sl.yml"    -> "version: 1\nload:\n  name: \"\"\n",
          "metadata/transform/kpi/dashboard.py" -> "print('x')\n"
        ),
        removed = Set("metadata/transform/kpi/dashboard.sql")
      )
      val before = tree(dir)
      val view = ProjectSnapshot.load(dir, "base", None).get
      view.nodes.keySet should contain(NodeRef("raw", "orders"))
      view.nodes(NodeRef("kpi", "dashboard")).shape.get.rowFingerprint should not be empty
      val after = tree(dir)
      val changedFiles = before.collect {
        case (path, content @ Some(_)) if !after.get(path).contains(content) => path
      }
      changedFiles shouldBe empty
      (after.keySet -- before.keySet) shouldBe empty
      after shouldBe before
      dir.delete()
    }
  }

  it should "render root-derived paths the same whatever the project directory" in {
    new WithSettings() {
      def revenue(sql: String): TaskShape = {
        val dir = ImpactFixtures.writeProject(
          File.newTemporaryDirectory("impact-"),
          overrides = Map("metadata/transform/kpi/revenue.sql" -> sql)
        )
        val view = ProjectSnapshot.load(dir, "base", None).get
        dir.delete()
        view.nodes(NodeRef("kpi", "revenue")).shape.get
      }
      val sql =
        "SELECT s.id, s.amount * 2 AS total, '{{SL_ROOT}}/out' AS root_path, " +
        "'{{SL_METADATA}}/x' AS meta_path, '{{SL_DATASETS}}/y' AS data_path FROM stg.orders s\n"
      val first = revenue(sql)
      val second = revenue(sql)
      first.unresolved shouldBe None
      first.column("root_path").get.fingerprint should include("__SL_ROOT__/out")
      first.column("meta_path").get.fingerprint should include("__SL_ROOT__/metadata/x")
      first.column("data_path").get.fingerprint should include("__SL_ROOT__/datasets/y")
      second shouldBe first
      val edited = revenue(sql.replace("* 2", "* 3"))
      edited.column("total").get.fingerprint should not be first.column("total").get.fingerprint
      edited.column("root_path") shouldBe first.column("root_path")
    }
  }
}
