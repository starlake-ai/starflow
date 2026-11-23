package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import better.files.File

import scala.util.Try

class ProjectSnapshotSpec extends TestHelper {

  /** Runs `f` on a fresh fixture project, deleted afterwards whatever happens. */
  private def withProject[A](
    overrides: Map[String, String] = Map.empty,
    removed: Set[String] = Set.empty
  )(f: File => A): A = {
    val dir = File.newTemporaryDirectory("impact-")
    try f(ImpactFixtures.writeProject(dir, overrides, removed))
    finally dir.delete(swallowIOExceptions = true)
  }

  private def tryLoad(
    overrides: Map[String, String] = Map.empty,
    removed: Set[String] = Set.empty
  )(implicit settings: Settings): Try[SnapshotView] =
    withProject(overrides, removed)(dir => ProjectSnapshot.load(dir, "base", None))

  private def load(
    overrides: Map[String, String] = Map.empty,
    removed: Set[String] = Set.empty
  )(implicit settings: Settings): SnapshotView = tryLoad(overrides, removed).get

  private val ordersWith: String => String = extra => s"""version: 1
       |table:
       |  name: orders
       |  pattern: "orders.*.csv"
       |$extra""".stripMargin

  private val ordersAttributes =
    """  attributes:
      |  - name: id
      |    type: long
      |  - name: amount
      |    type: decimal
      |  - name: status
      |    type: string
      |""".stripMargin

  "ProjectSnapshot.load" should "load load tables, tasks and lineage offline" in {
    new WithSettings() {
      val view = load()

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
    }
  }

  it should "list macros, types, refs and strategy templates as other files" in {
    new WithSettings() {
      val view = load(
        Map(
          "metadata/macros/util.j2"                   -> "{% macro one() %}1{% endmacro %}\n",
          "metadata/types/custom.sl.yml"              -> "version: 1\ntypes: []\n",
          "metadata/refs.sl.yml"                      -> "version: 1\nrefs: []\n",
          "metadata/write-strategies/custom.j2"       -> "-- write\n",
          "metadata/load-strategies/sub/custom.j2"    -> "-- load\n",
          "metadata/load/raw/not-listed-elsewhere.j2" -> "x\n"
        )
      )
      view.otherFiles.keySet should contain allOf (
        "macros/util.j2",
        "types/custom.sl.yml",
        "refs.sl.yml",
        "write-strategies/custom.j2",
        "load-strategies/sub/custom.j2"
      )
      view.otherFiles.keySet should not contain "load/raw/not-listed-elsewhere.j2"
    }
  }

  it should "load a project without a load folder, and an empty one" in {
    new WithSettings() {
      val transformsOnly = load(
        Map(
          "metadata/transform/kpi/revenue.sql" -> "SELECT 1 AS id, 2 AS total\n",
          "metadata/transform/kpi/dashboard.sql" ->
          "SELECT Sum(r.total) AS rev FROM kpi.revenue r\n"
        ),
        removed = ImpactFixtures.baseFiles.keySet.filter(p =>
          p.startsWith("metadata/load/") || p.startsWith("metadata/transform/stg/")
        )
      )
      transformsOnly.nodes.keySet shouldBe Set(
        NodeRef("kpi", "revenue"),
        NodeRef("kpi", "dashboard")
      )
      val dir = File.newTemporaryDirectory("impact-empty-")
      try ProjectSnapshot.load(dir, "empty", None).get.nodes shouldBe empty
      finally dir.delete(swallowIOExceptions = true)
    }
  }

  it should "not create anything in the current project" in {
    new WithSettings() {
      val stageOfRaw = File(DatasetArea.stage("raw").toUri.getPath)
      stageOfRaw.exists shouldBe false
      load()
      stageOfRaw.exists shouldBe false
    }
  }

  it should "lower-case the final names of load and task attributes" in {
    new WithSettings() {
      val view = load(
        Map(
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
    }
  }

  it should "mark a python task as unresolved" in {
    new WithSettings() {
      val view = load(
        Map("metadata/transform/kpi/dashboard.py" -> "print('x')\n"),
        Set("metadata/transform/kpi/dashboard.sql")
      )
      view.nodes(NodeRef("kpi", "dashboard")).shape.get.unresolved shouldBe Some("python task")
    }
  }

  it should "fingerprint the content of a python task" in {
    new WithSettings() {
      def pythonFingerprint(code: String): String =
        load(
          Map("metadata/transform/kpi/dashboard.py" -> code),
          Set("metadata/transform/kpi/dashboard.sql")
        ).nodes(NodeRef("kpi", "dashboard")).shape.get.rowFingerprint
      val base = pythonFingerprint("print('x')\n")
      base should not be empty
      pythonFingerprint("print('x')\n") shouldBe base
      pythonFingerprint("print('y')\n") should not be base
    }
  }

  it should "fail with a migrate hint on an unreadable project" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("impact-")
      try {
        (dir / "metadata" / "load" / "raw").createDirectories()
        (dir / "metadata" / "load" / "raw" / "orders.sl.yml").writeText("table: [not: valid")
        val result = ProjectSnapshot.load(dir, "base", None)
        result.isFailure shouldBe true
        result.failed.get.getMessage should include("migrate")
      } finally dir.delete(swallowIOExceptions = true)
    }
  }

  it should "fail instead of skipping an unreadable domain or transform config" in {
    new WithSettings() {
      List(
        "metadata/load/raw/_config.sl.yml",
        "metadata/transform/kpi/_config.sl.yml"
      ).foreach { config =>
        val result = tryLoad(Map(config -> "version: 1\nload: [not: valid"))
        withClue(config) {
          result.isFailure shouldBe true
          result.failed.get.getMessage should include("_config.sl.yml")
          result.failed.get.getMessage should include("migrate")
        }
      }
    }
  }

  it should "fail instead of skipping an unreadable task file" in {
    new WithSettings() {
      val result = tryLoad(Map("metadata/transform/kpi/revenue.sl.yml" -> "task: [not: valid"))
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("revenue.sl.yml")
      result.failed.get.getMessage should include("migrate")
    }
  }

  it should "mark only the task whose SQL does not render as unresolved" in {
    new WithSettings() {
      val sql = "SELECT {% include 'macros/missing.j2' %} AS rev FROM kpi.revenue r\n"
      val view = load(Map("metadata/transform/kpi/dashboard.sql" -> sql))
      val dashboard = view.nodes(NodeRef("kpi", "dashboard")).shape.get
      dashboard.unresolved.get should startWith("SQL render failed: ")
      dashboard.unresolved.get should include("missing.j2")
      dashboard.rowFingerprint should not be empty
      view.nodes(NodeRef("kpi", "revenue")).shape.get.unresolved shouldBe None
      val edited = load(Map("metadata/transform/kpi/dashboard.sql" -> sql.replace("rev", "rev2")))
      edited.nodes(NodeRef("kpi", "dashboard")).shape.get.rowFingerprint should not be
      dashboard.rowFingerprint
    }
  }

  it should "name the file and give no migrate hint when a YAML file does not render" in {
    new WithSettings() {
      val result = tryLoad(
        Map(
          "metadata/load/raw/orders.sl.yml" ->
          ordersWith("  comment: \"{% include 'macros/missing.j2' %}\"\n" + ordersAttributes)
        )
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("orders.sl.yml")
      result.failed.get.getMessage should not include "migrate"
    }
  }

  /** Every path under `dir`, directories included, with the content of the files. */
  private def tree(dir: File): Map[String, Option[Seq[Byte]]] =
    dir.listRecursively.map { f =>
      dir.relativize(f).toString -> (if (f.isRegularFile) Some(f.byteArray.toSeq) else None)
    }.toMap

  it should "never write into the project directory" in {
    new WithSettings() {
      withProject(
        Map(
          // an empty domain name makes the schema handler rewrite the file with the folder name
          "metadata/load/raw/_config.sl.yml"    -> "version: 1\nload:\n  name: \"\"\n",
          "metadata/transform/kpi/dashboard.py" -> "print('x')\n"
        ),
        Set("metadata/transform/kpi/dashboard.sql")
      ) { dir =>
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
      }
    }
  }

  it should "render root-derived paths the same whatever the project directory" in {
    new WithSettings() {
      val yaml = Map(
        "metadata/load/raw/orders.sl.yml" -> ordersWith(
          "  comment: \"{{SL_ROOT}}/doc\"\n" +
          "  filter: \"status <> '{{SL_ROOT}}/x'\"\n" +
          ordersAttributes
        ),
        "metadata/transform/kpi/revenue.sl.yml" ->
        """version: 1
            |task:
            |  name: revenue
            |  domain: kpi
            |  table: revenue
            |  writeStrategy:
            |    type: OVERWRITE_BY_PARTITION
            |    queryFilter: "path = '{{SL_ROOT}}/q'"
            |""".stripMargin
      )
      def snapshot(sql: String): SnapshotView =
        load(yaml + ("metadata/transform/kpi/revenue.sql" -> sql))
      def revenue(view: SnapshotView): TaskShape = view.nodes(NodeRef("kpi", "revenue")).shape.get
      val sql =
        "SELECT s.id, s.amount * 2 AS total, '{{SL_ROOT}}/out' AS root_path, " +
        "'{{SL_METADATA}}/x' AS meta_path, '{{SL_DATASETS}}/y' AS data_path FROM stg.orders s\n"
      val firstView = snapshot(sql)
      val secondView = snapshot(sql)
      val first = revenue(firstView)
      first.unresolved shouldBe None
      first.column("root_path").get.fingerprint should include("__SL_ROOT__/out")
      first.column("meta_path").get.fingerprint should include("__SL_ROOT__/metadata/x")
      first.column("data_path").get.fingerprint should include("__SL_ROOT__/datasets/y")
      revenue(secondView) shouldBe first
      val edited = revenue(snapshot(sql.replace("* 2", "* 3")))
      edited.column("total").get.fingerprint should not be first.column("total").get.fingerprint
      edited.column("root_path") shouldBe first.column("root_path")

      val rawOrders = NodeRef("raw", "orders")
      val revenueRef = NodeRef("kpi", "revenue")
      firstView.nodes(rawOrders).structural should include("__SL_ROOT__/x")
      firstView.nodes(rawOrders).metadata should include("__SL_ROOT__/doc")
      firstView.nodes(revenueRef).structural should include("__SL_ROOT__/q")
      secondView.nodes(rawOrders) shouldBe firstView.nodes(rawOrders)
      secondView.nodes(revenueRef).structural shouldBe firstView.nodes(revenueRef).structural
    }
  }

  it should "replace the root only as a whole path segment" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory("impact-")
      try {
        val root = dir.pathAsString
        val sql =
          s"SELECT s.id, '$root/a' AS inside, '${root}2/a' AS sibling, '$root' AS bare, " +
          s"'${root}_x' AS underscore, '$root.bak' AS dotted, '$root-old' AS dashed, " +
          s"concat('$root', 'x') AS separated FROM stg.orders s\n"
        ImpactFixtures.writeProject(dir, Map("metadata/transform/kpi/revenue.sql" -> sql))
        val shape =
          ProjectSnapshot.load(dir, "base", None).get.nodes(NodeRef("kpi", "revenue")).shape.get
        shape.column("inside").get.fingerprint should include("'__SL_ROOT__/a'")
        shape.column("sibling").get.fingerprint should include(s"'${root}2/a'")
        shape.column("bare").get.fingerprint should include("'__SL_ROOT__'")
        shape.column("underscore").get.fingerprint should include(s"'${root}_x'")
        shape.column("dotted").get.fingerprint should include(s"'$root.bak'")
        shape.column("dashed").get.fingerprint should include(s"'$root-old'")
        shape.column("separated").get.fingerprint should include("'__SL_ROOT__'")
      } finally dir.delete(swallowIOExceptions = true)
    }
  }

  it should "resolve includes against the copy of each project" in {
    new WithSettings() {
      def revenue(macroBody: String): TaskShape =
        load(
          Map(
            "metadata/macros/total.j2" -> macroBody,
            "metadata/transform/kpi/revenue.sql" ->
            "SELECT s.id, {% include 'macros/total.j2' %} AS total FROM stg.orders s\n"
          )
        ).nodes(NodeRef("kpi", "revenue")).shape.get
      val two = revenue("s.amount * 2")
      two.unresolved shouldBe None
      revenue("s.amount * 2") shouldBe two
      val three = revenue("s.amount * 3")
      three.unresolved shouldBe None
      three.column("total").get.fingerprint should not be two.column("total").get.fingerprint
    }
  }

  it should "always read the copy from the local file system" in {
    new WithSettings() {
      val remote = settings.copy(appConfig = settings.appConfig.copy(root = "gs://bucket/project"))
      remote.appConfig.fileSystem shouldBe "gs://bucket"
      withProject() { dir =>
        ProjectSnapshot.localSettings(dir, None)(remote).appConfig.fileSystem shouldBe "file://"
      }
    }
  }

  it should "treat changes of data-producing table and task fields as structural" in {
    new WithSettings() {
      val stg = NodeRef("stg", "orders")
      val raw = NodeRef("raw", "orders")
      def stgTask(extra: String) = Map(
        "metadata/transform/stg/orders.sl.yml" ->
        s"version: 1\ntask:\n  name: orders\n  domain: stg\n  table: orders\n$extra"
      )
      def structural(ref: NodeRef, overrides: Map[String, String]): String =
        load(overrides).nodes(ref).structural
      val baseTask = structural(stg, stgTask("  postsql:\n  - \"DELETE FROM a\"\n"))
      structural(stg, stgTask("  postsql:\n  - \"DELETE FROM b\"\n")) should not be baseTask
      structural(stg, stgTask("  presql:\n  - \"DELETE FROM a\"\n")) should not be baseTask
      structural(raw, statusWith("")) should not be structural(raw, statusWith("", "JSON"))
    }
  }

  private def statusWith(extra: String, format: String = "DSV", comment: String = "") = Map(
    "metadata/load/raw/orders.sl.yml" -> ordersWith(
      (if (comment.isEmpty) "" else s"  comment: \"$comment\"\n") +
      s"""  attributes:
         |  - name: id
         |    type: long
         |  - name: status
         |    type: string
         |$extra  metadata:
         |    format: $format
         |""".stripMargin
    )
  )

  it should "keep attribute data fields at the column level" in {
    new WithSettings() {
      val raw = NodeRef("raw", "orders")
      def node(extra: String): NodeView = load(statusWith(extra)).nodes(raw)
      def attribute(n: NodeView, name: String): AttributeView =
        n.attributes.find(_.key == name).get
      val base = node("")

      val added = node("  - name: added\n    type: string\n    default: \"x\"\n")
      added.structural shouldBe base.structural
      attribute(added, "added").dataFingerprint should include("default=x")

      List(
        "    script: \"upper(status)\"\n",
        "    default: \"x\"\n",
        "    privacy: HIDE\n"
      ).foreach { field =>
        withClue(field) {
          val changed = node(field)
          changed.structural shouldBe base.structural
          attribute(changed, "status").dataFingerprint should not be
          attribute(base, "status").dataFingerprint
          attribute(changed, "id") shouldBe attribute(base, "id")
        }
      }
      node("    privacy: HIDE\n").metadata shouldBe base.metadata

      def nested(innerType: String): AttributeView = attribute(
        node(
          s"  - name: nested\n    type: struct\n    attributes:\n    - name: inner\n      type: $innerType\n"
        ),
        "nested"
      )
      nested("string").dataFingerprint should not be nested("long").dataFingerprint

      val rooted = node("    default: \"{{SL_ROOT}}/d\"\n")
      attribute(rooted, "status").dataFingerprint should include("__SL_ROOT__/d")
      attribute(node("    default: \"{{SL_ROOT}}/d\"\n"), "status") shouldBe
      attribute(rooted, "status")
    }
  }

  it should "treat required attributes as structural" in {
    new WithSettings() {
      val raw = NodeRef("raw", "orders")
      def node(extra: String): NodeView = load(statusWith(extra)).nodes(raw)
      val base = node("")
      val flipped = node("    required: true\n")
      flipped.structural should not be base.structural
      flipped.structural should include("[status]")
      flipped.attributes.find(_.key == "status").get.dataFingerprint shouldBe
      base.attributes.find(_.key == "status").get.dataFingerprint
      val added = node("  - name: added\n    type: string\n    required: true\n")
      added.structural should not be base.structural
      added.structural should include("[added]")
      val nested = node(
        "  - name: nested\n    type: struct\n    attributes:\n    - name: inner\n" +
        "      type: string\n      required: true\n"
      )
      nested.structural should include("[nested.inner]")
    }
  }

  it should "neutralize a root that ends a YAML value" in {
    new WithSettings() {
      def node(): NodeView =
        load(
          statusWith(
            "    default: \"{{SL_ROOT}}\"\n    script: \"upper(status)\"\n",
            comment = "{{SL_ROOT}}"
          )
        ).nodes(NodeRef("raw", "orders"))
      val first = node()
      first.metadata should include(s"Some(${ProjectSnapshot.RootToken})")
      first.attributes.find(_.key == "status").get.dataFingerprint should include(
        s"default=${ProjectSnapshot.RootToken},"
      )
      node() shouldBe first
    }
  }

  it should "stop on a cycle of causes" in {
    val a = new RuntimeException("first")
    val b = new RuntimeException("second", a)
    a.initCause(b)
    ProjectSnapshot.messages(a) shouldBe "first: second"
  }

  it should "keep task attribute privacy at the column level" in {
    new WithSettings() {
      val stg = NodeRef("stg", "orders")
      def node(privacy: String): NodeView =
        load(
          Map(
            "metadata/transform/stg/orders.sl.yml" ->
            s"""version: 1
               |task:
               |  name: orders
               |  domain: stg
               |  table: orders
               |  attributes:
               |  - name: id
               |    type: long
               |  - name: amount
               |    type: decimal
               |$privacy""".stripMargin
          )
        ).nodes(stg)
      val base = node("")
      val hidden = node("    privacy: HIDE\n")
      hidden.structural shouldBe base.structural
      hidden.attributes.find(_.key == "amount").get.dataFingerprint should not be
      base.attributes.find(_.key == "amount").get.dataFingerprint
    }
  }
}
