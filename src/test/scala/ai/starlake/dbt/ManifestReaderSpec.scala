package ai.starlake.dbt

import ai.starlake.dbt.manifest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class ManifestReaderSpec extends AnyFlatSpec with Matchers {
  private lazy val manifest = DbtFixtures.shopManifest

  private def minimal(version: Int): java.nio.file.Path = {
    val dir = Files.createTempDirectory("dbt-manifest").resolve("target")
    Files.createDirectories(dir)
    val file = dir.resolve("manifest.json")
    Files.writeString(
      file,
      s"""{"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v$version.json",
         |"dbt_version": "1.x", "adapter_type": "postgres", "project_name": "p"},
         |"nodes": {}, "sources": {}, "macros": {}, "docs": {}, "disabled": {}}""".stripMargin
    )
    file
  }

  "ManifestReader" should "read the metadata of the fixture manifest" in {
    manifest.metadata.schemaVersion shouldBe 12
    manifest.metadata.adapterType shouldBe "duckdb"
    manifest.metadata.projectName shouldBe "shop"
    manifest.projectRoot shouldBe DbtFixtures.shopManifestPath.getParent.getParent
  }

  it should "read models with their resolved schema, alias, config and dependencies" in {
    val events = DbtFixtures.node(manifest, "customer_last_event")
    events.resourceType shouldBe "model"
    events.schema shouldBe "marts"
    events.alias shouldBe "customer_last_event"
    events.materialized shouldBe "incremental"
    events.config("incremental_strategy") shouldBe "merge"
    events.config("unique_key") shouldBe "customer_id"
    events.dependsOnNodes shouldBe List("model.shop.stg_events")
    val orders = DbtFixtures.node(manifest, "orders")
    orders.config("pre-hook") shouldBe List(
      Map(
        "sql" -> "create table if not exists {{ this.schema }}.audit_log (model_name varchar)",
        "transaction" -> true,
        "index"       -> None
      )
    )
    orders.tags shouldBe List("finance")
  }

  it should "read seeds with their file and config" in {
    val seed = DbtFixtures.node(manifest, "raw_orders")
    seed.resourceType shouldBe "seed"
    seed.schema shouldBe "raw"
    seed.originalFilePath shouldBe "seeds/raw_orders.csv"
    seed.config("column_types") shouldBe Map("amount_cents" -> "integer")
  }

  it should "read tests with their metadata and attached node" in {
    val test =
      manifest.nodes.find(_.uniqueId.startsWith("test.shop.not_null_stg_orders_amount")).get
    test.testMetadata.map(_.name) shouldBe Some("not_null")
    test.testMetadata.map(_.kwargs("column_name")) shouldBe Some("amount")
    test.attachedNode shouldBe Some("model.shop.stg_orders")
    test.config("where") shouldBe "status != 'returned'"
    val singular = manifest.nodes.find(_.name == "assert_no_future_orders").get
    singular.testMetadata shouldBe None
    singular.dependsOnNodes shouldBe List("model.shop.orders")
  }

  it should "read sources with identifier, freshness and columns" in {
    val accounts = manifest.sources.find(_.name == "crm_accounts").get
    accounts.identifier shouldBe "accounts"
    accounts.schema shouldBe "legacy"
    accounts.freshness shouldBe Some(
      DbtFreshness(Some(DbtPeriod(12, "hour")), Some(DbtPeriod(1, "day")))
    )
    accounts.loadedAtField shouldBe Some("synced_at")
    accounts.columns.map(c => c.name -> c.dataType) shouldBe List(
      "account_id" -> Some("integer"),
      "synced_at"  -> Some("timestamp")
    )
  }

  it should "read macros, docs and the snapshot" in {
    manifest.macros.map(_.packageName).toSet shouldBe Set("dbt", "dbt_duckdb", "dbt_utils", "shop")
    manifest.docs.find(_.name == "order_status").map(_.blockContents) shouldBe Some(
      "One of `placed`, `completed` or `returned`."
    )
    manifest.nodesOfType("snapshot").map(_.name) shouldBe List("customers_snapshot")
  }

  it should "accept manifest schemas v10 to v12" in {
    (10 to 12).foreach { v =>
      ManifestReader.read(minimal(v)).get.metadata.schemaVersion shouldBe v
    }
  }

  it should "refuse other versions, naming the version" in {
    List(9, 13).foreach { v =>
      val error = ManifestReader.read(minimal(v)).failed.get
      error shouldBe a[DbtImportException]
      error.getMessage should include(s"Unsupported dbt manifest schema v$v")
    }
  }

  it should "say how to produce a missing manifest" in {
    ManifestReader
      .read(java.nio.file.Paths.get("/nonexistent/target/manifest.json"))
      .failed
      .get
      .getMessage should include(
      "dbt deps && dbt parse"
    )
  }

  "DbtProjectFile" should "read the vars and the profile of dbt_project.yml" in {
    val project = DbtProjectFile.read(manifest.projectRoot).get.get
    project.name shouldBe "shop"
    project.profile shouldBe Some("shop")
    project.vars shouldBe Map("min_amount" -> 0, "shop" -> Map("currency" -> "EUR"))
  }

  "DbtVars" should "prefer --vars, then the package vars, then the global ones" in {
    val vars = DbtVars(
      Map("a" -> "global", "b" -> "global", "shop" -> Map("b" -> "scoped")),
      Set("shop"),
      Map("c" -> "cli")
    )
    vars.lookup("shop", "a") shouldBe Some("global")
    vars.lookup("shop", "b") shouldBe Some("scoped")
    vars.lookup("other", "b") shouldBe Some("global")
    vars.lookup("shop", "c") shouldBe Some("cli")
    DbtVars.parseCli("{load_source_data: true}").get shouldBe Map("load_source_data" -> true)
    DbtVars.parseCli("[1]").isFailure shouldBe true
  }

  "DbtProfiles" should "read a DuckDB target, its file relative to the dbt project" in {
    val profiles = Files.createTempFile("profiles", ".yml")
    Files.writeString(
      profiles,
      "shop:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: shop.duckdb\n      schema: main\n"
    )
    val target = DbtProfiles.read(profiles, "shop", None, manifest.projectRoot).get
    target shouldBe DbtTarget(
      "dev",
      "duckdb",
      Some("main"),
      Some("shop"),
      Some(manifest.projectRoot.resolve("shop.duckdb"))
    )
    DbtProfiles
      .read(profiles, "shop", Some("prod"), manifest.projectRoot)
      .failed
      .get
      .getMessage should include("target 'prod'")
  }

  it should "keep only models, seeds, snapshots and tests of the disabled section" in {
    def disabledNode(kind: String, name: String) =
      s"""{"unique_id": "$kind.p.$name", "resource_type": "$kind", "package_name": "p",
         |"name": "$name", "schema": "s", "alias": "$name", "config": {"enabled": false}}""".stripMargin
    val kinds =
      List("model", "seed", "snapshot", "test", "source", "analysis", "operation", "exposure")
    val disabled = kinds.map(k => s""""$k.p.x_$k": [${disabledNode(k, "x_" + k)}]""").mkString(", ")
    val dir = Files.createTempDirectory("dbt-manifest").resolve("target")
    Files.createDirectories(dir)
    val file = dir.resolve("manifest.json")
    Files.writeString(
      file,
      s"""{"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
         |"dbt_version": "1.x", "adapter_type": "postgres", "project_name": "p"},
         |"nodes": {}, "sources": {}, "macros": {}, "docs": {}, "disabled": {$disabled}}""".stripMargin
    )
    val read = ManifestReader.read(file).get
    read.disabled.map(_.resourceType).toSet shouldBe Set("model", "seed", "snapshot", "test")
    read.nodesById.keySet shouldBe Set(
      "model.p.x_model",
      "seed.p.x_seed",
      "snapshot.p.x_snapshot",
      "test.p.x_test"
    )
  }

  it should "reject an empty or non-map profiles file and a target that is not a map" in {
    def read(content: String) = {
      val profiles = Files.createTempFile("profiles", ".yml")
      Files.writeString(profiles, content)
      (profiles, DbtProfiles.read(profiles, "shop", None, profiles.getParent))
    }
    for (content <- List("", "- a\n- b\n", "just text")) {
      val (file, result) = read(content)
      result.failed.get shouldBe a[DbtImportException]
      result.failed.get.getMessage should include(file.toString)
    }
    val (file, result) = read("shop:\n  target: dev\n  outputs:\n    dev: nope\n")
    result.failed.get shouldBe a[DbtImportException]
    result.failed.get.getMessage should include(file.toString)
  }
}
