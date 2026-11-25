package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.job.Main
import ai.starlake.schema.model.TableAttribute

import java.nio.file.Files
import scala.util.Success

class DbtImportCmdSpec extends TestHelper {
  "DbtImportCmd" should "be the dbt command of Main" in {
    Main.commands.find(_.command == "dbt") shouldBe Some(DbtImportCmd)
  }

  it should "parse import and its options" in {
    DbtImportCmd.parse(
      Seq(
        "import",
        "--manifest",
        "m.json",
        "--profiles",
        "p.yml",
        "--target",
        "prod",
        "--vars",
        "{a: 1}",
        "--force"
      )
    ) shouldBe Some(
      DbtImportConfig(
        "import",
        "m.json",
        Some("p.yml"),
        Some("prod"),
        None,
        Some("{a: 1}"),
        force = true
      )
    )
  }

  it should "read --introspect and --introspect-max-rows" in {
    val c = DbtImportCmd
      .parse(Seq("import", "--manifest", "m.json", "--introspect", "--introspect-max-rows", "50"))
      .get
    c.introspect shouldBe true
    c.introspectMaxRows shouldBe 50
    DbtImportCmd.parse(Seq("import", "--manifest", "m.json")).get.introspect shouldBe false
    DbtImportCmd.parse(
      Seq("import", "--manifest", "m.json", "--introspect-max-rows", "0")
    ) shouldBe None
  }

  it should "refuse another action and a missing manifest" in {
    DbtImportCmd.parse(Seq("export", "--manifest", "m.json")) shouldBe None
    DbtImportCmd.parse(Seq("import")) shouldBe None
  }

  new WithSettings() {
    private val stub =
      Some((_: java.nio.file.Path, _: String) => Success(List(TableAttribute("id", "long"))))

    "An import" should "fail as a usage error on a manifest that does not exist" in {
      DbtImporter
        .run(DbtImportConfig("import", "/nonexistent/target/manifest.json"), stub)
        .failed
        .get shouldBe
      a[IllegalArgumentException]
    }

    it should "exit with 1, writing nothing, when its files exist; with 0 under --force" in {
      val config =
        DbtImportConfig(
          "import",
          DbtFixtures.shopManifestPath.toString,
          connection = Some("bigquery")
        )
      DbtImporter.run(config, stub).get.exitCode shouldBe 0
      val second = DbtImporter.run(config, stub).get
      second.exitCode shouldBe 1
      second.written shouldBe Nil
      second.conflicts should not be empty
      DbtImporter.run(config.copy(force = true), stub).get.exitCode shouldBe 0
    }

    it should "write the DuckDB connection of the dbt profile" in {
      val profiles = Files.createTempFile("profiles", ".yml")
      Files.writeString(
        profiles,
        "shop:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: shop.duckdb\n      schema: main\n"
      )
      val result = DbtImporter
        .run(
          DbtImportConfig(
            "import",
            DbtFixtures.shopManifestPath.toString,
            profiles = Some(profiles.toString),
            force = true
          ),
          stub
        )
        .get
      result.exitCode shouldBe 0
      result.report.general.exists(
        _.startsWith("connection dbt_dev added to application.sl.yml (jdbc:duckdb:")
      ) shouldBe true
    }
  }
}
