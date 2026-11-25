package ai.starlake.dbt

import ai.starlake.dbt.report.{BakedValue, EntryStatus}
import ai.starlake.TestHelper
import ai.starlake.dbt.write.ProjectWriter
import ai.starlake.schema.model.TableAttribute
import com.typesafe.config.ConfigFactory

import java.nio.file.{Files, Path, Paths}
import scala.util.Success

/** The importer on projects of its own: what the next steps run, the Jinja of profiles.yml and of
  * dbt_project.yml, the --connection option
  */
class DbtImporterSpec extends TestHelper {
  private val stub =
    Some((_: java.nio.file.Path, _: String) => Success(List(TableAttribute("id", "long"))))

  private def written(result: DbtImportResult, suffix: String): String =
    Files.readString(
      Paths.get(
        result.written
          .map(_.toUri.getPath)
          .find(_.endsWith(suffix))
          .getOrElse(fail(s"$suffix not written in ${result.written}"))
      )
    )

  private def profiles(output: String): Path = {
    val file = Files.createTempFile("profiles", ".yml")
    Files.writeString(file, s"p:\n  target: dev\n  outputs:\n    dev:\n$output")
    file
  }

  new WithSettings() {
    "The next steps" should "leave out every task downstream of a skipped model" in {
      // a is skipped; b (a view) reads a, c reads b, d reads c: none of them can run
      val manifest = DbtFixtures.miniProject(
        List(
          DbtFixtures.miniModel("a", "select {{ run_query('select 1') }} 1 as id"),
          DbtFixtures.miniModel(
            "b",
            "select * from {{ ref('a') }}",
            List("model.p.a"),
            materialized = "view"
          ),
          DbtFixtures.miniModel("c", "select * from {{ ref('b') }}", List("model.p.b")),
          DbtFixtures.miniModel("d", "select * from {{ ref('c') }}", List("model.p.c")),
          DbtFixtures.miniModel("ok", "select 1 as id")
        )
      )
      val result = DbtImporter
        .run(
          DbtImportConfig("import", manifest.toString, connection = Some("bigquery"), force = true),
          stub
        )
        .get
      result.report.general should contain("next: starlake transform --name s.ok")
      val md = result.report.toMarkdown
      md should include("`s.b` depends on skipped `model.p.a`")
      md should include("`s.c` depends on skipped `model.p.a`")
      md should include("`s.d` depends on skipped `model.p.a`")
    }

    "Jinja in profiles.yml" should "render the DuckDB path and the schema with env_var" in {
      val manifest = DbtFixtures.miniProject(
        List(DbtFixtures.miniModel("m", "select '{{ target.schema }}' as s"))
      )
      val file = profiles(
        "      type: duckdb\n" +
        "      path: \"{{ env_var('DBT_DUCKDB_PATH') }}\"\n" +
        "      schema: \"{{ env_var('DBT_SCHEMA', 'analytics') }}\"\n"
      )
      val env = Map("DBT_DUCKDB_PATH" -> "/data/shop.duckdb")
      val result = DbtImporter
        .run(
          DbtImportConfig(
            "import",
            manifest.toString,
            profiles = Some(file.toString),
            force = true
          ),
          stub,
          env.get
        )
        .get
      result.report.general should contain(
        "connection dbt_dev added to application.sl.yml (jdbc:duckdb:/data/shop.duckdb); the file is rewritten without its comments"
      )
      written(result, "/transform/s/m.sql") should include("'analytics' as s")
      result.report.envVars should contain allOf (
        BakedValue("DBT_DUCKDB_PATH", "/data/shop.duckdb", List("profiles.yml")),
        BakedValue("DBT_SCHEMA", "analytics", List("profiles.yml"))
      )
    }

    it should "write no connection when a value it uses cannot be rendered" in {
      val manifest = DbtFixtures.miniProject(List(DbtFixtures.miniModel("m", "select 1 as id")))
      val file = profiles(
        "      type: duckdb\n      path: \"{{ env_var('DBT_UNSET_PATH') }}\"\n      schema: main\n"
      )
      val result = DbtImporter
        .run(
          DbtImportConfig(
            "import",
            manifest.toString,
            profiles = Some(file.toString),
            force = true
          ),
          stub,
          Map.empty[String, String].get _
        )
        .get
      result.exitCode shouldBe 0
      result.written should not contain ProjectWriter.applicationPath
      val note = result.report.general.find(_.contains("DBT_UNSET_PATH")).getOrElse("")
      note should include("path")
      note should include("--connection")
    }

    "Jinja in the vars of dbt_project.yml" should "render with env_var, a var it cannot render skipping its models" in {
      val manifest = DbtFixtures.miniProject(
        List(
          DbtFixtures.miniModel("m1", "select '{{ var(\"region\") }}' as r"),
          DbtFixtures.miniModel("m2", "select '{{ var(\"missing\") }}' as r")
        ),
        projectYml = "name: p\nprofile: p\nvars:\n" +
          "  region: \"{{ env_var('DBT_REGION', 'eu') }}\"\n" +
          "  missing: \"{{ env_var('DBT_NOT_SET') }}\"\n"
      )
      val result = DbtImporter
        .run(
          DbtImportConfig("import", manifest.toString, connection = Some("bigquery"), force = true),
          stub,
          Map.empty[String, String].get _
        )
        .get
      written(result, "/transform/s/m1.sql") should include("'eu' as r")
      result.report.vars.find(_.name == "region").map(_.value) shouldBe Some("eu")
      result.report.envVars should contain(
        BakedValue("DBT_REGION", "eu", List("dbt_project.yml"))
      )
      result.report.entries.find(_.dbtId == "model.p.m2").map(_.status) match {
        case Some(EntryStatus.Skipped(reason)) => reason should include("DBT_NOT_SET")
        case other                             => fail(s"m2 not skipped: $other")
      }
    }

    "A test whose tables have the same depth" should "note that their build order is not known" in {
      val test = Map[String, Any](
        "unique_id"     -> "test.p.t",
        "resource_type" -> "test",
        "package_name"  -> "p",
        "name"          -> "t",
        "database"      -> "db",
        "schema"        -> "s",
        "alias"         -> "t",
        "raw_code" -> "select * from {{ ref('a') }} join {{ ref('b') }} using (id) where false",
        "original_file_path" -> "tests/t.sql",
        "config"             -> Map("enabled" -> true, "severity" -> "ERROR"),
        "depends_on"         -> Map("nodes" -> List("model.p.a", "model.p.b"), "macros" -> Nil)
      )
      val manifest = DbtFixtures.miniProject(
        List(
          DbtFixtures.miniModel("a", "select 1 as id"),
          DbtFixtures.miniModel("b", "select 1 as id"),
          test
        )
      )
      val result = DbtImporter
        .run(
          DbtImportConfig("import", manifest.toString, connection = Some("bigquery"), force = true),
          stub
        )
        .get
      val entry = result.report.entries.find(_.dbtId == "test.p.t").get
      entry.status shouldBe EntryStatus.Converted
      entry.notes.mkString should include("same depth")
    }

    "--connection" should "fail as a usage error when the project has no such connection" in {
      val manifest = DbtFixtures.miniProject(List(DbtFixtures.miniModel("m", "select 1 as id")))
      val failure = DbtImporter
        .run(DbtImportConfig("import", manifest.toString, connection = Some("nope")), stub)
        .failed
        .get
      failure shouldBe a[DbtImportException]
      failure.getMessage should include("nope")
    }
  }

  new WithSettings(
    ConfigFactory.parseString("connectionRef = \"\"").withFallback(super.testConfiguration)
  ) {
    "--connection" should "go on every task when the project has no default connection" in {
      val manifest = DbtFixtures.miniProject(List(DbtFixtures.miniModel("m", "select 1 as id")))
      val result = DbtImporter
        .run(
          DbtImportConfig("import", manifest.toString, connection = Some("bigquery"), force = true),
          stub
        )
        .get
      written(result, "/transform/s/m.sl.yml") should include regex "connectionRef: \"?bigquery\"?"
    }
  }

  private def duckdbConfiguration =
    ConfigFactory
      .parseString(
        s"""
           |connections.test-duckdb {
           |  type = "jdbc"
           |  loader = "native"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/introspect.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(duckdbConfiguration) {
    "--introspect without a connection" should "fail before writing anything" in {
      val result = DbtImporter.run(
        DbtImportConfig(
          action = "import",
          manifest = DbtFixtures.shopManifestPath.toString,
          introspect = true
        )
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe
      "--introspect needs a connection: pass --connection, or --profiles with a DuckDB target that has a file"
    }

    "--introspect on a DuckDB connection" should "convert the model that runs a query, and report the answer" in {
      val result = DbtImporter
        .run(
          DbtImportConfig(
            action = "import",
            manifest = DbtFixtures.shopManifestPath.toString,
            connection = Some("test-duckdb"),
            introspect = true,
            force = true
          )
        )
        .get
      result.report.converted.map(_.dbtId) should contain("model.shop.needs_warehouse")
      result.report.warehouseReads.map(r => (r.name, r.value, r.usedBy)) should contain(
        ("query: select 42 as answer", "1 rows", List("marts.needs_warehouse"))
      )
      result.report.general should contain("introspection on connection test-duckdb")
    }
  }
}
