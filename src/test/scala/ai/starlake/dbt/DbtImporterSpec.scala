package ai.starlake.dbt

import ai.starlake.dbt.report.EntryStatus
import ai.starlake.TestHelper
import ai.starlake.schema.model.TableAttribute

import java.nio.file.{Files, Paths}
import scala.util.Success

/** The importer on projects of its own: what the next steps run, the notes of the tests */
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

  }
}
