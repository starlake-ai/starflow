package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  AccessControlEntry,
  DomainInfo,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute
}
import better.files.File
import com.google.cloud.bigquery._
import org.apache.hadoop.fs.Path

import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A native BigQuery load that fails before writing its rows gives its files back to the stage
  * area. One that fails after its load job, on the access policies of its table, keeps them in the
  * ingesting area (#1934).
  */
class BigQueryFailedLoadFilesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_failed_load_files"

  // Concurrent CI runs share the dataset
  private val run = UUID.randomUUID().toString.replace("-", "")

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-failed-loads-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def writeDomain(table: String, acl: List[AccessControlEntry] = Nil): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        SchemaInfo(
          table,
          Pattern.compile(s"$table-.*[.]csv"),
          attributes = List(TableAttribute("id", "long")),
          metadata = Some(
            Metadata(format = Some(Format.DSV), withHeader = Some(true), separator = Some(";"))
          ),
          acl = acl
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  /** Delivers a file to the stage area of the domain, where `load` picks it up */
  private def deliver(table: String, content: String): Unit =
    settings.storageHandler().write(content, new Path(DatasetArea.stage(dataset), s"$table-1.csv"))

  private def load(table: String): Boolean =
    Try(new Main().run(Array("load", "--domains", dataset, "--tables", table))).getOrElse(false)

  private def filesIn(area: Path, table: String): List[String] =
    settings
      .storageHandler()
      .list(area, recursive = false)
      .map(_.path.getName)
      .filter(_.startsWith(s"$table-"))

  private def rows(table: String): Long =
    Try(
      bigquery
        .query(QueryJobConfiguration.newBuilder(s"SELECT COUNT(*) FROM `$dataset.$table`").build())
        .iterateAll()
        .asScala
        .head
        .get(0)
        .getLongValue
    ).getOrElse(0L)

  private def withTable[T](table: String)(test: => T): T =
    try test
    finally Try(bigquery.delete(TableId.of(dataset, table)))

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A native BigQuery load whose access policies fail after its load job" should
    "keep its files in the ingesting area" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val table = s"unprotected_$run"
        withTable(table) {
          // BigQuery refuses a table policy that grants a role to a user who does not exist
          writeDomain(
            table,
            List(
              AccessControlEntry(
                "roles/bigquery.dataViewer",
                Set(s"user:nobody-$run@example.invalid")
              )
            )
          )
          deliver(table, "id\n1\n2\n")
          load(table) shouldBe false
          rows(table) shouldBe 2
          filesIn(DatasetArea.ingesting(dataset), table) shouldBe List(s"$table-1.csv")
          filesIn(DatasetArea.stage(dataset), table) shouldBe Nil
        }
      }
    }

    "A native BigQuery load that rejects all its rows on error" should
    "give its files back to the stage area" in {
      withEnvs("SL_ROOT" -> project.pathAsString, "SL_REJECT_ALL_ON_ERROR" -> "true") {
        val table = s"rejected_$run"
        withTable(table) {
          writeDomain(table)
          deliver(table, "id\n1\nnot-a-number\n")
          load(table) shouldBe false
          rows(table) shouldBe 0
          filesIn(DatasetArea.stage(dataset), table) shouldBe List(s"$table-1.csv")
          filesIn(DatasetArea.ingesting(dataset), table) shouldBe Nil
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
