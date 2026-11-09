package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{AllSinks, DomainInfo, Format, Metadata, SchemaInfo, TableAttribute}
import better.files.File
import com.google.cloud.bigquery._

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The temporary tables of a two-step native load do not inherit the required partition filter of
  * its target, which would reject the queries of the second step (#1838).
  */
class BigQueryNativeTempTablesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_native_temp_tables"
  private val table = "partitioned"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-temp-tables-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def load(file: File): Boolean =
    Try(
      new Main().run(
        Array(
          "load",
          "--domains",
          dataset,
          "--tables",
          table,
          "--files",
          file.uri.toString,
          "--inPlace"
        )
      )
    ).getOrElse(false)

  private def writeDomain(): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        SchemaInfo(
          table,
          Pattern.compile(s"$table.*"),
          attributes = List(
            TableAttribute("id", "long", required = Some(true)),
            TableAttribute("day", "date"),
            TableAttribute("label", "string")
          ),
          metadata = Some(
            Metadata(
              format = Some(Format.DSV),
              withHeader = Some(true),
              separator = Some(";"),
              sink = Some(
                AllSinks(partition = Some(Seq("day")), requirePartitionFilter = Some(true))
              )
            )
          ),
          // A filter makes the load take two steps
          filter = Some("label != 'skipped'")
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A two-step native load" should "read its temporary tables when its target requires a partition filter" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        bigquery.delete(TableId.of(dataset, table))
        writeDomain()
        val file = (project / "incoming" / dataset).createDirectories() / s"$table-1.csv"
        file.overwrite(
          "id;day;label\n1;2026-11-01;first\n2;2026-11-02;second\n3;2026-11-02;skipped\n"
        )

        load(file) shouldBe true
        val ids = bigquery
          .query(
            QueryJobConfiguration
              .newBuilder(
                s"SELECT id FROM `$dataset.$table` WHERE day >= '2000-01-01' ORDER BY id"
              )
              .build()
          )
          .iterateAll()
          .asScala
          .map(_.get(0).getLongValue)
          .toList
        ids shouldBe List(1L, 2L)
        // The target keeps it
        val target = bigquery.getTable(TableId.of(dataset, table))
        val partitioning = target.getDefinition[StandardTableDefinition].getTimePartitioning
        (Option(target.getRequirePartitionFilter).exists(_.booleanValue) ||
        Option(partitioning)
          .flatMap(p => Option(p.getRequirePartitionFilter))
          .exists(
            _.booleanValue
          )) shouldBe true
      }
    }
  }
}
