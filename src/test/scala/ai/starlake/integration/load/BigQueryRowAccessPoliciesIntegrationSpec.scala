package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  DomainInfo,
  Format,
  Metadata,
  RowLevelSecurity,
  SchemaInfo,
  TableAttribute
}
import better.files.File
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery._

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The row access policies that a load declares replace the existing ones, without leaving the
  * table unprotected in between (#1913).
  */
class BigQueryRowAccessPoliciesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_row_access_policies"
  private val table = "protected"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-rls-")
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

  private def writeDomain(principal: String, predicate: String): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        SchemaInfo(
          table,
          Pattern.compile(s"$table.*"),
          attributes = List(TableAttribute("id", "long", required = Some(true))),
          metadata = Some(
            Metadata(format = Some(Format.DSV), withHeader = Some(true), separator = Some(";"))
          ),
          rls = List(RowLevelSecurity("declared", predicate, Set(s"sa:$principal")))
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  private def visibleIds(): Set[Long] =
    bigquery
      .query(QueryJobConfiguration.newBuilder(s"SELECT id FROM `$dataset.$table`").build())
      .iterateAll()
      .asScala
      .map(_.get(0).getLongValue)
      .toSet

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "The row access policies of a load" should "replace the declared ones and drop the others" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        val principal = GoogleCredentials.getApplicationDefault match {
          case account: ServiceAccountCredentials => account.getClientEmail
          case _ => cancel("The policies are granted to the service account of the test")
        }
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        val tableId = TableId.of(dataset, table)
        bigquery.delete(tableId)
        bigquery.create(
          TableInfo.of(
            tableId,
            StandardTableDefinition.of(Schema.of(Field.of("id", StandardSQLTypeName.INT64)))
          )
        )
        // Lets the principal read every row, as long as it exists
        bigquery.query(
          QueryJobConfiguration
            .newBuilder(
              s"""CREATE ROW ACCESS POLICY stale ON `$dataset.$table`
                 |GRANT TO ("serviceAccount:$principal") FILTER USING (TRUE)""".stripMargin
            )
            .build()
        )
        val file = (project / "incoming" / dataset).createDirectories() / s"$table-1.csv"
        file.overwrite("id\n1\n2\n3\n")

        writeDomain(principal, "id > 1")
        load(file) shouldBe true
        visibleIds() shouldBe Set(2L, 3L)

        // The policy exists now: it is replaced
        writeDomain(principal, "id > 2")
        load(file) shouldBe true
        visibleIds() shouldBe Set(3L)
      }
    }
  }
}
