package ai.starlake.integration.load

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File
import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._

/** The native BigQuery loader creates the columns with the BigQuery type of the ddlMapping of their
  * attribute type, and keeps the type of the columns that already exist.
  */
class LoadBQNativeTypesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_native_types"
  private val tables = List("typed", "typed_two_steps", "existing_ts", "existing_two_steps")

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-native-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-native-types-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    dir
  }

  override def theSampleFolder: File = project
  override def sampleDataDir: File = projectTemplate / "sample-data"

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def fields(table: String): List[Field] =
    bigquery
      .getTable(TableId.of(dataset, table))
      .getDefinition[StandardTableDefinition]()
      .getSchema
      .getFields
      .asScala
      .toList

  /** @return
    *   the type, max length and mode of each column
    */
  private def columns(table: String): Map[String, (StandardSQLTypeName, Option[Long], Field.Mode)] =
    fields(table).map { field =>
      field.getName -> (
        field.getType.getStandardType,
        Option(field.getMaxLength).map(_.longValue()),
        Option(field.getMode).getOrElse(Field.Mode.NULLABLE)
      )
    }.toMap

  private def count(table: String): Long =
    bigquery
      .query(QueryJobConfiguration.newBuilder(s"SELECT COUNT(*) FROM `$dataset.$table`").build())
      .iterateAll()
      .asScala
      .head
      .get(0)
      .getLongValue

  private def load(): Unit = {
    copyFilesToIncomingDir(sampleDataDir)
    assert(new Main().run(Array("stage")))
    assert(new Main().run(Array("load")))
  }

  private val expected = Map(
    "id"      -> (StandardSQLTypeName.INT64, None, Field.Mode.REQUIRED),
    "civil"   -> (StandardSQLTypeName.DATETIME, None, Field.Mode.NULLABLE),
    "wall"    -> (StandardSQLTypeName.TIME, None, Field.Mode.NULLABLE),
    "payload" -> (StandardSQLTypeName.JSON, None, Field.Mode.NULLABLE),
    "area"    -> (StandardSQLTypeName.GEOGRAPHY, None, Field.Mode.NULLABLE),
    "code"    -> (StandardSQLTypeName.STRING, Some(10L), Field.Mode.NULLABLE),
    // Default types are unchanged
    "amount" -> (StandardSQLTypeName.NUMERIC, None, Field.Mode.NULLABLE),
    "tags"   -> (StandardSQLTypeName.STRING, None, Field.Mode.REPEATED)
  )

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A native BigQuery load" should "create the columns with the BigQuery type of their ddlMapping" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        tables.foreach(table => bigquery.delete(TableId.of(dataset, table)))
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        // Columns created before with the type derived from the primitive type. In the two-step
        // table, id is nullable while the attribute is required.
        List("existing_ts" -> Field.Mode.REQUIRED, "existing_two_steps" -> Field.Mode.NULLABLE)
          .foreach { case (table, idMode) =>
            bigquery.create(
              TableInfo.of(
                TableId.of(dataset, table),
                StandardTableDefinition.of(
                  Schema.of(
                    Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(idMode).build(),
                    Field.of("civil", StandardSQLTypeName.TIMESTAMP)
                  )
                )
              )
            )
          }

        load()

        columns("typed") shouldBe expected
        columns("typed_two_steps") shouldBe expected
        columns("existing_ts") shouldBe expected +
        ("civil" -> (StandardSQLTypeName.TIMESTAMP, None, Field.Mode.NULLABLE))
        columns("existing_two_steps") shouldBe expected +
        ("civil" -> (StandardSQLTypeName.TIMESTAMP, None, Field.Mode.NULLABLE)) +
        ("id"    -> (StandardSQLTypeName.INT64, None, Field.Mode.NULLABLE))

        val rows = bigquery
          .query(
            QueryJobConfiguration
              .newBuilder(
                s"SELECT CAST(civil AS STRING), CAST(wall AS STRING), TO_JSON_STRING(payload), ST_ASTEXT(area), code FROM `$dataset.typed`"
              )
              .build()
          )
          .iterateAll()
          .asScala
          .map(row => (0 until 5).map(i => row.get(i).getStringValue).toList)
          .toList
        rows shouldBe List(
          List("2026-01-02 03:04:05", "12:34:56", """{"a":1}""", "POINT(1 2)", "abc")
        )

        // The next load keeps the types and their parameters
        load()
        columns("typed") shouldBe expected
        columns("typed_two_steps") shouldBe expected
        tables.foreach(table => count(table) shouldBe 2)
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
