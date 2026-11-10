package ai.starlake.integration.transform

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  DomainInfo,
  ExpectationItem,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute
}
import better.files.File
import com.google.cloud.bigquery._

import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A failed expectation fails a BigQuery transform under `expectations.failOnError`, and a two-step
  * native load runs its expectations once, after its audit row (#1936).
  */
class BigQueryExpectationsIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_expectations"

  // The audit tables are shared by every CI run
  private val run = UUID.randomUUID().toString.replace("-", "")

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-expectations-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // Written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    (dir / "metadata" / "transform" / dataset).createDirectories()
    (dir / "metadata" / "expectations").createDirectories()
    (dir / "metadata" / "expectations" / "it.j2")
      .overwrite("{% macro always_fails(marker) %}SELECT 1{% endmacro %}")
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def main(args: String*): Boolean = Try(new Main().run(args.toArray)).getOrElse(false)

  private def query(sql: String): List[FieldValueList] =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).build()).iterateAll().asScala.toList

  private def expectationResults(marker: String): List[String] =
    query(
      s"SELECT CAST(success AS STRING) FROM audit.expectations WHERE params LIKE '%$marker%'"
    ).map(_.get(0).getStringValue)

  private def transform(task: String, failOnError: Boolean): Boolean = {
    val folder = project / "metadata" / "transform" / dataset
    (folder / s"$task.sl.yml").overwrite(
      s"""version: 1
         |task:
         |  writeStrategy:
         |    type: "OVERWRITE"
         |  expectations:
         |    - expect: "always_fails('$task')"
         |      failOnError: $failOnError
         |""".stripMargin
    )
    (folder / s"$task.sql").overwrite("SELECT 1 AS a")
    main("transform", "--name", s"$dataset.$task")
  }

  private def writeDomain(table: String, expectation: ExpectationItem): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        SchemaInfo(
          table,
          Pattern.compile(s"$table.*"),
          attributes = List(TableAttribute("id", "long", rename = Some("identifier"))),
          metadata = Some(
            Metadata(format = Some(Format.DSV), withHeader = Some(true), separator = Some(";"))
          ),
          expectations = List(expectation)
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  private def withTables[T](tables: String*)(test: => T): T =
    try test
    finally tables.foreach(table => Try(bigquery.delete(TableId.of(dataset, table))))

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A BigQuery transform under expectations.failOnError" should
    "fail when an expectation fails, unless that expectation has failOnError false" in {
      withEnvs(
        "SL_ROOT"                       -> project.pathAsString,
        "SL_EXPECTATIONS_FAIL_ON_ERROR" -> "true"
      ) {
        val blocking = s"blocking_$run"
        val warning = s"warning_$run"
        withTables(blocking, warning) {
          transform(blocking, failOnError = true) shouldBe false
          // Expectations run after the write
          query(s"SELECT a FROM `$dataset.$blocking`").map(_.get(0).getLongValue) shouldBe List(1L)
          expectationResults(blocking) shouldBe List("false")
          // A dry run writes nothing: its expectations do not run
          main("transform", "--name", s"$dataset.$blocking", "--dry-run") shouldBe true
          expectationResults(blocking) shouldBe List("false")
          query(
            s"""SELECT CAST(success AS STRING) FROM audit.audit
               |WHERE domain = '$dataset' AND schema = '$blocking'""".stripMargin
          ).map(_.get(0).getStringValue) shouldBe List("false")

          transform(warning, failOnError = false) shouldBe true
          expectationResults(warning) shouldBe List("false")
        }
      }
    }

    "A two-step native BigQuery load under expectations.failOnError" should
    "run its expectations once and fail after its audit row" in {
      withEnvs(
        "SL_ROOT"                       -> project.pathAsString,
        "SL_EXPECTATIONS_FAIL_ON_ERROR" -> "true"
      ) {
        val table = s"orders_$run"
        withTables(table) {
          // The rename of its attribute makes the load take two steps
          writeDomain(table, ExpectationItem(s"always_fails('$table')"))
          val file = (project / "incoming" / dataset).createDirectories() / s"$table-1.csv"
          file.overwrite("id\n1\n2\n")
          main(
            "load",
            "--domains",
            dataset,
            "--tables",
            table,
            "--files",
            file.uri.toString,
            "--inPlace"
          ) shouldBe false

          query(s"SELECT identifier FROM `$dataset.$table`") should have size 2
          expectationResults(table) shouldBe List("false")
          query(
            s"""SELECT CAST(success AS STRING), countAccepted FROM audit.audit
               |WHERE step = 'LOAD' AND paths LIKE '%$table%'""".stripMargin
          ).map(row => row.get(0).getStringValue -> row.get(1).getLongValue) shouldBe List(
            "true" -> 2L
          )
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
