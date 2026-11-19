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
  TableAttribute,
  WriteStrategy,
  WriteStrategyType
}
import better.files.File
import com.google.cloud.bigquery._

import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A failed expectation fails a BigQuery transform under `expectations.failOnError`, and a two-step
  * native load runs its expectations once, after its audit row (#1936). Under
  * `expectations.beforeCommit`, the expectations declared `failOnError` run before the commit of
  * the write, which their failure rolls back (#1976).
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
      .overwrite(
        """{% macro always_fails(marker) %}SELECT 1{% endmacro %}
          |{% macro at_least_rows(n, marker) %}SELECT CASE WHEN COUNT(*) >= {{ n }} THEN 0 ELSE 1 END FROM sl_this{% endmacro %}
          |{% macro without_a(a, marker) %}SELECT COUNT(*) FROM sl_this WHERE a = {{ a }}{% endmacro %}
          |{% macro accepted_at_least(n, marker) %}SELECT CASE WHEN {{ sl_accepted_count }} >= {{ n }} AND {{ sl_rejected_count }} = 0 THEN 0 ELSE 1 END{% endmacro %}
          |""".stripMargin
      )
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
    ).map(_.get(0).getStringValue).sorted

  private def transform(task: String, failOnError: Boolean): Boolean =
    transform(task, "OVERWRITE", s"always_fails('$task')", failOnError, "SELECT 1 AS a")

  private def transform(
    task: String,
    strategy: String,
    expectation: String,
    failOnError: Boolean,
    sql: String,
    key: Option[String] = None
  ): Boolean = {
    val folder = project / "metadata" / "transform" / dataset
    (folder / s"$task.sl.yml").overwrite(
      s"""version: 1
         |task:
         |  writeStrategy:
         |    type: "$strategy"${key.map(k => "\n    key: [\"" + k + "\"]").getOrElse("")}
         |  expectations:
         |    - expect: "$expectation"
         |      failOnError: $failOnError
         |""".stripMargin
    )
    (folder / s"$task.sql").overwrite(sql)
    main("transform", "--name", s"$dataset.$task")
  }

  private def values(table: String): List[Long] =
    query(s"SELECT a FROM `$dataset.$table` ORDER BY a").map(_.get(0).getLongValue)

  private def writeDomain(
    table: String,
    expectation: ExpectationItem,
    strategy: Option[WriteStrategy] = None
  ): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        SchemaInfo(
          table,
          Pattern.compile(s"$table.*"),
          attributes = List(TableAttribute("id", "long", rename = Some("identifier"))),
          metadata = Some(
            Metadata(
              format = Some(Format.DSV),
              withHeader = Some(true),
              separator = Some(";"),
              writeStrategy = strategy
            )
          ),
          expectations = List(expectation)
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  private def load(table: String, suffix: String, content: String): Boolean = {
    val file = (project / "incoming" / dataset).createDirectories() / s"$table-$suffix.csv"
    file.overwrite(content)
    main(
      "load",
      "--domains",
      dataset,
      "--tables",
      table,
      "--files",
      file.uri.toString,
      "--inPlace"
    )
  }

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

    "A BigQuery transform under expectations.beforeCommit" should
    "keep the rows of its table when an expectation that it checks before the commit fails" in {
      withEnvs(
        "SL_ROOT"                       -> project.pathAsString,
        "SL_EXPECTATIONS_FAIL_ON_ERROR" -> "true",
        "SL_EXPECTATIONS_BEFORE_COMMIT" -> "true"
      ) {
        val overwritten = s"overwritten_$run"
        val appended = s"appended_$run"
        val created = s"created_$run"
        val upserted = s"upserted_$run"
        withTables(overwritten, appended, created, upserted) {
          // The OVERWRITE template checks them in its own transaction
          val atLeastTwo = s"at_least_rows(2, '$overwritten')"
          transform(
            overwritten,
            "OVERWRITE",
            atLeastTwo,
            failOnError = true,
            "SELECT 1 AS a UNION ALL SELECT 2 AS a"
          ) shouldBe true
          transform(
            overwritten,
            "OVERWRITE",
            atLeastTwo,
            failOnError = true,
            "SELECT 9 AS a"
          ) shouldBe false
          values(overwritten) shouldBe List(1L, 2L)
          expectationResults(overwritten) shouldBe List("false", "true")

          // The APPEND template runs in the transaction that ends with the check
          val withoutNine = s"without_a(9, '$appended')"
          transform(
            appended,
            "APPEND",
            withoutNine,
            failOnError = true,
            "SELECT 1 AS a"
          ) shouldBe true
          transform(
            appended,
            "APPEND",
            withoutNine,
            failOnError = true,
            "SELECT 3 AS a UNION ALL SELECT 9 AS a"
          ) shouldBe false
          values(appended) shouldBe List(1L)
          expectationResults(appended) shouldBe List("false", "true")

          // The table that the first run creates is dropped
          transform(
            created,
            "OVERWRITE",
            s"at_least_rows(2, '$created')",
            failOnError = true,
            "SELECT 9 AS a"
          ) shouldBe false
          Option(bigquery.getTable(TableId.of(dataset, created))) shouldBe None
          expectationResults(created) shouldBe List("false")

          // The MERGE of the UPSERT_BY_KEY template runs in the transaction too
          val upsertCheck = s"without_a(9, '$upserted')"
          transform(
            upserted,
            "UPSERT_BY_KEY",
            upsertCheck,
            true,
            "SELECT 1 AS a",
            Some("a")
          ) shouldBe true
          transform(
            upserted,
            "UPSERT_BY_KEY",
            upsertCheck,
            true,
            "SELECT 2 AS a UNION ALL SELECT 9 AS a",
            Some("a")
          ) shouldBe false
          values(upserted) shouldBe List(1L)
          expectationResults(upserted) shouldBe List("false", "true")
        }
      }
    }

    "A two-step native BigQuery load under expectations.beforeCommit" should
    "keep the rows of its table when an expectation that reads its counters fails" in {
      withEnvs(
        "SL_ROOT"                       -> project.pathAsString,
        "SL_EXPECTATIONS_FAIL_ON_ERROR" -> "true",
        "SL_EXPECTATIONS_BEFORE_COMMIT" -> "true"
      ) {
        val table = s"clients_$run"
        withTables(table) {
          writeDomain(
            table,
            ExpectationItem(s"accepted_at_least(2, '$table')"),
            Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
          )
          load(table, "1", "id\n1\n2\n") shouldBe true
          load(table, "2", "id\n9\n") shouldBe false
          query(s"SELECT identifier FROM `$dataset.$table` ORDER BY identifier")
            .map(_.get(0).getLongValue) shouldBe List(1L, 2L)
          expectationResults(table) shouldBe List("false", "true")
          query(
            s"""SELECT CAST(success AS STRING) FROM audit.audit
               |WHERE step = 'LOAD' AND paths LIKE '%$table%'""".stripMargin
          ).map(_.get(0).getStringValue).sorted shouldBe List("false", "true")
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
