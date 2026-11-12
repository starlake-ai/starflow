package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.schema.model.{
  DomainInfo,
  ExpectationItem,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute
}
import ai.starlake.workflow.IngestionWorkflow
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.util.Try

/** A failed expectation fails its transform under `expectations.failOnError`, and a two-step native
  * load runs its expectations once, after its audit row (#1936).
  */
class TransformExpectationsSpec extends TestHelper {

  private val failing = "always_fails()"
  private val passing = "always_passes()"

  private def duckDbConfiguration(failOnError: Boolean): Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |expectations.failOnError = $failOnError
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/expectations_$failOnError.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private def sparkConfiguration: Config =
    ConfigFactory
      .parseString("expectations.failOnError = true")
      .withFallback(super.testConfiguration)

  private abstract class Project(configuration: Config, auditConnection: String = "test-duckdb")
      extends WithSettings(configuration) {
    private def writeMacros(): Unit =
      storageHandler.write(
        """{% macro always_fails() %}SELECT 1{% endmacro %}
          |{% macro always_passes() %}SELECT 0{% endmacro %}
          |""".stripMargin,
        new Path(DatasetArea.expectations, "transform_expectations.j2")
      )

    def transform(
      task: String,
      expectation: ExpectationItem,
      test: Boolean = false
    ): Try[String] = {
      writeMacros()
      val folder = s"$starlakeMetadataPath/transform/expectations_it"
      storageHandler.write(
        s"""version: 1
           |task:
           |  writeStrategy:
           |    type: "OVERWRITE"
           |  expectations:
           |    - expect: "${expectation.expect}"
           |      failOnError: ${expectation.failOnError}
           |""".stripMargin,
        new Path(s"$folder/$task.sl.yml")
      )
      storageHandler.write("SELECT 1 AS a", new Path(s"$folder/$task.sql"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = s"expectations_it.$task", test = test))
    }

    def load(table: String, expectation: ExpectationItem, postsql: List[String] = Nil): Try[_] = {
      writeMacros()
      DomainInfo(
        name = "expectations_load",
        metadata = Some(Metadata(directory = Some(s"$starlakeTestRoot/incoming"))),
        tables = List(
          SchemaInfo(
            table,
            Pattern.compile(s"$table.*"),
            attributes = List(TableAttribute("id", "long")),
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(";"),
                loader = Some("native")
              )
            ),
            expectations = List(expectation),
            postsql = postsql
          )
        )
      ).writeDomainAsYaml(DatasetArea.load)(storageHandler)
      val file = File(s"$starlakeTestRoot/incoming/$table-1.csv")
      file.parent.createDirectories()
      file.overwrite("id\n1\n2\n")
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true)).load(
        LoadConfig(
          domains = List("expectations_load"),
          tables = List(table),
          accessToken = None,
          test = false,
          files = Some(List(file.uri.toString)),
          scheduledDate = None,
          inPlace = true
        )
      )
    }

    private def rows(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(auditConnection).options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection).toList.map(_.toMap)
      }

    def expectationResults(domain: String, table: String): List[String] =
      rows(
        s"""SELECT CAST(success AS VARCHAR) AS result FROM audit.expectations
           |WHERE domain = '$domain' AND "schema" = '$table'""".stripMargin
      ).map(_("result"))

    def auditRows(domain: String, table: String, step: String): List[(String, String)] =
      rows(
        s"""SELECT CAST(success AS VARCHAR) AS result, CAST(countAccepted AS VARCHAR) AS accepted
           |FROM audit.audit
           |WHERE domain = '$domain' AND "schema" = '$table' AND step = '$step'""".stripMargin
      ).map(row => row("result") -> row("accepted"))

    def tableRows(table: String): Int =
      rows(s"SELECT COUNT(*) AS total FROM $table").head("total").toInt
  }

  new Project(duckDbConfiguration(failOnError = true)) {
    "A JDBC transform under expectations.failOnError" should "fail when an expectation fails" in {
      val result = transform("blocking", ExpectationItem(failing))
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      // Expectations run after the write
      tableRows("expectations_it.blocking") shouldBe 1
      expectationResults("expectations_it", "blocking") shouldBe List("false")
      auditRows("expectations_it", "blocking", "TRANSFORM").map(_._1) shouldBe List("false")
    }

    it should "succeed when its expectations pass" in {
      transform("clean", ExpectationItem(passing)).isSuccess shouldBe true
      expectationResults("expectations_it", "clean") shouldBe List("true")
      auditRows("expectations_it", "clean", "TRANSFORM").map(_._1) shouldBe List("true")
    }

    it should "succeed when the expectation that fails has failOnError false" in {
      transform("warning", ExpectationItem(failing, failOnError = false)).isSuccess shouldBe true
      expectationResults("expectations_it", "warning") shouldBe List("false")
      auditRows("expectations_it", "warning", "TRANSFORM").map(_._1) shouldBe List("true")
    }

    it should "report an expectation that cannot be rendered as a failed one" in {
      transform(
        "broken",
        ExpectationItem("always_passes(1 +)", failOnError = false)
      ).isSuccess shouldBe true
      expectationResults("expectations_it", "broken") shouldBe List("false")
    }

    it should "leave a failed expectation to the test framework in test mode" in {
      transform("tested", ExpectationItem(failing), test = true).isSuccess shouldBe true
    }

    "A two-step native load under expectations.failOnError" should
    "run its expectations once and fail after its audit row" in {
      val result = load("orders_blocking", ExpectationItem(failing))
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      tableRows("expectations_load.orders_blocking") shouldBe 2
      expectationResults("expectations_load", "orders_blocking") shouldBe List("false")
      auditRows("expectations_load", "orders_blocking", "LOAD") shouldBe List("true" -> "2")
      // Its second step writes no audit row of its own (#1838)
      auditRows("expectations_load", "orders_blocking", "TRANSFORM") shouldBe Nil
    }

    it should "not fail on an expectation with failOnError false" in {
      load("orders_warning", ExpectationItem(failing, failOnError = false)).isSuccess shouldBe true
      expectationResults("expectations_load", "orders_warning") shouldBe List("false")
      auditRows("expectations_load", "orders_warning", "LOAD") shouldBe List("true" -> "2")
      auditRows("expectations_load", "orders_warning", "TRANSFORM") shouldBe Nil
    }
  }

  new Project(duckDbConfiguration(failOnError = false)) {
    "A JDBC transform without expectations.failOnError" should
    "succeed and record the expectation that fails" in {
      transform("recorded", ExpectationItem(failing)).isSuccess shouldBe true
      expectationResults("expectations_it", "recorded") shouldBe List("false")
    }

    "A two-step native load without expectations.failOnError" should
    "run its expectations once and succeed" in {
      load("orders_recorded", ExpectationItem(failing)).isSuccess shouldBe true
      expectationResults("expectations_load", "orders_recorded") shouldBe List("false")
    }

    "A two-step native load whose second step fails" should
    "write the TRANSFORM audit row of the failure, as a Spark load does (#1838)" in {
      load(
        "orders_postsql",
        ExpectationItem(passing),
        postsql = List("SELECT * FROM expectations_load.no_such_table")
      ).isFailure shouldBe true
      auditRows("expectations_load", "orders_postsql", "TRANSFORM") shouldBe List("false" -> "-1")
    }
  }

  new Project(sparkConfiguration, auditConnection = "audit") {
    "A Spark transform under expectations.failOnError" should "fail when an expectation fails" in {
      val result = transform("spark_blocking", ExpectationItem(failing))
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      expectationResults("expectations_it", "spark_blocking") shouldBe List("false")
    }

    it should "succeed when the expectation that fails has failOnError false" in {
      transform(
        "spark_warning",
        ExpectationItem(failing, failOnError = false)
      ).isSuccess shouldBe true
      expectationResults("expectations_it", "spark_warning") shouldBe List("false")
    }
  }
}
