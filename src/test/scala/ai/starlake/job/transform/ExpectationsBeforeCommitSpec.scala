package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.schema.model.{
  AllSinks,
  DomainInfo,
  ExpectationItem,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute,
  WriteStrategy,
  WriteStrategyType
}
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.util.Try

/** Under `expectations.beforeCommit`, the expectations declared `failOnError` of a JDBC transform
  * and of a native load in two steps run inside the transaction of the write: a failure rolls the
  * write back, and a load gives its files back to the stage area (#1976).
  */
class ExpectationsBeforeCommitSpec extends TestHelper {

  private val taskFolder = "commit_it"
  private val domainName = "commit_loads"

  private def configuration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |expectations.failOnError = true
           |expectations.beforeCommit = true
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/before_commit_1976.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private def rows(ids: Int*): String = ids.map(id => s"SELECT $id AS id").mkString(" UNION ALL ")

  new WithSettings(configuration) {
    private def writeMacros(): Unit =
      storageHandler.write(
        """{% macro at_least_rows(n) %}SELECT CASE WHEN COUNT(*) >= {{ n }} THEN 0 ELSE 1 END FROM sl_this{% endmacro %}
          |{% macro without_id(id) %}SELECT COUNT(*) FROM sl_this WHERE id = {{ id }}{% endmacro %}
          |{% macro always_fails() %}SELECT 1{% endmacro %}
          |{% macro load_counts(n) %}SELECT CASE WHEN {{ sl_input_count }} = {{ n }} AND {{ sl_accepted_count }} = {{ n }} AND {{ sl_rejected_count }} = -1 THEN 0 ELSE 1 END{% endmacro %}
          |{% macro all_rejected() %}SELECT CASE WHEN {{ sl_rejected_count }} > 0 AND {{ sl_accepted_count }} = 0 THEN 1 ELSE 0 END{% endmacro %}
          |{% macro unknown_column() %}SELECT COUNT(no_such_column) FROM sl_this{% endmacro %}
          |""".stripMargin,
        new Path(DatasetArea.expectations, "before_commit.j2")
      )

    private def workflow(): IngestionWorkflow =
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))

    private def expectationsYaml(expectations: List[ExpectationItem], indent: String): String =
      expectations
        .map(e => s"""$indent- expect: "${e.expect}"
                     |$indent  failOnError: ${e.failOnError}""".stripMargin)
        .mkString("\n")

    def transform(
      task: String,
      strategy: WriteStrategyType,
      expectations: List[ExpectationItem],
      sql: String
    ): Try[String] = {
      writeMacros()
      val folder = s"$starlakeMetadataPath/transform/$taskFolder"
      storageHandler.write(
        s"""version: 1
           |task:
           |  writeStrategy:
           |    type: "${strategy.value}"
           |  expectations:
           |${expectationsYaml(expectations, "    ")}
           |""".stripMargin,
        new Path(s"$folder/$task.sl.yml")
      )
      storageHandler.write(sql, new Path(s"$folder/$task.sql"))
      workflow().autoJob(TransformConfig(name = s"$taskFolder.$task"))
    }

    def writeTable(
      table: String,
      strategy: WriteStrategyType,
      expectations: List[ExpectationItem],
      format: Format = Format.DSV
    ): Unit = {
      writeMacros()
      val extension = if (format == Format.PARQUET) "parquet" else "csv"
      DomainInfo(
        name = domainName,
        metadata = Some(Metadata(directory = Some(s"$starlakeTestRoot/incoming"))),
        tables = List(
          SchemaInfo(
            table,
            Pattern.compile(s"$table-.*[.]$extension"),
            attributes = List(TableAttribute("id", "long", required = Some(true))),
            metadata = Some(
              Metadata(
                format = Some(format),
                withHeader = Some(true),
                separator = Some(";"),
                loader = Some("native"),
                sink = Some(AllSinks(connectionRef = Some("test-duckdb"))),
                writeStrategy = Some(WriteStrategy(`type` = Some(strategy)))
              )
            ),
            expectations = expectations
          )
        )
      ).writeDomainAsYaml(DatasetArea.load)(storageHandler)
    }

    def stage: Path = DatasetArea.stage(domainName)
    def ingesting: Path = DatasetArea.ingesting(domainName)

    def deliver(table: String, content: String, suffix: String): Unit =
      storageHandler.write(content, new Path(stage, s"$table-$suffix.csv"))

    /** Delivers the rows of a DuckDB SELECT as a Parquet file */
    def deliverParquet(table: String, select: String, suffix: String): Unit = {
      storageHandler.mkdirs(stage)
      val file = new java.io.File(new Path(stage, s"$table-$suffix.parquet").toUri.getPath)
      scala.util.Using.resource(java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
        connection =>
          scala.util.Using.resource(connection.createStatement()) { statement =>
            statement.execute(
              s"COPY ($select) TO '${file.getPath.replace("'", "''")}' (FORMAT parquet)"
            )
          }
      }
    }

    def load(table: String): Try[_] =
      workflow().load(
        LoadConfig(
          domains = List(domainName),
          tables = List(table),
          accessToken = None,
          test = false,
          scheduledDate = None
        )
      )

    /** The files of the table in the area: the tests share their areas */
    def filesIn(area: Path, table: String): List[String] =
      storageHandler
        .list(area, recursive = false)
        .map(_.path.getName)
        .filter(_.startsWith(s"$table-"))
        .sorted

    private def query(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection).toList.map(_.toMap)
      }

    def ids(table: String): List[Long] =
      query(s"SELECT id AS id FROM $table ORDER BY id").map(_("id").toLong)

    def tableExists(schema: String, table: String): Boolean =
      query(
        s"""SELECT COUNT(*) AS total FROM information_schema.tables
           |WHERE table_schema = '$schema' AND table_name = '$table'""".stripMargin
      ).head("total").toInt > 0

    def expectationResults(domain: String, table: String): List[String] =
      query(
        s"""SELECT CAST(success AS VARCHAR) AS result FROM audit.expectations
           |WHERE domain = '$domain' AND "schema" = '$table'""".stripMargin
      ).map(_("result")).sorted

    def expectationErrors(domain: String, table: String): List[String] =
      query(
        s"""SELECT exception AS error FROM audit.expectations
           |WHERE domain = '$domain' AND "schema" = '$table'""".stripMargin
      ).map(_("error"))

    def auditResults(domain: String, table: String, step: String): List[String] =
      query(
        s"""SELECT CAST(success AS VARCHAR) AS result FROM audit.audit
           |WHERE domain = '$domain' AND "schema" = '$table' AND step = '$step'""".stripMargin
      ).map(_("result")).sorted

    "A JDBC transform in OVERWRITE" should
    "keep its previous rows when an expectation checked before the commit fails" in {
      val expectations = List(ExpectationItem("at_least_rows(2)"))
      transform(
        "overwritten",
        WriteStrategyType.OVERWRITE,
        expectations,
        rows(1, 2, 3)
      ).isSuccess shouldBe true
      val result = transform("overwritten", WriteStrategyType.OVERWRITE, expectations, rows(9))
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      // It names the expectation, and the write is rolled back entirely on DuckDB
      result.failed.get.getMessage should include("at_least_rows(2). The write is rolled back.")
      ids(s"$taskFolder.overwritten") shouldBe List(1L, 2L, 3L)
      expectationResults(taskFolder, "overwritten") shouldBe List("false", "true")
      auditResults(taskFolder, "overwritten", "TRANSFORM") shouldBe List("false", "true")
    }

    "A JDBC transform in APPEND" should "append no row when an expectation checked before the commit fails" in {
      val expectations = List(ExpectationItem("without_id(9)"))
      transform(
        "appended",
        WriteStrategyType.APPEND,
        expectations,
        rows(1, 2)
      ).isSuccess shouldBe true
      transform(
        "appended",
        WriteStrategyType.APPEND,
        expectations,
        rows(3, 9)
      ).isFailure shouldBe true
      ids(s"$taskFolder.appended") shouldBe List(1L, 2L)
      expectationResults(taskFolder, "appended") shouldBe List("false", "true")
    }

    "The first run of a JDBC transform" should "leave no table when an expectation checked before the commit fails" in {
      transform(
        "created",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("at_least_rows(2)")),
        rows(9)
      ).isFailure shouldBe true
      tableExists(taskFolder, "created") shouldBe false
      expectationResults(taskFolder, "created") shouldBe List("false")
    }

    "A JDBC transform with an expectation that does not fail it" should
    "run that expectation after the commit, with those checked before it" in {
      transform(
        "mixed",
        WriteStrategyType.OVERWRITE,
        List(
          ExpectationItem("at_least_rows(1)"),
          ExpectationItem("always_fails()", failOnError = false)
        ),
        rows(1)
      ).isSuccess shouldBe true
      ids(s"$taskFolder.mixed") shouldBe List(1L)
      expectationResults(taskFolder, "mixed") shouldBe List("false", "true")
    }

    "A JDBC transform whose expectation checked before the commit cannot be rendered" should
    "keep its previous rows" in {
      transform(
        "unrendered",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("at_least_rows(1)")),
        rows(1, 2)
      ).isSuccess shouldBe true
      transform(
        "unrendered",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("at_least_rows(1 +)")),
        rows(9)
      ).isFailure shouldBe true
      ids(s"$taskFolder.unrendered") shouldBe List(1L, 2L)
      expectationResults(taskFolder, "unrendered") shouldBe List("false", "true")
    }

    "A native load in OVERWRITE" should
    "keep the previous rows and give its file back to stage when an expectation checked before the commit fails" in {
      writeTable("clients", WriteStrategyType.OVERWRITE, List(ExpectationItem("at_least_rows(2)")))
      deliver("clients", "id\n1\n2\n3\n", "1")
      load("clients").isSuccess shouldBe true
      ids(s"$domainName.clients") shouldBe List(1L, 2L, 3L)

      deliver("clients", "id\n9\n", "2")
      val result = load("clients")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      ids(s"$domainName.clients") shouldBe List(1L, 2L, 3L)
      // Nothing is written: the file goes back to stage (#1934)
      filesIn(stage, "clients") shouldBe List("clients-2.csv")
      filesIn(ingesting, "clients") shouldBe Nil
      expectationResults(domainName, "clients") shouldBe List("false", "true")
      auditResults(domainName, "clients", "LOAD") shouldBe List("false", "true")
      // Its second step writes the audit row of its failure only, as any failed second step (#1838)
      auditResults(domainName, "clients", "TRANSFORM") shouldBe List("false")
    }

    "A native load in APPEND" should "append no row when an expectation checked before the commit fails" in {
      writeTable("events", WriteStrategyType.APPEND, List(ExpectationItem("without_id(9)")))
      deliver("events", "id\n1\n2\n", "1")
      load("events").isSuccess shouldBe true
      deliver("events", "id\n3\n9\n", "2")
      load("events").isFailure shouldBe true
      ids(s"$domainName.events") shouldBe List(1L, 2L)
      filesIn(stage, "events") shouldBe List("events-2.csv")
    }

    "The expectations of a native load" should
    "read its counters, before the commit and once the load is audited" in {
      writeTable(
        "counted",
        WriteStrategyType.APPEND,
        List(
          ExpectationItem("load_counts(2)"),
          ExpectationItem("load_counts(2)", failOnError = false)
        )
      )
      deliver("counted", "id\n1\n2\n", "1")
      load("counted").isSuccess shouldBe true
      // A variable that a macro lacks would make its query fail
      expectationResults(domainName, "counted") shouldBe List("true", "true")
    }

    "A JDBC transform whose expectation query fails before the commit" should
    "roll its write back and not run the next expectations, in an aborted transaction" in {
      transform(
        "aborted",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("at_least_rows(1)")),
        rows(1, 2)
      ).isSuccess shouldBe true
      transform(
        "aborted",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("unknown_column()"), ExpectationItem("at_least_rows(1)")),
        rows(9)
      ).isFailure shouldBe true
      ids(s"$taskFolder.aborted") shouldBe List(1L, 2L)
      expectationResults(taskFolder, "aborted") shouldBe List("false", "false", "true")
      expectationErrors(taskFolder, "aborted").exists(
        _.contains("Not run: the query of the expectation unknown_column() failed")
      ) shouldBe true
    }

    "A native Parquet load in OVERWRITE whose rows are all rejected" should
    "keep the previous rows when its expectation reads its counters" in {
      writeTable(
        "rejected",
        WriteStrategyType.OVERWRITE,
        List(ExpectationItem("all_rejected()")),
        Format.PARQUET
      )
      deliverParquet("rejected", "SELECT * FROM (VALUES (1), (2)) AS t(id)", "1")
      load("rejected").isSuccess shouldBe true
      ids(s"$domainName.rejected") shouldBe List(1L, 2L)

      // Under rejectMaxRecords, the load would write no row
      deliverParquet("rejected", "SELECT CAST(NULL AS BIGINT) AS id", "2")
      load("rejected").isFailure shouldBe true
      ids(s"$domainName.rejected") shouldBe List(1L, 2L)
      expectationResults(domainName, "rejected") shouldBe List("false", "true")
    }
  }
}
