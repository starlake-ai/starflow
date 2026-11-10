package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
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

/** A load that fails before writing its rows gives its files back to the stage area, where the next
  * load finds them, and holds the later files of its table unless they are appended. One that may
  * have written them leaves them in the ingesting area (#1934).
  */
class FailedLoadFilesSpec extends TestHelper {

  private val domainName = "failed_loads"

  private def configuration(extra: String): Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |expectations.failOnError = true
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/loads_1934.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |connections.broken-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:/nonexistent-dir-of-starlake-tests/loads_1934.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |$extra
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private abstract class Project(extra: String = "") extends WithSettings(configuration(extra)) {
    def writeTable(
      table: String,
      connection: String,
      expectations: List[ExpectationItem] = Nil,
      presql: List[String] = Nil,
      postsql: List[String] = Nil,
      strategy: WriteStrategyType = WriteStrategyType.APPEND,
      types: Option[Map[String, String]] = None,
      pattern: Option[String] = None,
      loader: String = "native"
    ): Unit = {
      storageHandler.write(
        "{% macro always_fails() %}SELECT 1{% endmacro %}",
        new Path(DatasetArea.expectations, "failed_loads.j2")
      )
      DomainInfo(
        name = domainName,
        metadata = Some(Metadata(directory = Some(s"$starlakeTestRoot/incoming"))),
        tables = List(
          SchemaInfo(
            table,
            Pattern.compile(pattern.getOrElse(s"$table-.*[.]csv")),
            attributes = List(TableAttribute("id", "long")),
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(";"),
                loader = Some(loader),
                sink = Some(AllSinks(connectionRef = Some(connection))),
                writeStrategy = Some(WriteStrategy(`type` = Some(strategy), types = types))
              )
            ),
            presql = presql,
            postsql = postsql,
            expectations = expectations
          )
        )
      ).writeDomainAsYaml(DatasetArea.load)(storageHandler)
    }

    def deliver(
      area: Path,
      table: String,
      content: String = "id\n1\n2\n",
      suffix: String = "1"
    ): Path = {
      val file = new Path(area, s"$table-$suffix.csv")
      storageHandler.write(content, file)
      file
    }

    /** Loads take the files of stage from the oldest to the newest */
    def age(file: Path, seconds: Int): Unit =
      new java.io.File(file.toUri).setLastModified(System.currentTimeMillis() - seconds * 1000L)

    private def workflow(): IngestionWorkflow =
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))

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

    def ingest(table: String, path: Path, test: Boolean = false): Try[_] = {
      val ingestion = workflow()
      val domain = settings.schemaHandler().getDomain(domainName).get
      val schema = domain.tables.find(_.name == table).get
      ingestion.ingest(domain, schema, List(path), Map.empty, None, test, None)
    }

    def stage: Path = DatasetArea.stage(domainName)
    def ingesting: Path = DatasetArea.ingesting(domainName)
    def archive: Path = DatasetArea.archive(domainName)

    /** The files of the table in the area: the tests of a project share their areas */
    def filesIn(area: Path, table: String): List[String] =
      storageHandler
        .list(area, recursive = false)
        .map(_.path.getName)
        .filter(_.startsWith(s"$table-"))

    def ids(table: String): List[Long] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils
          .executeQueryAsMap(s"SELECT id FROM $domainName.$table ORDER BY id", connection)
          .map(_("id").toLong)
          .toList
      }

    def tableRows(table: String): Int =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils
          .executeQueryAsMap(s"SELECT COUNT(*) AS total FROM $domainName.$table", connection)
          .head("total")
          .toInt
      }
  }

  new Project() {
    "A load that fails before writing its rows" should "give its files back to stage, for the next load" in {
      writeTable("orders", "broken-duckdb")
      deliver(stage, "orders")
      load("orders").isFailure shouldBe true
      filesIn(stage, "orders") shouldBe List("orders-1.csv")
      filesIn(ingesting, "orders") shouldBe Nil

      writeTable("orders", "test-duckdb")
      load("orders").isSuccess shouldBe true
      filesIn(stage, "orders") shouldBe Nil
      filesIn(ingesting, "orders") shouldBe Nil
      filesIn(archive, "orders") shouldBe List("orders-1.csv")
      tableRows("orders") shouldBe 2
    }

    // A plain table takes two steps on DuckDB, and its presql and postsql run in the second one
    "A native DuckDB load whose second step fails before writing" should "fail and give its files back to stage" in {
      writeTable("presql", "test-duckdb", presql = List("SELECT no_such_function_of_starlake(1)"))
      deliver(stage, "presql")
      val result = load("presql")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("no_such_function_of_starlake")
      filesIn(stage, "presql") shouldBe List("presql-1.csv")
      filesIn(ingesting, "presql") shouldBe Nil
      filesIn(archive, "presql") shouldBe Nil
    }

    "A native DuckDB load whose postsql fails" should "keep its files in the ingesting area" in {
      writeTable("posted", "test-duckdb", postsql = List("SELECT no_such_function_of_starlake(1)"))
      deliver(stage, "posted")
      val result = load("posted")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("no_such_function_of_starlake")
      // DuckDB rolls the rows back, where an engine that commits implicitly would keep them
      tableRows("posted") shouldBe 0
      filesIn(ingesting, "posted") shouldBe List("posted-1.csv")
      filesIn(stage, "posted") shouldBe Nil
      filesIn(archive, "posted") shouldBe Nil
    }

    "A Spark load whose postsql fails" should "keep its files in the ingesting area" in {
      writeTable(
        "spark_posted",
        "spark",
        postsql = List("SELECT no_such_function_of_starlake(1)"),
        loader = "spark"
      )
      deliver(stage, "spark_posted")
      load("spark_posted").isFailure shouldBe true
      filesIn(ingesting, "spark_posted") shouldBe List("spark_posted-1.csv")
      filesIn(stage, "spark_posted") shouldBe Nil
    }

    "A load that fails after writing its rows" should "leave its files in the ingesting area" in {
      writeTable("audited", "test-duckdb", expectations = List(ExpectationItem("always_fails()")))
      deliver(stage, "audited")
      val result = load("audited")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("1 Expectations failed")
      tableRows("audited") shouldBe 2
      filesIn(ingesting, "audited") shouldBe List("audited-1.csv")
      filesIn(stage, "audited") shouldBe Nil
    }

    "A failed load of a file delivered to stage again since" should "keep the new file" in {
      writeTable("redelivered", "broken-duckdb")
      val ingestingFile = deliver(ingesting, "redelivered", "id\n1\n")
      val stageFile = deliver(stage, "redelivered", "id\n3\n4\n")
      ingest("redelivered", ingestingFile).isFailure shouldBe true
      storageHandler.read(stageFile) shouldBe "id\n3\n4\n"
      storageHandler.read(ingestingFile) shouldBe "id\n1\n"
    }

    "A failed load of a file given by its own path" should "leave the file where it is" in {
      writeTable("own", "broken-duckdb")
      val ownFile = deliver(new Path(s"$starlakeTestRoot/incoming"), "own")
      ingest("own", ownFile).isFailure shouldBe true
      storageHandler.exists(ownFile) shouldBe true
      filesIn(stage, "own") shouldBe Nil
    }

    "A failed load in test mode" should "leave its files in the ingesting area" in {
      writeTable("tested", "broken-duckdb")
      val ingestingFile = deliver(ingesting, "tested")
      ingest("tested", ingestingFile, test = true).isFailure shouldBe true
      storageHandler.exists(ingestingFile) shouldBe true
      filesIn(stage, "tested") shouldBe Nil
    }
  }

  // Files load one by one, in their order
  new Project("grouped = false") {
    "A failed load of a table that is not appended to" should
    "give back its later files without loading them, to keep their order" in {
      writeTable("ordered", "test-duckdb", strategy = WriteStrategyType.OVERWRITE)
      val older = deliver(stage, "ordered", "id\nnot-a-number\n", suffix = "1")
      age(older, 60)
      deliver(stage, "ordered", "id\n3\n", suffix = "2")
      val result = load("ordered")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("an earlier file of the table failed")
      filesIn(stage, "ordered").sorted shouldBe List("ordered-1.csv", "ordered-2.csv")
      filesIn(ingesting, "ordered") shouldBe Nil

      storageHandler.write("id\n1\n2\n", older)
      age(older, 60)
      load("ordered").isSuccess shouldBe true
      // The newer file overwrote the older one
      ids("ordered") shouldBe List(3L)
      filesIn(archive, "ordered").sorted shouldBe List("ordered-1.csv", "ordered-2.csv")
    }

    "A failed load of a table whose adaptive strategy may overwrite it" should
    "give back its later files, even after a file that it appends" in {
      writeTable(
        "adaptive",
        "test-duckdb",
        types = Some(
          Map(
            WriteStrategyType.OVERWRITE.value -> """group(1) == "FULL"""",
            WriteStrategyType.APPEND.value    -> """group(1) == "DELTA""""
          )
        ),
        pattern = Some("adaptive-(FULL|DELTA)-[0-9][.]csv")
      )
      val older = deliver(stage, "adaptive", "id\nnot-a-number\n", suffix = "DELTA-1")
      age(older, 60)
      deliver(stage, "adaptive", "id\n3\n", suffix = "FULL-2")
      load("adaptive").isFailure shouldBe true
      filesIn(stage, "adaptive").sorted shouldBe List("adaptive-DELTA-1.csv", "adaptive-FULL-2.csv")
    }

    "A failed load of a table that is appended to" should "load its later files" in {
      writeTable("appended", "test-duckdb")
      val older = deliver(stage, "appended", "id\nnot-a-number\n", suffix = "1")
      age(older, 60)
      deliver(stage, "appended", "id\n3\n", suffix = "2")
      load("appended").isFailure shouldBe true
      ids("appended") shouldBe List(3L)
      filesIn(stage, "appended") shouldBe List("appended-1.csv")
      filesIn(archive, "appended") shouldBe List("appended-2.csv")
    }
  }

  new Project("""audit.sink.connectionRef = "broken-duckdb"""") {
    "A load whose audit row cannot be written" should "leave its files in the ingesting area" in {
      writeTable("unaudited", "test-duckdb")
      deliver(stage, "unaudited")
      load("unaudited").isFailure shouldBe true
      tableRows("unaudited") shouldBe 2
      filesIn(ingesting, "unaudited") shouldBe List("unaudited-1.csv")
      filesIn(stage, "unaudited") shouldBe Nil
    }
  }

  new Project("rejectAllOnError = true") {
    "A Spark load that rejects all its rows on error" should "give its files back to stage" in {
      writeTable("rejected", "spark", loader = "spark")
      deliver(stage, "rejected", "id\n1\nnot-a-number\n")
      val result = load("rejected")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("Fail on rejected count requested")
      filesIn(stage, "rejected") shouldBe List("rejected-1.csv")
      filesIn(ingesting, "rejected") shouldBe Nil
    }
  }
}
