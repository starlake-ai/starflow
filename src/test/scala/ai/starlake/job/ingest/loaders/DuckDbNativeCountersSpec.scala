package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.*
import ai.starlake.workflow.IngestionWorkflow
import better.files.File
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.sql.DriverManager
import java.util.regex.Pattern
import scala.util.{Try, Using}

/** The audit row of a native DuckDB load counts the rows that the load accepts, whatever its write
  * strategy does with the rows of the table (#1980). It counted the change of the number of rows of
  * the table instead: the rows that an OVERWRITE, an OVERWRITE_BY_PARTITION or an UPSERT replaces
  * were subtracted.
  */
class DuckDbNativeCountersSpec extends TestHelper {
  private val domainName = "counted_loads"

  private def configuration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/native_counters_1980.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private abstract class Project extends WithSettings(configuration) {
    private def incoming(name: String): java.io.File = {
      val folder = new java.io.File(starlakeTestRoot, "counted-incoming")
      folder.mkdirs()
      new java.io.File(folder, name)
    }

    def writeParquet(name: String, select: String): Path = {
      val file = incoming(name)
      Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { connection =>
        Using.resource(connection.createStatement()) { statement =>
          statement.execute(
            s"COPY ($select) TO '${file.getPath.replace("'", "''")}' (FORMAT parquet)"
          )
        }
      }
      new Path(file.getPath)
    }

    def writeJson(name: String, lines: String*): Path = {
      val file = incoming(name)
      File(file.getPath).overwrite(lines.mkString("", "\n", "\n"))
      new Path(file.getPath)
    }

    def load(table: SchemaInfo, file: Path): Unit = {
      // The test project defines time as a string: use the built-in types
      deliverTestFile("/sample/new-types/types.sl.yml", new Path(DatasetArea.types, "types.sl.yml"))
      DomainInfo(name = domainName, tables = List(table))
        .writeDomainAsYaml(DatasetArea.load)(storageHandler)
      val schemaHandler = settings.schemaHandler(reload = true)
      val domain = schemaHandler.getDomain(domainName).get
      val result = Try(
        new IngestionWorkflow(storageHandler, schemaHandler)
          .ingest(
            domain,
            domain.tables.find(_.name == table.name).get,
            List(file),
            Map.empty,
            None,
            test = false,
            None
          )
          .get
      )
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
    }

    def query(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection)
      }

    def rows(table: String): String =
      query(s"SELECT COUNT(*) AS cnt FROM $domainName.$table").head("cnt")

    def table(
      name: String,
      format: Format,
      strategy: WriteStrategy,
      attributes: List[TableAttribute],
      partition: Option[List[String]] = None,
      filter: Option[String] = None
    ): SchemaInfo = {
      val extension = if (format == Format.PARQUET) "parquet" else "json"
      SchemaInfo(
        name,
        Pattern.compile(s"$name-.*[.]$extension"),
        attributes = attributes,
        filter = filter,
        metadata = Some(
          Metadata(
            format = Some(format),
            loader = Some("native"),
            sink = Some(AllSinks(connectionRef = Some("test-duckdb"), partition = partition)),
            writeStrategy = Some(strategy)
          )
        )
      )
    }

    /** The counters of the audit rows of the loads of the table, in their order */
    def counters(table: String): List[(String, String, String)] =
      query(
        s"""SELECT "count" AS total, countAccepted AS accepted, countRejected AS rejected
           |FROM audit.audit WHERE "schema" = '$table' ORDER BY "timestamp"""".stripMargin
      ).map(row => (row("total"), row("accepted"), row("rejected")))
  }

  new Project() {
    "The audit row of a native DuckDB load in two steps" should "count the rows that an OVERWRITE accepts, not the change of the table" in {
      val overwritten = table(
        "overwritten",
        Format.PARQUET,
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)),
        List(TableAttribute("id", "long"))
      )
      load(overwritten, writeParquet("overwritten-1.parquet", "SELECT * FROM range(3) t(id)"))
      load(overwritten, writeParquet("overwritten-2.parquet", "SELECT * FROM range(2) t(id)"))
      rows("overwritten") shouldBe "2"
      // The change of the rows of the table gave 2 - 3
      counters("overwritten") shouldBe List(("3", "3", "0"), ("2", "2", "0"))
    }

    it should "count the rows that an OVERWRITE_BY_PARTITION accepts, not the change of the table" in {
      val partitioned = table(
        "partitioned",
        Format.PARQUET,
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE_BY_PARTITION)),
        List(TableAttribute("day", "date"), TableAttribute("id", "long")),
        partition = Some(List("day"))
      )
      // 14 rows of a day, 3 of the next one
      load(
        partitioned,
        writeParquet(
          "partitioned-1.parquet",
          "SELECT CASE WHEN i < 14 THEN DATE '2026-11-01' ELSE DATE '2026-11-02' END AS day, i AS id FROM range(17) t(i)"
        )
      )
      // 2 rows replace the 14 rows of the first day
      load(
        partitioned,
        writeParquet(
          "partitioned-2.parquet",
          "SELECT DATE '2026-11-01' AS day, i + 100 AS id FROM range(2) t(i)"
        )
      )
      rows("partitioned") shouldBe "5"
      // The change of the rows of the table gave 2 - 14
      counters("partitioned") shouldBe List(("17", "17", "0"), ("2", "2", "0"))
    }

    it should "count the rows that an UPSERT_BY_KEY accepts, updated ones included" in {
      val upserted = table(
        "upserted",
        Format.PARQUET,
        WriteStrategy(`type` = Some(WriteStrategyType.UPSERT_BY_KEY), key = List("id")),
        List(TableAttribute("id", "long"), TableAttribute("v", "string"))
      )
      load(
        upserted,
        writeParquet(
          "upserted-1.parquet",
          "SELECT * FROM (VALUES (1::BIGINT, 'a'), (2::BIGINT, 'b'), (3::BIGINT, 'c')) t(id, v)"
        )
      )
      // One row updated, one added
      load(
        upserted,
        writeParquet(
          "upserted-2.parquet",
          "SELECT * FROM (VALUES (2::BIGINT, 'B'), (4::BIGINT, 'd')) t(id, v)"
        )
      )
      rows("upserted") shouldBe "4"
      // The change of the rows of the table gave 1
      counters("upserted") shouldBe List(("3", "3", "0"), ("2", "2", "0"))
    }

    it should "not count the rows that the filter of the table drops" in {
      val filtered = table(
        "filtered",
        Format.PARQUET,
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)),
        List(TableAttribute("id", "long")),
        filter = Some("id >= 2")
      )
      load(filtered, writeParquet("filtered-1.parquet", "SELECT * FROM range(5) t(id)"))
      rows("filtered") shouldBe "3"
      counters("filtered") shouldBe List(("3", "3", "0"))
    }

    it should "count the rows of other files than Parquet, whose rejected rows are not counted" in {
      val records = table(
        "records",
        Format.JSON,
        WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)),
        List(TableAttribute("id", "long"))
      )
      load(records, writeJson("records-1.json", """{"id": 1}""", """{"id": 2}""", """{"id": 3}"""))
      load(records, writeJson("records-2.json", """{"id": 4}""", """{"id": 5}"""))
      rows("records") shouldBe "2"
      counters("records") shouldBe List(("3", "3", "-1"), ("2", "2", "-1"))
    }

    "The audit row of a native DuckDB load in one step" should "count the rows that an OVERWRITE or an APPEND accepts" in {
      def documents(strategy: WriteStrategyType): SchemaInfo =
        table(
          "documents",
          Format.JSON,
          WriteStrategy(`type` = Some(strategy)),
          List(TableAttribute("doc", "variant"))
        )
      // A VARIANT table loads in one step
      load(
        documents(WriteStrategyType.OVERWRITE),
        writeJson("documents-1.json", """{"a": 1}""", """{"a": 2}""", """{"a": 3}""")
      )
      load(
        documents(WriteStrategyType.OVERWRITE),
        writeJson("documents-2.json", """{"a": 4}""", """{"a": 5}""")
      )
      rows("documents") shouldBe "2"
      load(
        documents(WriteStrategyType.APPEND),
        writeJson("documents-3.json", """{"a": 6}""", """{"a": 7}""")
      )
      rows("documents") shouldBe "4"
      // The rejected rows are not counted in other files than Parquet. The change of the rows of
      // the table gave 2 - 3 for the second load
      counters("documents") shouldBe List(("3", "3", "-1"), ("2", "2", "-1"), ("2", "2", "-1"))
    }
  }
}
