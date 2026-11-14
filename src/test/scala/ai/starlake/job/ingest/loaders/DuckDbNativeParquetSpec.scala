package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.*
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.sql.DriverManager
import java.time.Instant
import java.util.regex.Pattern
import scala.util.{Try, Using}

/** The native DuckDB loader reads Parquet files with read_parquet, which loads the types that Spark
  * does not read, such as the TIME columns of BigQuery exports, and rejects the rows as Spark does
  * (#1952)
  */
class DuckDbNativeParquetSpec extends TestHelper {

  private val parquet = Metadata(format = Some(Format.PARQUET))

  "isNativelySupported" should "load PARQUET files natively on DuckDB" in {
    IngestionJob.isNativelySupported("duckdb", Format.PARQUET, array = false) shouldBe true
  }

  "parquetFeaturesNotLoadedByDuckDb" should "accept a flat Parquet table" in {
    IngestionJob.parquetFeaturesNotLoadedByDuckDb(
      parquet.copy(options = Some(Map("format" -> "PARQUET"))),
      List(
        TableAttribute("id", "long", required = Some(true), rename = Some("identifier")),
        TableAttribute("visit_time", "time"),
        TableAttribute("code", "string", trim = Some(Trim.NONE)),
        // Not loaded
        TableAttribute("ignored", "string", default = Some("x"), ignore = Some(true)),
        TableAttribute("computed", "string", script = Some("upper(code)"))
      )
    ) shouldBe Nil
  }

  it should "list what the native DuckDB loader would not load as Spark does" in {
    IngestionJob.parquetFeaturesNotLoadedByDuckDb(
      parquet.copy(options = Some(Map("format" -> "orc", "mergeSchema" -> "true"))),
      List(
        TableAttribute("id", "long"),
        TableAttribute("name", "string", default = Some("none"), trim = Some(Trim.BOTH)),
        TableAttribute("tags", "string", array = Some(true)),
        TableAttribute("doc", "variant"),
        TableAttribute("address", "struct", attributes = List(TableAttribute("city", "string"))),
        // Columns of the first step table too
        TableAttribute("ignored", "string", array = Some(true), ignore = Some(true)),
        TableAttribute("computed", "string", array = Some(true), script = Some("['x']"))
      )
    ) shouldBe List(
      "Spark read options (format orc, mergeSchema)",
      "struct, variant or array attributes (tags, doc, address, ignored, computed)",
      "default values (name)",
      "trim (name)"
    )
  }

  "DuckDbParquetRows" should "check the values that need a conversion, and the required ones" in {
    val rows = DuckDbParquetRows(
      List(
        DuckDbParquetRows.Column(
          TableAttribute("id", "long", required = Some(true)),
          Type("long", "[-|\\+|0-9][0-9]*", PrimitiveType.long),
          "BIGINT",
          Some("BIGINT")
        ),
        DuckDbParquetRows.Column(
          TableAttribute("bytes", "hex_bytes"),
          Type("hex_bytes", "hex", PrimitiveType.binary),
          "BLOB",
          Some("VARCHAR")
        ),
        DuckDbParquetRows.Column(
          TableAttribute(
            "secret",
            "string",
            privacy = Some(TransformInput("SQL: md5(secret)", true))
          ),
          Type("string", ".+"),
          "VARCHAR",
          Some("INTEGER")
        ),
        // Not in the file
        DuckDbParquetRows.Column(
          TableAttribute("missing", "string"),
          Type("string", ".+"),
          "VARCHAR",
          None
        )
      ),
      emptyIsNull = true,
      withValue = true
    )
    rows.checks.map(_.condition) shouldBe List(
      // A BIGINT column needs no conversion
      "\"id\" IS NOT NULL",
      // As Spark: an even number of hexadecimal digits, between spaces
      "(NULLIF(\"bytes\", '') IS NULL OR TRY_CAST(CASE WHEN length(trim(NULLIF(\"bytes\", ''))) % 2 = 0 THEN TRY(unhex(trim(NULLIF(\"bytes\", '')))) END AS BLOB) IS NOT NULL)",
      "(\"secret\" IS NULL OR TRY_CAST(\"secret\" AS VARCHAR) IS NOT NULL)"
    )
    rows.checks.map(_.error) shouldBe List(
      "'`id`  can''t be null'",
      "'`bytes` cannot be converted to `binary` with sl type `hex_bytes`' || ': ' || \"bytes\"::VARCHAR",
      // A rejected row does not show a value with a privacy
      "'`secret` cannot be converted to `string` with sl type `string`'"
    )
    rows.rejectedRowsSql("['f.parquet']", 10) should include(
      "to_json(struct_pack(\"id\" := \"id\", \"bytes\" := \"bytes\")) AS row_json"
    )
    rows.insertSql("d.t", "['f.parquet']") should startWith(
      "INSERT INTO d.t (\"id\", \"bytes\", \"secret\")"
    )
  }

  private val domainName = "parquet_loads"

  private def configuration(extra: String): Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/native_parquet_1952.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |$extra
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private abstract class Project(extra: String = "") extends WithSettings(configuration(extra)) {

    /** Writes a Parquet file with DuckDB, which writes the TIME WITH TIME ZONE columns as BigQuery
      * exports TIME columns: TIME(isAdjustedToUTC = true, unit = MICROS)
      */
    def writeParquet(name: String, select: String): Path = {
      val folder = new java.io.File(starlakeTestRoot, "parquet-incoming")
      folder.mkdirs()
      val file = new java.io.File(folder, name)
      Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { connection =>
        Using.resource(connection.createStatement()) { statement =>
          statement.execute(
            s"COPY ($select) TO '${file.getPath.replace("'", "''")}' (FORMAT parquet)"
          )
        }
      }
      new Path(file.getPath)
    }

    def load(table: SchemaInfo, files: Path*): Try[_] = {
      // The test project defines time as a string: use the built-in types
      deliverTestFile("/sample/new-types/types.sl.yml", new Path(DatasetArea.types, "types.sl.yml"))
      DomainInfo(name = domainName, tables = List(table))
        .writeDomainAsYaml(DatasetArea.load)(storageHandler)
      val schemaHandler = settings.schemaHandler(reload = true)
      val domain = schemaHandler.getDomain(domainName).get
      new IngestionWorkflow(storageHandler, schemaHandler).ingest(
        domain,
        domain.tables.find(_.name == table.name).get,
        files.toList,
        Map.empty,
        None,
        test = false,
        None
      )
    }

    def query(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection)
      }

    def table(name: String, attributes: List[TableAttribute]): SchemaInfo =
      SchemaInfo(
        name,
        Pattern.compile(s"$name-.*[.]parquet"),
        attributes = attributes,
        metadata = Some(
          parquet.copy(
            loader = Some("native"),
            sink = Some(AllSinks(connectionRef = Some("test-duckdb"))),
            writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
          )
        )
      )

    /** The error lines of the rejected records of the table in the audit, whose columns are named
      * in upper case
      */
    def rejectedErrors(table: String): List[String] =
      query(s"""SELECT error AS error FROM audit.rejected WHERE "schema" = '$table'""")
        .map(_("error"))

    /** The counters of the audit row of the load of the table */
    def counters(table: String): List[(String, String, String)] =
      query(
        s"""SELECT "count" AS total, countAccepted AS accepted, countRejected AS rejected
           |FROM audit.audit WHERE "schema" = '$table'""".stripMargin
      ).map(row => (row("total"), row("accepted"), row("rejected")))
  }

  new Project() {
    "A native DuckDB load of Parquet files" should "load the columns of the attributes by name, TIME columns included" in {
      // As a BigQuery export, with columns that are not attributes, in another order
      val exported = writeParquet(
        "visits-1.parquet",
        """SELECT * FROM (VALUES
          |  ('x', TIMETZ '12:30:00+00', 1::BIGINT, 'a', 'i', 12.5::DECIMAL(38, 9), TIMESTAMPTZ '2024-01-01 10:00:00+00'),
          |  ('y', TIMETZ '23:59:59+00', 2::BIGINT, 'b', 'j', 1.25::DECIMAL(38, 9), TIMESTAMPTZ '2024-06-01 10:00:00+00')
          |) t(other, visit_time, id, name, ignored, amount, seen_at)""".stripMargin
      )
      // A TIME column not adjusted to UTC, and no amount or seen_at column
      val written = writeParquet(
        "visits-2.parquet",
        "SELECT 3::BIGINT AS id, TIME '08:00:00' AS visit_time, 'c' AS name"
      )
      val visits = table(
        "visits",
        List(
          TableAttribute("id", "long", required = Some(true), rename = Some("identifier")),
          TableAttribute("name", "string"),
          TableAttribute("visit_time", "time"),
          TableAttribute("amount", "decimal"),
          TableAttribute("seen_at", "timestamp"),
          TableAttribute("ignored", "string", ignore = Some(true)),
          TableAttribute("computed", "string", script = Some("upper(name)"))
        )
      )
      val result = load(visits, exported, written)
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }

      query(s"DESCRIBE $domainName.visits").map(_("column_name")) should contain theSameElementsAs
      List("identifier", "name", "visit_time", "amount", "seen_at", "computed")
      query(
        s"""SELECT identifier, name, visit_time::VARCHAR AS visit_time, amount::DOUBLE AS amount,
           |  epoch(seen_at)::BIGINT AS seen_at, computed
           |FROM $domainName.visits ORDER BY identifier""".stripMargin
      ) shouldBe List(
        Map(
          "identifier" -> "1",
          "name"       -> "a",
          "visit_time" -> "12:30:00",
          "amount"     -> "12.5",
          "seen_at"    -> Instant.parse("2024-01-01T10:00:00Z").getEpochSecond.toString,
          "computed"   -> "A"
        ),
        Map(
          "identifier" -> "2",
          "name"       -> "b",
          "visit_time" -> "23:59:59",
          "amount"     -> "1.25",
          "seen_at"    -> Instant.parse("2024-06-01T10:00:00Z").getEpochSecond.toString,
          "computed"   -> "B"
        ),
        Map(
          "identifier" -> "3",
          "name"       -> "c",
          "visit_time" -> "08:00:00",
          "amount"     -> "null",
          "seen_at"    -> "null",
          "computed"   -> "C"
        )
      )
      counters("visits") shouldBe List(("3", "3", "0"))
    }

    it should "fail when a file holds no column of the attributes, as Spark" in {
      val unrelated = writeParquet("unrelated-1.parquet", "SELECT 1 AS x, 'a' AS y")
      val result = load(table("unrelated", List(TableAttribute("id", "long"))), unrelated)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("No attribute of unrelated found in")
    }

    it should "reject the rows whose value does not convert or whose required attribute has no value, as Spark" in {
      val file = writeParquet(
        "rejecting-1.parquet",
        """SELECT * FROM (VALUES ('1', 'a'), ('abc', 'b'), (NULL, 'c'), ('', 'd'), ('5', ''))
          |  t(id, name)""".stripMargin
      )
      val result = load(
        table(
          "rejecting",
          List(
            TableAttribute("id", "long", required = Some(true)),
            TableAttribute("name", "string")
          )
        ),
        file
      )
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      // With emptyIsNull, an empty string is no value
      query(s"SELECT id, name FROM $domainName.rejecting ORDER BY id") shouldBe List(
        Map("id" -> "1", "name" -> "a"),
        Map("id" -> "5", "name" -> "null")
      )
      val errors = rejectedErrors("rejecting")
      errors should have size 3
      errors.count(
        _.contains("`id` cannot be converted to `long` with sl type `long`")
      ) shouldBe 1
      errors.count(_.contains("`id`  can't be null")) shouldBe 2
      all(errors) should include(s"FILE -> $file")
      counters("rejecting") shouldBe List(("5", "2", "3"))
    }

    it should "decode the binary attributes that a file holds as text, as Spark" in {
      val file = writeParquet(
        "encoded-1.parquet",
        // As Spark, whitespace is not part of the bytes and the padding is not required
        "SELECT * FROM (VALUES (1, 'aGk='), (2, '!!!'), (3, 'a Gk')) t(id, payload)"
      )
      val result = load(
        table("encoded", List(TableAttribute("id", "long"), TableAttribute("payload", "binary"))),
        file
      )
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      query(
        s"SELECT id, decode(payload) AS payload FROM $domainName.encoded ORDER BY id"
      ) shouldBe List(Map("id" -> "1", "payload" -> "hi"), Map("id" -> "3", "payload" -> "hi"))
      rejectedErrors("encoded").map(
        _.contains("`payload` cannot be converted to `binary` with sl type `binary`")
      ) shouldBe List(true)
    }

    it should "fail with the error of a script that DuckDB does not run, without a schema sync hint" in {
      val file = writeParquet("scripted-1.parquet", "SELECT 1::BIGINT AS id, 'a' AS name")
      val result = load(
        table(
          "scripted",
          List(
            TableAttribute("id", "long", rename = Some("identifier")),
            TableAttribute("name", "string"),
            TableAttribute(
              "computed",
              "string",
              script = Some("no_such_function_of_starlake(name)")
            )
          )
        ),
        file
      )
      result.isFailure shouldBe true
      val message = result.failed.get.getMessage
      message should include("no_such_function_of_starlake")
      // The quoted columns of the second step are those of the table
      message should not include "--sync-apply"
    }

    "A Parquet table with a feature that the native DuckDB loader does not apply" should "keep loading with Spark" in {
      val padded = writeParquet("padded-1.parquet", "SELECT 1::BIGINT AS id, '  a  ' AS name")
      val result = load(
        table(
          "padded",
          List(
            TableAttribute("id", "long"),
            TableAttribute("name", "string", trim = Some(Trim.BOTH))
          )
        ),
        padded
      )
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      // Spark trims the value, which the native loader would not do
      query(s"SELECT name FROM $domainName.padded").map(_("name")) shouldBe List("a")
    }
  }

  // The tables of the first step quote their columns on such a connection only. Another database
  // file: the pool would open the same one twice, with other options. No audit: the JDBC audit
  // rows do not escape a quote of a file name.
  new Project(
    s"""connections.test-duckdb.options.url = "jdbc:duckdb:$starlakeTestRoot/native_parquet_quoted.db"
       |connections.test-duckdb.options.quoteIdentifiers = "true"
       |audit.active = false""".stripMargin
  ) {
    "A native DuckDB load of Parquet files on a connection that quotes identifiers" should "load an attribute named like a keyword" in {
      val file = writeParquet("keywords-1.parquet", "SELECT 1::BIGINT AS \"order\"")
      val result = load(table("keywords", List(TableAttribute("order", "long"))), file)
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      query(s"""SELECT "order" AS value FROM $domainName.keywords""").map(_("value")) shouldBe
      List("1")
    }

    it should "load a file whose name holds a quote" in {
      val file = writeParquet("quoted-o'clock.parquet", "SELECT 1::BIGINT AS id")
      val result = load(table("quoted", List(TableAttribute("id", "long"))), file)
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      query(s"SELECT id FROM $domainName.quoted").map(_("id")) shouldBe List("1")
    }

    "A native DuckDB load of DSV files on a connection that quotes identifiers" should "load an attribute named like a keyword too" in {
      val folder = new java.io.File(starlakeTestRoot, "dsv-incoming")
      folder.mkdirs()
      val file = new java.io.File(folder, "dsv_keywords-1.csv")
      java.nio.file.Files.writeString(file.toPath, "order\n1\n")
      val dsv = table("dsv_keywords", List(TableAttribute("order", "long"))).copy(
        pattern = Pattern.compile("dsv_keywords-.*[.]csv"),
        metadata = Some(
          Metadata(
            format = Some(Format.DSV),
            withHeader = Some(true),
            separator = Some(","),
            loader = Some("native"),
            sink = Some(AllSinks(connectionRef = Some("test-duckdb"))),
            writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
          )
        )
      )
      val result = load(dsv, new Path(file.getPath))
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      query(s"""SELECT "order" AS value FROM $domainName.dsv_keywords""").map(_("value")) shouldBe
      List("1")
    }
  }

  // The limits apply to the rejected rows of all the files of the load
  new Project("rejectMaxRecords = 2") {
    private def twoFiles(name: String, badRows: Int): List[Path] =
      (1 to 2).toList.map { index =>
        val bad = (1 to badRows).map(row => s"('x$row', 'bad')").mkString(", ")
        writeParquet(
          s"$name-$index.parquet",
          s"SELECT * FROM (VALUES ('$index', 'good'), $bad) t(id, name)"
        )
      }

    private def limited(name: String): SchemaInfo =
      table(name, List(TableAttribute("id", "long"), TableAttribute("name", "string")))

    "A native DuckDB load of Parquet files with rejectMaxRecords" should "load the valid rows of all its files when they reject no more rows" in {
      val result = load(limited("within_limit"), twoFiles("within_limit", 1): _*)
      withClue(result.failed.map(_.getMessage).getOrElse("")) {
        result.isSuccess shouldBe true
      }
      query(s"SELECT id FROM $domainName.within_limit ORDER BY id").map(_("id")) shouldBe
      List("1", "2")
      rejectedErrors("within_limit") should have size 2
      counters("within_limit") shouldBe List(("4", "2", "2"))
    }

    it should "keep the rejected rows of all its files, then load none, when they reject more" in {
      val result = load(limited("beyond_limit"), twoFiles("beyond_limit", 2): _*)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("4 rows of")
      result.failed.get.getMessage should include("No row is loaded: rejectMaxRecords is 2")
      rejectedErrors("beyond_limit") should have size 4
      query(
        s"""SELECT count(*) AS tables FROM information_schema.tables
           |WHERE table_schema = '$domainName' AND table_name = 'beyond_limit'""".stripMargin
      ).map(_("tables")) shouldBe List("0")
    }
  }

  new Project(
    """rejectAllOnError = true
      |sinkReplayToFile = true
      |rejectWithValue = true""".stripMargin
  ) {
    "A native DuckDB load of Parquet files under rejectAllOnError" should "keep the rejected rows with their values and in a replay file, then load none" in {
      val file = writeParquet(
        "all_or_nothing-1.parquet",
        "SELECT * FROM (VALUES ('1', 'a'), ('abc', 'b')) t(id, name)"
      )
      val result = load(
        table(
          "all_or_nothing",
          List(TableAttribute("id", "long"), TableAttribute("name", "string"))
        ),
        file
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("No row is loaded: rejectAllOnError is true")
      query(
        s"""SELECT count(*) AS tables FROM information_schema.tables
           |WHERE table_schema = '$domainName' AND table_name = 'all_or_nothing'""".stripMargin
      ).map(_("tables")) shouldBe List("0")
      val errors = rejectedErrors("all_or_nothing")
      errors should have size 1
      errors.head should include(
        "`id` cannot be converted to `long` with sl type `long`: abc"
      )
      errors.head should include("""ROW -> {"id":"abc","name":"b"}""")
      val replays = storageHandler
        .list(DatasetArea.replay(domainName), recursive = false)
        .map(_.path)
        .filter(_.getName.startsWith(s"$domainName.all_or_nothing."))
      replays.map(_.getName.endsWith(".parquet")) shouldBe List(true)
      Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { connection =>
        JdbcDbUtils.executeQueryAsMap(
          s"SELECT * FROM read_parquet('${StorageHandler.localFile(replays.head).pathAsString}')",
          connection
        )
      } shouldBe List(Map("id" -> "abc", "name" -> "b"))
    }

    it should "keep neither a replay file nor a table of the first step when a later file fails" in {
      val rejecting = writeParquet(
        "later_failure-1.parquet",
        "SELECT * FROM (VALUES ('1', 'a'), ('abc', 'b')) t(id, name)"
      )
      val unrelated = writeParquet("later_failure-2.parquet", "SELECT 1 AS x")
      val result = load(
        table(
          "later_failure",
          List(TableAttribute("id", "long"), TableAttribute("name", "string"))
        ),
        rejecting,
        unrelated
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("No attribute of later_failure found in")
      storageHandler
        .list(DatasetArea.replay(domainName), recursive = false)
        .map(_.path.getName)
        .filter(_.startsWith(s"$domainName.later_failure.")) shouldBe Nil
      query(
        s"""SELECT count(*) AS tables FROM information_schema.tables
           |WHERE table_schema = '$domainName' AND table_name LIKE 'zztmp_later_failure%'""".stripMargin
      ).map(_("tables")) shouldBe List("0")
    }
  }
}
