package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{DomainInfo, Format, Metadata, SchemaInfo, TableAttribute}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery._
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The rows of CSV and JSON files that native BigQuery loads reject are kept as the Spark loads
  * keep theirs (#1857): in the audit `rejected` table, with one error line per invalid attribute,
  * and in a replay file that the table loads again. BigQuery only counts them: they are found again
  * by the rules of its load jobs, checked here against the rows that BigQuery loads.
  *
  * Every check is reported before the test fails, so that a run tells all that differs. The probes
  * only report the values on which BigQuery and the rules disagree.
  */
class BigQueryNativeRejectedRowsIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_rejected_rows"
  // A one-step CSV load, with a value of every type
  private val csvTable = "csv_rows"
  // A two-step JSON load (a scripted attribute), with nested and repeated fields
  private val jsonTable = "json_rows"
  // A CSV row that BigQuery cannot parse, whatever the types
  private val structuralTable = "csv_structural"
  // A load job failing on its bad records
  private val failedTable = "csv_failed"
  private val csvProbes = "csv_probes"
  private val jsonProbes = "json_probes"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-rejected-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    (dir / "metadata" / "types").createDirectories()
    (dir / "metadata" / "types" / "types.sl.yml").overwrite(
      """version: 1
        |types:
        |  - name: "code3"
        |    primitiveType: "string"
        |    pattern: ".*"
        |    ddlMapping:
        |      bigquery: "STRING(3)"
        |  - name: "small"
        |    primitiveType: "decimal"
        |    pattern: "[-+]?\\d*\\.?\\d+"
        |    zone: "5,2"
        |""".stripMargin
    )
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private val mapper = new ObjectMapper()

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  private def load(table: String, file: File): Boolean =
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

  /** @return
    *   the values of a column of a table, as strings
    */
  private def values(table: String, column: String): List[String] =
    Try(
      run(s"SELECT CAST($column AS STRING) FROM `$dataset.$table`")
        .iterateAll()
        .asScala
        .map(row => Option(row.get(0)).filterNot(_.isNull).map(_.getStringValue).getOrElse("NULL"))
        .toList
        .sorted
    ).getOrElse(Nil)

  /** A record of the audit rejected table: its error lines, its file and its row as JSON */
  private case class Record(errors: List[String], file: String, row: Option[String]) {
    def field(name: String): Option[String] =
      row
        .flatMap(row => Try(mapper.readTree(row)).toOption)
        .flatMap(node => Option(node.get(name)))
        .filterNot(_.isNull)
        .map(_.asText)
  }

  private def record(error: String): Record = {
    val parts = error.stripPrefix("ERR -> \n").split("\n\nFILE -> ", 2)
    val (file, row) = parts.lift(1).getOrElse("").split("\n\nROW -> ", 2) match {
      case Array(file, row) => (file, Some(row))
      case Array(file)      => (file, None)
    }
    Record(parts.head.split("\n").toList.filter(_.nonEmpty), file, row)
  }

  /** @return
    *   the records that the loads of a file wrote since this time: the files have names of their
    *   own
    */
  private def rejected(table: String, since: Long, file: File): List[Record] =
    run(
      s"""SELECT error FROM `audit.rejected`
         |WHERE domain = '$dataset' AND schema = '$table' AND timestamp >= TIMESTAMP_MILLIS($since)
         |AND path = '${new Path(file.uri.toString).toString}'""".stripMargin
    ).iterateAll()
      .asScala
      .map(row => record(row.get(0).getStringValue))
      .toList

  /** @return
    *   the accepted and rejected counts of the audit log of the loads of a file since this time.
    *   The audit log keeps the second of its timestamp only (`AuditLog.asSelect`).
    */
  private def auditCounts(table: String, since: Long, file: File): List[(Long, Long)] =
    run(
      s"""SELECT COUNTACCEPTED, COUNTREJECTED FROM `audit.audit`
         |WHERE DOMAIN = '$dataset' AND SCHEMA = '$table' AND STEP = 'LOAD'
         |AND TIMESTAMP >= TIMESTAMP_SECONDS(${since / 1000}) AND PATHS LIKE '%${file.name}%'""".stripMargin
    ).iterateAll()
      .asScala
      .map(row => row.get(0).getLongValue -> row.get(1).getLongValue)
      .toList

  /** @return
    *   the replay files of a table in the replay area of the domain
    */
  private def replayFiles(table: String, extension: String): Set[File] =
    Try(
      settings
        .storageHandler()
        .list(DatasetArea.replay(dataset), recursive = false)
        .map(_.path)
        .filter(
          _.getName.matches(s"$dataset\\.$table\\.\\d{14}\\.replay-[0-9a-f]{8}\\.$extension")
        )
        .map(path => File(path.toUri))
        .toSet
    ).getOrElse(Set.empty)

  /** The checks that failed, reported together at the end */
  private val failures = ListBuffer.empty[String]

  private def check(description: String)(condition: => Boolean): Unit = {
    val ok = Try(condition).getOrElse(false)
    info(s"${if (ok) "OK" else "FAILED"}: $description")
    if (!ok) failures += description
  }

  /** @param expected
    *   the first error lines of the rejected rows, by their key: an actual line can be longer
    */
  private def checkRecords(
    name: String,
    records: List[Record],
    key: Record => String,
    expected: Map[String, List[String]],
    file: File
  ): Unit = {
    records.foreach(r =>
      info(s"$name record: ${r.errors.mkString(" | ")} ROW ${r.row.getOrElse("")}")
    )
    val actual = records.map(r => key(r) -> r.errors).toMap
    check(s"$name: one record per rejected row") {
      records.map(key).sorted == expected.keys.toList.sorted
    }
    expected.toList.sortBy(_._1).foreach { case (rowKey, lines) =>
      check(s"$name: the errors of '$rowKey' are $lines, got ${actual.get(rowKey)}") {
        actual
          .get(rowKey)
          .exists(errors =>
            errors.size == lines.size && errors.zip(lines).forall { case (a, e) => a.startsWith(e) }
          )
      }
    }
    check(s"$name: the records name the file") {
      records.forall(_.file == new Path(file.uri.toString).toString)
    }
  }

  private def writeDomain(): Unit = {
    def table(name: String, format: Format, attributes: List[TableAttribute]) =
      SchemaInfo(
        name,
        Pattern.compile(s"$name.*"),
        attributes = attributes,
        metadata = Some(
          if (format == Format.DSV)
            Metadata(format = Some(Format.DSV), withHeader = Some(true), separator = Some(";"))
          else Metadata(format = Some(format))
        )
      )
    val typed = List(
      TableAttribute("flag", "boolean"),
      TableAttribute("amount", "double"),
      TableAttribute("price", "decimal"),
      TableAttribute("small", "small"),
      TableAttribute("big", "bignumeric"),
      TableAttribute("day", "date"),
      TableAttribute("civil", "datetime"),
      TableAttribute("wall", "time"),
      TableAttribute("instant", "timestamp"),
      TableAttribute("payload", "binary"),
      TableAttribute("area", "geography"),
      TableAttribute("duration", "interval"),
      TableAttribute("doc", "variant")
    )
    val idAndLabel = List(
      TableAttribute("id", "long", required = Some(true)),
      TableAttribute("label", "string")
    )
    val nested = List(
      TableAttribute("tags", "string", array = Some(true)),
      TableAttribute(
        "record",
        "struct",
        attributes = List(
          TableAttribute("name", "string", required = Some(true)),
          TableAttribute("day", "date")
        )
      )
    )
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        table(
          csvTable,
          Format.DSV,
          (TableAttribute("id", "long", required = Some(true)) :: typed) ++ List(
            TableAttribute("days", "range_date"),
            TableAttribute("code", "code3"),
            TableAttribute("label", "string")
          )
        ),
        table(
          jsonTable,
          Format.JSON,
          (TableAttribute("id", "long", required = Some(true)) :: typed) ++ List(
            TableAttribute("code", "code3"),
            TableAttribute("label", "string")
          ) ++ nested :+ TableAttribute("loaded_from", "string", script = Some("'json'"))
        ),
        table(structuralTable, Format.DSV, idAndLabel),
        table(failedTable, Format.DSV, idAndLabel),
        table(
          csvProbes,
          Format.DSV,
          (TableAttribute("probe", "string") :: TableAttribute("id", "long") :: typed) ++ List(
            TableAttribute("days", "range_date"),
            TableAttribute("code", "code3")
          )
        ),
        table(
          jsonProbes,
          Format.JSON,
          (TableAttribute("probe", "string") :: TableAttribute("id", "long") :: typed) ++ List(
            TableAttribute("label", "string")
          ) ++ nested.map(_.copy(required = None))
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
  }

  private val csvColumns = List(
    "id",
    "flag",
    "amount",
    "price",
    "small",
    "big",
    "day",
    "civil",
    "wall",
    "instant",
    "payload",
    "area",
    "duration",
    "doc",
    "days",
    "code",
    "label"
  )

  /** A CSV line with these values, the others being empty */
  private def csvLine(columns: List[String], values: (String, String)*): String = {
    val byColumn = values.toMap
    columns.map(column => byColumn.getOrElse(column, "")).mkString(";")
  }

  /** The rows of the probes that BigQuery and the rules do not treat alike */
  private def reportProbes(
    name: String,
    probes: List[(String, String, String)],
    loaded: Set[String],
    records: List[Record]
  ): Unit = {
    val rejectedByRules = records.flatMap(r => r.field("probe").map(_ -> r.errors)).toMap
    info(s"$name: ${loaded.size} rows loaded, ${records.size} records")
    probes.foreach { case (probe, column, value) =>
      val loadedByBigQuery = loaded.contains(probe)
      val rules = rejectedByRules.get(probe)
      val agree = loadedByBigQuery == rules.isEmpty
      info(
        s"${if (agree) "same" else "DIFFERENT"} $name $probe $column=$value: BigQuery ${if (loadedByBigQuery) "loads it"
          else "rejects it"}, the rules ${rules.map(e => s"reject it (${e.mkString(" | ")})").getOrElse("accept it")}"
      )
    }
    records.filter(_.field("probe").isEmpty).foreach { r =>
      info(s"$name other record: ${r.errors.mkString(" | ")}")
    }
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Native CSV and JSON loads" should "keep the rows that BigQuery rejects, in the audit and in replay files" in {
      withEnvs(
        "SL_ROOT"                -> project.pathAsString,
        "SL_REJECT_WITH_VALUE"   -> "true",
        "SL_SINK_REPLAY_TO_FILE" -> "true",
        // The audit log is read by the test
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        List(csvTable, jsonTable, structuralTable, failedTable, csvProbes, jsonProbes).foreach(
          name => bigquery.delete(TableId.of(dataset, name))
        )
        writeDomain()
        val incoming = (project / "incoming" / dataset).createDirectories()

        // CSV, in one step: an invalid value of each type, a missing required value, two errors
        val csvValid = csvLine(
          csvColumns,
          "id"       -> "1",
          "flag"     -> "true",
          "amount"   -> "1.5",
          "price"    -> "12.34",
          "small"    -> "123.45",
          "big"      -> "123.5",
          "day"      -> "2024-01-02",
          "civil"    -> "2024-01-02 03:04:05",
          "wall"     -> "03:04:05",
          "instant"  -> "2024-01-02 03:04:05",
          "payload"  -> "AP8Q",
          "area"     -> "POINT(1 2)",
          "duration" -> "1-2 3 4:5:6",
          "doc"      -> "\"{\"\"a\"\": 1}\"",
          "days"     -> "[2024-01-01, 2024-02-01)",
          "code"     -> "abc",
          "label"    -> "valid"
        )
        val csvInvalid = List(
          ("id", "x", "INT64"),
          ("flag", "maybe", "BOOL"),
          ("amount", "x", "FLOAT64"),
          ("price", "x", "NUMERIC"),
          ("small", "1234.5", "NUMERIC(5, 2)"),
          ("big", "x", "BIGNUMERIC"),
          ("day", "2024-13-45", "DATE"),
          ("civil", "x", "DATETIME"),
          ("wall", "25:61:00", "TIME"),
          ("instant", "x", "TIMESTAMP"),
          ("payload", "!!!", "BYTES"),
          ("area", "x", "GEOGRAPHY"),
          ("duration", "x", "INTERVAL"),
          ("doc", "{x", "JSON"),
          ("days", "x", "RANGE<DATE>"),
          ("code", "abcd", "STRING(3)")
        )
        val csvRejected = csvInvalid.zipWithIndex.map { case ((column, value, tpe), index) =>
          val label = s"bad $column"
          (
            label,
            csvLine(csvColumns, "id" -> s"${index + 2}", column -> value, "label" -> label),
            List(s"`$column` is not a valid $tpe")
          )
        } ++ List(
          (
            "no id",
            csvLine(csvColumns, "flag" -> "true", "label" -> "no id"),
            List("`id`  can't be null")
          ),
          (
            "two errors",
            csvLine(
              csvColumns,
              "id"     -> "99",
              "flag"   -> "maybe",
              "amount" -> "x",
              "label"  -> "two errors"
            ),
            List("`flag` is not a valid BOOL", "`amount` is not a valid FLOAT64")
          )
        )
        val csvHeader = csvColumns.mkString(";")
        val csvFile = incoming / s"$csvTable-1.csv"
        csvFile.overwrite((csvHeader :: csvValid :: csvRejected.map(_._2)).mkString("", "\n", "\n"))

        // JSON, in two steps: an invalid value of each type, nested and repeated fields, lines that
        // are not JSON objects
        val jsonValid =
          """{"id": 1, "flag": true, "amount": 1.5, "price": "12.34", "small": "123.45", "big": "123.5", "day": "2024-01-02", "civil": "2024-01-02 03:04:05", "wall": "03:04:05", "instant": "2024-01-02T03:04:05Z", "payload": "AP8Q", "area": "POINT(1 2)", "duration": "P1Y2M3DT4H5M6S", "doc": {"a": 1}, "code": "abc", "label": "valid", "tags": ["a", "b"], "record": {"name": "n", "day": "2024-01-02"}}"""
        val jsonInvalid = List(
          ("flag", "\"maybe\"", "`flag` is not a valid BOOL"),
          ("amount", "\"x\"", "`amount` is not a valid FLOAT64"),
          ("price", "\"x\"", "`price` is not a valid NUMERIC"),
          ("small", "\"1234.5\"", "`small` is not a valid NUMERIC(5, 2)"),
          ("big", "\"x\"", "`big` is not a valid BIGNUMERIC"),
          ("day", "\"2024-13-45\"", "`day` is not a valid DATE"),
          ("civil", "\"x\"", "`civil` is not a valid DATETIME"),
          ("wall", "\"25:61:00\"", "`wall` is not a valid TIME"),
          ("instant", "\"x\"", "`instant` is not a valid TIMESTAMP"),
          ("payload", "\"!!!\"", "`payload` is not a valid BYTES"),
          ("area", "\"x\"", "`area` is not a valid GEOGRAPHY"),
          ("duration", "\"x\"", "`duration` is not a valid INTERVAL"),
          ("code", "\"abcd\"", "`code` is not a valid STRING(3)"),
          ("tags", "[\"a\", null]", "`tags` holds a null element"),
          ("tags", "\"a\"", "`tags` is not a JSON array"),
          ("record", "\"x\"", "`record` is not a JSON object"),
          ("record", "{\"day\": \"2024-01-02\"}", "`record.name`  can't be null"),
          ("record", "{\"name\": \"n\", \"day\": \"x\"}", "`record.day` is not a valid DATE")
        )
        val jsonRejected = jsonInvalid.zipWithIndex.map { case ((field, value, error), index) =>
          val label = s"bad $field $index"
          (label, s"""{"id": ${index + 2}, "$field": $value, "label": "$label"}""", List(error))
        } ++ List(
          ("bad id", """{"id": "x", "label": "bad id"}""", List("`id` is not a valid INT64")),
          ("no id", """{"flag": true, "label": "no id"}""", List("`id`  can't be null")),
          (
            "object for a number",
            """{"id": 98, "amount": {"a": 1}, "label": "object for a number"}""",
            List("`amount` is not a valid FLOAT64")
          ),
          ("not json", "not json", List("The line is not valid JSON")),
          ("[1, 2]", "[1, 2]", List("The line is not a JSON object"))
        )
        val jsonFile = incoming / s"$jsonTable-1.json"
        jsonFile.overwrite((jsonValid :: jsonRejected.map(_._2)).mkString("", "\n", "\n"))
        def jsonKey(r: Record): String = r.field("label").orElse(r.row).getOrElse("")

        // A CSV row that BigQuery cannot parse is only counted, with the message of BigQuery
        val structuralFile = incoming / s"$structuralTable-1.csv"
        structuralFile.overwrite("id;label\n1;one\n2;\"a\"b\nx;three\n")

        val start = System.currentTimeMillis()
        val csvLoaded = load(csvTable, csvFile)
        val jsonLoaded = load(jsonTable, jsonFile)
        val structuralLoaded = load(structuralTable, structuralFile)

        // CSV
        check("the CSV load succeeds")(csvLoaded)
        check(s"BigQuery loads the valid CSV row only: ${values(csvTable, "label")}") {
          values(csvTable, "label") == List("valid")
        }
        check(
          s"the audit log of the CSV load counts the rows: ${auditCounts(csvTable, start, csvFile)}"
        ) {
          auditCounts(csvTable, start, csvFile) == List(1L -> csvRejected.size.toLong)
        }
        val csvRecords = rejected(csvTable, start, csvFile)
        checkRecords(
          "CSV",
          csvRecords,
          _.field("label").getOrElse(""),
          csvRejected.map { case (label, _, errors) => label -> errors }.toMap,
          csvFile
        )
        val csvReplays = replayFiles(csvTable, "csv")
        check(s"one CSV replay file: $csvReplays")(csvReplays.size == 1)
        csvReplays.headOption.foreach { replay =>
          val lines = replay.lines.toList
          info(s"CSV replay file:\n${lines.mkString("\n")}")
          check("the CSV replay file has the header and the rejected lines") {
            lines.headOption
              .contains(csvHeader) && lines.tail.sorted == csvRejected.map(_._2).sorted
          }
          val replayLoad = System.currentTimeMillis()
          check("the CSV replay file loads")(load(csvTable, replay))
          check("its rows are rejected again")(values(csvTable, "label") == List("valid"))
          checkRecords(
            "CSV replay",
            rejected(csvTable, replayLoad, replay),
            _.field("label").getOrElse(""),
            csvRejected.map { case (label, _, errors) => label -> errors }.toMap,
            replay
          )
        }

        // JSON
        check("the JSON load succeeds")(jsonLoaded)
        check(s"BigQuery loads the valid JSON line only: ${values(jsonTable, "label")}") {
          values(jsonTable, "label") == List("valid")
        }
        check(
          s"the audit log of the JSON load counts the rows: ${auditCounts(jsonTable, start, jsonFile)}"
        ) {
          auditCounts(jsonTable, start, jsonFile) == List(1L -> jsonRejected.size.toLong)
        }
        val jsonRecords = rejected(jsonTable, start, jsonFile)
        checkRecords(
          "JSON",
          jsonRecords,
          jsonKey,
          jsonRejected.map { case (label, _, errors) => label -> errors }.toMap,
          jsonFile
        )
        val jsonReplays = replayFiles(jsonTable, "json")
        check(s"one JSON replay file: $jsonReplays")(jsonReplays.size == 1)
        jsonReplays.headOption.foreach { replay =>
          val lines = replay.lines.toList
          info(s"JSON replay file:\n${lines.mkString("\n")}")
          check("the JSON replay file has the rejected lines") {
            lines.sorted == jsonRejected.map(_._2).sorted
          }
          val replayLoad = System.currentTimeMillis()
          check("the JSON replay file loads")(load(jsonTable, replay))
          check("its lines are rejected again")(values(jsonTable, "label") == List("valid"))
          checkRecords(
            "JSON replay",
            rejected(jsonTable, replayLoad, replay),
            jsonKey,
            jsonRejected.map { case (label, _, errors) => label -> errors }.toMap,
            replay
          )
        }

        // A structural error
        check("the CSV load with a structural error succeeds")(structuralLoaded)
        check(s"BigQuery loads the valid row only: ${values(structuralTable, "label")}") {
          values(structuralTable, "label") == List("one")
        }
        val structuralRecords = rejected(structuralTable, start, structuralFile)
        structuralRecords.foreach(r => info(s"Structural record: ${r.errors.mkString(" | ")}"))
        check("the row of the structural error has the message of BigQuery") {
          structuralRecords.count(_.field("label").isEmpty) == 1
        }
        check("the other rejected row is found") {
          structuralRecords.flatMap(r => r.field("label").map(_ -> r.errors)) ==
            List("three" -> List("`id` is not a valid INT64"))
        }
        val structuralReplays = replayFiles(structuralTable, "csv")
        check(s"its replay file has the rejected row only: $structuralReplays") {
          structuralReplays.size == 1 &&
          structuralReplays.head.lines.toList == List("id;label", "x;three")
        }

        // Probes: values whose reading the documentation of BigQuery leaves open
        val csvProbeValues = List(
          "id"       -> " 7",
          "id"       -> "+7",
          "id"       -> "0x1F",
          "id"       -> "1.0",
          "flag"     -> "Yes",
          "flag"     -> "Y",
          "flag"     -> " true",
          "amount"   -> "1e3",
          "amount"   -> "NaN",
          "amount"   -> "inf",
          "amount"   -> " 1.5",
          "amount"   -> "\"\"",
          "price"    -> "1.1234567891",
          "price"    -> "1e3",
          "price"    -> "123456789012345678901234567890",
          "day"      -> "2024-1-2",
          "day"      -> "2024/01/02",
          "day"      -> " 2024-01-02",
          "civil"    -> "2024-01-02T03:04:05",
          "civil"    -> "2024-01-02",
          "civil"    -> "2024-01-02 03:04",
          "wall"     -> "3:04:05",
          "wall"     -> "03:04",
          "instant"  -> "2024/01/02 03:04",
          "instant"  -> "2024-01-02",
          "instant"  -> "1700000000",
          "instant"  -> "1700000000.5",
          "instant"  -> "2024-01-02 03:04:05 Europe/Paris",
          "instant"  -> "2024-01-02T03:04:05.123456789Z",
          "instant"  -> "2024-01-02 03:04:05+01",
          "payload"  -> "AP8",
          "payload"  -> "AP8Q AP8Q",
          "area"     -> "0101000000000000000000F03F0000000000000040",
          "area"     -> "\"{\"\"type\"\": \"\"Point\"\", \"\"coordinates\"\": [1, 2]}\"",
          "duration" -> "P1Y",
          "duration" -> "1-2",
          "duration" -> "0-0 10 0:0:0",
          "doc"      -> "12",
          "doc"      -> "123456789012345678901234567890",
          "days"     -> "[2024-01-01, UNBOUNDED)",
          "days"     -> "[2024-02-01, 2024-01-01)",
          "days"     -> "[2024-01-01,2024-02-01)",
          "code"     -> "été"
        ).zipWithIndex.map { case ((column, value), index) => (f"p$index%02d", column, value) }
        val csvProbeColumns = "probe" :: csvColumns.filterNot(_ == "label")
        val csvProbeFile = incoming / s"$csvProbes-1.csv"
        csvProbeFile.overwrite(
          (csvProbeColumns.mkString(";") :: csvProbeValues.map { case (probe, column, value) =>
            csvLine(csvProbeColumns, "probe" -> probe, column -> value)
          }).mkString("", "\n", "\n")
        )
        val jsonProbeValues = List(
          "id"       -> "\"12\"",
          "id"       -> "1.0",
          "id"       -> "1e2",
          "id"       -> "true",
          "id"       -> "\" 12\"",
          "flag"     -> "\"yes\"",
          "flag"     -> "1",
          "flag"     -> "\"1\"",
          "flag"     -> "\"TRUE\"",
          "amount"   -> "\"NaN\"",
          "amount"   -> "\"1e3\"",
          "amount"   -> "true",
          "price"    -> "1.1234567891",
          "price"    -> "12345678901234567890123456789.5",
          "civil"    -> "\"2024-01-02T03:04:05\"",
          "civil"    -> "\"2024-01-02\"",
          "wall"     -> "\"03:04\"",
          "instant"  -> "1700000000",
          "instant"  -> "\"1700000000\"",
          "instant"  -> "\"2024-01-02\"",
          "instant"  -> "\"2024/01/02 03:04\"",
          "instant"  -> "\"2024-01-02 03:04:05 Europe/Paris\"",
          "duration" -> "\"1-2 3 4:5:6\"",
          "duration" -> "\"-P1Y\"",
          "area"     -> """{"type": "Point", "coordinates": [1, 2]}""",
          "area"     -> """"{\"type\": \"Point\", \"coordinates\": [1, 2]}"""",
          "area"     -> "\"0101000000000000000000F03F0000000000000040\"",
          "label"    -> "12",
          "label"    -> "true",
          "label"    -> """{"a": 1}""",
          "label"    -> "[1]",
          "Amount"   -> "\"x\"",
          "payload"  -> "\"AP8\"",
          "doc"      -> "12",
          "tags"     -> "[]",
          "tags"     -> "[[1]]",
          "record"   -> "{}",
          "record"   -> "[]"
        ).zipWithIndex.map { case ((field, value), index) => (f"p$index%02d", field, value) }
        val jsonProbeFile = incoming / s"$jsonProbes-1.json"
        jsonProbeFile.overwrite(
          jsonProbeValues
            .map { case (probe, field, value) => s"""{"probe": "$probe", "$field": $value}""" }
            .mkString("", "\n", "\n")
        )
        val probesStart = System.currentTimeMillis()
        info(s"CSV probes loaded: ${load(csvProbes, csvProbeFile)}")
        reportProbes(
          "CSV",
          csvProbeValues,
          values(csvProbes, "probe").toSet,
          rejected(csvProbes, probesStart, csvProbeFile)
        )
        info(s"JSON probes loaded: ${load(jsonProbes, jsonProbeFile)}")
        reportProbes(
          "JSON",
          jsonProbeValues,
          values(jsonProbes, "probe").toSet,
          rejected(jsonProbes, probesStart, jsonProbeFile)
        )

        // A load job that fails on its bad records keeps the messages of BigQuery, as before
        val failedFile = incoming / s"$failedTable-1.csv"
        failedFile.overwrite("id;label\n1;one\nx;two\ny;three\n")
        val failedStart = System.currentTimeMillis()
        val failedLoaded = withEnvs("SL_REJECT_MAX_RECORDS" -> "1")(load(failedTable, failedFile))
        check("the load failing on its bad records fails")(!failedLoaded)
        val failedRecords = rejected(failedTable, failedStart, failedFile)
        failedRecords.foreach(r => info(s"Failed load record: ${r.errors.mkString(" | ")}"))
        check("it keeps the messages of BigQuery") {
          failedRecords.nonEmpty && failedRecords.forall(_.errors.exists(_.contains("line_number")))
        }

        failures.toList shouldBe Nil
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
