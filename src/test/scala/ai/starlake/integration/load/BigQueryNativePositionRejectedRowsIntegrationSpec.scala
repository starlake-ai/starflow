package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{DomainInfo, Format, Metadata, Position, SchemaInfo, TableAttribute}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery._
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The lines of POSITION files whose values do not convert are rejected by native BigQuery loads,
  * as by the Spark loads (#1886): counted, kept in the audit `rejected` table and in a replay file,
  * and under `rejectMaxRecords` the load fails without loading any row. The values of the types
  * with a pattern are read with it.
  *
  * Every check is reported before the test fails, so that a run tells all that differs.
  */
class BigQueryNativePositionRejectedRowsIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_position_rows"
  private val rowsTable = "position_rows"
  private val thresholdTable = "position_threshold"

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-position-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    (dir / "metadata" / "types").createDirectories()
    (dir / "metadata" / "types" / "types.sl.yml").overwrite(
      """version: 1
        |types:
        |  - name: "dmy"
        |    primitiveType: "date"
        |    pattern: "dd/MM/yyyy"
        |    ddlMapping:
        |      bigquery: "DATE"
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
    *   the loaded rows of a table, by trimmed label: id, day, flag and born as strings
    */
  private def rows(table: String): Map[String, List[String]] =
    Try(
      run(
        s"SELECT TRIM(label), CAST(id AS STRING), CAST(day AS STRING), CAST(flag AS STRING), CAST(born AS STRING) FROM `$dataset.$table`"
      ).iterateAll()
        .asScala
        .map { row =>
          val values = (0 to 4).toList.map(index =>
            Option(row.get(index)).filterNot(_.isNull).map(_.getStringValue).getOrElse("NULL")
          )
          values.head -> values.tail
        }
        .toMap
    ).getOrElse(Map.empty)

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
    *   the records that the loads of a file wrote since this time
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
    *   The audit log keeps the second of its timestamp only.
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

  private def replayFiles(table: String): Set[File] =
    Try(
      settings
        .storageHandler()
        .list(DatasetArea.replay(dataset), recursive = false)
        .map(_.path)
        .filter(_.getName.matches(s"$dataset\\.$table\\.\\d{14}\\.replay-[0-9a-f]{8}\\.txt"))
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
    *   the first error lines of the rejected rows, by the slice of their id: an actual line can be
    *   longer
    */
  private def checkRecords(
    name: String,
    records: List[Record],
    expected: Map[String, List[String]],
    file: File
  ): Unit = {
    records.foreach(r =>
      info(s"$name record: ${r.errors.mkString(" | ")} ROW ${r.row.getOrElse("")}")
    )
    val actual = records.map(r => r.field("id").getOrElse("") -> r.errors).toMap
    check(s"$name: one record per rejected row") {
      records.flatMap(_.field("id")).sorted == expected.keys.toList.sorted
    }
    expected.toList.sortBy(_._1).foreach { case (id, lines) =>
      check(s"$name: the errors of '$id' are $lines, got ${actual.get(id)}") {
        actual
          .get(id)
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
    // A header line by default
    def table(name: String, withHeader: Boolean, filter: Option[String] = None) =
      SchemaInfo(
        name,
        Pattern.compile(s"$name.*"),
        attributes = List(
          TableAttribute("id", "long", required = Some(true), position = Some(Position(0, 3))),
          TableAttribute("day", "basic_iso_date", position = Some(Position(4, 11))),
          TableAttribute("flag", "boolean", position = Some(Position(12, 12))),
          TableAttribute("born", "dmy", position = Some(Position(13, 22))),
          TableAttribute("label", "string", position = Some(Position(23, 32)))
        ),
        metadata = Some(
          Metadata(format = Some(Format.POSITION), withHeader = Some(withHeader))
        ),
        filter = filter
      )
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        // The filter reads the values of the line, as the second step does
        table(rowsTable, withHeader = true, filter = Some("TRIM(label) != 'TRAILER'")),
        table(thresholdTable, withHeader = false)
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
  }

  /** A line of the files: the values are padded to the width of their field */
  private def line(id: String, day: String, flag: String, born: String, label: String): String =
    List(id -> 4, day -> 8, flag -> 1, born -> 10, label -> 10).map { case (value, width) =>
      value.padTo(width, ' ')
    }.mkString

  private val booleanPattern = "(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f"

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Native POSITION loads" should "reject the lines whose values do not convert, as the Spark loads" in {
      withEnvs(
        "SL_ROOT"                -> project.pathAsString,
        "SL_REJECT_WITH_VALUE"   -> "true",
        "SL_SINK_REPLAY_TO_FILE" -> "true",
        // The audit log is read by the test
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        List(rowsTable, thresholdTable).foreach(name => bigquery.delete(TableId.of(dataset, name)))
        writeDomain()
        val incoming = (project / "incoming" / dataset).createDirectories()

        val loaded = List(
          line("0001", "20261030", "Y", "30/10/2026", "valid"),
          // Padded values are trimmed, a blank optional value is NULL
          line(" 12 ", "20261031", "n", "", "padded"),
          line("0003", "", "1", "01/01/2026", "blank day")
        )
        val rejectedLines = List(
          (
            "00x2",
            line("00x2", "20261030", "Y", "30/10/2026", "bad id"),
            List("`id` is not a valid INT64: 00x2")
          ),
          (
            "0004",
            line("0004", "2026103x", "Y", "30/10/2026", "bad day"),
            List("`day` is not a valid TIMESTAMP with the pattern `BASIC_ISO_DATE`: 2026103x")
          ),
          (
            "0005",
            line("0005", "20261340", "Y", "30/10/2026", "no month"),
            List("`day` is not a valid TIMESTAMP with the pattern `BASIC_ISO_DATE`: 20261340")
          ),
          (
            "0006",
            line("0006", "20261030", "x", "30/10/2026", "bad flag"),
            List(s"`flag` is not a valid BOOL with the pattern `$booleanPattern`: x")
          ),
          (
            "0007",
            line("0007", "20261030", "Y", "32/01/2026", "bad born"),
            List("`born` is not a valid DATE with the pattern `dd/MM/yyyy`: 32/01/2026")
          ),
          (
            "    ",
            line("", "20261030", "Y", "30/10/2026", "no id"),
            List("`id`  can't be null")
          ),
          (
            "00x8",
            line("00x8", "2026103x", "Y", "30/10/2026", "two errors"),
            List(
              "`id` is not a valid INT64: 00x8",
              "`day` is not a valid TIMESTAMP with the pattern `BASIC_ISO_DATE`: 2026103x"
            )
          ),
          (
            // A short line
            "0009",
            "0009202610",
            List("`day` is not a valid TIMESTAMP with the pattern `BASIC_ISO_DATE`: 202610")
          )
        )
        val expected = rejectedLines.map { case (id, _, errors) => id -> errors }.toMap
        val file = incoming / s"$rowsTable-1.txt"
        // A trailer of another layout, which the filter drops: neither loaded nor rejected
        val trailer = line("TRL", "11", "", "", "TRAILER")
        file.overwrite(
          ("id  day     fborn      label" :: loaded ++ rejectedLines.map(_._2) :+ trailer)
            .mkString("", "\n", "\n")
        )

        val start = System.currentTimeMillis()
        check("the load succeeds")(load(rowsTable, file))
        val loadedRows = rows(rowsTable)
        info(s"Loaded rows: $loadedRows")
        check("the valid lines are loaded, with their values read with their pattern") {
          loadedRows == Map(
            "valid"     -> List("1", "2026-10-30 00:00:00+00", "true", "2026-10-30"),
            "padded"    -> List("12", "2026-10-31 00:00:00+00", "false", "NULL"),
            "blank day" -> List("3", "NULL", "true", "2026-01-01")
          )
        }
        check(s"the audit log counts the rows: ${auditCounts(rowsTable, start, file)}") {
          auditCounts(rowsTable, start, file) == List(
            loaded.size.toLong -> rejectedLines.size.toLong
          )
        }
        checkRecords("POSITION", rejected(rowsTable, start, file), expected, file)
        val replays = replayFiles(rowsTable)
        check(s"one replay file: $replays")(replays.size == 1)
        replays.headOption.foreach { replay =>
          val lines = replay.lines.toList
          info(s"Replay file:\n${lines.mkString("\n")}")
          check("the replay file has a header line and the rejected lines") {
            lines.headOption.contains("id day flag born label") &&
            lines.tail.sorted == rejectedLines.map(_._2).sorted
          }
          val replayLoad = System.currentTimeMillis()
          check("the replay file loads")(load(rowsTable, replay))
          check("its lines are rejected again")(rows(rowsTable).keySet == loadedRows.keySet)
          checkRecords("POSITION replay", rejected(rowsTable, replayLoad, replay), expected, replay)
        }

        // Above rejectMaxRecords, no row is loaded and the load fails, its rejected rows kept
        val thresholdFile = incoming / s"$thresholdTable-1.txt"
        thresholdFile.overwrite(
          (loaded.head :: rejectedLines.take(2).map(_._2)).mkString("", "\n", "\n")
        )
        val thresholdStart = System.currentTimeMillis()
        val thresholdLoaded =
          withEnvs("SL_REJECT_MAX_RECORDS" -> "1")(load(thresholdTable, thresholdFile))
        check("the load above rejectMaxRecords fails")(!thresholdLoaded)
        check("it loads no row")(rows(thresholdTable).isEmpty)
        checkRecords(
          "Threshold",
          rejected(thresholdTable, thresholdStart, thresholdFile),
          expected.filter { case (id, _) => Set("00x2", "0004").contains(id) },
          thresholdFile
        )

        failures.toList shouldBe Nil
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
