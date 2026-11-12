package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  AllSinks,
  DomainInfo,
  Format,
  Metadata,
  Position,
  SchemaInfo,
  TableAttribute,
  Trim,
  WriteStrategy,
  WriteStrategyType
}
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery._
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The rows of DSV and POSITION files with a typed value or a scripted conversion that fails are
  * rejected by native BigQuery loads (#1935): counted, kept in the audit `rejected` table and in a
  * replay file, and under `rejectMaxRecords` the load fails without loading any row, the bad
  * records of the load jobs counted too.
  *
  * Every check is reported before the test fails, so that a run tells all that differs.
  */
class BigQueryNativeRejectedValuesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_rejected_values"
  private val rowsTable = "dsv_rows"
  private val trimTable = "dsv_trim"
  private val thresholdTable = "dsv_threshold"
  private val plainTable = "dsv_plain"
  private val positionTable = "position_scripts"
  private val partitionedTable = "dsv_partitioned"
  private val filesTable = "dsv_files"
  private val tables =
    List(
      rowsTable,
      trimTable,
      thresholdTable,
      plainTable,
      positionTable,
      partitionedTable,
      filesTable
    )

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-rejected-values-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    (dir / "metadata" / "types").createDirectories()
    (dir / "metadata" / "types" / "types.sl.yml").overwrite(
      """version: 1
        |types:
        |  - name: "date_yyyymmdd"
        |    primitiveType: "date"
        |    pattern: "yyyyMMdd"
        |    sample: "20260115"
        |    ddlMapping:
        |      bigquery: "DATE"
        |  - name: "yes_no"
        |    primitiveType: "boolean"
        |    pattern: "Y<-TF->N"
        |    sample: "Y"
        |    ddlMapping:
        |      bigquery: "BOOL"
        |""".stripMargin
    )
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private val mapper = new ObjectMapper()

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  private def load(table: String, file: File, others: File*): Boolean =
    Try(
      new Main().run(
        Array(
          "load",
          "--domains",
          dataset,
          "--tables",
          table,
          "--files",
          (file +: others).map(_.uri.toString).mkString(","),
          "--inPlace"
        )
      )
    ).getOrElse(false)

  /** @return
    *   the loaded rows of a table, by their first column, every column as a string
    */
  private def rows(table: String, columns: List[String]): Map[String, List[String]] =
    Try(
      run(
        s"SELECT ${columns.map(c => s"CAST($c AS STRING)").mkString(", ")} FROM `$dataset.$table`"
      ).iterateAll()
        .asScala
        .map { row =>
          val values = columns.indices.toList.map(index =>
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

  private def path(file: File): String = new Path(file.uri.toString).toString

  /** @return
    *   the records that the loads of a file wrote since this time, none if they cannot be read: the
    *   checks that read them fail, and the others are still reported
    */
  private def rejected(table: String, since: Long, file: File): List[Record] =
    Try(
      run(
        s"""SELECT error FROM `audit.rejected`
           |WHERE domain = '$dataset' AND schema = '$table' AND timestamp >= TIMESTAMP_MILLIS($since)
           |AND path = '${path(file)}'""".stripMargin
      ).iterateAll()
        .asScala
        .map(row => record(row.get(0).getStringValue))
        .toList
    ).getOrElse(Nil)

  /** @return
    *   the rows of the audit log of the loads of a file since this time, none if they cannot be
    *   read. The audit log keeps the second of its timestamp only.
    */
  private def audit(table: String, since: Long, file: File): List[FieldValueList] =
    Try(
      run(
        s"""SELECT COUNTACCEPTED, COUNTREJECTED, SUCCESS, MESSAGE FROM `audit.audit`
           |WHERE DOMAIN = '$dataset' AND SCHEMA = '$table' AND STEP = 'LOAD'
           |AND TIMESTAMP >= TIMESTAMP_SECONDS(${since / 1000}) AND PATHS LIKE '%${path(
            file
          )}%'""".stripMargin
      ).iterateAll().asScala.toList
    ).getOrElse(Nil)

  /** @return
    *   the accepted and rejected counts of the audit log of the loads of a file since this time
    */
  private def auditCounts(table: String, since: Long, file: File): List[(Long, Long)] =
    audit(table, since, file).map(row => row.get(0).getLongValue -> row.get(1).getLongValue)

  /** @return
    *   true if a load of a file since this time failed with this error
    */
  private def failedWith(table: String, since: Long, file: File, error: String): Boolean =
    audit(table, since, file).exists(row =>
      !row.get(2).getBooleanValue && Option(row.get(3))
        .filterNot(_.isNull)
        .exists(_.getStringValue.contains(error))
    )

  private def replayFiles(table: String): Set[File] =
    Try(
      settings
        .storageHandler()
        .list(DatasetArea.replay(dataset), recursive = false)
        .map(_.path)
        .filter(_.getName.matches(s"$dataset\\.$table\\.\\d{14}\\.replay-[0-9a-f]{8}\\.csv"))
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
    *   the first error lines of the rows rejected by the checks of the second step, by the value of
    *   their `ID`: an actual line can be longer
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
    val actual = records.flatMap(r => r.field("ID").map(_ -> r.errors)).toMap
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
      records.nonEmpty && records.forall(_.file == new Path(file.uri.toString).toString)
    }
  }

  private def dsv(
    name: String,
    attributes: List[TableAttribute],
    sink: Option[AllSinks] = None
  ): SchemaInfo =
    SchemaInfo(
      name,
      Pattern.compile(s"$name.*"),
      attributes = attributes,
      metadata = Some(
        Metadata(
          format = Some(Format.DSV),
          separator = Some("|"),
          withHeader = Some(false),
          nullValue = Some(""),
          writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND))),
          sink = sink
        )
      )
    )

  /** The table of the issue: a typed value, a number read by the load job, two scripts. BigQuery
    * accepts no parameterized type in a CAST: the issue's `DECIMAL(18,2)` fails every query.
    */
  private val issueAttributes = List(
    TableAttribute("ID", "string"),
    TableAttribute("D_TYPED", "date_yyyymmdd"),
    TableAttribute("N_TYPED", "decimal"),
    TableAttribute("N_RAW", "string", ignore = Some(true)),
    TableAttribute("D_RAW", "string", ignore = Some(true)),
    TableAttribute("N_CALC", "decimal", script = Some("CAST(N_RAW AS NUMERIC)")),
    TableAttribute("D_CALC", "date", script = Some("PARSE_DATE('%Y%m%d', D_RAW)"))
  )

  private def writeDomain(): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        dsv(rowsTable, issueAttributes),
        dsv(thresholdTable, issueAttributes),
        dsv(filesTable, issueAttributes),
        // Partitioned on a column that the first step reads as a string, with an ignored attribute
        // read with its pattern, which a script reads
        dsv(
          partitionedTable,
          List(
            TableAttribute("ID", "string"),
            TableAttribute("DAY", "date_yyyymmdd"),
            TableAttribute("D_IGN", "date_yyyymmdd", ignore = Some(true)),
            TableAttribute("NEXT", "date", script = Some("DATE_ADD(D_IGN, INTERVAL 1 DAY)"))
          ),
          Some(AllSinks(partition = Some(Seq("DAY"))))
        ),
        dsv(
          trimTable,
          List(
            TableAttribute("ID", "string"),
            TableAttribute("N", "long", trim = Some(Trim.BOTH)),
            TableAttribute("F", "yes_no"),
            TableAttribute("R", "string", required = Some(true), trim = Some(Trim.BOTH))
          )
        ),
        dsv(plainTable, List(TableAttribute("ID", "string"), TableAttribute("D", "date"))),
        SchemaInfo(
          positionTable,
          Pattern.compile(s"$positionTable.*"),
          attributes = List(
            TableAttribute("ID", "string", position = Some(Position(0, 3))),
            TableAttribute(
              "D",
              "date",
              script = Some("PARSE_DATE(\"%Y%m%d\", SUBSTR(value, 5, 8))")
            )
          ),
          metadata = Some(
            Metadata(
              format = Some(Format.POSITION),
              withHeader = Some(false),
              writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.APPEND)))
            )
          )
        )
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Native DSV and POSITION loads" should "reject the rows whose typed values or scripted conversions fail" in {
      withEnvs(
        "SL_ROOT"                -> project.pathAsString,
        "SL_REJECT_WITH_VALUE"   -> "true",
        "SL_SINK_REPLAY_TO_FILE" -> "true",
        // The audit log is read by the test
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        tables.foreach(name => bigquery.delete(TableId.of(dataset, name)))
        writeDomain()
        val incoming = (project / "incoming" / dataset).createDirectories()

        // The reproduction of the issue
        val rejectedLines = List(
          "2" -> "2|2026AB15|10|100|20260115",
          "4" -> "4|20260115|10|10A0|20260115",
          "5" -> "5|20260115|10|100|2026AB15"
        )
        // A bad record of the load job
        val badRecord = "3|20260115|10A0|100|20260115"
        val file = incoming / s"$rowsTable-1.csv"
        file.overwrite(
          (("1|20260115|10|100|20260115" :: rejectedLines.map(_._2)) :+ badRecord)
            .mkString("", "\n", "\n")
        )
        val start = System.currentTimeMillis()
        check("the load succeeds")(load(rowsTable, file))
        val columns = List("ID", "D_TYPED", "N_TYPED", "N_CALC", "D_CALC")
        val loadedRows = rows(rowsTable, columns)
        info(s"Loaded rows: $loadedRows")
        check("the valid row is loaded, with its date read with its pattern") {
          loadedRows == Map("1" -> List("2026-01-15", "10", "100", "2026-01-15"))
        }
        check(s"the audit log counts the rows: ${auditCounts(rowsTable, start, file)}") {
          auditCounts(rowsTable, start, file) == List(1L -> 4L)
        }
        val records = rejected(rowsTable, start, file)
        checkRecords(
          "DSV",
          records,
          Map(
            "2" -> List("`D_TYPED` is not a valid DATE with the pattern `yyyyMMdd`: 2026AB15"),
            "4" -> List("`N_CALC` is not computed: CAST(N_RAW AS NUMERIC) fails: 10A0"),
            "5" -> List("`D_CALC` is not computed: PARSE_DATE('%Y%m%d', D_RAW) fails: 2026AB15")
          ),
          file
        )
        check(s"the bad record of the load job is kept too: ${records.size} records") {
          records.size == 4
        }
        val replays = replayFiles(rowsTable)
        val replayed = replays.toList.flatMap(_.lines.toList)
        info(s"Replay files: $replays\n${replayed.mkString("\n")}")
        check("the replay files hold the rejected rows as they are in the file") {
          replayed.sorted == (badRecord :: rejectedLines.map(_._2)).sorted
        }
        replays.foreach { replay =>
          val replayLoad = System.currentTimeMillis()
          check(s"the replay file ${replay.name} loads")(load(rowsTable, replay))
          check("its rows are rejected again") {
            rows(rowsTable, columns).keySet == Set("1") &&
            rejected(rowsTable, replayLoad, replay).nonEmpty
          }
        }

        // The declared trim, and a boolean pattern that the load job does not apply
        val trimFile = incoming / s"$trimTable-1.csv"
        trimFile.overwrite(
          List("1| 12 |Y| a ", "2| 1x |N|b", "3|5|X|c", "4|5|Y|   ").mkString("", "\n", "\n")
        )
        val trimStart = System.currentTimeMillis()
        check("the load with trimmed values succeeds")(load(trimTable, trimFile))
        check("the trimmed values are loaded") {
          rows(trimTable, List("ID", "N", "F", "R")) == Map("1" -> List("12", "true", "a"))
        }
        checkRecords(
          "Trim",
          rejected(trimTable, trimStart, trimFile),
          Map(
            "2" -> List("`N` is not a valid INT64:  1x "),
            "3" -> List("`F` is not a valid BOOL with the pattern `Y<-TF->N`: X"),
            "4" -> List("`R`  can't be null")
          ),
          trimFile
        )

        // Above rejectMaxRecords, the bad records of the load jobs count with the rows of the
        // checks: no row is loaded and the load fails, its rejected rows kept
        val thresholdFile = incoming / s"$thresholdTable-1.csv"
        thresholdFile.overwrite(
          List("1|20260115|10|100|20260115", rejectedLines.head._2, badRecord)
            .mkString("", "\n", "\n")
        )
        val thresholdStart = System.currentTimeMillis()
        val thresholdLoaded =
          withEnvs("SL_REJECT_MAX_RECORDS" -> "1")(load(thresholdTable, thresholdFile))
        check("the load above rejectMaxRecords fails")(!thresholdLoaded)
        check("the load fails because of rejectMaxRecords") {
          failedWith(thresholdTable, thresholdStart, thresholdFile, "rejectMaxRecords is 1")
        }
        check("it loads no row")(rows(thresholdTable, columns).isEmpty)
        checkRecords(
          "Threshold",
          rejected(thresholdTable, thresholdStart, thresholdFile),
          Map("2" -> List("`D_TYPED` is not a valid DATE with the pattern `yyyyMMdd`: 2026AB15")),
          thresholdFile
        )

        // Above rejectMaxRecords over the files of a grouped load, even when the second step
        // rejects nothing: each load job allows rejectMaxRecords bad records
        val firstFile = incoming / s"$filesTable-1.csv"
        val secondFile = incoming / s"$filesTable-2.csv"
        firstFile.overwrite(s"1|20260115|10|100|20260115\n$badRecord\n")
        secondFile.overwrite(s"6|20260115|10|100|20260115\n${badRecord.replace("3|", "7|")}\n")
        val filesStart = System.currentTimeMillis()
        val filesLoaded = withEnvs("SL_REJECT_MAX_RECORDS" -> "1", "SL_GROUPED" -> "true")(
          load(filesTable, firstFile, secondFile)
        )
        check("the grouped load above rejectMaxRecords over its files fails")(!filesLoaded)
        check("the grouped load fails because of rejectMaxRecords") {
          failedWith(filesTable, filesStart, firstFile, "rejectMaxRecords is 1")
        }
        check("it loads no row")(rows(filesTable, columns).isEmpty)

        // A partition column read as a string, and an ignored attribute read with its pattern
        val partitionedFile = incoming / s"$partitionedTable-1.csv"
        partitionedFile.overwrite("1|20260115|20260131\n2|2026AB15|20260131\n3|20260116|2026XX31\n")
        val partitionedStart = System.currentTimeMillis()
        check("the partitioned load succeeds")(load(partitionedTable, partitionedFile))
        check("its valid row is loaded, with the script on the converted ignored value") {
          rows(partitionedTable, List("ID", "DAY", "NEXT")) ==
            Map("1" -> List("2026-01-15", "2026-02-01"))
        }
        checkRecords(
          "Partitioned",
          rejected(partitionedTable, partitionedStart, partitionedFile),
          Map(
            "2" -> List("`DAY` is not a valid DATE with the pattern `yyyyMMdd`: 2026AB15"),
            "3" -> List("`D_IGN` is not a valid DATE with the pattern `yyyyMMdd`: 2026XX31")
          ),
          partitionedFile
        )

        // A table without such an attribute loads as before
        val plainFile = incoming / s"$plainTable-1.csv"
        plainFile.overwrite("1|2026-01-15\n")
        check("a plain table loads")(load(plainTable, plainFile))
        check("its row is loaded") {
          rows(plainTable, List("ID", "D")) == Map("1" -> List("2026-01-15"))
        }

        // The scripts of POSITION files are checked too
        val positionFile = incoming / s"$positionTable-1.txt"
        positionFile.overwrite("000120260115\n00022026AB15\n")
        val positionStart = System.currentTimeMillis()
        check("the POSITION load succeeds")(load(positionTable, positionFile))
        check("its valid line is loaded") {
          rows(positionTable, List("ID", "D")) == Map("0001" -> List("2026-01-15"))
        }
        checkRecords(
          "POSITION",
          rejected(positionTable, positionStart, positionFile),
          Map(
            "0002" -> List(
              "`D` is not computed: PARSE_DATE('%Y%m%d', SUBSTR(value, 5, 8)) fails: 2026AB15"
            )
          ),
          positionFile
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
