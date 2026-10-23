package ai.starlake.integration.transform

import ai.starlake.config.DatasetArea
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{Format, Metadata, WriteStrategy, WriteStrategyType}
import better.files.File
import com.google.cloud.bigquery._
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.{
  BlobId,
  BlobInfo,
  Storage,
  StorageOptions
}

import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A BigQuery transform whose sink is a set of Parquet, Avro or JSON files on Cloud Storage runs as
  * an EXPORT DATA statement (#1834). The Parquet export of a view, then the native load of its
  * files, relocates a table with the same values.
  */
class BigQueryExportTaskIntegrationSpec extends IntegrationTestBase {
  private val sourceDataset = "starlake_it_bq_export_source"
  private val targetDataset = "starlake_it_bq_export"
  private val items = "items"
  private val itemsView = "items_view"
  private val exportDomain = "exports"

  // EXPORT DATA writes to Cloud Storage: a bucket, or a bucket and a folder
  private lazy val (bucket, exportPrefix): (Option[String], String) = {
    val location = sys.env
      .get("TEMPORARY_GCS_BUCKET")
      .map(_.stripPrefix("gs://").stripSuffix("/"))
      .filter(_.nonEmpty)
    val folder = location.flatMap(_.split("/", 2).lift(1)).map(_ + "/").getOrElse("")
    (
      location.map(_.split("/", 2).head),
      s"${folder}starlake-it/bq-export-data/${UUID.randomUUID()}"
    )
  }
  private lazy val storage: Storage = StorageOptions.newBuilder().build().getService

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-export-data").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-export-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain and the transform tasks are written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    (dir / "metadata" / "transform" / exportDomain).createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  private def values(sql: String): List[String] =
    run(sql)
      .iterateAll()
      .asScala
      .toList
      .flatMap(row => row.asScala.map(value => Option(value.getValue).map(_.toString).orNull))

  /** @return
    *   the row count and fingerprint of the result of the query
    */
  private def fingerprint(query: String): List[String] =
    values(s"SELECT COUNT(*), BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM ($query) t")

  /** @return
    *   the names of the objects of the folder, without the folder
    */
  private def objects(folder: String): List[String] =
    storage
      .list(bucket.get, Storage.BlobListOption.prefix(folder))
      .iterateAll()
      .asScala
      .map(_.getName.stripPrefix(folder))
      .toList
      .sorted

  private def put(name: String): Unit =
    storage.create(BlobInfo.newBuilder(BlobId.of(bucket.get, name)).build(), "x".getBytes)

  /** Writes the task exporting the result of the SQL to files of this format and path */
  private def writeTask(
    name: String,
    sql: String,
    format: String,
    path: String,
    strategy: WriteStrategyType
  ): Unit = {
    val folder = project / "metadata" / "transform" / exportDomain
    (folder / s"$name.sql").overwrite(sql)
    (folder / s"$name.sl.yml").overwrite(
      s"""version: 1
         |task:
         |  name: $name
         |  table: $name
         |  connectionRef: "bigquery"
         |  sink:
         |    connectionRef: "gcs"
         |    format: "$format"
         |    path: "$path"
         |  writeStrategy:
         |    type: "$strategy"
         |""".stripMargin
    )
  }

  private def transform(name: String, args: String*): Boolean =
    Try(new Main().run(Array("transform", "--name", s"$exportDomain.$name") ++ args))
      .getOrElse(false)

  private def createSource(): Unit = {
    List(sourceDataset, targetDataset).foreach { dataset =>
      if (bigquery.getDataset(dataset) == null)
        bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
    }
    run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$items` (
           |  id INT64 NOT NULL,
           |  label STRING,
           |  amount NUMERIC,
           |  big BIGNUMERIC,
           |  day DATE,
           |  civil DATETIME,
           |  wall TIME,
           |  instant TIMESTAMP,
           |  payload BYTES,
           |  area GEOGRAPHY,
           |  doc JSON,
           |  duration INTERVAL,
           |  days RANGE<DATE>,
           |  tags ARRAY<STRING>,
           |  record STRUCT<rank INT64, note STRING>
           |)""".stripMargin)
    run(s"""INSERT INTO `$sourceDataset.$items` VALUES
           |(1, 'one', NUMERIC '-12345678901234567890123456789.123456789',
           | BIGNUMERIC '1234567890123456789012345678901234567.12345678901234567890123456789012345678',
           | DATE '2019-12-31', DATETIME '2019-12-31 23:59:59.999999', TIME '23:59:59.999999',
           | TIMESTAMP '2019-12-31 23:59:59.999999 UTC', b'\\x00\\xff\\x10', ST_GEOGPOINT(1, 2),
           | JSON '{"a": [1, "b"]}', INTERVAL '1-2 3 4:5:6.789' YEAR TO SECOND,
           | RANGE(DATE '2019-01-01', DATE '2019-12-31'), ['x', 'y'], STRUCT(1, 'first')),
           |(2, 'two', NUMERIC '2.5', NULL, DATE '2020-01-01', NULL, NULL, NULL, NULL, NULL,
           | JSON '"text"', NULL, RANGE(DATE '2020-01-01', NULL), ['z'], STRUCT(2, NULL)),
           |(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, [], NULL)
           |""".stripMargin)
    run(
      s"CREATE OR REPLACE VIEW `$sourceDataset.$itemsView` AS SELECT * FROM `$sourceDataset.$items`"
    )
  }

  private val start = System.currentTimeMillis()

  /** The audit log of the last run of the task in this test, when the audit table is in BigQuery.
    * The test writes the audit logs interactively, not in batch mode.
    */
  private def lastAudit(name: String): Option[List[String]] =
    Option(bigquery.getTable(TableId.of("audit", "audit"))).map { _ =>
      values(
        s"""SELECT CAST(success AS STRING), CAST(count AS STRING), message FROM `audit.audit`
           |WHERE domain = '$exportDomain' AND schema = '$name' AND step = 'TRANSFORM'
           |AND timestamp >= TIMESTAMP_MILLIS($start)
           |ORDER BY timestamp DESC LIMIT 1""".stripMargin
      )
    }

  if (
    sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean &&
    sys.env.get("TEMPORARY_GCS_BUCKET").exists(_.nonEmpty)
  ) {
    "A BigQuery transform writing files to Cloud Storage" should "export them natively" in {
      withEnvs(
        "SL_ROOT"                        -> project.pathAsString,
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        try {
          createSource()
          val runIdPattern = "\\d{17}_[0-9a-f]{8}"
          def runIds(files: List[String], prefix: String, suffix: String): List[String] =
            files.collect {
              case name if name.matches(s"$prefix$runIdPattern-\\d{12}$suffix") =>
                name.stripPrefix(prefix).take(26)
            }.distinct

          // OVERWRITE of a view to Parquet, in the domain folder, twice: the files of the previous
          // exports are deleted once the export succeeded, the other files are kept
          val domainFolder = s"$exportPrefix/$exportDomain/"
          val olderExport = "items_parquet-20200101000000000_0123abcd-000000000099.parquet"
          val keptFiles = List("keep.txt", "202610221200.parquet", "items_parquet.ack")
          (olderExport :: keptFiles).foreach(name => put(domainFolder + name))
          writeTask(
            "items_parquet",
            s"SELECT * FROM `$sourceDataset.$itemsView`",
            "parquet",
            s"gs://${bucket.get}/$exportPrefix",
            WriteStrategyType.OVERWRITE
          )
          transform("items_parquet") shouldBe true
          val firstRun = runIds(objects(domainFolder), "items_parquet-", "\\.parquet")
          firstRun should have size 1
          transform("items_parquet") shouldBe true
          val parquetFiles = objects(domainFolder)
          val lastRun = runIds(parquetFiles, "items_parquet-", "\\.parquet")
          lastRun should have size 1
          lastRun should not be firstRun
          parquetFiles should contain allElementsOf keptFiles
          parquetFiles should not contain olderExport
          lastAudit("items_parquet").foreach { audit =>
            audit.take(2) shouldBe List("true", "3")
            audit(2) should include(
              s"gs://${bucket.get}/${domainFolder}items_parquet-${lastRun.head}-*.parquet"
            )
          }

          // The native load of these files, with the domain that extract-bq-schema writes,
          // relocates the table
          val extracted = new ExtractBigQuerySchema(
            TablesExtractConfig(
              connectionRef = Some("bigquery"),
              tables = Map(sourceDataset -> List(items)),
              tableTypes = List("TABLE")
            )
          ).extractSchemasAndTables(settings.schemaHandler(), Map(sourceDataset -> List(items)))
            .head
          extracted
            .copy(
              name = targetDataset,
              database = None,
              metadata = Some(
                Metadata(
                  format = Some(Format.PARQUET),
                  directory = Some(s"{{root_path}}/incoming/$targetDataset"),
                  writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
                )
              ),
              tables = extracted.tables.map(table =>
                table.copy(pattern = Pattern.compile("items_parquet-.*\\.parquet"))
              )
            )
            .writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
          new Main().run(
            Array(
              "load",
              "--domains",
              targetDataset,
              "--tables",
              items,
              "--files",
              s"gs://${bucket.get}/${domainFolder}items_parquet-*.parquet",
              "--inPlace"
            )
          ) shouldBe true
          fingerprint(s"SELECT * FROM `$targetDataset.$items`") shouldBe
          fingerprint(s"SELECT * FROM `$sourceDataset.$items`")
          // A SQL NULL stays a SQL NULL, not a JSON null
          values(s"SELECT COUNTIF(doc IS NULL) FROM `$targetDataset.$items`") shouldBe List("1")

          // APPEND of a query to Avro, where the path says: each run writes files of its own,
          // except the runs of the same scheduled date, whose last one replaces the others. The
          // RANGE values are exported as text.
          val avroFolder = s"$exportPrefix/avro/"
          writeTask(
            "items_avro",
            s"SELECT id, label, days FROM `$sourceDataset.$items` WHERE id <= 2",
            "avro",
            s"gs://${bucket.get}/${avroFolder}items_*.avro",
            WriteStrategyType.APPEND
          )
          transform("items_avro") shouldBe true
          transform("items_avro") shouldBe true
          transform("items_avro", "--scheduledDate", "2026-01-01T00:00:00") shouldBe true
          transform("items_avro", "--scheduledDate", "2026-01-01T00:00:00") shouldBe true
          val avroRuns = runIds(objects(avroFolder), "items_", "\\.avro")
          avroRuns should have size 3
          avroRuns.count(_.startsWith("20260101000000000_")) shouldBe 1
          val avroTable = TableId.of(targetDataset, "items_avro")
          val avroLoad = bigquery
            .create(
              JobInfo.of(
                LoadJobConfiguration
                  .newBuilder(
                    avroTable,
                    s"gs://${bucket.get}/${avroFolder}items_*.avro",
                    FormatOptions.avro()
                  )
                  .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                  .setUseAvroLogicalTypes(true)
                  .build()
              )
            )
            .waitFor()
          Option(avroLoad.getStatus.getError) shouldBe None
          values(
            s"SELECT COUNT(*), COUNTIF(days = '[2019-01-01, 2019-12-31)') FROM `$targetDataset.items_avro`"
          ) shouldBe List("6", "3")

          // A dry run writes nothing
          def jsonFiles(): List[String] =
            objects(domainFolder).filter(_.startsWith("items_json-"))
          writeTask(
            "items_json",
            s"SELECT id, label, amount, day, instant, tags, record FROM `$sourceDataset.$itemsView`",
            "json",
            s"gs://${bucket.get}/$exportPrefix",
            WriteStrategyType.OVERWRITE
          )
          transform("items_json", "--dry-run") shouldBe true
          jsonFiles() shouldBe empty
          transform("items_json") shouldBe true
          runIds(jsonFiles(), "items_json-", "\\.json") should have size 1
          // Probe: the documentation does not say whether JSON exports write RANGE values
          writeTask(
            "ranges_json",
            s"SELECT id, days FROM `$sourceDataset.$items`",
            "json",
            s"gs://${bucket.get}/$exportPrefix",
            WriteStrategyType.OVERWRITE
          )
          info(s"EXPORT DATA writes RANGE values to JSON files: ${transform("ranges_json")}")
          // Probe: nor whether they write the other types of the source
          writeTask(
            "all_types_json",
            s"SELECT * EXCEPT (days) FROM `$sourceDataset.$items`",
            "json",
            s"gs://${bucket.get}/$exportPrefix",
            WriteStrategyType.OVERWRITE
          )
          info(
            s"EXPORT DATA writes BIGNUMERIC, DATETIME, TIME, BYTES, GEOGRAPHY, JSON and INTERVAL values to JSON files: ${transform("all_types_json")}"
          )

          // The error of BigQuery fails the task
          writeTask(
            "items_missing",
            "SELECT 1 AS id",
            "parquet",
            s"gs://starlake-it-missing-${UUID.randomUUID()}/out",
            WriteStrategyType.OVERWRITE
          )
          transform("items_missing") shouldBe false
          lastAudit("items_missing").foreach(_.headOption shouldBe Some("false"))
        } finally {
          Try(
            storage
              .list(bucket.get, Storage.BlobListOption.prefix(s"$exportPrefix/"))
              .iterateAll()
              .asScala
              .foreach(blob => storage.delete(blob.getBlobId))
          )
        }
      }
    }
  }
}
