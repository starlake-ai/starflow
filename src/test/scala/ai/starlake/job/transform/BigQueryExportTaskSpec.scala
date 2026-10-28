package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.schema.model.{
  AllSinks,
  AutoTaskInfo,
  BigQuerySink,
  FsSink,
  WriteStrategy,
  WriteStrategyType
}
import com.google.cloud.bigquery.{Field, FieldElementType, Schema, StandardSQLTypeName}

import scala.util.Failure

/** BigQuery transforms writing their files with EXPORT DATA (#1834). No BigQuery access. */
class BigQueryExportTaskSpec extends TestHelper {
  import BigQueryExportTask.*

  private val runId = "20261022101500123_0123abcd"

  "exportTarget" should "write the files of a folder path to its domain folder, named after the table" in {
    val target = exportTarget("gs://bucket/incoming", "sales", "orders", ".parquet")
    target shouldBe ExportTarget("gs://bucket/incoming/sales/", "orders-", ".parquet")
    target.uri(runId) shouldBe s"gs://bucket/incoming/sales/orders-$runId-*.parquet"
    target.bucketAndFolder shouldBe ("bucket", "incoming/sales/")
    exportTarget("gs://bucket/", "sales", "orders", ".avro").uri(runId) shouldBe
    s"gs://bucket/sales/orders-$runId-*.avro"
    exportTarget("gs://bucket", "sales", "orders", ".json").bucketAndFolder shouldBe
    ("bucket", "sales/")
  }

  it should "write the files of a path with a wildcard where it says" in {
    val target = exportTarget("gs://bucket/out/orders_*.snappy.parquet", "sales", "orders", ".x")
    target shouldBe ExportTarget("gs://bucket/out/", "orders_", ".snappy.parquet")
    target.uri(runId) shouldBe s"gs://bucket/out/orders_$runId-*.snappy.parquet"
    target.bucketAndFolder shouldBe ("bucket", "out/")
    exportTarget("gs://bucket/*", "sales", "orders", ".x").bucketAndFolder shouldBe ("bucket", "")
  }

  it should "refuse the paths that EXPORT DATA or the cleanup of the files cannot use" in {
    List(
      "gs://bucket/out/a-*-*.parquet",
      "gs://bucket/*/a.parquet",
      "gs://*.parquet",
      "s3://bucket/out"
    ).foreach { path =>
      withClue(path) {
        an[IllegalArgumentException] should be thrownBy exportTarget(path, "d", "t", ".parquet")
      }
    }
  }

  "runIdOf" should "recognise the files that the exports to the target wrote, and their run" in {
    val target = exportTarget("gs://bucket/incoming", "sales", "orders", ".parquet")
    target.runIdOf(s"orders-$runId-000000000000.parquet") shouldBe Some(runId)
    target.runIdOf(s"orders-$runId-1000000000000.parquet") shouldBe Some(runId)
    List(
      "orders-000000000000.parquet",
      "202610221200.parquet",
      s"orders_items-$runId-000000000000.parquet",
      s"orders-items-$runId-000000000000.parquet",
      s"orders-$runId-00000000000.parquet",
      s"orders-$runId-000000000000.parquet.bak",
      s"orders-$runId-000000000000.json",
      "orders.ack",
      "keep.txt"
    ).foreach(name => withClue(name) { target.runIdOf(name) shouldBe None })
  }

  "newRunId" should "name a run after its scheduled date, or its start time" in {
    newRunId(None) should fullyMatch regex "\\d{17}_[0-9a-f]{8}"
    newRunId(Some("2026-10-22 10:15:00.123+0000")) should fullyMatch regex
    "20261022101500123_[0-9a-f]{8}"
    newRunId(Some("2026-10-22")) should fullyMatch regex "20261022000000000_[0-9a-f]{8}"
    newRunId(Some("2026-10-22")) should not be newRunId(Some("2026-10-22"))
  }

  "replacesFile" should "replace the files of all the previous runs of an OVERWRITE, of the same scheduled date of an APPEND" in {
    val target = exportTarget("gs://bucket/incoming", "sales", "orders", ".parquet")
    val sameRun = s"orders-$runId-000000000001.parquet"
    val sameDate = "orders-20261022101500123_99999999-000000000000.parquet"
    val otherDate = "orders-20261021000000000_99999999-000000000000.parquet"
    def replaced(strategy: WriteStrategyType, scheduled: Boolean) =
      List(sameRun, sameDate, otherDate, "orders-000000000000.parquet", "keep.txt")
        .filter(replacesFile(target, runId, strategy, scheduled, _))
    replaced(WriteStrategyType.OVERWRITE, scheduled = false) shouldBe List(sameDate, otherDate)
    replaced(WriteStrategyType.OVERWRITE, scheduled = true) shouldBe List(sameDate, otherDate)
    replaced(WriteStrategyType.APPEND, scheduled = true) shouldBe List(sameDate)
    replaced(WriteStrategyType.APPEND, scheduled = false) shouldBe Nil
  }

  "exportOptions" should "keep the EXPORT DATA options of the format, and compress Parquet and Avro" in {
    exportOptions("PARQUET", Map.empty) shouldBe (List("compression" -> "'SNAPPY'"), Nil)
    exportOptions("AVRO", Map.empty) shouldBe (List("compression" -> "'SNAPPY'"), Nil)
    exportOptions("JSON", Map.empty) shouldBe (Nil, Nil)
    exportOptions("JSON", Map("Compression" -> "gzip")) shouldBe
    (List("compression" -> "'GZIP'"), Nil)
    exportOptions("PARQUET", Map("compression" -> "none")) shouldBe (Nil, Nil)
    exportOptions("PARQUET", Map("compression" -> " ")) shouldBe (Nil, Nil)
    exportOptions(
      "AVRO",
      Map("use_avro_logical_types" -> "TRUE", "header" -> "true", "partitionOverwriteMode" -> "x")
    ) shouldBe (
      List("compression" -> "'SNAPPY'", "use_avro_logical_types" -> "true"),
      List("header", "partitionOverwriteMode")
    )
    exportOptions("PARQUET", Map("use_avro_logical_types" -> "true"))._2 shouldBe
    List("use_avro_logical_types")
    an[IllegalArgumentException] should be thrownBy
    exportOptions("AVRO", Map("use_avro_logical_types" -> "yes"))
  }

  "defaultExtension" should "follow the format and the compression of JSON" in {
    defaultExtension("PARQUET", Nil) shouldBe ".parquet"
    defaultExtension("AVRO", Nil) shouldBe ".avro"
    defaultExtension("JSON", Nil) shouldBe ".json"
    defaultExtension("JSON", exportOptions("JSON", Map("compression" -> "gzip"))._1) shouldBe
    ".json.gz"
  }

  "exportDataSql" should "export the select to the URI, overwriting the files of the same names" in {
    exportDataSql(
      "gs://bucket/it's/*.parquet",
      "PARQUET",
      List("compression" -> "'SNAPPY'"),
      "SELECT 1 AS id"
    ) shouldBe
    """EXPORT DATA OPTIONS (uri = 'gs://bucket/it\'s/*.parquet', format = 'PARQUET', overwrite = true, compression = 'SNAPPY') AS
      |SELECT 1 AS id""".stripMargin
  }

  "textColumns" should "export as text the top level columns of the types that the format refuses" in {
    val schema = Schema.of(
      Field.of("id", StandardSQLTypeName.INT64),
      Field.of("doc", StandardSQLTypeName.JSON),
      Field.of("duration", StandardSQLTypeName.INTERVAL),
      Field
        .newBuilder("days", StandardSQLTypeName.RANGE)
        .setRangeElementType(FieldElementType.newBuilder().setType("DATE").build())
        .build(),
      Field.newBuilder("docs", StandardSQLTypeName.JSON).setMode(Field.Mode.REPEATED).build(),
      Field.of("record", StandardSQLTypeName.STRUCT, Field.of("doc", StandardSQLTypeName.JSON))
    )
    textColumns("PARQUET", schema) shouldBe List(
      "doc"      -> "IF(`doc` IS NULL, NULL, TO_JSON_STRING(`doc`))",
      "duration" -> "CAST(`duration` AS STRING)",
      "days"     -> "CAST(`days` AS STRING)"
    )
    textColumns("AVRO", schema) shouldBe List("days" -> "CAST(`days` AS STRING)")
    textColumns("JSON", schema) shouldBe List("days" -> "CAST(`days` AS STRING)")
    selectWithTextColumns("SELECT * FROM t", textColumns("AVRO", schema)) shouldBe
    "SELECT * REPLACE (CAST(`days` AS STRING) AS `days`) FROM (\nSELECT * FROM t\n)"
    selectWithTextColumns("SELECT * FROM t", Nil) shouldBe "SELECT * FROM t"
  }

  new WithSettings() {
    implicit val schemaHandler: ai.starlake.schema.handlers.SchemaHandler =
      settings.schemaHandler()
    implicit val storage: ai.starlake.schema.handlers.StorageHandler = settings.storageHandler()

    // The BQ connection runs on BigQuery, the spark connection writes files
    private val exportTask = AutoTaskInfo(
      name = "orders_export",
      sql = Some("SELECT 1 AS id"),
      database = None,
      domain = "sales",
      table = "orders",
      connectionRef = Some("BQ"),
      sink = Some(
        AllSinks(
          connectionRef = Some("spark"),
          format = Some("parquet"),
          path = Some("gs://bucket/{{folder}}")
        )
      ),
      writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
    )

    private val upsert =
      Some(WriteStrategy(`type` = Some(WriteStrategyType.UPSERT_BY_KEY), key = List("id")))

    private def task(taskDesc: AutoTaskInfo): AutoTask =
      TransformContext(
        appId = None,
        taskDesc = taskDesc,
        commandParameters = Map("folder" -> "exports"),
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        resultPageSize = 1,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = None,
        syncSchema = false
      ).toTask(taskDesc.getRunEngine())

    private def withSink(sink: AllSinks => AllSinks): AutoTaskInfo =
      exportTask.copy(sink = exportTask.sink.map(sink))

    "exportFormat" should "name the EXPORT DATA formats of the file sinks" in {
      exportFormat(FsSink(format = Some("Parquet"))) shouldBe Some("PARQUET")
      exportFormat(FsSink(format = Some("avro"))) shouldBe Some("AVRO")
      exportFormat(FsSink(format = Some("json"))) shouldBe Some("JSON")
      List("csv", "xls", "delta", "iceberg").foreach { format =>
        exportFormat(FsSink(format = Some(format))) shouldBe None
      }
      // The default write format of the tests is delta
      exportFormat(FsSink()) shouldBe None
    }

    "nativeExportPath" should "render the Cloud Storage path of the file sinks of an exported format" in {
      val sink = FsSink(format = Some("parquet"), path = Some("gs://bucket/{{folder}}"))
      nativeExportPath(sink, Map("folder" -> "out")) shouldBe Some("gs://bucket/out")
      nativeExportPath(sink.copy(path = Some("{{root}}/out")), Map("root" -> "gs://b")) shouldBe
      Some("gs://b/out")
      nativeExportPath(sink.copy(path = Some("/tmp/out")), Map.empty) shouldBe None
      nativeExportPath(sink.copy(path = None), Map.empty) shouldBe None
      nativeExportPath(sink.copy(format = Some("csv")), Map("folder" -> "out")) shouldBe None
      nativeExportPath(BigQuerySink(), Map.empty) shouldBe None
    }

    "toTask" should "export natively the BigQuery transforms writing files to Cloud Storage" in {
      task(exportTask) shouldBe a[BigQueryExportTask]
      task(withSink(_.copy(format = Some("avro")))) shouldBe a[BigQueryExportTask]
    }

    it should "name its BigQuery jobs after the task, which has no table" in {
      val job = new ai.starlake.job.sink.bigquery.BigQueryNativeJob(
        task(exportTask).asInstanceOf[BigQueryExportTask].bqConfig,
        ""
      )
      job.name should include("sales.orders")
    }

    it should "keep the other transforms on their task" in {
      task(withSink(_.copy(format = Some("csv")))) shouldBe a[SparkExportTask]
      task(withSink(_.copy(path = Some("/tmp/exports")))) shouldBe a[SparkAutoTask]
      task(withSink(_.copy(path = None))) shouldBe a[SparkAutoTask]
      task(exportTask.copy(python = Some(new org.apache.hadoop.fs.Path("/tmp/job.py")))) shouldBe
      a[SparkAutoTask]
    }

    it should "refuse the write strategies and the partitions that EXPORT DATA cannot apply" in {
      val upserted = task(exportTask.copy(writeStrategy = upsert)).run()
      upserted shouldBe a[Failure[_]]
      upserted.failed.get.getMessage should include("must be APPEND or OVERWRITE")

      val partitioned = task(withSink(_.copy(partition = Some(Seq("day"))))).run()
      partitioned shouldBe a[Failure[_]]
      partitioned.failed.get.getMessage should include("cannot partition its files by day")

      an[IllegalArgumentException] should be thrownBy
      task(exportTask.copy(writeStrategy = upsert)).buildAllSQLQueries(None)
    }

    "checkValidity" should "report the write strategies and the partitions that EXPORT DATA cannot apply" in {
      exportTask.checkValidity() shouldBe Right(true)
      exportTask
        .copy(writeStrategy = upsert)
        .checkValidity()
        .swap
        .getOrElse(Nil)
        .map(_.message) shouldBe
      List(
        "The BigQuery export of orders_export writes files: its write strategy must be APPEND or OVERWRITE, not UPSERT_BY_KEY"
      )
      withSink(_.copy(partition = Some(Seq("day")))).checkValidity().isLeft shouldBe true
      // A file sink that BigQuery does not export keeps its strategy
      withSink(_.copy(format = Some("delta"))).copy(writeStrategy = upsert).checkValidity() shouldBe
      Right(true)
    }

    "The csv and xls exports" should "keep the file of their own path options" in {
      FsSink(path = Some("gs://bucket/out")).finalPath shouldBe None
      FsSink(
        path = Some("gs://bucket/out"),
        options = Some(Map("csv:path" -> "out.csv"))
      ).finalPath shouldBe
      Some("out.csv")
    }
  }
}
