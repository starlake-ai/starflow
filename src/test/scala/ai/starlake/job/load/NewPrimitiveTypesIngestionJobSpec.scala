package ai.starlake.job.load

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.workflow.IngestionWorkflow
import ai.starlake.job.ingest.LoadConfig
import better.files.File
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.time.{Instant, LocalDateTime}

/** The datetime, time and binary primitive types loaded with Spark */
class NewPrimitiveTypesIngestionJobSpec extends TestHelper {

  private def bytes(row: Row, index: Int): Option[List[Byte]] =
    Option(row.getAs[Array[Byte]](index)).map(_.toList)

  private def dateTimes(rows: List[Row], index: Int): List[Option[LocalDateTime]] =
    rows.map(row => Option(row.getAs[LocalDateTime](index)))

  private val hello = "Hello".getBytes("UTF-8").toList

  new WithSettings() {

    /** A project without the datetime and time types of the test project */
    private def deliverNewTypesProject(): Unit = {
      cleanMetadata
      withSettings.deliverTestFile(
        "/sample/new-types/types.sl.yml",
        new Path(DatasetArea.types, "types.sl.yml")
      )
      settings.schemaHandler(reload = true)
    }

    "A DSV file" should "load datetime, time and binary values parsed with their pattern" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/new-types/_config.sl.yml",
        datasetDomainName = "newtypes",
        sourceDatasetPathName = "/sample/new-types/events-1.dsv"
      ) {
        try {
          deliverNewTypesProject()
          deliverSourceDomain()
          deliverSourceTable("/sample/new-types/events.sl.yml")
          loadPending.isSuccess shouldBe true

          val events = sparkSession.table("newtypes.events")
          events.schema.map(field => field.name -> field.dataType).toList shouldBe List(
            "id"        -> LongType,
            "created"   -> TimestampNTZType,
            "civil"     -> TimestampNTZType,
            "civil_iso" -> TimestampNTZType,
            "since"     -> TimestampNTZType,
            "wall"      -> StringType,
            "payload"   -> BinaryType,
            "hex"       -> BinaryType,
            "fallback"  -> BinaryType
          )
          // Each other row has a value that does not match its type, or no required value
          val rows = events.orderBy("id").collect().toList
          rows.map(_.getLong(0)) shouldBe List(1L, 2L)
          // SQL_DATE_TIME reads a space or a T, and the fraction of the seconds
          dateTimes(rows, 1) shouldBe List(
            Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2)),
            Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2))
          )
          dateTimes(rows, 2) shouldBe List(
            Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2, 500000000)),
            None
          )
          dateTimes(rows, 3) shouldBe List(
            Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2, 123456000)),
            None
          )
          // The default of an empty value is read with the pattern, like a value
          dateTimes(rows, 4) shouldBe List(
            Some(LocalDateTime.of(2000, 1, 1, 0, 0)),
            Some(LocalDateTime.of(2020, 1, 1, 10, 0))
          )
          // The canonical text of a time, without the digits beyond the microsecond
          rows.map(row => Option(row.getString(5))) shouldBe List(
            Some("23:59:02.123456"),
            Some("12:00:00")
          )
          rows.map(bytes(_, 6)) shouldBe List(Some(hello), None)
          rows.map(bytes(_, 7)) shouldBe List(Some(hello), None)
          rows.map(bytes(_, 8)) shouldBe List(Some(List[Byte](0, 1)), Some(List[Byte](0, -1)))
        } finally {
          sparkSession.sql("DROP TABLE IF EXISTS newtypes.events")
        }
      }
    }

    "A JSON file" should "load datetime, time and binary values, and reject a number for a datetime" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/new-types/_config.sl.yml",
        datasetDomainName = "newtypes",
        sourceDatasetPathName = "/sample/new-types/events_json-1.json"
      ) {
        try {
          deliverNewTypesProject()
          deliverSourceDomain()
          deliverSourceTable("/sample/new-types/events_json.sl.yml")
          loadPending.isSuccess shouldBe true

          // stamp is read as a number: its row is rejected, the load does not fail
          val rows = sparkSession.table("newtypes.events_json").collect().toList
          rows.map(_.getLong(0)) shouldBe List(1L)
          dateTimes(rows, 1) shouldBe List(
            Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2, 123456000))
          )
          dateTimes(rows, 2) shouldBe List(None)
          rows.map(_.getString(3)) shouldBe List("10:15:00")
          rows.map(bytes(_, 4)) shouldBe List(Some(hello))
        } finally {
          sparkSession.sql("DROP TABLE IF EXISTS newtypes.events_json")
        }
      }
    }

    "A Spark load" should "fail on a decimal wider than the precision of Spark, instead of losing digits" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/new-types/_config.sl.yml",
        datasetDomainName = "newtypes",
        sourceDatasetPathName = "/sample/new-types/wide-1.dsv"
      ) {
        try {
          deliverNewTypesProject()
          deliverSourceDomain()
          deliverSourceTable("/sample/new-types/wide.sl.yml")
          val result = loadPending
          result.isFailure shouldBe true
          result.failed.get.getMessage should include(
            "cannot be loaded with Spark, whose decimals have a precision of 38 at most: the precision of amount is above"
          )
          sparkSession.catalog.tableExists("newtypes.wide") shouldBe false
        } finally {
          sparkSession.sql("DROP TABLE IF EXISTS newtypes.wide")
        }
      }
    }

    "A Parquet file" should "load its TIMESTAMP_NTZ, timestamp and binary columns as datetime and binary" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/new-types/_config.sl.yml",
        datasetDomainName = "newtypes",
        // The Parquet file is written by the test
        sourceDatasetPathName = ""
      ) {
        val timeZone = sparkSession.conf.get("spark.sql.session.timeZone")
        try {
          deliverNewTypesProject()
          deliverSourceDomain()
          deliverSourceTable("/sample/new-types/typed.sl.yml")
          val schemaHandler = settings.schemaHandler(reload = true)
          // A timestamp is a datetime of the session time zone
          sparkSession.conf.set("spark.sql.session.timeZone", "UTC")

          val civil = LocalDateTime.of(2019, 12, 31, 23, 59, 2, 123456000)
          val payload = Array[Byte](0, 1, -1)
          val written = File.newTemporaryDirectory("typed-parquet-")
          try {
            sparkSession
              .createDataFrame(
                sparkSession.sparkContext.parallelize(
                  Seq(
                    Row(
                      1L,
                      civil,
                      java.sql.Timestamp.from(Instant.parse("2019-12-31T23:59:02Z")),
                      payload,
                      payload
                    ),
                    Row(2L, null, null, null, null)
                  )
                ),
                StructType(
                  List(
                    StructField("id", LongType, nullable = false),
                    StructField("civil", TimestampNTZType),
                    StructField("instant", TimestampType),
                    StructField("payload", BinaryType),
                    StructField("fallback", BinaryType)
                  )
                )
              )
              .coalesce(1)
              .write
              .parquet((written / "data").pathAsString)
            val parquetFile = (written / "data").list.find(_.name.endsWith(".parquet")).get
            val stagePath = new Path(DatasetArea.stage("newtypes"), "typed-1.parquet")
            storageHandler.mkdirs(stagePath.getParent)
            storageHandler.copyFromLocal(new Path(parquetFile.pathAsString), stagePath)
          } finally {
            written.delete(swallowIOExceptions = true)
          }

          DatasetArea.initMetadata(storageHandler)
          DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))
          new IngestionWorkflow(storageHandler, schemaHandler)
            .load(LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None))
            .isSuccess shouldBe true

          val typed = sparkSession.table("newtypes.typed")
          typed.schema.map(field => field.name -> field.dataType).toList shouldBe List(
            "id"       -> LongType,
            "civil"    -> TimestampNTZType,
            "instant"  -> TimestampNTZType,
            "payload"  -> BinaryType,
            "fallback" -> BinaryType
          )
          val rows = typed.orderBy("id").collect().toList
          dateTimes(rows, 1) shouldBe List(Some(civil), None)
          dateTimes(rows, 2) shouldBe List(Some(LocalDateTime.of(2019, 12, 31, 23, 59, 2)), None)
          rows.map(bytes(_, 3)) shouldBe List(Some(payload.toList), None)
          // The default of a binary column is decoded from its text
          rows.map(bytes(_, 4)) shouldBe List(Some(payload.toList), Some(List[Byte](0, 1)))
        } finally {
          sparkSession.conf.set("spark.sql.session.timeZone", timeZone)
          sparkSession.sql("DROP TABLE IF EXISTS newtypes.typed")
        }
      }
    }
  }
}
