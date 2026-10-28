package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.schema.model.{WriteStrategy, WriteStrategyType}
import com.google.cloud.bigquery.{Field, LegacySQLTypeName, Schema => BQSchema}

import scala.jdk.CollectionConverters._

/** The schema sync of a BigQuery SCD2 task uses the validity columns of its strategy (#1878) */
class BigQuerySCD2ColumnsSpec extends TestHelper {
  new WithSettings() {
    private def schema(columns: String*): BQSchema =
      BQSchema.of(columns.map(Field.of(_, LegacySQLTypeName.STRING)).asJava)

    private def columns(schema: BQSchema): List[(String, String, String)] =
      schema.getFields.asScala.toList.map { field =>
        (
          field.getName,
          field.getType.toString,
          Option(field.getMode).map(_.toString).getOrElse("")
        )
      }

    private val scd2 = WriteStrategy(
      `type` = Some(WriteStrategyType.SCD2),
      key = List("id"),
      timestamp = Some("updated_at"),
      startTs = Some("valid_from"),
      endTs = Some("valid_to")
    )

    "The schema of a BigQuery SCD2 table" should "keep the validity columns of the strategy" in {
      val incoming = schema("id", "updated_at", "valid_from", "valid_to")
      BigQueryAutoTask.withSCD2Columns(incoming, scd2) shouldBe incoming
      // BigQuery column names are case insensitive
      val upper = schema("id", "updated_at", "VALID_FROM", "Valid_To")
      BigQueryAutoTask.withSCD2Columns(upper, scd2) shouldBe upper
    }

    it should "add each validity column of the strategy that it lacks, never the global ones" in {
      columns(BigQueryAutoTask.withSCD2Columns(schema("id", "updated_at"), scd2)) shouldBe List(
        ("id", "STRING", ""),
        ("updated_at", "STRING", ""),
        ("valid_from", "TIMESTAMP", "NULLABLE"),
        ("valid_to", "TIMESTAMP", "NULLABLE")
      )
      columns(
        BigQueryAutoTask.withSCD2Columns(schema("id", "updated_at", "valid_from"), scd2)
      ).map(_._1) shouldBe List("id", "updated_at", "valid_from", "valid_to")
    }

    it should "add the global validity columns when the strategy does not name its own" in {
      val unnamed = scd2.copy(startTs = None, endTs = None)
      columns(BigQueryAutoTask.withSCD2Columns(schema("id"), unnamed)).map(_._1) shouldBe List(
        "id",
        settings.appConfig.scd2StartTimestamp,
        settings.appConfig.scd2EndTimestamp
      )
    }

    it should "keep the global validity columns that the table has from earlier runs" in {
      // A schema update cannot drop a column
      val existing = BQSchema.of(
        (schema("id", "updated_at", "valid_from", "valid_to").getFields.asScala.toList ++ List(
          Field
            .newBuilder(settings.appConfig.scd2StartTimestamp, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build(),
          Field
            .newBuilder(settings.appConfig.scd2EndTimestamp, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build()
        )).asJava
      )
      columns(
        BigQueryAutoTask.withSCD2Columns(
          schema("id", "updated_at", "valid_from", "valid_to"),
          scd2,
          Some(existing)
        )
      ) shouldBe columns(existing)
      // Not the other columns of the table
      BigQueryAutoTask
        .withSCD2Columns(schema("id", "updated_at"), scd2, Some(schema("id", "other")))
        .getFields
        .asScala
        .map(_.getName) shouldBe List("id", "updated_at", "valid_from", "valid_to")
    }

    it should "leave the schema of the other strategies" in {
      val incoming = schema("id")
      BigQueryAutoTask.withSCD2Columns(
        incoming,
        WriteStrategy(`type` = Some(WriteStrategyType.UPSERT_BY_KEY), key = List("id"))
      ) shouldBe incoming
    }
  }
}
