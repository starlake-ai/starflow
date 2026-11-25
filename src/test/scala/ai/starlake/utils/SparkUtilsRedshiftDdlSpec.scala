package ai.starlake.utils

import ai.starlake.TestHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

class SparkUtilsRedshiftDdlSpec extends TestHelper {

  private val redshiftUrl = "jdbc:redshift://example.redshift.amazonaws.com:5439/dev"

  private val schema = StructType(
    Seq(
      StructField("id", StringType),
      StructField("signup", TimestampType)
    )
  )

  new WithSettings() {
    "sqlSchemaString" should "create Redshift timestamps as TIMESTAMP, not TIMESTAMP WITH TIME ZONE" in {
      val ddl =
        SparkUtils.sqlSchemaString(
          schema,
          caseSensitive = false,
          redshiftUrl,
          "redshift",
          Map.empty,
          0
        )
      ddl should include("\"signup\" TIMESTAMP")
      ddl should not include "TIME ZONE"
    }

    it should "keep a per attribute ddl override on Redshift" in {
      val ddl = SparkUtils.sqlSchemaString(
        schema,
        caseSensitive = false,
        redshiftUrl,
        "redshift",
        Map("signup" -> Map("redshift" -> "TIMESTAMPTZ")),
        0
      )
      ddl should include("\"signup\" TIMESTAMPTZ")
    }

    it should "leave PostgreSQL timestamps to Spark's dialect" in {
      val ddl = SparkUtils.sqlSchemaString(
        schema,
        caseSensitive = false,
        "jdbc:postgresql://localhost:5432/db",
        "postgresql",
        Map.empty,
        0
      )
      ddl should include("TIMESTAMP WITH TIME ZONE")
    }
  }
}
