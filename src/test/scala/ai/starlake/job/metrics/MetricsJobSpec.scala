package ai.starlake.job.metrics

import ai.starlake.{JdbcChecks, TestHelper}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MetricsJobSpec extends TestHelper with JdbcChecks {
  val expectedContinuousMetricsSchema: StructType = StructType(
    Array(
      StructField("attribute", StringType, nullable = true),
      StructField("min", DoubleType, nullable = true),
      StructField("max", DoubleType, nullable = true),
      StructField("mean", DoubleType, nullable = true),
      StructField("missingValues", LongType, nullable = true),
      StructField("variance", DoubleType, nullable = true),
      StructField("standardDev", DoubleType, nullable = true),
      StructField("sum", DoubleType, nullable = true),
      StructField("skewness", DoubleType, nullable = true),
      StructField("kurtosis", DoubleType, nullable = true),
      StructField("percentile25", DoubleType, nullable = true),
      StructField("median", DoubleType, nullable = true),
      StructField("percentile75", DoubleType, nullable = true),
      StructField("cometMetric", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    )
  )

  val expectedFreqMetricsSchema: StructType = StructType(
    Array(
      StructField("attribute", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("frequency", DoubleType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    )
  )
  val expectedDiscreteMetricsSchema: StructType = StructType(
    Array(
      StructField(
        "attribute",
        StringType,
        nullable = true
      ),
      StructField("countDistinct", LongType, nullable = true),
      StructField(
        "missingValuesDiscrete",
        LongType,
        nullable = true
      ),
      StructField("cometMetric", StringType, nullable = true),
      StructField("jobId", StringType, nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("schema", StringType, nullable = true),
      StructField("count", LongType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    )
  )

  val expectedDiscreteMetricsJDBCSchema: StructType = StructType(
    expectedDiscreteMetricsSchema.fields.map(field =>
      field.copy(metadata = Metadata.fromJson("""{"scale":0}"""))
    )
  )

  val sparkConfiguration: Config = {
    val config = ConfigFactory
      .parseString("""
          |connectionRef = "spark"
          |audit.sink.connectionRef = "spark"
          |""".stripMargin)
    val result = config.withFallback(super.testConfiguration)
    result
  }

  new WithSettings(sparkConfiguration) {
    "Yelp Business Metrics" should "produce correct metrics in  file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/yelp/yelp.sl.yml",
        datasetDomainName = "yelp",
        sourceDatasetPathName = "/sample/yelp/business.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable("/sample/yelp/business.sl.yml")
        loadPending

        val discreteMetricsDf: DataFrame = sparkSession.table("audit.discrete")
        logger.info(discreteMetricsDf.showString(truncate = 0))
        val discreteMetricsFields =
          discreteMetricsDf.schema.fields.map(f => (f.name.toLowerCase(), f.dataType.typeName))
        val expectedDiscreteMetricsFields =
          expectedDiscreteMetricsSchema.fields.map(f => (f.name.toLowerCase(), f.dataType.typeName))

        discreteMetricsFields shouldBe expectedDiscreteMetricsFields

        val session: SparkSession = sparkSession

        import session.implicits._

        val discreteMetricsSelectedColumns: Array[(String, String, String)] =
          discreteMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .distinct()
            .collect()
        discreteMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city"),
          ("yelp", "business", "is_open"),
          ("yelp", "business", "postal_code"),
          ("yelp", "business", "state"),
          ("yelp", "business", "is_open")
        )

        val continuousMetricsDf: DataFrame = sparkSession.table("audit.continuous")
        logger.info(continuousMetricsDf.showString(truncate = 0))

        val continuousMetricsFields =
          continuousMetricsDf.schema.fields.map(f => (f.name.toLowerCase(), f.dataType.typeName))
        val expectedContinuousMetricsFields =
          expectedContinuousMetricsSchema.fields.map(f =>
            (f.name.toLowerCase(), f.dataType.typeName)
          )
        continuousMetricsFields shouldBe expectedContinuousMetricsFields

        val continuousMetricsSelectedColumns: Array[(String, String, String)] =
          continuousMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .distinct()
            .collect()

        continuousMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "review_count"),
          ("yelp", "business", "stars")
        )

        val freqMetricsDf: DataFrame = sparkSession.table("audit.frequencies")
        logger.info(freqMetricsDf.showString(truncate = 0))

        val freqMetricsFields =
          freqMetricsDf.schema.fields.map(f => (f.name.toLowerCase(), f.dataType.typeName))
        val expectedFreqMetricsFields =
          expectedFreqMetricsSchema.fields.map(f => (f.name.toLowerCase(), f.dataType.typeName))
        freqMetricsFields shouldBe expectedFreqMetricsFields

        val freqMetricsSelectedColumns: Array[(String, String, String)] =
          freqMetricsDf
            .select("domain", "schema", "attribute")
            .map(r => (r.getString(0), r.getString(1), r.getString(2)))
            .distinct()
            .collect()
        freqMetricsDf.show(false)
        freqMetricsSelectedColumns should contain allElementsOf Array(
          ("yelp", "business", "city")
        )
      }
    }
    "All Metrics Config" should "be known and taken  into account" in {
      val rendered = MetricsCmd.usage()
      val expected =
        """
          |Usage: starlake metrics [options]
          |Compute data quality metrics (continuous, discrete, and frequency metrics) on loaded tables. See [Metrics Guide](/starflow/guides/load/metrics).
          |  --domain <value>   Domain Name
          |  --schema <value>   Schema Name
          |  --authInfo <value> Auth Info.  Google Cloud use: gcpProjectId and gcpSAJsonKey
          |  --reportFormat <value> Report format: console, json, html
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
