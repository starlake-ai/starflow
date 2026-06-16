package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, TableId}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class SparkParquetToBigQuerySpec extends TestHelper with BeforeAndAfterAll {

  private val outputDataset = "SL_BQ_TEST_DS"
  private val outputTable = "SL_BQ_TEST_PARQUET_TO_BQ"
  private val outputTablePySpark = "SL_BQ_TEST_PARQUET_TO_BQ_PYSPARK"

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      val bigquery = BigQueryOptions.newBuilder().build().getService
      bigquery.delete(TableId.of(outputDataset, outputTable))
      bigquery.delete(TableId.of(outputDataset, outputTablePySpark))
    }
  }

  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      val bigquery = BigQueryOptions.newBuilder().build().getService
      bigquery.delete(TableId.of(outputDataset, outputTable))
      bigquery.delete(TableId.of(outputDataset, outputTablePySpark))
    }
    super.afterAll()
  }

  "Spark AutoTask" should "read parquet from local FS and sink to BigQuery" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      val bigQueryConfiguration: Config = {
        val config = ConfigFactory.parseString(
          """
            |connections.sparkbq {
            |  sparkFormat = "bigquery"
            |  type = "bigquery"
            |  options {
            |    gcsBucket: starlake-app
            |    writeMethod: indirect
            |    location: "europe-west1"
            |    authType: APPLICATION_DEFAULT
            |  }
            |}
            |""".stripMargin
        )
        config.withFallback(super.testConfiguration)
      }
      new WithSettings(bigQueryConfiguration) {
        // Write test JSON data as parquet files
        val parquetPath = starlakeTestRoot + "/tmp/parquet_source"
        sparkSession.read
          .option("inferSchema", "true")
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
          .write
          .mode("overwrite")
          .parquet(parquetPath)

        // Create _config.sl.yml for the transform domain
        val configJob = AutoJobInfo("", Nil)
        val configJobDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
        val configPath =
          new Path(starlakeMetadataPath + s"/transform/$outputDataset/_config.sl.yml")
        storageHandler.mkdirs(configPath.getParent)
        storageHandler.write(configJobDef, configPath)

        // Create task: read from parquet, sink to BigQuery
        val taskInfo = AutoTaskInfo(
          name = "parquetToBq",
          sql = Some(
            s"SELECT firstname, lastname, age FROM parquet.`$parquetPath`"
          ),
          database = None,
          domain = outputDataset,
          table = outputTable,
          sink = Some(
            BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()
          ),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val taskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, taskInfo))
        val taskPath =
          new Path(starlakeMetadataPath + s"/transform/$outputDataset/parquetToBq.sl.yml")
        storageHandler.write(taskDef, taskPath)

        // Run the auto job
        val schemaHandler = settings.schemaHandler()
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
        val result = workflow.autoJob(
          TransformConfig(name = s"$outputDataset.parquetToBq")
        )
        result.isSuccess shouldBe true

        // Verify BigQuery table was created with correct row count
        val bigquery = BigQueryOptions.newBuilder().build().getService
        val createdTable = bigquery.getTable(TableId.of(outputDataset, outputTable))
        createdTable should not be null
        createdTable.getNumRows.intValue() shouldBe 7
      }
    }
  }

  it should "read parquet from local FS using PySpark and sink to BigQuery" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      val bigQueryConfiguration: Config = {
        val config = ConfigFactory.parseString(
          """
            |connections.sparkbq {
            |  sparkFormat = "bigquery"
            |  type = "bigquery"
            |  options {
            |    gcsBucket: starlake-app
            |    writeMethod: indirect
            |    location: "europe-west1"
            |    authType: APPLICATION_DEFAULT
            |  }
            |}
            |""".stripMargin
        )
        config.withFallback(super.testConfiguration)
      }
      new WithSettings(bigQueryConfiguration) {
        // Write test JSON data as parquet files
        val parquetPath = starlakeTestRoot + "/tmp/parquet_source_pyspark"
        sparkSession.read
          .option("inferSchema", "true")
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
          .write
          .mode("overwrite")
          .parquet(parquetPath)

        val transformDir = starlakeMetadataPath + s"/transform/$outputDataset"

        // Create _config.sl.yml for the transform domain
        val configJob = AutoJobInfo("", Nil)
        val configJobDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
        val configPath = new Path(transformDir + "/_config.sl.yml")
        storageHandler.mkdirs(configPath.getParent)
        storageHandler.write(configJobDef, configPath)

        // In production, end users would write a standard PySpark script:
        //
        //   from pyspark.sql import SparkSession
        //
        //   spark = SparkSession.builder.getOrCreate()
        //   df = spark.read.parquet("/path/to/parquet")
        //   df = df.select("firstname", "lastname", "age")
        //   df.createOrReplaceTempView("SL_THIS")
        //
        // In SBT tests, PythonRunner's Py4J gateway cannot create a new SparkContext
        // since one already exists in the JVM. We access the active session directly.
        val pyScript =
          s"""from pyspark.context import SparkContext
             |
             |SparkContext._ensure_initialized()
             |jvm = SparkContext._jvm
             |spark = jvm.SparkSession.active()
             |df = spark.read().parquet("$parquetPath")
             |df.createOrReplaceTempView("SL_THIS")
             |""".stripMargin
        val pyPath = new Path(transformDir + "/parquetToBqPyspark.py")
        storageHandler.write(pyScript, pyPath)

        // Create task YAML with no SQL (PySpark will be discovered by convention)
        val taskInfo = AutoTaskInfo(
          name = "parquetToBqPyspark",
          sql = None,
          database = None,
          domain = outputDataset,
          table = outputTablePySpark,
          sink = Some(
            BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()
          ),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val taskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, taskInfo))
        val taskPath = new Path(transformDir + "/parquetToBqPyspark.sl.yml")
        storageHandler.write(taskDef, taskPath)

        // Run the auto job
        val schemaHandler = settings.schemaHandler()
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
        val result = workflow.autoJob(
          TransformConfig(name = s"$outputDataset.parquetToBqPyspark")
        )
        result.isSuccess shouldBe true

        // Verify BigQuery table was created with correct row count
        val bigquery = BigQueryOptions.newBuilder().build().getService
        val createdTable =
          bigquery.getTable(TableId.of(outputDataset, outputTablePySpark))
        createdTable should not be null
        createdTable.getNumRows.intValue() shouldBe 7
      }
    }
  }
}
