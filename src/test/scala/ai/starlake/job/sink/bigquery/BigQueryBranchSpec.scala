package ai.starlake.job.sink.bigquery

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.google.cloud.bigquery.{BigQueryOptions, DatasetId, TableId}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class BigQueryBranchSpec extends TestHelper with BeforeAndAfterAll {

  private val sourceDataset = "SL_BQ_TEST_DS"
  private val sourceTable = "SL_BQ_TEST_BRANCH_SOURCE"
  private val branchName = "sl_test_branch"
  private val outputTable = "SL_BQ_TEST_BRANCH_TARGET"

  private lazy val bigquery = BigQueryOptions.newBuilder().build().getService

  private def setEnv(key: String, value: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map =
      field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  private def delEnv(key: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map =
      field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.remove(key)
  }

  private def cleanupBranch(): Unit = {
    try { bigquery.delete(TableId.of(branchName, sourceTable)) }
    catch { case _: Exception => }
    try { bigquery.delete(TableId.of(branchName, outputTable)) }
    catch { case _: Exception => }
    try { bigquery.delete(DatasetId.of(branchName)) }
    catch { case _: Exception => }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of(sourceDataset, sourceTable))
      bigquery.delete(TableId.of(sourceDataset, outputTable))
      cleanupBranch()
    }
  }

  override def afterAll(): Unit = {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      bigquery.delete(TableId.of(sourceDataset, sourceTable))
      bigquery.delete(TableId.of(sourceDataset, outputTable))
      cleanupBranch()
    }
    super.afterAll()
  }

  private val bigQueryConfiguration: Config = {
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
        |spark.datasource.bigquery {
        |  materializationDataset: "SL_BQ_TEST_DS"
        |  location: "europe-west1"
        |}
        |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  "BigQuery Branch Transform" should "write to branch dataset instead of original" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      new WithSettings(bigQueryConfiguration) {
        // Step 1: Create source table with test data via Spark (parquet -> BQ)
        val parquetPath = starlakeTestRoot + "/tmp/branch_source"
        sparkSession.read
          .option("inferSchema", "true")
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
          .write
          .mode("overwrite")
          .parquet(parquetPath)

        val configJob = AutoJobInfo("", Nil)
        val configJobDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))

        val sourceConfigPath =
          new Path(starlakeMetadataPath + s"/transform/$sourceDataset/_config.sl.yml")
        storageHandler.mkdirs(sourceConfigPath.getParent)
        storageHandler.write(configJobDef, sourceConfigPath)

        // Source task uses default Spark engine to read parquet and sink to BQ
        val sourceTaskInfo = AutoTaskInfo(
          name = "createSource",
          sql = Some(s"SELECT firstname, lastname, age FROM parquet.`$parquetPath`"),
          database = None,
          domain = sourceDataset,
          table = sourceTable,
          sink = Some(BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val sourceTaskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, sourceTaskInfo))
        storageHandler.write(
          sourceTaskDef,
          new Path(starlakeMetadataPath + s"/transform/$sourceDataset/createSource.sl.yml")
        )

        val schemaHandler = settings.schemaHandler()
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
        workflow
          .autoJob(TransformConfig(name = s"$sourceDataset.createSource"))
          .isSuccess shouldBe true

        val createdSource = bigquery.getTable(TableId.of(sourceDataset, sourceTable))
        createdSource should not be null
        val sourceRowCount = createdSource.getNumRows.intValue()
        sourceRowCount should be > 0

        // Step 2: Create branch transform task using BigQuery native engine
        val branchTaskInfo = AutoTaskInfo(
          name = "branchTransform",
          sql = Some(s"SELECT firstname, lastname, age FROM $sourceDataset.$sourceTable"),
          connectionRef = Some("sparkbq"), // Use BQ connection as run engine
          database = None,
          domain = sourceDataset,
          table = outputTable,
          sink = Some(BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val branchTaskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, branchTaskInfo))
        storageHandler.write(
          branchTaskDef,
          new Path(starlakeMetadataPath + s"/transform/$sourceDataset/branchTransform.sl.yml")
        )

        // Step 3: Run transform WITH branch active
        setEnv("SL_DATA_BRANCH", branchName)
        try {
          val branchSchemaHandler = settings.schemaHandler(reload = true)
          val branchWorkflow = new IngestionWorkflow(storageHandler, branchSchemaHandler)
          branchWorkflow
            .autoJob(TransformConfig(name = s"$sourceDataset.branchTransform"))
            .isSuccess shouldBe true
        } finally {
          delEnv("SL_DATA_BRANCH")
          settings.schemaHandler(reload = true) // Reset cache
        }

        // Step 4: Verify results
        // Branch dataset should have the target table
        val branchTarget = bigquery.getTable(TableId.of(branchName, outputTable))
        branchTarget should not be null
        branchTarget.getNumRows.intValue() shouldBe sourceRowCount

        // Original dataset should NOT have the target table (it went to branch)
        val originalTarget = bigquery.getTable(TableId.of(sourceDataset, outputTable))
        originalTarget shouldBe null
      }
    }
  }

  "BigQuery Branch Transform with connectionRef" should "write to branch dataset with task connectionRef" in {
    if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
      new WithSettings(bigQueryConfiguration) {
        // Step 1: Create source table with test data via Spark
        val parquetPath = starlakeTestRoot + "/tmp/branch_source_native"
        sparkSession.read
          .option("inferSchema", "true")
          .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
          .write
          .mode("overwrite")
          .parquet(parquetPath)

        val configJob = AutoJobInfo("", Nil)
        val configJobDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))

        val sourceConfigPath =
          new Path(starlakeMetadataPath + s"/transform/$sourceDataset/_config.sl.yml")
        storageHandler.mkdirs(sourceConfigPath.getParent)
        storageHandler.write(configJobDef, sourceConfigPath)

        val sourceTaskInfo = AutoTaskInfo(
          name = "createSourceNative",
          sql = Some(s"SELECT firstname, lastname, age FROM parquet.`$parquetPath`"),
          database = None,
          domain = sourceDataset,
          table = sourceTable,
          sink = Some(BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val sourceTaskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, sourceTaskInfo))
        storageHandler.write(
          sourceTaskDef,
          new Path(starlakeMetadataPath + s"/transform/$sourceDataset/createSourceNative.sl.yml")
        )

        val schemaHandler = settings.schemaHandler()
        val workflow = new IngestionWorkflow(storageHandler, schemaHandler)
        workflow
          .autoJob(TransformConfig(name = s"$sourceDataset.createSourceNative"))
          .isSuccess shouldBe true

        val createdSource = bigquery.getTable(TableId.of(sourceDataset, sourceTable))
        createdSource should not be null
        val sourceRowCount = createdSource.getNumRows.intValue()
        sourceRowCount should be > 0

        // Step 2: Create branch transform task with BQ native run engine
        val branchTaskInfo = AutoTaskInfo(
          name = "branchTransformNative",
          sql = Some(s"SELECT firstname, lastname, age FROM $sourceDataset.$sourceTable"),
          connectionRef = Some("sparkbq"),
          database = None,
          domain = sourceDataset,
          table = outputTable,
          sink = Some(BigQuerySink(connectionRef = Some("sparkbq")).toAllSinks()),
          writeStrategy = Some(WriteStrategy.Overwrite)
        )
        val branchTaskDef = mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(TaskDesc(latestSchemaVersion, branchTaskInfo))
        storageHandler.write(
          branchTaskDef,
          new Path(
            starlakeMetadataPath + s"/transform/$sourceDataset/branchTransformNative.sl.yml"
          )
        )

        // Step 3: Run transform WITH branch active
        setEnv("SL_DATA_BRANCH", branchName)
        try {
          val branchSchemaHandler = settings.schemaHandler(reload = true)
          val branchWorkflow = new IngestionWorkflow(storageHandler, branchSchemaHandler)
          branchWorkflow
            .autoJob(TransformConfig(name = s"$sourceDataset.branchTransformNative"))
            .isSuccess shouldBe true
        } finally {
          delEnv("SL_DATA_BRANCH")
          settings.schemaHandler(reload = true)
        }

        // Step 4: Verify results
        val branchTarget = bigquery.getTable(TableId.of(branchName, outputTable))
        branchTarget should not be null
        branchTarget.getNumRows.intValue() shouldBe sourceRowCount

        // Original dataset should NOT have the target table
        val originalTarget = bigquery.getTable(TableId.of(sourceDataset, outputTable))
        originalTarget shouldBe null
      }
    }
  }
}
