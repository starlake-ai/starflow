package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{
  AllSinks,
  AutoTaskInfo,
  ConnectionType,
  WriteStrategy,
  WriteStrategyType
}
import ai.starlake.tests.StarlakeTestData

class TransformContextSpec extends TestHelper {

  // The BQ connection runs on BigQuery, the spark connection writes files
  private val fileExport = AutoTaskInfo(
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
        path = Some("/tmp/exports")
      )
    ),
    writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE)))
  )

  private def task(taskDesc: AutoTaskInfo, test: Boolean)(implicit
    settings: Settings,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    implicit val storage: StorageHandler = settings.storageHandler()
    TransformContext(
      appId = None,
      taskDesc = taskDesc,
      commandParameters = Map.empty,
      interactive = None,
      truncate = false,
      test = test,
      logExecution = false,
      resultPageSize = 1,
      resultPageNumber = 1,
      dryRun = false,
      scheduledDate = None,
      syncSchema = false
    ).toTask(taskDesc.getRunEngine())
  }

  "toTask" should "run on the test database a task whose sink is on another connection (#1897)" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      // As `starlake test` runs every connection on the DuckDB database of the test
      val testSettings = StarlakeTestData.createDuckDbSettings(settings, "/tmp/test.db")
      task(fileExport, test = true)(testSettings, schemaHandler) shouldBe a[JdbcAutoTask]
    }
  }

  it should "keep on Spark a JDBC task whose sink is on another database" in {
    new WithSettings() {
      implicit val schemaHandler: SchemaHandler = settings.schemaHandler()
      def duckdb(file: String) = ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> s"jdbc:duckdb:$file", "driver" -> "org.duckdb.DuckDBDriver")
      )
      val databases = settings.copy(appConfig =
        settings.appConfig.copy(connections =
          settings.appConfig.connections ++
          Map(
            "a"     -> duckdb("/tmp/a.db"),
            "b"     -> duckdb("/tmp/b.db"),
            "a2"    -> duckdb("/tmp/a.db"),
            "a2spk" -> duckdb("/tmp/a.db").copy(sparkFormat = Some("jdbc"))
          )
        )
      )
      val jdbcTask = fileExport.copy(connectionRef = Some("a"))
      def withSink(ref: String) = jdbcTask.copy(sink = Some(AllSinks(connectionRef = Some(ref))))
      task(withSink("b"), test = false)(databases, schemaHandler) shouldBe a[SparkAutoTask]
      task(withSink("a2"), test = false)(databases, schemaHandler) shouldBe a[JdbcAutoTask]
      task(withSink("a"), test = false)(databases, schemaHandler) shouldBe a[JdbcAutoTask]
      // A sink declaring a Spark format, and a Python task, keep running on Spark
      task(withSink("a2spk"), test = false)(databases, schemaHandler) shouldBe a[SparkAutoTask]
      val pythonTask =
        withSink("a2").copy(python = Some(new org.apache.hadoop.fs.Path("/tmp/job.py")))
      task(pythonTask, test = false)(databases, schemaHandler) shouldBe a[SparkAutoTask]
    }
  }
}
