package ai.starlake.tests

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.sql.StarlakeJdbcDialects
import better.files.File

import java.sql.DriverManager
import scala.util.Using

class StarlakeTestsFrameworkSpec extends IntegrationTestBase {

  override def theSampleFolder: File =
    starlakeDir / "src" / "test" / "resources" / "test-framework"

  val projectDir: File = File.newTemporaryDirectory("sl-test-framework")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    theSampleFolder.copyTo(projectDir, overwrite = true)
    // As `starlake test` does, through Main.run
    StarlakeJdbcDialects.registerDialects()
  }

  override protected def cleanup(): Unit = {
    // project lives in a temp dir, nothing to clean between tests
  }

  private def runTransformTest(
    domain: String,
    task: String,
    testName: String
  ): List[StarlakeTestResult] =
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      val tests =
        StarlakeTestData.loadTests(load = false, domain, task, testName)(settings)
      StarlakeTestData.runTransforms(tests, StarlakeTestConfig())(settings)._1
    }

  private def runLoadTest(
    domain: String,
    table: String,
    testName: String
  ): List[StarlakeTestResult] =
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      val tests =
        StarlakeTestData.loadTests(load = true, domain, table, testName)(settings)
      StarlakeTestData.runLoads(tests, StarlakeTestConfig())(settings)._1
    }

  "a transform unit test" should "execute the transform and succeed when _expected matches" in {
    val results = runTransformTest("kpi", "order_summary", "test_ok")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "fail when _expected does not match the transform output" in {
    val results = runTransformTest("kpi", "order_summary", "test_ko")
    results should not be empty
    results.exists(!_.success) shouldBe true
  }

  it should "apply the SCD2 write strategy against the preloaded target" in {
    val results = runTransformTest("kpi", "customer_history", "scd2")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "run presql against a target that does not exist yet" in {
    val results = runTransformTest("kpi", "channel_refresh", "no_target")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "transpile presql and expectations to DuckDB and apply presql to a preloaded target" in {
    val results = runTransformTest("kpi", "channel_refresh", "preloaded_target")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "write the result of a task with a file sink to its table and compare it (#1897)" in {
    val results = runTransformTest("kpi", "order_export", "file_sink")
    withClue(results.flatMap(_.exception).mkString("\n")) {
      results should not be empty
      results.filter(!_.success) shouldBe empty
    }
  }

  "a load unit test" should "load the incoming file and succeed when _expected matches" in {
    val results = runLoadTest("sales", "orders", "test_ok")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "fail when _expected does not match the loaded data" in {
    val results = runLoadTest("sales", "orders", "test_ko")
    results should not be empty
    results.exists(!_.success) shouldBe true
  }

  it should "apply the SCD2 write strategy when loading into an existing table" in {
    val results = runLoadTest("sales", "categories", "scd2")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "load a Parquet file with a TIME column on DuckDB, as BigQuery exports it (#1952)" in {
    val incoming =
      projectDir / "metadata" / "tests" / "load" / "sales" / "visits" / "bigquery_export" /
      "_incoming.visits-1.parquet"
    // DuckDB writes a TIME WITH TIME ZONE column as BigQuery exports a TIME column:
    // TIME(isAdjustedToUTC = true, unit = MICROS), which Spark does not read
    Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute(
          s"""COPY (SELECT * FROM (VALUES (1::BIGINT, TIMETZ '12:30:00+00'), (2::BIGINT, TIMETZ '23:59:59+00'))
             |  t(id, visit_time)) TO '${incoming.pathAsString}' (FORMAT parquet)""".stripMargin
        )
      }
    }
    val results = runLoadTest("sales", "visits", "bigquery_export")
    withClue(results.flatMap(_.exception).mkString("\n")) {
      results should not be empty
      results.filter(!_.success) shouldBe empty
    }
  }
}
