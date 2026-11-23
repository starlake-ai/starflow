package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

/** A transform run with SL_DATA_BRANCH set writes to the branch only: production keeps its rows
  * until the branch is applied. Each subclass names the connection the transforms write through.
  */
abstract class DataBranchSpecBase(engine: String) extends TestHelper {

  protected val branch = s"br_$engine"

  /** The `options` block of the connection, as HOCON */
  protected def connectionOptions: String

  /** Whether the engine is available to this run */
  protected def enabled: Boolean = true

  /** Called before each case, before production is reset */
  protected def prepare(): Unit = ()

  /** Whether a temporary view outlives its statement, which UPSERT_BY_KEY needs for SL_INCOMING */
  protected def keepsTemporaryViews: Boolean = true

  lazy val branchConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-branch"
         |connections.test-branch {
         |    type = "jdbc"
         |    options {
         |$connectionOptions
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  private def setEnv(key: String, value: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
      .put(key, value)
  }

  private def delEnv(key: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
      .remove(key)
  }

  if (enabled) new WithSettings(branchConfiguration) {
    private def options = settings.appConfig.connections("test-branch").options

    private def execute(dataBranch: Option[String], sql: String*): Unit =
      JdbcDbUtils.withJDBCConnection(dataBranch, options) { conn =>
        sql.foreach(conn.createStatement().execute(_))
      }

    private def rows(dataBranch: Option[String], table: String): List[(String, Int)] =
      JdbcDbUtils.withJDBCConnection(dataBranch, options) { conn =>
        val rs = conn.createStatement().executeQuery(s"SELECT id, amount FROM $table ORDER BY id")
        val buf = scala.collection.mutable.ListBuffer[(String, Int)]()
        while (rs.next()) buf += ((rs.getString(1), rs.getInt(2)))
        buf.toList
      }

    private def tableExists(schema: String, table: String): Boolean =
      JdbcDbUtils.withJDBCConnection(None, options) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery(
            "SELECT count(*) FROM information_schema.tables " +
            s"WHERE table_schema = '$schema' AND table_name = '$table'"
          )
        rs.next()
        rs.getInt(1) > 0
      }

    private def runOnBranch(table: String, sql: String, writeStrategy: WriteStrategy): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "sales",
        table = table,
        sink = Some(JdbcSink(connectionRef = Some("test-branch")).toAllSinks()),
        python = None,
        writeStrategy = Some(writeStrategy)
      )
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(starlakeMetadataPath + s"/transform/sales/$table.sl.yml")
      )
      storageHandler.write(sql, new Path(starlakeMetadataPath + s"/transform/sales/$table.sql"))
      setEnv("SL_DATA_BRANCH", branch)
      try {
        val schemaHandler = settings.schemaHandler(reload = true)
        schemaHandler.dataBranch() shouldBe Some(branch)
        new IngestionWorkflow(storageHandler, schemaHandler)
          .autoJob(TransformConfig(name = s"sales.$table"))
          .isSuccess
      } finally {
        delEnv("SL_DATA_BRANCH")
        settings.schemaHandler(reload = true)
      }
    }

    private def resetProduction(): Unit = {
      prepare()
      execute(
        None,
        "CREATE SCHEMA IF NOT EXISTS sales",
        "CREATE OR REPLACE TABLE sales.orders(id VARCHAR, amount INTEGER)",
        "INSERT INTO sales.orders VALUES ('1', 100), ('2', 200)",
        "DROP TABLE IF EXISTS sales.new_orders",
        s"DROP SCHEMA IF EXISTS $branch CASCADE"
      )
    }

    private val newRows = "SELECT * FROM (VALUES ('2', 250), ('3', 300)) AS t(id, amount)"

    s"A $engine APPEND transform on a data branch" should "leave production untouched" in {
      resetProduction()
      runOnBranch("orders", newRows, WriteStrategy.Append) shouldBe true
      rows(Some(branch), "sales.orders") shouldBe List(
        ("1", 100),
        ("2", 200),
        ("2", 250),
        ("3", 300)
      )
      rows(None, "sales.orders") shouldBe List(("1", 100), ("2", 200))
    }

    s"A $engine OVERWRITE transform on a data branch" should "leave production untouched" in {
      resetProduction()
      runOnBranch("orders", newRows, WriteStrategy.Overwrite) shouldBe true
      rows(Some(branch), "sales.orders") shouldBe List(("2", 250), ("3", 300))
      rows(None, "sales.orders") shouldBe List(("1", 100), ("2", 200))
    }

    private def upsertCase(test: => Any): Unit =
      if (keepsTemporaryViews)
        s"A $engine UPSERT_BY_KEY transform on a data branch" should "leave production untouched" in test
      else
        s"A $engine UPSERT_BY_KEY transform on a data branch" should "leave production untouched" ignore test

    upsertCase {
      resetProduction()
      val upsert = WriteStrategy(
        `type` = Some(WriteStrategyType.UPSERT_BY_KEY),
        key = List("id"),
        on = Some(MergeOn.TARGET)
      )
      runOnBranch("orders", newRows, upsert) shouldBe true
      rows(Some(branch), "sales.orders") shouldBe List(("1", 100), ("2", 250), ("3", 300))
      rows(None, "sales.orders") shouldBe List(("1", 100), ("2", 200))
    }

    s"A $engine transform creating its table on a data branch" should "not create it in production" in {
      resetProduction()
      runOnBranch("new_orders", newRows, WriteStrategy.Overwrite) shouldBe true
      rows(Some(branch), "sales.new_orders") shouldBe List(("2", 250), ("3", 300))
      tableExists("sales", "new_orders") shouldBe false
    }
  }
}

/** DuckLake attached to an in-process DuckDB */
class DuckLakeDataBranchSpec extends DataBranchSpecBase("ducklake") {
  private lazy val lakeRoot = s"$starlakeTestRoot/ducklake-branch"

  override protected def connectionOptions: String =
    s"""
       |"url": "jdbc:duckdb:"
       |"driver": "org.duckdb.DuckDBDriver"
       |"preActions": "INSTALL ducklake;LOAD ducklake;ATTACH IF NOT EXISTS 'ducklake:$lakeRoot/meta.ducklake' AS lake (DATA_PATH '$lakeRoot/data/');USE lake;"
       |""".stripMargin

  override protected def prepare(): Unit = new java.io.File(s"$lakeRoot/data").mkdirs()
}

/** A DuckLake database served by Quack on Demand over Flight SQL. Runs when SL_QOD_BRANCH_TEST_URL
  * names a Flight SQL url (tenant, pool, TLS options included), with SL_QOD_BRANCH_TEST_USER and
  * SL_QOD_BRANCH_TEST_PASSWORD, and a Flight SQL driver on the test classpath. Every case drops and
  * recreates sales.orders and sales.new_orders, and drops the br_qod schema, in that database.
  */
class QodDataBranchSpec extends DataBranchSpecBase("qod") {
  override protected def enabled: Boolean = sys.env.contains("SL_QOD_BRANCH_TEST_URL")

  // QoD runs each statement in a fresh node session, so the SL_INCOMING view is gone by the
  // DELETE that reads it: UPSERT_BY_KEY fails on QoD with or without a data branch
  override protected def keepsTemporaryViews: Boolean = false

  override protected def connectionOptions: String =
    s"""
       |"url": "${sys.env.getOrElse("SL_QOD_BRANCH_TEST_URL", "")}"
       |"driver": "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
       |"user": "${sys.env.getOrElse("SL_QOD_BRANCH_TEST_USER", "")}"
       |"password": "${sys.env.getOrElse("SL_QOD_BRANCH_TEST_PASSWORD", "")}"
       |""".stripMargin
}
