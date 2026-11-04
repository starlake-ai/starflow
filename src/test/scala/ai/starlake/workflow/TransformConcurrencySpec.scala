package ai.starlake.workflow

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.{SparkAutoTask, TransformConfig}
import ai.starlake.utils.{SparkUtils, YamlSerde}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
import scala.util.Try

/** Real tasks on real threads (#1785). All of these interleavings are timing dependent, so each
  * test starts its threads on a barrier to make the overlap the common case rather than the rare
  * one.
  */
class TransformConcurrencySpec extends TestHelper {

  private val taskCount = 8

  private def concurrently[T](count: Int)(action: Int => T): List[Try[T]] = {
    val barrier = new CyclicBarrier(count)
    val pool = Executors.newFixedThreadPool(count)
    try {
      val futures = (0 until count).toList.map { i =>
        pool.submit(new Callable[Try[T]] {
          override def call(): Try[T] = Try {
            barrier.await()
            action(i)
          }
        })
      }
      futures.map(_.get(5, TimeUnit.MINUTES))
    } finally pool.shutdownNow()
  }

  private def deliverTasks(count: Int)(implicit settings: Settings): Unit = {
    val storage = settings.storageHandler()
    val job = new Path(DatasetArea.transform, "conc")
    storage.mkdirs(job)
    storage.write(
      """version: 1
        |transform:
        |  default:
        |    writeStrategy:
        |      type: OVERWRITE
        |""".stripMargin,
      new Path(job, "_config.sl.yml")
    )
    (0 until count).foreach { i =>
      storage.write(s"SELECT $i AS id", new Path(job, s"t$i.sql"))
    }
  }

  "Reading YAML files" should "not touch the default locale of the JVM" in {
    new WithSettings() {
      deliverTasks(1)
      val handler = settings.schemaHandler(reload = true)
      val locale = Locale.getDefault
      val done = new AtomicBoolean(false)
      try {
        val results = concurrently(2) {
          case 0 =>
            try (0 until 100).foreach(_ => handler.taskOnly("conc.t0", reload = true).get)
            finally done.set(true)
          case _ =>
            while (!done.get()) Locale.getDefault shouldBe locale
        }
        results.foreach(_.get)
      } finally Locale.setDefault(locale)
    }
  }

  it should "report validation errors in English whatever the default locale" in {
    val locale = Locale.getDefault
    Locale.setDefault(Locale.FRENCH)
    try {
      val error = intercept[Exception] {
        YamlSerde.validateConfigFile(
          "task",
          "version: 1\ntask:\n  writeStrategy: not_an_object\n",
          "t.sl.yml",
          Nil
        )
      }
      error.getMessage should include("object")
      error.getMessage should not include "objet"
    } finally Locale.setDefault(locale)
  }

  "taskOnly with reload" should "not lose a task of a job reloaded from several threads" in {
    new WithSettings() {
      deliverTasks(taskCount)
      val handler = settings.schemaHandler(reload = true)
      val expected = (0 until taskCount).map(i => s"conc.t$i").toList.sorted
      handler.tasks(reload = true).map(_.fullName()).sorted shouldBe expected

      (0 until 50).foreach { _ =>
        concurrently(taskCount) { i =>
          handler.taskOnly(s"conc.t$i", reload = true).get
        }.foreach(_.get)
        handler.tasks().map(_.fullName()).sorted shouldBe expected
      }
    }
  }

  it should "keep a task visible to other threads while it reloads" in {
    new WithSettings() {
      deliverTasks(1)
      val handler = settings.schemaHandler(reload = true)
      handler.tasks(reload = true)
      val done = new AtomicBoolean(false)

      val results = concurrently(2) {
        case 0 =>
          try (0 until 300).foreach(_ => handler.taskOnly("conc.t0", reload = true).get)
          finally done.set(true)
        case _ =>
          while (!done.get()) handler.taskByFullName("conc.t0").get
      }
      results.foreach(_.get)
    }
  }

  "Spark tasks sinking concurrently" should "write only their own rows to their own table" in {
    new WithSettings() {
      deliverTasks(taskCount)
      val handler = settings.schemaHandler(reload = true)
      val workflow = new IngestionWorkflow(settings.storageHandler(), handler)
      val tasks = (0 until taskCount).map { i =>
        workflow
          .buildTask(TransformConfig(name = s"conc.t$i", scheduledDate = None))
          .asInstanceOf[SparkAutoTask]
      }
      val session = tasks.head.session
      session.sql("CREATE SCHEMA IF NOT EXISTS conc")

      concurrently(taskCount) { i =>
        tasks(i).sink(session.range(1).selectExpr(s"$i AS id"))
      }.foreach(_.get shouldBe true)

      (0 until taskCount).foreach { i =>
        session.table(s"conc.t$i").collect().map(_.getInt(0)).toList shouldBe List(i)
      }
    }
  }

  "Spark transforms running concurrently" should "write only their own rows to their own table" in {
    new WithSettings() {
      deliverTasks(taskCount)
      val handler = settings.schemaHandler(reload = true)
      handler.tasks(reload = true)
      val workflow = new IngestionWorkflow(settings.storageHandler(), handler)

      concurrently(taskCount) { i =>
        workflow.transform(TransformConfig(name = s"conc.t$i", scheduledDate = None)).get
      }.foreach(_.get)

      (0 until taskCount).foreach { i =>
        sparkSession.table(s"conc.t$i").collect().map(_.getInt(0)).toList shouldBe List(i)
      }
    }
  }

  it should "log every task to the audit table when it does not exist yet" in {
    val config = ConfigFactory
      .parseString("""audit { active = true, sink { connectionRef = "spark" } }""")
      .withFallback(super.testConfiguration)
    new WithSettings(config) {
      deliverTasks(taskCount)
      val handler = settings.schemaHandler(reload = true)
      handler.tasks(reload = true)
      val auditTable = s"${settings.appConfig.audit.getDomain()}.audit"
      sparkSession.sql(s"DROP TABLE IF EXISTS $auditTable")
      val workflow = new IngestionWorkflow(settings.storageHandler(), handler)

      concurrently(taskCount) { i =>
        workflow.transform(TransformConfig(name = s"conc.t$i", scheduledDate = None)).get
      }.foreach(_.get)

      sparkSession.table(auditTable).count() shouldBe taskCount
    }
  }

  "Spark pre and post SQL of concurrent loads" should "read their own table as SL_THIS" in {
    new WithSettings() {
      val session = sparkSession
      session.sql("CREATE SCHEMA IF NOT EXISTS slthis")
      (0 until taskCount).foreach { i =>
        session.sql(s"CREATE TABLE slthis.source$i USING parquet AS SELECT $i AS id")
      }

      concurrently(taskCount) { i =>
        IngestionJob.runSparkPrePostSql(
          session,
          s"slthis.source$i",
          List(s"CREATE TABLE slthis.copy$i USING parquet AS SELECT * FROM SL_THIS")
        )
      }.foreach(_.get)

      (0 until taskCount).foreach { i =>
        session.table(s"slthis.copy$i").collect().map(_.getInt(0)).toList shouldBe List(i)
      }
    }
  }

  it should "not read the SL_THIS of an earlier load when their table does not exist" in {
    new WithSettings() {
      val session = sparkSession
      session.sql("CREATE SCHEMA IF NOT EXISTS slthis")
      session.sql("DROP TABLE IF EXISTS slthis.stale_copy")
      session.sql("SELECT 'from-earlier' AS id").createOrReplaceTempView("SL_THIS")

      val error = intercept[Exception] {
        IngestionJob.runSparkPrePostSql(
          session,
          "slthis.not_created_yet",
          List("CREATE TABLE slthis.stale_copy USING parquet AS SELECT * FROM SL_THIS")
        )
      }
      error.getMessage should include("SL_THIS")
      SparkUtils.tableExists(session, "slthis.stale_copy") shouldBe false
    }
  }

  private lazy val duckDbConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/test_concurrent.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  "DuckDB transforms running concurrently" should "write only their own data to their own table" in {
    new WithSettings(duckDbConfiguration) {
      deliverTasks(taskCount)
      val handler = settings.schemaHandler(reload = true)
      handler.tasks(reload = true)
      val workflow = new IngestionWorkflow(settings.storageHandler(), handler)

      // The first round creates the schema and the tables, the second one replaces their content
      (0 until 2).foreach { _ =>
        concurrently(taskCount) { i =>
          workflow.transform(TransformConfig(name = s"conc.t$i", scheduledDate = None)).get
        }.foreach(_.get)
      }

      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(handler.dataBranch(), options) { conn =>
        (0 until taskCount).foreach { i =>
          val rs = conn.createStatement().executeQuery(s"SELECT id FROM conc.t$i")
          rs.next() shouldBe true
          rs.getInt(1) shouldBe i
          rs.next() shouldBe false
        }
      }
    }
  }
}
