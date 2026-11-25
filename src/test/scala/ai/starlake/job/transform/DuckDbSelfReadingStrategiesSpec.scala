package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.job.strategies.TransformStrategiesBuilder
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

/** A DELETE_THEN_INSERT or UPSERT_BY_KEY task whose SELECT reads its own table inserts the rows
  * that SELECT returned before the delete: the DuckDB templates read them from a temporary table,
  * not from a view that the delete changes. On a data branch or over Arrow Flight SQL (QoD) they
  * keep the view.
  */
class DuckDbSelfReadingStrategiesSpec extends TestHelper {

  lazy val duckDbConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/self_reading.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  /** Rows from the last day already loaded on: a SELECT that reads its target */
  private val sql =
    """SELECT d, k, count(*) AS n FROM selfdb.src
      |{% if sl_incremental %}
      |WHERE d >= (SELECT max(d) FROM selfdb.target)
      |{% endif %}
      |GROUP BY d, k""".stripMargin

  new WithSettings(duckDbConfiguration) {
    private def withDuckDb[T](f: java.sql.Connection => T): T =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      )(f)

    private def reset(): Unit = withDuckDb { conn =>
      val stmt = conn.createStatement()
      stmt.execute("CREATE SCHEMA IF NOT EXISTS selfdb")
      stmt.execute("DROP TABLE IF EXISTS selfdb.target")
      stmt.execute("CREATE OR REPLACE TABLE selfdb.src(d INTEGER, k VARCHAR)")
      stmt.execute("INSERT INTO selfdb.src VALUES (1, 'a'), (1, 'a'), (2, 'b')")
    }

    private def target(): List[(Int, String, Int)] = withDuckDb { conn =>
      val rs =
        conn.createStatement().executeQuery("SELECT d, k, n FROM selfdb.target ORDER BY d, k")
      val buf = scala.collection.mutable.ListBuffer[(Int, String, Int)]()
      while (rs.next()) buf += ((rs.getInt("d"), rs.getString("k"), rs.getInt("n")))
      buf.toList
    }

    private def run(
      strategy: WriteStrategyType,
      on: MergeOn = MergeOn.TARGET
    ): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = "selfdb",
        table = "target",
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(strategy),
            key = List("d", "k"),
            on = Some(on),
            timestamp = Some("n")
          )
        )
      )
      val folder = new Path(starlakeMetadataPath + "/transform/selfdb")
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(folder, "target.sl.yml")
      )
      storageHandler.write(sql, new Path(folder, "target.sql"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = "selfdb.target"))
        .isSuccess
    }

    private val strategies =
      List(WriteStrategyType.DELETE_THEN_INSERT, WriteStrategyType.UPSERT_BY_KEY)

    for {
      strategy <- strategies
      on       <- List(MergeOn.TARGET, MergeOn.SOURCE_AND_TARGET)
    } {
      s"$strategy on $on on DuckDB" should "insert the rows its SELECT returned before the delete" in {
        reset()
        run(strategy, on) shouldBe true
        target() shouldBe List((1, "a", 2), (2, "b", 1))
        withDuckDb(
          _.createStatement().execute("INSERT INTO selfdb.src VALUES (2, 'b'), (3, 'c')")
        )
        run(strategy, on) shouldBe true
        // the second run reads d >= 2: (2, b) is replaced, (3, c) added, (1, a) left alone
        target() shouldBe List((1, "a", 2), (2, "b", 2), (3, "c", 1))
      }

      it should "leave a table of the user named SL_INCOMING alone" in {
        reset()
        // main is where an unqualified name resolves; selfdb is the schema of the target
        val userTables = List("main.SL_INCOMING", "selfdb.SL_INCOMING")
        withDuckDb { conn =>
          val stmt = conn.createStatement()
          userTables.foreach { t =>
            stmt.execute(s"CREATE OR REPLACE TABLE $t(x INTEGER)")
            stmt.execute(s"INSERT INTO $t VALUES (42)")
          }
        }
        run(strategy, on) shouldBe true
        run(strategy, on) shouldBe true
        target() shouldBe List((1, "a", 2), (2, "b", 1))
        withDuckDb { conn =>
          val stmt = conn.createStatement()
          userTables.foreach { t =>
            val rs = stmt.executeQuery(s"SELECT x FROM $t")
            withClue(t) {
              rs.next() shouldBe true
              rs.getInt("x") shouldBe 42
            }
            stmt.execute(s"DROP TABLE $t")
          }
        }
      }
    }

    private def render(
      strategy: WriteStrategyType,
      on: MergeOn,
      sinkConnection: ConnectionInfo,
      dataBranch: Option[String]
    ): String =
      new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(
          `type` = Some(strategy),
          key = List("d", "k"),
          on = Some(on),
          timestamp = Some("n")
        ),
        "SELECT d, k, n FROM selfdb.src",
        TransformStrategiesBuilder.TableComponents("", "selfdb", "target", List("d", "k", "n")),
        targetTableExists = true,
        truncate = false,
        materializedView = Materialization.TABLE,
        settings.appConfig.jdbcEngines("duckdb"),
        AllSinks().getSink(),
        JdbcAutoTask.strategyTemplateVariables(sinkConnection, dataBranch)
      )

    private def connection(url: String) =
      ConnectionInfo(ConnectionType.JDBC, options = Map("url" -> url))

    private val local = connection("jdbc:duckdb:/tmp/self_reading.db")
    private val flightSql = connection("jdbc:arrow-flight-sql://qod.example.com:31338")

    /** The statements that create SL_INCOMING before this change, one per strategy */
    private def incomingView(strategy: WriteStrategyType): String = strategy match {
      case WriteStrategyType.DELETE_THEN_INSERT =>
        "CREATE OR REPLACE VIEW SL_INCOMING AS SELECT d, k, n FROM selfdb.src;"
      case _ => "CREATE TEMPORARY VIEW SL_INCOMING AS SELECT d, k, n FROM selfdb.src;"
    }

    for {
      strategy <- strategies
      on       <- List(MergeOn.TARGET, MergeOn.SOURCE_AND_TARGET)
      (label, sink, branch) <- List(
        ("over Arrow Flight SQL", flightSql, None),
        ("on a data branch", local, Some("feature"))
      )
    } {
      s"The $strategy template on $on $label" should "read the incoming rows from a view" in {
        val sql = render(strategy, on, sink, branch)
        sql should include(incomingView(strategy))
        sql should not include "TEMPORARY TABLE"
        sql should not include "temp."
        if (strategy == WriteStrategyType.DELETE_THEN_INSERT) {
          sql should include("DROP VIEW IF EXISTS SL_INCOMING;")
          if (on == MergeOn.SOURCE_AND_TARGET)
            sql should include("CREATE OR REPLACE VIEW SL_VIEW_WITH_ROWNUM AS")
        } else {
          if (on == MergeOn.TARGET)
            sql should include("INSERT INTO selfdb.target SELECT d, k, n FROM selfdb.src")
          else
            sql should include("CREATE TEMPORARY VIEW SL_DEDUP AS")
        }
      }
    }

    strategies.foreach { strategy =>
      s"The $strategy template on a local DuckDB" should "copy the incoming rows to a temporary table" in {
        val sql = render(strategy, MergeOn.TARGET, local, None)
        sql should include("CREATE TEMPORARY TABLE SL_INCOMING AS")
        sql should include("DROP TABLE IF EXISTS temp.SL_INCOMING;")
      }
    }
  }
}
