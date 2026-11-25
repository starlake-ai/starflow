package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.strategies.TransformStrategiesBuilder
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path

/** A DELETE_THEN_INSERT or UPSERT_BY_KEY task whose SELECT reads its own table inserts the rows
  * that SELECT returned before the delete (#2013). Each subclass runs the scenario on one engine,
  * through the connection its configuration names.
  */
abstract class SelfReadingStrategiesSpecBase(engine: String) extends TestHelper {

  /** The configuration of the run, whose connectionRef names the connection of the engine */
  protected def engineConfiguration: Config

  /** The schema of the tables: the domain of the task */
  protected def domain: String = "selfdb"

  /** Whether the engine is available to this run */
  protected def enabled: Boolean = true

  /** The statements that create the schema of the tables, when the engine needs them */
  protected def createSchema: List[String] = List(s"CREATE SCHEMA IF NOT EXISTS $domain")

  protected def stringType: String = "VARCHAR(10)"

  /** Whether the scenario runs the SQL of the write strategy directly, in autocommit, instead of
    * running the task: for an engine whose writes need autocommit, which a task does not use
    */
  protected def runsStrategySqlOnly: Boolean = false

  /** The strategies and `on` modes the engine supports */
  protected def cases: List[(WriteStrategyType, MergeOn)] =
    for {
      strategy <- List(WriteStrategyType.DELETE_THEN_INSERT, WriteStrategyType.UPSERT_BY_KEY)
      on       <- List(MergeOn.TARGET, MergeOn.SOURCE_AND_TARGET)
    } yield (strategy, on)

  /** Rows from the last day already loaded on: a SELECT that reads its target */
  private def sql =
    s"""SELECT d, k, count(*) AS n FROM $domain.src
       |{% if sl_incremental %}
       |WHERE d >= (SELECT max(d) FROM $domain.target)
       |{% endif %}
       |GROUP BY d, k""".stripMargin

  if (enabled) new WithSettings(engineConfiguration) {
    private def withConnection[T](f: java.sql.Connection => T): T =
      JdbcDbUtils.withJDBCConnection(
        None,
        settings.appConfig.connections(settings.appConfig.connectionRef).options
      )(f)

    private def execute(statements: String*): Unit = withConnection { conn =>
      statements.foreach(conn.createStatement().execute(_))
    }

    private def reset(): Unit =
      execute(
        createSchema ++ List(
          s"DROP TABLE IF EXISTS $domain.target",
          s"DROP TABLE IF EXISTS $domain.src",
          s"CREATE TABLE $domain.src(d INTEGER, k $stringType)",
          s"INSERT INTO $domain.src VALUES (1, 'a'), (1, 'a'), (2, 'b')"
        ): _*
      )

    private def target(): List[(Int, String, Int)] = withConnection { conn =>
      val rs =
        conn.createStatement().executeQuery(s"SELECT d, k, n FROM $domain.target ORDER BY d, k")
      val buf = scala.collection.mutable.ListBuffer[(Int, String, Int)]()
      while (rs.next()) buf += ((rs.getInt("d"), rs.getString("k"), rs.getInt("n")))
      buf.toList
    }

    /** The SQL of the write strategy, rendered as a task on the connection renders it, run in
      * autocommit, one statement at a time
      */
    private def runStrategySql(strategy: WriteStrategyType, on: MergeOn): Boolean = {
      val targetExists = withConnection(
        _.getMetaData.getTables(null, domain, "target", null).next()
      )
      val select = sql
        .replace("{% if sl_incremental %}", "")
        .replace("{% endif %}", "")
        .linesIterator
        .filter(line => targetExists || !line.contains("max(d)"))
        .mkString("\n")
      val connection = settings.appConfig.connections(settings.appConfig.connectionRef)
      val rendered = new TransformStrategiesBuilder().buildTransform(
        WriteStrategy(
          `type` = Some(strategy),
          key = List("d", "k"),
          on = Some(on),
          timestamp = Some("n")
        ),
        select,
        TransformStrategiesBuilder.TableComponents("", domain, "target", List("d", "k", "n")),
        targetTableExists = targetExists,
        truncate = false,
        materializedView = Materialization.TABLE,
        settings.appConfig.jdbcEngines(connection.getJdbcEngineName().toString),
        AllSinks().getSink(),
        JdbcAutoTask.strategyTemplateVariables(connection, None)
      )
      execute(rendered.splitSql().map(_.trim).filter(_.nonEmpty): _*)
      true
    }

    private def run(strategy: WriteStrategyType, on: MergeOn): Boolean =
      if (runsStrategySqlOnly) runStrategySql(strategy, on) else runTask(strategy, on)

    private def runTask(strategy: WriteStrategyType, on: MergeOn): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = domain,
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
      val folder = new Path(starlakeMetadataPath + s"/transform/$domain")
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(folder, "target.sl.yml")
      )
      storageHandler.write(sql, new Path(folder, "target.sql"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = s"$domain.target"))
        .isSuccess
    }

    cases.foreach { case (strategy, on) =>
      s"$strategy on $on on $engine" should "insert the rows its SELECT returned before the delete" in {
        reset()
        run(strategy, on) shouldBe true
        target() shouldBe List((1, "a", 2), (2, "b", 1))
        execute(s"INSERT INTO $domain.src VALUES (2, 'b'), (3, 'c')")
        run(strategy, on) shouldBe true
        // the second run reads d >= 2: (2, b) is replaced, (3, c) added, (1, a) left alone
        target() shouldBe List((1, "a", 2), (2, "b", 2), (3, "c", 1))
      }
    }
  }
}
