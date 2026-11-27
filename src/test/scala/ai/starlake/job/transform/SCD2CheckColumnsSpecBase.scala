package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.strategies.TransformStrategiesBuilder
import ai.starlake.schema.model._
import ai.starlake.utils.Formatter._
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer

/** An SCD2 task with `checkColumns` opens a version of a key only when one of the checked columns
  * changes, a null on one side included. Each subclass runs the scenario on one engine.
  */
abstract class SCD2CheckColumnsSpecBase(engine: String) extends EngineSpec {

  override protected def domain: String = "scd2check"

  private def sql = s"SELECT id, label, note FROM $domain.src"

  private def strategy(checkColumns: List[String]): WriteStrategy =
    WriteStrategy(
      `type` = Some(WriteStrategyType.SCD2),
      key = List("id"),
      checkColumns = Some(checkColumns),
      startTs = Some("valid_from"),
      endTs = Some("valid_to")
    )

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
          s"CREATE TABLE $domain.src(id INTEGER, label $stringType, note $stringType)",
          s"INSERT INTO $domain.src VALUES (1, 'a', NULL)"
        ): _*
      )

    /** The versions of the key: label, note, and whether the version is current */
    private def versions(): List[(String, String, Boolean)] = withConnection { conn =>
      val rs = conn
        .createStatement()
        .executeQuery(s"SELECT label, note, valid_to FROM $domain.target")
      val buf = ListBuffer.empty[(String, String, Boolean)]
      while (rs.next()) buf += ((rs.getString(1), rs.getString(2), rs.getObject(3) == null))
      buf.toList.sortBy { case (label, note, current) =>
        (current, String.valueOf(label), String.valueOf(note))
      }
    }

    private def runTask(checkColumns: List[String]): Boolean = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = domain,
        table = "target",
        writeStrategy = Some(strategy(checkColumns))
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

    /** The SQL of the write strategy, rendered as a task on the connection renders it, run in
      * autocommit, one statement at a time
      */
    private def runStrategySql(checkColumns: List[String]): Boolean = {
      val targetExists = withConnection(
        _.getMetaData.getTables(null, domain, "target", null).next()
      )
      val connection = settings.appConfig.connections(settings.appConfig.connectionRef)
      val rendered = new TransformStrategiesBuilder().buildTransform(
        strategy(checkColumns),
        sql,
        TransformStrategiesBuilder
          .TableComponents("", domain, "target", List("id", "label", "note")),
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

    private def run(checkColumns: List[String]): Boolean =
      if (runsStrategySqlOnly) runStrategySql(checkColumns) else runTask(checkColumns)

    List("the listed columns" -> List("label", "note"), "'*'" -> List("*")).foreach {
      case (name, checkColumns) =>
        s"SCD2 with checkColumns $name on $engine" should "open a version only when a checked column changes, a null included" in {
          reset()
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, true))
          // the same row again: no new version
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, true))
          // null to a value
          execute(s"UPDATE $domain.src SET note = 'x'")
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, false), ("a", "x", true))
          execute(s"UPDATE $domain.src SET label = 'b'")
          run(checkColumns) shouldBe true
          versions() shouldBe List(("a", null, false), ("a", "x", false), ("b", "x", true))
        }
    }

    s"SCD2 with checkColumns on $engine" should "not open a version when only an unchecked column changes" in {
      reset()
      run(List("label")) shouldBe true
      execute(s"UPDATE $domain.src SET note = 'x'")
      run(List("label")) shouldBe true
      versions() shouldBe List(("a", null, true))
    }
  }
}
