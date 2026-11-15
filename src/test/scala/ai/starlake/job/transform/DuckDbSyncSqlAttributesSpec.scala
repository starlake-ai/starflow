package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.utils.Utils
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import scala.util.Try

/** `transform --sync-preview` and `--sync-apply` never remove the attributes of a task whose SELECT
  * cannot run, nor the SCD2 validity columns, which the SELECT does not return (#1949), and write
  * only the attributes of the task, into its YAML as written (#1953)
  */
class DuckDbSyncSqlAttributesSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_sync_sql_attributes.db"

  lazy val duckDbConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |syncSqlWithYaml = true
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$duckDbPath"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(duckDbConfiguration) {

    private def execute(statements: String*): Unit = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        statements.foreach(conn.createStatement().execute)
      }
    }

    private def setupSource(): Unit =
      execute(
        "CREATE SCHEMA IF NOT EXISTS mydb",
        "CREATE OR REPLACE TABLE mydb.mytable(id VARCHAR, name VARCHAR, amount INTEGER)",
        "INSERT INTO mydb.mytable VALUES ('1', 'Alice', 100), ('2', 'Bob', 200)"
      )

    private val declared = List(
      TableAttribute(name = "id"),
      TableAttribute(name = "name", comment = Some("written by hand, to keep"))
    )

    private def taskPath(task: String) = new Path(
      s"$starlakeMetadataPath/transform/mydb/$task.sl.yml"
    )
    private def sqlPath(task: String) = new Path(s"$starlakeMetadataPath/transform/mydb/$task.sql")

    /** Writes the task, then reloads the project, and returns the content of its two files */
    private def writeTask(
      task: String,
      sql: String,
      attributes: List[TableAttribute] = declared,
      presql: List[String] = Nil,
      writeStrategy: WriteStrategy = WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE))
    ): (String, String) = {
      val taskInfo = AutoTaskInfo(
        name = "",
        sql = None,
        database = None,
        domain = "mydb",
        table = task,
        presql = presql,
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(writeStrategy),
        attributes = attributes
      )
      storageHandler.write(
        mapper
          .writer()
          .withAttribute(classOf[Settings], settings)
          .writeValueAsString(taskInfo),
        taskPath(task)
      )
      storageHandler.write(sql, sqlPath(task))
      settings.schemaHandler(reload = true)
      files(task)
    }

    private def files(task: String): (String, String) =
      (storageHandler.read(taskPath(task)), storageHandler.read(sqlPath(task)))

    private def sync(
      task: String,
      apply: Boolean,
      query: Option[String] = None,
      domain: String = "mydb"
    ): Try[_] =
      TransformCmd.run(
        TransformConfig(
          name = s"$domain.$task",
          syncApply = apply,
          syncPreview = !apply,
          query = query
        ),
        settings.schemaHandler()
      )

    private def attributesOf(task: String): List[TableAttribute] =
      settings.schemaHandler(reload = true).taskByFullName(s"mydb.$task").get.attributes

    "transform --sync-preview and --sync-apply" should "fail with the error of the engine, and leave the files untouched, when the SELECT cannot run" in {
      setupSource()
      List(
        // A table that does not exist
        "missing_table" -> "SELECT id, name FROM mydb.previous_state",
        // An unknown variable leaves the FROM empty
        "unknown_variable" -> "SELECT id, name FROM {{ unknown_table }}"
      ).foreach { case (task, sql) =>
        withClue(task) {
          val before = writeTask(task, sql)
          List(false, true).foreach { apply =>
            val result = sync(task, apply)
            result.isFailure shouldBe true
            result.failed.get.getMessage should include(
              s"Could not get the columns of the SELECT of task mydb.$task"
            )
            // No hint about a presql that the task does not have
            result.failed.get.getMessage should not include "--query"
            files(task) shouldBe before
          }
        }
      }
      // The error of the engine, not only the SQL that failed
      val result = sync("missing_table", apply = true)
      result.failed.get.getMessage should include("Table with name previous_state does not exist")
    }

    it should "fail on a SELECT that reads a temporary table of the presql, and point to --query" in {
      setupSource()
      val task = "with_presql"
      val before = writeTask(
        task,
        "SELECT id, name, amount FROM previous_state",
        presql = List("CREATE TEMP TABLE previous_state AS SELECT * FROM mydb.mytable")
      )
      List(false, true).foreach { apply =>
        val result = sync(task, apply)
        result.isFailure shouldBe true
        val message = result.failed.get.getMessage
        message should include("Table with name previous_state does not exist")
        message should include(
          "reads temporary tables that the presql of the task creates (previous_state)"
        )
        message should include("transform --sync-apply --query")
        files(task) shouldBe before
      }
      // The same hint for a --query that reads it
      sync(
        task,
        apply = true,
        query = Some("SELECT id FROM previous_state")
      ).failed.get.getMessage should
      include("(previous_state)")
      // No hint for a failure that has nothing to do with the presql
      val unrelated =
        sync(task, apply = true, query = Some("SELECT id FROM mydb.missing")).failed.get
      unrelated.getMessage should include("Table with name missing does not exist")
      unrelated.getMessage should not include "--query"
      // nor for a SELECT that defines it in a WITH clause
      sync(
        task,
        apply = true,
        query =
          Some("WITH previous_state AS (SELECT * FROM mydb.missing) SELECT id FROM previous_state")
      ).failed.get.getMessage should not include "--query"
      files(task) shouldBe before
      // The equivalent SELECT, which the message suggests
      sync(
        task,
        apply = true,
        query = Some(
          "WITH previous_state AS (SELECT * FROM mydb.mytable) SELECT id, name, amount FROM previous_state"
        )
      ).isSuccess shouldBe true
      attributesOf(task).map(attribute => (attribute.name, attribute.comment)) shouldBe List(
        "id"     -> None,
        "name"   -> Some("written by hand, to keep"),
        "amount" -> None
      )
      // The SQL of the task is kept
      files(task)._2 shouldBe before._2
    }

    it should "keep the SCD2 validity columns that the SELECT does not return, with their type and description" in {
      setupSource()
      val task = "scd2"
      writeTask(
        task,
        "SELECT id, name, amount FROM mydb.mytable",
        attributes = declared ++ List(
          TableAttribute(name = "obsolete"),
          TableAttribute(name = "VALID_TO", `type` = "date", comment = Some("end of validity")),
          // Declared by its rename
          TableAttribute(name = "start_date", rename = Some("valid_from"), `type` = "date")
        ),
        writeStrategy = WriteStrategy(
          `type` = Some(WriteStrategyType.SCD2),
          key = List("id"),
          startTs = Some("valid_from"),
          endTs = Some("valid_to")
        )
      )
      settings
        .schemaHandler()
        .syncPreviewSqlWithDb(s"mydb.$task", None, None)
        .map { case (attribute, status) => attribute.name -> status } shouldBe List(
        "id"         -> AttributeStatus.UNCHANGED,
        "name"       -> AttributeStatus.UNCHANGED,
        "amount"     -> AttributeStatus.ADDED,
        "VALID_TO"   -> AttributeStatus.UNCHANGED,
        "start_date" -> AttributeStatus.UNCHANGED,
        // An attribute that the SELECT no longer returns is still removed
        "obsolete" -> AttributeStatus.REMOVED
      )
      sync(task, apply = true).isSuccess shouldBe true
      attributesOf(task).map(attribute =>
        (attribute.name, attribute.`type`, attribute.comment)
      ) shouldBe
      List(
        ("id", "string", None),
        ("name", "string", Some("written by hand, to keep")),
        ("amount", "integer", None),
        ("VALID_TO", "date", Some("end of validity")),
        ("start_date", "date", None)
      )
      val synced = settings.schemaHandler().taskByFullName(s"mydb.$task").get
      // A validity column that the SELECT returns replaces the attribute that renames it
      synced
        .diffSqlAttributesWithYaml(
          List("id", "name", "amount", "valid_from").map(name => TableAttribute(name = name))
        )
        .collect {
          case (attribute, status) if attribute.getFinalName() == "valid_from" =>
            attribute.name -> status
        } shouldBe List(
        "valid_from" -> AttributeStatus.ADDED,
        "start_date" -> AttributeStatus.REMOVED
      )
      // A SELECT read as returning no column removes nothing, validity columns or not
      val error = intercept[IllegalStateException] {
        synced.diffSqlAttributesWithYaml(Nil)
      }
      error.getMessage should include(s"The SELECT of task mydb.$task returned no column")
    }

    it should "never write a task without the attributes that it declares" in {
      val task = "all_removed"
      val before = writeTask(task, "SELECT id, name FROM mydb.mytable")
      val taskInfo = settings.schemaHandler().taskByFullName(s"mydb.$task").get
      val error = intercept[IllegalStateException] {
        settings
          .schemaHandler()
          .syncApplySqlWithYaml(taskInfo, declared.map(_ -> AttributeStatus.REMOVED), None)
      }
      error.getMessage should include("would remove all its attributes")
      files(task) shouldBe before
    }

    private def jinjaPath(task: String, extension: String) =
      new Path(s"$starlakeMetadataPath/transform/jinja/$task.$extension")

    /** A task written with Jinja (#1953): its SQL imports a macro of the project and reads a
      * variable passed by `--options`, its SQL file has a presql section, and its write strategy
      * comes from the `_config.sl.yml` of its folder. Returns the content of its two files.
      */
    private def writeJinjaTask(
      task: String,
      yaml: Option[String],
      sql: String
    ): (String, String) = {
      execute("CREATE SCHEMA IF NOT EXISTS jinja")
      storageHandler.write(
        """{% macro first_day(d) -%}
          |DATE_TRUNC('month', {{ d }})
          |{%- endmacro %}
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/macros/m.sql.j2")
      )
      storageHandler.write(
        """version: 1
          |transform:
          |  default:
          |    writeStrategy:
          |      type: "OVERWRITE"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/transform/jinja/_config.sl.yml")
      )
      yaml.foreach(storageHandler.write(_, jinjaPath(task, "sl.yml")))
      storageHandler.write(sql, jinjaPath(task, "sql"))
      // The Jinja engine reads the macros from the project of the settings that it was built with
      Utils.resetJinjaClassLoader()
      // As `starlake transform --options my_date=2026-01-15,reader_sa=...`
      settings.schemaHandler(
        cliEnv = Map("my_date" -> "2026-01-15", "reader_sa" -> "user:someone@example.com")
      )
      jinjaFiles(task)
    }

    private def jinjaFiles(task: String): (String, String) = {
      val yamlPath = jinjaPath(task, "sl.yml")
      (
        if (storageHandler.exists(yamlPath)) storageHandler.read(yamlPath) else "",
        storageHandler.read(jinjaPath(task, "sql"))
      )
    }

    private val jinjaSql =
      """{%- import "macros/m.sql.j2" as m -%}
        |-- SL_PRESQL
        |CREATE TEMP TABLE prev AS SELECT DATE '{{ my_date }}' AS d
        |-- SL_SQL
        |-- Business rule: first day of the month of the application date.
        |SELECT 1 AS id, {{ m.first_day("DATE '" ~ my_date ~ "'") }} AS month_start
        |""".stripMargin

    /** The YAML of the task, with a Jinja value and comments */
    private def jinjaYaml(jinjaKey: String): String =
      s"""# Header comment: kept by hand, explains the task.
         |version: 1
         |task:
         |$jinjaKey
         |  attributes:
         |    # The key of the table
         |    - name: id
         |      type: integer
         |      comment: Hand-written description # kept
         |""".stripMargin

    // DuckDB types DATE_TRUNC as a timestamp
    private val addedMonthStart =
      """    - name: "month_start"
        |      type: "timestamp"
        |""".stripMargin

    "transform --sync-apply on a task written with Jinja" should "keep its SQL file, and every line of its YAML outside the new attributes" in {
      val task = "with_jinja"
      val (yaml, sql) = writeJinjaTask(
        task,
        Some(
          jinjaYaml(
            """  acl:
              |    - role: "roles/bigquery.dataViewer"
              |      grants:
              |        - "{{ reader_sa }}"""".stripMargin
          )
        ),
        jinjaSql
      )
      val loaded = settings.schemaHandler().taskByFullName(s"jinja.$task").get
      // The task is loaded rendered and merged with the default of its folder
      loaded.acl.flatMap(_.grants) shouldBe List("user:someone@example.com")
      loaded.presql shouldBe List("CREATE TEMP TABLE prev AS SELECT DATE '2026-01-15' AS d")
      loaded.writeStrategy.flatMap(_.`type`) shouldBe Some(WriteStrategyType.OVERWRITE)

      val list = settings.schemaHandler().syncPreviewSqlWithDb(s"jinja.$task", None, None)
      list.map { case (attribute, status) => attribute.name -> status } shouldBe List(
        "id"          -> AttributeStatus.UNCHANGED,
        "month_start" -> AttributeStatus.ADDED
      )
      sync(task, apply = true, domain = "jinja").isSuccess shouldBe true
      jinjaFiles(task) shouldBe (yaml + addedMonthStart, sql)
      // Nothing is written once the attributes are in sync
      sync(task, apply = true, domain = "jinja").isSuccess shouldBe true
      jinjaFiles(task) shouldBe (yaml + addedMonthStart, sql)
    }

    it should "write the SQL text given on purpose, as the SQL editor of the UI does" in {
      val task = "ui_sql"
      val (yaml, _) =
        writeJinjaTask(task, Some(jinjaYaml("  comment: \"Read by {{ reader_sa }}\"")), jinjaSql)
      val schemaHandler = settings.schemaHandler()
      val list = schemaHandler.syncPreviewSqlWithDb(s"jinja.$task", None, None)
      val editorSql = jinjaSql + "-- Edited in the UI\n"
      schemaHandler.syncApplySqlWithYaml(
        schemaHandler.taskByFullName(s"jinja.$task").get,
        list,
        Some(editorSql)
      )
      jinjaFiles(task) shouldBe (yaml + addedMonthStart, editorSql)
    }

    it should "create the YAML of a task that has only an SQL file, with its attributes only" in {
      val task = "sql_only"
      val (_, sql) = writeJinjaTask(task, None, jinjaSql)
      sync(task, apply = true, domain = "jinja").isSuccess shouldBe true
      jinjaFiles(task) shouldBe (
        """version: 1
          |task:
          |  attributes:
          |    - name: "id"
          |      type: "integer"
          |""".stripMargin + addedMonthStart,
        sql
      )
      val synced = settings.schemaHandler(reload = true).taskByFullName(s"jinja.$task").get
      synced.attributes.map(_.name) shouldBe List("id", "month_start")
      synced.writeStrategy.flatMap(_.`type`) shouldBe Some(WriteStrategyType.OVERWRITE)
    }

    it should "refuse to rewrite attributes written with Jinja, and leave the files untouched" in {
      val task = "jinja_attributes"
      val yaml =
        """version: 1
          |task:
          |  attributes:
          |    - name: id
          |      type: integer
          |      comment: "Read by {{ reader_sa }}"
          |""".stripMargin
      // Attributes in sync: nothing to write
      val inSync = writeJinjaTask(task, Some(yaml), "SELECT 1 AS id\n")
      sync(task, apply = true, domain = "jinja").isSuccess shouldBe true
      jinjaFiles(task) shouldBe inSync

      val before = writeJinjaTask(task, Some(yaml), jinjaSql)
      val result = sync(task, apply = true, domain = "jinja")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("its attributes use Jinja or variables")
      jinjaFiles(task) shouldBe before
      // Nothing is written, not even the SQL text given by the SQL editor of the UI
      val schemaHandler = settings.schemaHandler()
      Try(
        schemaHandler.syncApplySqlWithYaml(
          schemaHandler.taskByFullName(s"jinja.$task").get,
          schemaHandler.syncPreviewSqlWithDb(s"jinja.$task", None, None),
          Some("SELECT 2 AS id")
        )
      ).isFailure shouldBe true
      jinjaFiles(task) shouldBe before
      // A run with syncSqlWithYaml runs with the declared attributes
      new IngestionWorkflow(storageHandler, settings.schemaHandler())
        .autoJob(TransformConfig(name = s"jinja.$task", options = Map("my_date" -> "2026-01-15")))
        .isSuccess shouldBe true
      jinjaFiles(task) shouldBe before
    }

    it should "refuse a task whose files are not in the folder of its domain, without writing any" in {
      val task = "moved"
      val before = writeJinjaTask(
        task,
        Some(
          """version: 1
            |task:
            |  domain: "elsewhere"
            |  attributes:
            |    - name: id
            |      type: integer
            |""".stripMargin
        ),
        "SELECT 1 AS id, 2 AS extra\n"
      )
      val result = sync(task, apply = true, domain = "elsewhere")
      result.failed.get.getMessage should include(
        "Neither the YAML file of task elsewhere.moved nor its SQL or Python file"
      )
      jinjaFiles(task) shouldBe before
      storageHandler.exists(
        new Path(s"$starlakeMetadataPath/transform/elsewhere/moved.sl.yml")
      ) shouldBe false
    }

    it should "refuse a task declared in the _config.sl.yml of its folder, without writing a task file" in {
      storageHandler.write(
        """version: 1
          |transform:
          |  tasks:
          |    - name: "declared"
          |      sql: "SELECT 1 AS id, 2 AS extra"
          |      attributes:
          |        - name: "id"
          |          type: "integer"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/transform/inline/_config.sl.yml")
      )
      settings.schemaHandler(reload = true)
      val result = sync("declared", apply = true, domain = "inline")
      result.failed.get.getMessage should include(
        "Task inline.declared is declared in the _config.sl.yml"
      )
      storageHandler.exists(
        new Path(s"$starlakeMetadataPath/transform/inline/declared.sl.yml")
      ) shouldBe false
    }

    "A transform run with syncSqlWithYaml" should "keep the SQL file and the YAML as written, but for the new attributes, of a task written with Jinja" in {
      val task = "run_with_jinja"
      val (yaml, sql) =
        writeJinjaTask(task, Some(jinjaYaml("  comment: \"Read by {{ reader_sa }}\"")), jinjaSql)
      new IngestionWorkflow(storageHandler, settings.schemaHandler())
        .autoJob(TransformConfig(name = s"jinja.$task", options = Map("my_date" -> "2026-01-15")))
        .isSuccess shouldBe true
      jinjaFiles(task) shouldBe (yaml + addedMonthStart, sql)
    }

    it should "run with the declared attributes, and leave the YAML untouched, when the sync fails" in {
      setupSource()
      val task = "run_with_presql"
      val before = writeTask(
        task,
        "SELECT id, name FROM previous_state",
        presql = List("CREATE TEMP TABLE previous_state AS SELECT * FROM mydb.mytable")
      )
      new IngestionWorkflow(storageHandler, settings.schemaHandler())
        .autoJob(TransformConfig(name = s"mydb.$task"))
        .isSuccess shouldBe true
      files(task) shouldBe before
      attributesOf(task) shouldBe declared
    }
  }
}
