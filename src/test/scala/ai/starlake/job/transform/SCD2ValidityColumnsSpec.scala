package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ListBuffer
import scala.util.Using

/** The validity columns of an SCD2 task on DuckDB: the first run creates the table even when the
  * SELECT returns one of them (#1884), and they take the type that the task declares or names
  * (#1885)
  */
class SCD2ValidityColumnsSpec extends TestHelper {
  private val connection = "test-duckdb"

  private def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "$connection"
           |connections.$connection {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/scd2_validity.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(engineConfiguration) {
    private def query(sql: String): List[List[String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(connection).options
      ) { conn =>
        Using.resource(conn.createStatement()) { statement =>
          Using.resource(statement.executeQuery(sql)) { resultSet =>
            val rows = ListBuffer.empty[List[String]]
            val count = resultSet.getMetaData.getColumnCount
            while (resultSet.next())
              rows += (1 to count).map(i => String.valueOf(resultSet.getObject(i))).toList
            rows.toList
          }
        }
      }

    /** Writes the task and its SQL, then runs it */
    private def run(table: String, task: String, sql: String): Boolean = {
      storageHandler.write(
        s"""version: 1
           |task:
           |$task
           |""".stripMargin,
        new Path(s"$starlakeMetadataPath/transform/dom/$table.sl.yml")
      )
      storageHandler.write(sql, new Path(s"$starlakeMetadataPath/transform/dom/$table.sql"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
        .autoJob(TransformConfig(name = s"dom.$table"))
        .isSuccess
    }

    /** The columns of the table, with their types */
    private def columns(table: String): List[(String, String)] =
      query(
        s"""SELECT column_name, data_type FROM information_schema.columns
           |WHERE table_schema = 'dom' AND table_name = '$table' ORDER BY ordinal_position""".stripMargin
      ).map { case List(name, tpe) => name -> tpe }

    private val strategy =
      """  writeStrategy:
        |    type: "SCD2"
        |    key: ["id"]
        |    timestamp: "updated_at"
        |    startTs: "valid_from"
        |    endTs: "valid_to"""".stripMargin

    private def select(label: String, day: String, extra: String = ""): String =
      s"SELECT 1 AS id, '$label' AS label, TIMESTAMP '2026-10-$day 00:00:00' AS updated_at$extra"

    /** The versions of the row: label, whether the version is open, and the type of its validity */
    private def versions(table: String): List[List[String]] =
      query(
        s"""SELECT label, valid_to IS NULL, typeof(valid_to), typeof(valid_from) FROM dom.$table
           |ORDER BY updated_at""".stripMargin
      )

    "The first run of an SCD2 task" should "create its table when the SELECT returns the start column" in {
      val table = "t_with_start"
      assert(
        run(table, strategy, select("a", "01", ", TIMESTAMP '2026-10-01 00:00:00' AS valid_from"))
      )
      columns(table).map(_._1) shouldBe List("id", "label", "updated_at", "valid_from", "valid_to")
      query(s"SELECT label FROM dom.$table") shouldBe List(List("a"))
      // The next runs insert the start column once
      assert(
        run(table, strategy, select("b", "02", ", TIMESTAMP '2026-10-02 00:00:00' AS valid_from"))
      )
      query(s"SELECT label, valid_to IS NULL FROM dom.$table ORDER BY updated_at") shouldBe List(
        List("a", "false"),
        List("b", "true")
      )
      // The first run keeps the start that the SELECT returns, the next ones write the time of the run
      query(
        s"SELECT label, valid_from = TIMESTAMP '2026-10-01 00:00:00' FROM dom.$table ORDER BY updated_at"
      ) shouldBe List(List("a", "true"), List("b", "false"))
    }

    it should "add both validity columns when the SELECT returns neither" in {
      val table = "t_without"
      assert(run(table, strategy, select("a", "01")))
      columns(table) shouldBe List(
        "id"         -> "INTEGER",
        "label"      -> "VARCHAR",
        "updated_at" -> "TIMESTAMP",
        "valid_from" -> "TIMESTAMP",
        "valid_to"   -> "TIMESTAMP"
      )
    }

    "The validity columns of an SCD2 task" should "take the type that its attributes declare" in {
      val table = "t_date"
      val task =
        s"""$strategy
           |  attributes:
           |    - name: "id"
           |      type: "int"
           |    - name: "label"
           |      type: "string"
           |    - name: "updated_at"
           |      type: "timestamp"
           |    - name: "valid_from"
           |      type: "date"
           |    - name: "valid_to"
           |      type: "date"""".stripMargin
      assert(run(table, task, select("a", "01")))
      columns(table).toMap.view.filterKeys(_.startsWith("valid")).toMap shouldBe Map(
        "valid_from" -> "DATE",
        "valid_to"   -> "DATE"
      )
      // The next run closes the version, and opens another one, at the date of the run
      assert(run(table, task, select("b", "02")))
      versions(table) shouldBe List(
        List("a", "false", "DATE", "DATE"),
        List("b", "true", "DATE", "DATE")
      )
      query(
        s"SELECT label FROM dom.$table WHERE valid_to = CURRENT_DATE OR valid_from = CURRENT_DATE"
      ) shouldBe List(List("a"), List("b"))
    }

    it should "take the type that its strategy names, when its attributes do not declare them" in {
      val table = "t_named"
      val task =
        s"""$strategy
           |    startTsType: "date"
           |    endTsType: "date"""".stripMargin
      assert(run(table, task, select("a", "01")))
      assert(run(table, task, select("b", "02")))
      versions(table) shouldBe List(
        List("a", "false", "DATE", "DATE"),
        List("b", "true", "DATE", "DATE")
      )
    }

    it should "be timestamps when the task neither declares nor names their type" in {
      val table = "t_timestamp"
      assert(run(table, strategy, select("a", "01")))
      assert(run(table, strategy, select("b", "02")))
      versions(table) shouldBe List(
        List("a", "false", "TIMESTAMP", "TIMESTAMP"),
        List("b", "true", "TIMESTAMP", "TIMESTAMP")
      )
    }

    private def task(startTsType: Option[String], validFromType: String = "date"): AutoTaskInfo =
      AutoTaskInfo(
        name = "t",
        sql = Some("SELECT 1"),
        database = None,
        domain = "dom",
        table = "t",
        python = None,
        attributes = List(
          TableAttribute("VALID_FROM", validFromType),
          TableAttribute("valid_to", "date")
        ),
        writeStrategy = Some(
          WriteStrategy(
            `type` = Some(WriteStrategyType.SCD2),
            key = List("id"),
            timestamp = Some("updated_at"),
            startTs = Some("valid_from"),
            endTs = Some("valid_to"),
            startTsType = startTsType
          )
        )
      )

    "The strategy of an SCD2 task" should "type its validity columns from their declaration, then its properties" in {
      def types(startTsType: Option[String], validFromType: String = "date") =
        task(startTsType, validFromType).getStrategy().validityColumnTypes()
      // The declaration, matched whatever the case, wins over the property
      types(None) shouldBe (PrimitiveType.date, PrimitiveType.date)
      types(Some("timestamp")) shouldBe (PrimitiveType.date, PrimitiveType.date)
      // A declaration that is not a date or a timestamp type of the project is ignored
      types(
        Some("date"),
        validFromType = "string"
      ) shouldBe (PrimitiveType.date, PrimitiveType.date)
      types(None, validFromType = "string") shouldBe (PrimitiveType.timestamp, PrimitiveType.date)
      types(
        Some("date"),
        validFromType = "unknown"
      ) shouldBe (PrimitiveType.date, PrimitiveType.date)
    }

    "validate" should "report the types of the validity columns that are not dates or timestamps" in {
      def messages(startTsType: Option[String], validFromType: String = "date") =
        task(startTsType, validFromType).checkValidity() match {
          case Left(messages) =>
            messages
              .filter(m => m.message.contains("TsType") || m.message.contains("validity column"))
              .map(m => m.severity -> m.message)
          case Right(_) => Nil
        }
      messages(None) shouldBe Nil
      messages(Some("date")) shouldBe Nil
      messages(Some("timestamp")) shouldBe List(
        Severity.Error -> "startTsType: 'timestamp' contradicts the type 'date' of the declared column valid_from in table: t"
      )
      messages(Some("string")) shouldBe List(
        Severity.Error -> "startTsType: 'string' is not a date or a timestamp type in table: t"
      )
      messages(Some("unknown")) shouldBe List(
        Severity.Error -> "startTsType: 'unknown' is not a type of the project in table: t"
      )
      // A declaration that is ignored is only a warning: projects may list the column without its type
      List("string", "unknown").foreach { validFromType =>
        messages(None, validFromType) shouldBe List(
          Severity.Warning -> s"SCD2 validity column valid_from: its declared type '$validFromType' is not a date or a timestamp type of the project, so it is ignored in table: t"
        )
      }
    }
  }
}
