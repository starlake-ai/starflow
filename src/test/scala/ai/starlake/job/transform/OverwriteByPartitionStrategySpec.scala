package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

/** OVERWRITE_BY_PARTITION transforms (#1875): the SELECT of a task may read the partitions it
  * replaces, and the NULL partition is replaced like the others.
  *
  * @param connection
  *   the connection of the engine, whose templates the transforms run
  * @param domain
  *   the schema of the target table
  * @param createSchema
  *   false when the schema exists and the user may not create one
  */
abstract class OverwriteByPartitionStrategySpec(
  connection: String,
  domain: String,
  createSchema: Boolean = true
) extends TestHelper {

  private val table = "parts"

  def engineConfiguration: Config =
    ConfigFactory
      .parseString(s"""connectionRef: "$connection"""")
      .withFallback(super.testConfiguration)

  new WithSettings(engineConfiguration) {

    private def withConnection[T](f: java.sql.Connection => T): T =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(connection).options
      )(f)

    /** Three rows in the partition 2026-10-01, one in 2026-10-02, one in the NULL partition */
    private def setup(): Unit = withConnection { conn =>
      conn.setAutoCommit(true)
      val statement = conn.createStatement()
      if (createSchema) statement.execute(s"CREATE SCHEMA IF NOT EXISTS $domain")
      statement.execute(s"DROP TABLE IF EXISTS $domain.$table")
      statement.execute(s"CREATE TABLE $domain.$table (p DATE, k INT, v INT, note VARCHAR(10))")
      statement.execute(
        s"""INSERT INTO $domain.$table VALUES
           |(DATE '2026-10-01', 1, 100, 'a'), (DATE '2026-10-01', 2, 200, 'b'),
           |(DATE '2026-10-01', 3, 300, 'c'), (DATE '2026-10-02', 4, 400, 'd'), (NULL, 9, 900, 'e')
           |""".stripMargin
      )
      statement.close()
    }

    private def rows(): List[(Option[String], Int, Int, Option[String])] = withConnection { conn =>
      val resultSet = conn
        .createStatement()
        .executeQuery(s"SELECT p, k, v, note FROM $domain.$table ORDER BY k, v")
      val rows = scala.collection.mutable.ListBuffer[(Option[String], Int, Int, Option[String])]()
      while (resultSet.next())
        rows += (
          (
            Option(resultSet.getString("p")),
            resultSet.getInt("k"),
            resultSet.getInt("v"),
            Option(resultSet.getString("note"))
          )
        )
      rows.toList
    }

    private def transform(sql: String): Unit = {
      val task = AutoTaskInfo(
        name = "",
        sql = Some(sql),
        database = None,
        domain = domain,
        table = table,
        sink = Some(AllSinks(connectionRef = Some(connection), partition = Some(List("p")))),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE_BY_PARTITION)))
      )
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(task),
        new Path(s"$starlakeMetadataPath/transform/$domain/$table.sl.yml")
      )
      storageHandler.write(
        task.getSql(),
        new Path(s"$starlakeMetadataPath/transform/$domain/$table.sql")
      )
      val workflow = new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))
      workflow.autoJob(TransformConfig(name = s"$domain.$table")).isSuccess shouldBe true
    }

    "OVERWRITE_BY_PARTITION" should "replace the partitions that the SELECT reads" in {
      setup()
      // An untyped NULL is inserted into the text column as before
      transform(
        s"SELECT p, k, v + 1 AS v, NULL AS note FROM $domain.$table WHERE p = DATE '2026-10-01'"
      )
      rows() shouldBe List(
        (Some("2026-10-01"), 1, 101, None),
        (Some("2026-10-01"), 2, 201, None),
        (Some("2026-10-01"), 3, 301, None),
        (Some("2026-10-02"), 4, 400, Some("d")),
        (None, 9, 900, Some("e"))
      )
    }

    it should "replace the NULL partition" in {
      setup()
      transform("SELECT CAST(NULL AS DATE) AS p, 9 AS k, 901 AS v, 'f' AS note")
      rows() shouldBe List(
        (Some("2026-10-01"), 1, 100, Some("a")),
        (Some("2026-10-01"), 2, 200, Some("b")),
        (Some("2026-10-01"), 3, 300, Some("c")),
        (Some("2026-10-02"), 4, 400, Some("d")),
        (None, 9, 901, Some("f"))
      )
    }
  }
}

class DuckDbOverwriteByPartitionSpec
    extends OverwriteByPartitionStrategySpec("test-duckdb", "partitions") {
  override def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connections.test-duckdb {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/overwrite_by_partition.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.engineConfiguration)
}

class PostgresOverwriteByPartitionSpec
    extends OverwriteByPartitionStrategySpec("test-pg", "partitions")

/** MariaDB runs the MySQL templates. The user of the container may only use its database. */
class MariaDbOverwriteByPartitionSpec
    extends OverwriteByPartitionStrategySpec("test-mariadb", "starlake", createSchema = false)
