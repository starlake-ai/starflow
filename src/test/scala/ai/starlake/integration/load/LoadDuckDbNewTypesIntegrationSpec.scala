package ai.starlake.integration.load

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

import scala.util.Using

/** The native DuckDB loader creates the columns of the datetime and time types with their DuckDB
  * type. A table with a binary attribute is loaded with Spark: DuckDB would not decode its base64
  * text.
  */
class LoadDuckDbNewTypesIntegrationSpec extends IntegrationTestBase {
  private val projectTemplate: File =
    File(getClass.getResource("/integration/duckdb-new-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-duckdb-new-types-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    dir
  }

  override def theSampleFolder: File = project
  override def sampleDataDir: File = projectTemplate / "sample-data"

  private def query(sql: String): List[List[String]] =
    JdbcDbUtils.withJDBCConnection(
      None,
      Map(
        "url"    -> s"jdbc:duckdb:${project.pathAsString}/datasets/duckdb.db",
        "driver" -> "org.duckdb.DuckDBDriver"
      )
    ) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        Using.resource(statement.executeQuery(sql)) { resultSet =>
          val columns = resultSet.getMetaData.getColumnCount
          Iterator
            .continually(resultSet)
            .takeWhile(_.next())
            .map(row => (1 to columns).map(row.getString).toList)
            .toList
        }
      }
    }

  private def columnTypes(table: String): List[List[String]] =
    query(
      s"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'newtypes' AND table_name = '$table' ORDER BY ordinal_position"
    )

  if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
    "A native DuckDB load" should "create the columns of the datetime and time types" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        (project / "datasets").createDirectoryIfNotExists(createParents = true)
        copyFilesToIncomingDir(sampleDataDir)
        assert(new Main().run(Array("stage")))
        assert(new Main().run(Array("load")))

        columnTypes("events") shouldBe List(
          List("id", "BIGINT"),
          List("civil", "TIMESTAMP"),
          List("wall", "TIME")
        )
        query(
          "SELECT id, CAST(civil AS VARCHAR), CAST(wall AS VARCHAR) FROM newtypes.events ORDER BY id"
        ) shouldBe List(
          List("1", "2019-12-31 23:59:02.123456", "23:59:02.123456"),
          List("2", null, null)
        )
        // Loaded with Spark, which decodes the base64 text
        columnTypes("blobs").map(_.head) shouldBe List("id", "payload")
        query("SELECT id, hex(payload) FROM newtypes.blobs ORDER BY id") shouldBe List(
          List("1", "4869"),
          List("2", null)
        )
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
