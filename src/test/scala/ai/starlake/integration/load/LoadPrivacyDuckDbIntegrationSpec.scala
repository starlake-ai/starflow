package ai.starlake.integration.load

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.utils.Sha256
import better.files.File

/** A table with a non-SQL privacy is loaded with Spark instead of the native DuckDB loader, which
  * would load its values in clear text. A table without privacy keeps the native loader.
  */
class LoadPrivacyDuckDbIntegrationSpec extends IntegrationTestBase {
  private val projectTemplate: File =
    File(getClass.getResource("/integration/privacy-duckdb").getPath)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-privacy-duckdb-")
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
      val resultSet = connection.createStatement().executeQuery(sql)
      val columns = resultSet.getMetaData.getColumnCount
      Iterator
        .continually(resultSet)
        .takeWhile(_.next())
        .map(row => (1 to columns).map(row.getString).toList)
        .toList
    }

  if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
    "A native load of a table with privacy" should "fall back to Spark and mask the values" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        (project / "datasets").createDirectoryIfNotExists(createParents = true)
        copyFilesToIncomingDir(sampleDataDir)
        assert(new Main().run(Array("stage")))
        assert(new Main().run(Array("load")))

        query("SELECT id, email, city FROM privacy.customers ORDER BY id") shouldBe List(
          List("1", Sha256.crypt("alice@example.com", Map.empty, Nil), "Paris"),
          List("2", Sha256.crypt("bob@example.com", Map.empty, Nil), "Lyon")
        )
        query("SELECT id, email FROM privacy.plain") shouldBe List(
          List("1", "carol@example.com")
        )
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
