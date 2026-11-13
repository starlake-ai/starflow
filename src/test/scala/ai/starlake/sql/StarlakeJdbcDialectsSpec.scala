package ai.starlake.sql

import ai.starlake.TestHelper
import org.apache.spark.sql.jdbc.JdbcDialects

import java.nio.file.Files
import java.sql.DriverManager

class StarlakeJdbcDialectsSpec extends TestHelper {

  "the DuckDB dialect" should "connect without the options of the Spark data source (#1897)" in {
    new WithSettings() {
      StarlakeJdbcDialects.registerDialects()
      val url = s"jdbc:duckdb:${Files.createTempDirectory("duckdb-dialect").resolve("test.db")}"
      // Spark's own connections filter these options too
      JdbcDialects.get(url) shouldBe StarlakeDuckDbDialect
      val conn = DriverManager.getConnection(url)
      try conn.createStatement().execute("CREATE TABLE t AS SELECT 1 AS id, 'a' AS label")
      finally conn.close()
      // DuckDB refused query and fetchsize as unknown settings
      val rows = sparkSession.read
        .format("jdbc")
        .option("query", "SELECT id, label FROM t")
        .option("fetchsize", "10")
        .options(Map("url" -> url, "driver" -> "org.duckdb.DuckDBDriver"))
        .load()
        .collect()
      rows.map(row => (row.getInt(0), row.getString(1))) shouldBe Array((1, "a"))
    }
  }
}
