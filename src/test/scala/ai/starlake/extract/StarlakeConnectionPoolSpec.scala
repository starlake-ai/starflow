package ai.starlake.extract

import org.duckdb.DuckDBDriver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import scala.jdk.CollectionConverters._

class StarlakeConnectionPoolSpec extends AnyFlatSpec with Matchers {

  private def options(db: String): Map[String, String] =
    Map(
      "url"    -> s"jdbc:duckdb:/tmp/pool_spec_$db.duckdb",
      "driver" -> "org.duckdb.DuckDBDriver"
    )

  "clearDuckdbPool with a predicate" should "close only the matching entries" in {
    StarlakeConnectionPool.clearDuckdbPool() // start from a clean pool
    val connA = StarlakeConnectionPool.getConnection(None, options("scoped_a"))
    val connB = StarlakeConnectionPool.getConnection(None, options("scoped_b"))

    val cleared = StarlakeConnectionPool.clearDuckdbPool(_.contains("pool_spec_scoped_a"))
    cleared shouldBe 1

    // the untouched entry still serves queries
    val rs = connB.createStatement().executeQuery("select 42")
    rs.next() shouldBe true
    rs.getInt(1) shouldBe 42
    connB.close()
    connA.close()

    // only the surviving entry remains in the pool
    StarlakeConnectionPool.clearDuckdbPool(_ => true) shouldBe 1
  }

  "shouldPreloadDuckDb" should "be true only for a transform running on DuckDB" in {
    val cases = List(
      ("transform", Some("jdbc:duckdb:"), true),
      ("job", Some("jdbc:duckdb:/data/lake.duckdb"), true),
      ("transform", Some("jdbc:postgresql://db/x"), false),
      ("transform", Some("jdbc:arrow-flight-sql://qod:31338"), false),
      ("transform", None, false),
      ("load", Some("jdbc:duckdb:"), false),
      ("validate", Some("jdbc:duckdb:"), false)
    )
    cases.foreach { case (command, url, expected) =>
      withClue(s"$command on $url: ") {
        StarlakeConnectionPool.shouldPreloadDuckDb(command, url) shouldBe expected
      }
    }
  }

  "getConnection" should "open DuckDB without DriverManager, whose scan loads every JDBC driver" in {
    val registered = DriverManager.getDrivers.asScala.collect { case d: DuckDBDriver => d }.toList
    registered.foreach(DriverManager.deregisterDriver)
    StarlakeConnectionPool.clearDuckdbPool()
    try {
      val conn = StarlakeConnectionPool.getConnection(None, options("no_scan"))
      val rs = conn.createStatement().executeQuery("select 42")
      rs.next() shouldBe true
      rs.getInt(1) shouldBe 42
      conn.close()
    } finally {
      StarlakeConnectionPool.clearDuckdbPool()
      registered.foreach(DriverManager.registerDriver)
    }
  }

  "withQodToken" should "add the QoD token to a tokenless Flight SQL connection" in {
    val flight = Map("url" -> "jdbc:arrow-flight-sql://qod:31338", "pool" -> "bi")
    StarlakeConnectionPool.withQodToken(flight, Some("t")) shouldBe flight + ("token" -> "t")
  }

  it should "keep an explicit token" in {
    val flight = Map("url" -> "jdbc:arrow-flight-sql://qod:31338", "token" -> "own")
    StarlakeConnectionPool.withQodToken(flight, Some("t")) shouldBe flight
  }

  it should "leave connections with stored credentials to authenticate as their own user" in {
    val flight =
      Map("url" -> "jdbc:arrow-flight-sql://qod:31338", "user" -> "u", "password" -> "p")
    StarlakeConnectionPool.withQodToken(flight, Some("t")) shouldBe flight
  }

  it should "ignore non Flight SQL connections and a missing token" in {
    StarlakeConnectionPool.withQodToken(options("x"), Some("t")) shouldBe options("x")
    val flight = Map("url" -> "jdbc:arrow-flight-sql://qod:31338")
    StarlakeConnectionPool.withQodToken(flight, None) shouldBe flight
  }
}
