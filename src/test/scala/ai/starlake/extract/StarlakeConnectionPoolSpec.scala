package ai.starlake.extract

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
