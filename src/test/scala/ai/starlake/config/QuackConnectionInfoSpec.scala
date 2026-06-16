package ai.starlake.config

import ai.starlake.TestHelper
import ai.starlake.schema.model.ConnectionType

class QuackConnectionInfoSpec extends TestHelper {
  new WithSettings() {

    def conn(opts: (String, String)*): ConnectionInfo =
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map("driver" -> "org.duckdb.DuckDBDriver") ++ opts.toMap
      )

    "isQuackClient" should "be true for preActions with 'quack: attach" in {
      conn(
        "url"        -> "jdbc:duckdb:",
        "preActions" -> "INSTALL quack; LOAD quack; ATTACH 'quack:host:9494' AS remote;"
      ).isQuackClient() shouldBe true
    }

    it should "be false when preActions only has 'ducklake:quack: (server-attached DuckLake bypass)" in {
      conn(
        "url"        -> "jdbc:duckdb:",
        "preActions" -> "ATTACH 'ducklake:quack:host' AS lake;"
      ).isQuackClient() shouldBe false
    }

    it should "be false when no preActions" in {
      conn("url" -> "jdbc:duckdb:").isQuackClient() shouldBe false
    }

    "isQuackServer" should "be true when quackServerToken option is set" in {
      conn(
        "url"              -> "jdbc:duckdb:",
        "quackServerToken" -> "secret"
      ).isQuackServer() shouldBe true
    }

    it should "be false when quackServerToken is absent" in {
      conn("url" -> "jdbc:duckdb:").isQuackServer() shouldBe false
    }

    "quackBind" should "default to 127.0.0.1" in {
      conn("url" -> "jdbc:duckdb:").quackBind() shouldBe "127.0.0.1"
    }

    it should "honor quackBind option" in {
      conn("url" -> "jdbc:duckdb:", "quackBind" -> "0.0.0.0").quackBind() shouldBe "0.0.0.0"
    }

    "quackPort" should "default to 9494" in {
      conn("url" -> "jdbc:duckdb:").quackPort() shouldBe 9494
    }

    it should "honor quackPort option" in {
      conn("url" -> "jdbc:duckdb:", "quackPort" -> "5555").quackPort() shouldBe 5555
    }

    "quackServerToken" should "return Some when set" in {
      conn("url" -> "jdbc:duckdb:", "quackServerToken" -> "tok").quackServerToken() shouldBe Some(
        "tok"
      )
    }

    it should "return None when absent" in {
      conn("url" -> "jdbc:duckdb:").quackServerToken() shouldBe None
    }
  }
}
