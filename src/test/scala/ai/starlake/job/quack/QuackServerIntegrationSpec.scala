package ai.starlake.job.quack

import ai.starlake.TestHelper
import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
import org.scalatest.BeforeAndAfterAll

import java.net.ServerSocket

class QuackServerIntegrationSpec extends TestHelper with BeforeAndAfterAll {
  private val enabled = sys.env.get("SL_QUACK_E2E").contains("1")

  private def freePort(): Int = {
    val s = new ServerSocket(0)
    try s.getLocalPort
    finally s.close()
  }

  new WithSettings() {

    "QuackServer" should "serve and accept a client query" in {
      assume(enabled, "Set SL_QUACK_E2E=1 to run this test")

      val port = freePort()
      val token = "test-token"
      val conn = ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map(
          "url"              -> "jdbc:duckdb:",
          "driver"           -> "org.duckdb.DuckDBDriver",
          "quackServerToken" -> token,
          "quackBind"        -> "127.0.0.1",
          "quackPort"        -> port.toString
        )
      )

      val server = new QuackServer("e2e-server", conn, "127.0.0.1", port, token)
      val thread = new Thread(() => server.runUntilTerminated(), "quack-e2e")
      thread.setDaemon(true)
      thread.start()

      // Wait for bind, max 10s
      val ok = QuackCmd_TestAccess.waitForBindForTest("127.0.0.1", port, 10000L, 100L)
      ok shouldBe true

      // Open a second DuckDB JDBC, attach as Quack client, run SELECT 1
      val clientConn =
        java.sql.DriverManager.getConnection("jdbc:duckdb:", new java.util.Properties())
      val stmt = clientConn.createStatement()
      stmt.execute("INSTALL quack;")
      stmt.execute("LOAD quack;")
      stmt.execute(s"CREATE SECRET (TYPE quack, TOKEN '$token');")
      stmt.execute(s"ATTACH 'quack:127.0.0.1:$port' AS remote;")
      val rs = stmt.executeQuery("SELECT 1 AS x")
      rs.next() shouldBe true
      rs.getInt("x") shouldBe 1
      rs.close()
      stmt.close()
      clientConn.close()

      // Trigger shutdown
      thread.interrupt()
    }
  }
}

/** Tiny test-only adapter to reach the private waitForBind method. */
private object QuackCmd_TestAccess {
  def waitForBindForTest(host: String, port: Int, totalMs: Long, intervalMs: Long): Boolean = {
    val deadline = System.currentTimeMillis() + totalMs
    while (System.currentTimeMillis() < deadline) {
      val socket = new java.net.Socket()
      try {
        socket.connect(new java.net.InetSocketAddress(host, port), 250)
        return true
      } catch {
        case _: Throwable => Thread.sleep(intervalMs)
      } finally {
        scala.util.Try(socket.close())
      }
    }
    false
  }
}
