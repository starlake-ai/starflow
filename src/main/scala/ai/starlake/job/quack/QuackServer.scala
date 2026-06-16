package ai.starlake.job.quack

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.extract.StarlakeConnectionPool
import com.typesafe.scalalogging.LazyLogging

import java.sql.Connection
import java.util.concurrent.CountDownLatch
import scala.util.{Failure, Success, Try}

/** Runs a Quack server in the current JVM, blocking until shutdown. */
class QuackServer(
  connectionName: String,
  conn: ConnectionInfo,
  bind: String,
  port: Int,
  token: String
)(implicit settings: Settings)
    extends LazyLogging {

  private val latch = new CountDownLatch(1)

  /** Block until SIGTERM / Ctrl-C / shutdown hook fires. */
  def runUntilTerminated(): Unit = {
    val jdbc: Connection =
      StarlakeConnectionPool.getConnection(dataBranch = None, conn.options)
    Try {
      val stmt = jdbc.createStatement()
      try {
        stmt.execute("INSTALL quack;")
        stmt.execute("LOAD quack;")
        val escapedToken = token.replace("'", "''")
        val serveSql =
          s"CALL quack_serve('quack:$bind:$port', token => '$escapedToken')"
        logger.info(s"Starting Quack server on $bind:$port (connection=$connectionName)")
        if (bind == "0.0.0.0") {
          logger.warn(
            "Quack server bound to 0.0.0.0 — for production use, terminate TLS at a reverse proxy."
          )
        }
        stmt.execute(serveSql)
      } finally stmt.close()
    } match {
      case Failure(e) =>
        Try(jdbc.close())
        throw e
      case Success(_) =>
    }

    Runtime.getRuntime.addShutdownHook(new Thread("quack-server-shutdown") {
      override def run(): Unit = {
        logger.info(s"Stopping Quack server on $bind:$port (connection=$connectionName)")
        Try {
          val stmt = jdbc.createStatement()
          try stmt.execute("CALL quack_shutdown();")
          finally stmt.close()
        } // ignore if quack_shutdown is unavailable
        Try(jdbc.close())
        latch.countDown()
      }
    })

    latch.await()
  }
}
