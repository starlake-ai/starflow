package ai.starlake.job.quack

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, StarlakeConfigException}
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import java.lang.ProcessBuilder.Redirect
import scala.jdk.OptionConverters._
import scala.util.{Failure, Success, Try}

object QuackCmd extends Cmd[QuackConfig] with StrictLogging {
  override def command: String = "quack"

  override def pageDescription: String =
    "Manage Quack DuckDB query servers — serve in foreground or detach as a background daemon."
  override def pageKeywords: Seq[String] =
    Seq("starlake quack", "quack server", "DuckLake", "DuckDB remote", "Quack extension")

  val parser: OParser[Unit, QuackConfig] = {
    val builder = OParser.builder[QuackConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Manage Quack DuckDB query servers.
          |Actions: serve, start, stop, list, stop-all""".stripMargin
      ),
      builder
        .arg[String]("action")
        .required()
        .action((x, c) => c.copy(action = x))
        .text("Action to perform: serve, start, stop, list, stop-all"),
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connectionName = Some(x)))
        .optional()
        .text("Connection name (required for serve, start, stop)"),
      builder
        .opt[String]("bind")
        .action((x, c) => c.copy(bind = Some(x)))
        .optional()
        .text("Bind address (default: 127.0.0.1; overrides quackBind connection option)"),
      builder
        .opt[Int]("port")
        .action((x, c) => c.copy(port = Some(x)))
        .optional()
        .text("Port (default: 9494; overrides quackPort connection option)"),
      builder
        .opt[String]("token")
        .action((x, c) => c.copy(token = Some(x)))
        .optional()
        .text("Server token (overrides quackServerToken connection option)"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  override def parse(args: Seq[String]): Option[QuackConfig] =
    OParser.parse(parser, args, QuackConfig(), setup)

  override def run(config: QuackConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    config.action.toLowerCase match {
      case "serve"    => serveAction(config)
      case "start"    => startAction(config)
      case "stop"     => stopAction(config)
      case "list"     => listAction()
      case "stop-all" => stopAllAction()
      case other      => Failure(new IllegalArgumentException(s"Unknown quack action: $other"))
    }
  }

  // ---------- serve (foreground) ----------

  private def serveAction(config: QuackConfig)(implicit settings: Settings): Try[JobResult] = {
    resolveServerInputs(config).flatMap { case (name, conn, bind, port, token) =>
      Try {
        new QuackServer(name, conn, bind, port, token).runUntilTerminated()
        QuackJobResult(
          List("connection", "bind", "port", "status"),
          List(List(name, bind, port.toString, "stopped"))
        )
      }
    }
  }

  // ---------- start (detached daemon) ----------

  private def startAction(config: QuackConfig)(implicit settings: Settings): Try[JobResult] = {
    resolveServerInputs(config).flatMap { case (name, _, bind, port, token) =>
      val existing = QuackState.read(name)
      existing.foreach { st =>
        if (java.lang.ProcessHandle.of(st.pid).isPresent) {
          return Failure(
            new StarlakeConfigException(
              s"Quack server '$name' already running on ${st.bind}:${st.port} (pid ${st.pid})"
            )
          )
        } else QuackState.delete(name)
      }

      val startedAt = System.currentTimeMillis()
      QuackState.logsDir.createDirectoryIfNotExists(createParents = true)
      val logFile = QuackState.logFile(name)
      if (logFile.exists) {
        scala.util.Try(logFile.renameTo(s"${name}.log.$startedAt")).failed.foreach { e =>
          logger.warn(s"Failed to rotate prior Quack log $logFile: ${e.getMessage}")
        }
      }

      val javaCmd =
        java.lang.ProcessHandle.current().info().command().toScala.getOrElse("java")
      val classpath = System.getProperty("java.class.path")
      val childCmd = new java.util.ArrayList[String]()
      childCmd.add(javaCmd)
      childCmd.add("-cp"); childCmd.add(classpath)
      childCmd.add("ai.starlake.job.Main")
      childCmd.add("quack"); childCmd.add("serve")
      childCmd.add("--connection"); childCmd.add(name)
      childCmd.add("--bind"); childCmd.add(bind)
      childCmd.add("--port"); childCmd.add(port.toString)
      childCmd.add("--token"); childCmd.add(token)

      val pb = new ProcessBuilder(childCmd)
      pb.redirectOutput(Redirect.to(logFile.toJava))
      pb.redirectError(Redirect.to(logFile.toJava))
      val child = pb.start()
      val pid = child.pid()

      if (!waitForBind(bind, port, 10000L, 100L)) {
        child.destroyForcibly()
        child.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)
        val tail = Try(logFile.lines.toList.takeRight(50).mkString("\n")).getOrElse("")
        return Failure(
          new RuntimeException(s"Quack server failed to bind $bind:$port:\n$tail")
        )
      }

      val state = QuackState(
        connection = name,
        pid = pid,
        bind = bind,
        port = port,
        logFile = logFile.pathAsString,
        startedAt = startedAt
      )
      QuackState.write(state)
      Success(
        QuackJobResult(
          List("connection", "pid", "bind", "port", "log"),
          List(List(name, pid.toString, bind, port.toString, logFile.pathAsString))
        )
      )
    }
  }

  private def waitForBind(host: String, port: Int, totalMs: Long, intervalMs: Long): Boolean = {
    val deadline = System.currentTimeMillis() + totalMs
    while (System.currentTimeMillis() < deadline) {
      val socket = new java.net.Socket()
      try {
        socket.connect(new java.net.InetSocketAddress(host, port), 250)
        socket.close()
        return true
      } catch {
        case _: Throwable => Thread.sleep(intervalMs)
      } finally {
        Try(socket.close())
      }
    }
    false
  }

  // ---------- stop ----------

  private def stopAction(config: QuackConfig)(implicit settings: Settings): Try[JobResult] = {
    val name = config.connectionName.getOrElse {
      return Failure(
        new IllegalArgumentException("--connection is required for the stop action")
      )
    }
    QuackState.read(name) match {
      case None =>
        Success(
          QuackJobResult(
            List("connection", "message"),
            List(List(name, s"no Quack server for connection $name"))
          )
        )
      case Some(st) =>
        java.lang.ProcessHandle.of(st.pid).ifPresent { ph =>
          ph.destroy()
          val stopped = ph
            .onExit()
            .toCompletableFuture
            .thenApply[java.lang.Boolean](_ => java.lang.Boolean.TRUE)
            .completeOnTimeout(java.lang.Boolean.FALSE, 5, java.util.concurrent.TimeUnit.SECONDS)
            .join()
          if (!stopped) ph.destroyForcibly()
        }
        QuackState.delete(name)
        Success(
          QuackJobResult(
            List("connection", "pid", "status"),
            List(List(name, st.pid.toString, "stopped"))
          )
        )
    }
  }

  // ---------- list ----------

  private def listAction()(implicit settings: Settings): Try[JobResult] = Try {
    val states = QuackState.list()
    QuackJobResult(
      List("connection", "pid", "bind", "port", "startedAt", "log"),
      states.map { st =>
        List(
          st.connection,
          st.pid.toString,
          st.bind,
          st.port.toString,
          new java.sql.Timestamp(st.startedAt).toString,
          st.logFile
        )
      }
    )
  }

  // ---------- stop-all ----------

  private def stopAllAction()(implicit settings: Settings): Try[JobResult] = Try {
    val states = QuackState.list()
    val rows = states.map { st =>
      val cfg = QuackConfig(action = "stop", connectionName = Some(st.connection))
      stopAction(cfg) match {
        case Success(_) => List(st.connection, "stopped")
        case Failure(e) => List(st.connection, s"error: ${e.getMessage}")
      }
    }
    QuackJobResult(List("connection", "status"), rows)
  }

  // ---------- input resolution ----------

  /** Resolve (name, conn, bind, port, token) or a Failure with the appropriate message. */
  private def resolveServerInputs(config: QuackConfig)(implicit
    settings: Settings
  ): Try[(String, ai.starlake.config.ConnectionInfo, String, Int, String)] = Try {
    val name = config.connectionName.getOrElse {
      throw new IllegalArgumentException("--connection is required")
    }
    val conn = settings.appConfig
      .connection(name)
      .getOrElse(throw new StarlakeConfigException(s"Connection not found: $name"))
    if (!conn.isDuckDb()) {
      throw new StarlakeConfigException(
        s"Connection $name must be a DuckDB connection (got ${conn.getJdbcEngineName()}) to host a Quack server"
      )
    }
    val bind = config.bind.getOrElse(conn.quackBind())
    val port = config.port.getOrElse(conn.quackPort())
    val token = config.token.orElse(conn.quackServerToken()).getOrElse {
      throw new StarlakeConfigException(
        "quackServerToken is required (set in connection options or pass --token)"
      )
    }
    (name, conn, bind, port, token)
  }
}

case class QuackJobResult(headers: List[String], rows: List[List[String]]) extends JobResult {
  override def prettyPrint(format: String, dryRun: Boolean = false): String =
    prettyPrint(format, headers, rows)

  override def asList(): List[List[(String, Any)]] =
    rows.map(value => headers.zip(value))
}
