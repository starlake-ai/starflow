package ai.starlake.job.gizmo

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, StarlakeConfigException}
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import scala.util.{Failure, Success, Try}

object GizmoCmd extends Cmd[GizmoConfig] with StrictLogging {
  override def command: String = "gizmosql"

  override def pageDescription: String =
    "Run SQL queries on any database using JDBC with Spark, supporting session parameters and variable substitutions."
  override def pageKeywords: Seq[String] =
    Seq("starlake gizmosql", "SQL runner", "JDBC query", "database query")

  val parser: OParser[Unit, GizmoConfig] = {
    val builder = OParser.builder[GizmoConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Manage GizmoSQL processes.
          |Actions: start, stop, list, stop-all""".stripMargin
      ),
      builder
        .arg[String]("action")
        .required()
        .action((x, c) => c.copy(action = x))
        .text("Action to perform: start, stop, list, stop-all"),
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connectionName = Some(x)))
        .optional()
        .text("Connection name (required for start)"),
      builder
        .opt[String]("process-name")
        .action((x, c) => c.copy(processName = Some(x)))
        .optional()
        .text("Process name (required for stop)"),
      builder
        .opt[Int]("port")
        .action((x, c) => c.copy(port = Some(x)))
        .optional()
        .text("Port for the GizmoSQL process (optional, for start)"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  override def parse(args: Seq[String]): Option[GizmoConfig] =
    OParser.parse(parser, args, GizmoConfig(), setup)

  override def run(config: GizmoConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val gizmoUrl = settings.appConfig.gizmosql.url
    val gizmoApiKey = {
      val key = settings.appConfig.gizmosql.apiKey
      if (key.nonEmpty) Some(key) else None
    }
    val client = new GizmoProcessClient(gizmoUrl, gizmoApiKey)

    config.action.toLowerCase match {
      case "start" =>
        startProcess(config, client, schemaHandler)
      case "stop" =>
        stopProcess(config, client)
      case "list" =>
        listProcesses(client)
      case "stop-all" =>
        stopAllProcesses(client)
      case other =>
        Failure(new IllegalArgumentException(s"Unknown gizmo action: $other"))
    }
  }

  private def startProcess(
    config: GizmoConfig,
    client: GizmoProcessClient,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Try[JobResult] = {
    val connectionName = config.connectionName.getOrElse {
      return Failure(
        new IllegalArgumentException("--connection is required for the start action")
      )
    }

    val connection = settings.appConfig
      .connection(connectionName)
      .getOrElse(
        return Failure(new Exception(s"Connection not found: $connectionName"))
      )

    val options = connection.options
    require(
      options.getOrElse("preActions", "").contains("'ducklake:"),
      "Connection is not a Ducklake connection"
    )

    val preActions = options("preActions")
    val dbName = getDbName(preActions)
    val dataPath = options.getOrElse("SL_DATA_PATH", s"ducklake_files/$dbName")
    val finalPort = config.port.orElse(options.get("SL_DUCKDB_PORT").map(_.toInt))

    val pgHost = options.getOrElse("PG_HOST", "localhost")
    val pgPort = options.getOrElse("PG_PORT", "5432")
    val pgUser = options.getOrElse("PG_USERNAME", "")
    val pgPassword = options.getOrElse("PG_PASSWORD", "")

    val storageType = options.getOrElse("storageType", "local")
    val extraVars =
      if (Set("gcs", "s3").contains(storageType.toLowerCase)) {
        Map(
          "AWS_KEY_ID"   -> options.getOrElse("fs.s3a.access.key", ""),
          "AWS_SECRET"   -> options.getOrElse("fs.s3a.secret.key", ""),
          "AWS_REGION"   -> options.getOrElse("fs.s3a.endpoint.region", "us-east-1"),
          "AWS_ENDPOINT" -> options.getOrElse("fs.s3a.endpoint", "https://s3.amazonaws.com"),
          "STORAGE_TYPE" -> storageType,
          "AWS_SCOPE"    -> options.getOrElse("SL_DATA_PATH", dataPath)
        )
      } else {
        Map.empty[String, String]
      }

    val envVars = Map(
      "PG_HOST"            -> (if (pgHost == "localhost") "host.docker.internal" else pgHost),
      "PG_PORT"            -> pgPort,
      "PG_USERNAME"        -> pgUser,
      "PG_PASSWORD"        -> pgPassword,
      "SL_DB_ID"           -> dbName,
      "SL_DATA_PATH"       -> options.getOrElse("SL_DATA_PATH", dataPath),
      "GIZMOSQL_USERNAME"  -> "gizmosql_username",
      "GIZMOSQL_PASSWORD"  -> "gizmosql_password",
      "GIZMOSQL_LOG_LEVEL" -> "debug",
      "DATABASE_FILENAME"  -> ":memory:",
      "JWT_SECRET_KEY"     -> scala.util.Random.alphanumeric.take(32).mkString
    ) ++ extraVars

    val processName = s"$connectionName"

    val request = StartProcessRequest(processName, connectionName, finalPort, envVars)
    client.startProcess(request) match {
      case Right(response) =>
        logger.info(s"Started process ${response.processName} on port ${response.port}")
        Success(
          GizmoJobResult(
            List("processName", "connectionName", "port", "message"),
            List(
              List(
                response.processName,
                response.connectionName,
                response.port.toString,
                response.message
              )
            )
          )
        )
      case Left(error) =>
        Failure(new Exception(error.error))
    }
  }

  private def stopProcess(
    config: GizmoConfig,
    client: GizmoProcessClient
  ): Try[JobResult] = {
    val processName = config.processName.getOrElse {
      return Failure(
        new IllegalArgumentException("--process-name is required for the stop action")
      )
    }
    client.stopProcess(StopProcessRequest(processName)) match {
      case Right(response) =>
        logger.info(s"Stopped process ${response.processName}: ${response.message}")
        Success(
          GizmoJobResult(
            List("processName", "message"),
            List(List(response.processName, response.message))
          )
        )
      case Left(error) =>
        Failure(new Exception(error.error))
    }
  }

  private def listProcesses(
    client: GizmoProcessClient
  ): Try[JobResult] = {
    client.listProcesses() match {
      case Right(response) =>
        val rows = response.processes.map { p =>
          List(p.processName, p.port.toString, p.pid.map(_.toString).getOrElse("N/A"), p.status)
        }
        Success(
          GizmoJobResult(
            List("processName", "port", "pid", "status"),
            rows
          )
        )
      case Left(error) =>
        Failure(new Exception(error.error))
    }
  }

  private def stopAllProcesses(
    client: GizmoProcessClient
  ): Try[JobResult] = {
    client.stopAll() match {
      case Right(response) =>
        logger.info(s"Stopped all processes: ${response.message}")
        Success(
          GizmoJobResult(
            List("processName", "message"),
            List(List(response.processName, response.message))
          )
        )
      case Left(error) =>
        Failure(new Exception(error.error))
    }
  }

  private def getDbName(preActions: String): String = {
    preActions
      .split(";")
      .find(_.contains("USE "))
      .getOrElse(throw new StarlakeConfigException("No USE <<db>> command found in preActions"))
      .split(" ")
      .last
  }
}

case class GizmoJobResult(headers: List[String], rows: List[List[String]]) extends JobResult {
  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    prettyPrint(format, headers, rows)
  }

  override def asList(): List[List[(String, Any)]] = {
    rows.map { value => headers.zip(value) }
  }
}
