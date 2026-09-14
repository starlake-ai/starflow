package ai.starlake.job.run

import java.sql.{Connection, DriverManager, SQLException, Timestamp}
import java.time.Instant
import scala.io.Source
import scala.util.control.NonFatal

/** Run log in starlake-api's PostgreSQL.
  *
  * Targets PostgreSQL specifically: the store exists so the api can see what the CLI it spawned is
  * doing, and that database is PostgreSQL. It is not a generic multi-engine store, hence the JSONB
  * casts below.
  *
  * The api owns the schema (`run-store/postgres-schema.sql` in this jar) and its migrations, so
  * this class issues no DDL. It inserts events, updates its own run row, and selects.
  *
  * One insert per event with autocommit, never a batch: a batch still in the buffer when the JVM
  * dies is exactly the failure this feature exists to survive. Event volume is about two rows per
  * task.
  */
class JdbcRunStore(connection: Connection, correlationId: Option[String]) extends RunStore {

  def start(header: RunLogEvent): Int = {
    val sql =
      "INSERT INTO sl_run(run_id, created_at, schema_version, fingerprint, selection, options," +
      " env, correlation_id, status) VALUES (?, ?, ?, ?, CAST(? AS JSONB), CAST(? AS JSONB), ?, ?, 'RUNNING')"
    withStatement(sql) { statement =>
      statement.setString(1, header.runId)
      statement.setTimestamp(2, timestampOf(header.ts))
      statement.setInt(3, header.schemaVersion)
      statement.setString(4, header.fingerprint.getOrElse(""))
      header.selection match {
        case Some(values) => statement.setString(5, jsonArray(values))
        case None         => statement.setNull(5, java.sql.Types.VARCHAR)
      }
      header.options match {
        case Some(values) => statement.setString(6, jsonObject(values))
        case None         => statement.setNull(6, java.sql.Types.VARCHAR)
      }
      statement.setString(7, header.env.orNull)
      statement.setString(8, correlationId.orNull)
      statement.executeUpdate()
    }
    append(header)
    1
  }

  def reopen(runId: String): Int = {
    val maxAttempt = withStatement(
      "SELECT max(attempt) AS max_attempt FROM sl_run_event WHERE run_id = ?"
    ) { statement =>
      statement.setString(1, runId)
      val rs = statement.executeQuery()
      if (!rs.next()) None
      else {
        val value = rs.getInt("max_attempt")
        if (rs.wasNull()) None else Some(value)
      }
    }
    val previous =
      maxAttempt.getOrElse(throw new RunStoreException(s"Unknown run '$runId' in the run store"))
    withStatement(
      "UPDATE sl_run SET status = 'RUNNING', exit_code = NULL, finished_at = NULL WHERE run_id = ?"
    ) { statement =>
      statement.setString(1, runId)
      statement.executeUpdate()
    }
    previous + 1
  }

  def append(event: RunLogEvent): Unit = {
    val sql =
      "INSERT INTO sl_run_event(run_id, attempt, seq, ts, type, task_id, task_name, node_type," +
      " duration_millis, error_type, message, reason, payload)" +
      " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSONB))"
    withStatement(sql) { statement =>
      statement.setString(1, event.runId)
      statement.setInt(2, event.attempt)
      statement.setInt(3, event.seq)
      statement.setTimestamp(4, timestampOf(event.ts))
      statement.setString(5, event.`type`)
      statement.setString(6, event.taskId.orNull)
      statement.setString(7, event.taskName.orNull)
      statement.setString(8, event.nodeType.orNull)
      event.durationMillis match {
        case Some(millis) => statement.setLong(9, millis)
        case None         => statement.setNull(9, java.sql.Types.BIGINT)
      }
      statement.setString(10, event.errorType.orNull)
      statement.setString(11, event.message.orNull)
      statement.setString(12, event.reason.orNull)
      statement.setString(13, RunLog.toJson(event))
      statement.executeUpdate()
    }
    if (event.`type` == RunLogEventType.RunFinished) {
      // Guarded on the attempt: a replayed RunFinished from an older attempt must not mark the run
      // finished while a newer attempt (already recorded in sl_run_event) is running. sl_run.status
      // is what the api trusts, so it can only move forward with the highest attempt seen so far.
      withStatement(
        "UPDATE sl_run SET status = 'FINISHED', exit_code = ?, finished_at = ? WHERE run_id = ?" +
        " AND ? = (SELECT max(attempt) FROM sl_run_event WHERE run_id = ?)"
      ) { statement =>
        event.exitCode match {
          case Some(code) => statement.setInt(1, code)
          case None       => statement.setNull(1, java.sql.Types.INTEGER)
        }
        statement.setTimestamp(2, timestampOf(event.ts))
        statement.setString(3, event.runId)
        statement.setInt(4, event.attempt)
        statement.setString(5, event.runId)
        statement.executeUpdate()
      }
    }
  }

  def read(runId: String): Option[RunHistory] = {
    val events = withStatement(
      "SELECT payload FROM sl_run_event WHERE run_id = ? ORDER BY attempt, seq"
    ) { statement =>
      statement.setString(1, runId)
      val rs = statement.executeQuery()
      val buffer = scala.collection.mutable.ListBuffer[RunLogEvent]()
      while (rs.next()) {
        RunLog.fromJson(rs.getString("payload")) match {
          case Right(event) => buffer += event
          case Left(error) =>
            throw new RunStoreException(s"Corrupt run store payload for run '$runId': $error")
        }
      }
      buffer.toList
    }
    RunHistory.fold(events)
  }

  def latest(): Option[RunHistory] = {
    // A crash between sl_run's insert and sl_run_event's insert of start() leaves a run row with
    // no events. Taking the newest sl_run row unconditionally would fold that orphan to nothing
    // and report no run to resume, even though an older, complete run is sitting right behind it.
    val runId = withStatement(
      "SELECT r.run_id FROM sl_run r" +
      " WHERE EXISTS (SELECT 1 FROM sl_run_event e WHERE e.run_id = r.run_id)" +
      " ORDER BY r.created_at DESC, r.run_id DESC LIMIT 1"
    ) { statement =>
      val rs = statement.executeQuery()
      if (rs.next()) Some(rs.getString("run_id")) else None
    }
    runId.flatMap(read)
  }

  def close(): Unit =
    try connection.close()
    catch {
      case NonFatal(e) =>
        throw new RunStoreException(s"Cannot close the run store: ${e.getMessage}", e)
    }

  private[run] def checkSchema(): Unit =
    try withStatement("SELECT run_id FROM sl_run LIMIT 1")(_.executeQuery())
    catch {
      case e: RunStoreException =>
        throw new RunStoreException(
          "The run store tables (sl_run, sl_run_event) are missing or unreadable in the configured" +
          s" database: ${e.getMessage}. starlake-api owns this schema; create it from" +
          s" ${JdbcRunStore.SchemaResource} in the starlake jar.",
          e
        )
    }

  private def withStatement[T](sql: String)(body: java.sql.PreparedStatement => T): T = {
    val statement = connection.prepareStatement(sql)
    try body(statement)
    catch {
      case e: SQLException =>
        throw new RunStoreException(s"Run store query failed: ${e.getMessage}", e)
    } finally statement.close()
  }

  private def timestampOf(ts: String): Timestamp =
    try Timestamp.from(Instant.parse(ts))
    catch {
      case NonFatal(e) =>
        throw new RunStoreException(s"Invalid run log timestamp '$ts': ${e.getMessage}", e)
    }

  private def jsonArray(values: List[String]): String = RunLog.toJsonValue(values)

  private def jsonObject(values: Map[String, String]): String = RunLog.toJsonValue(values)
}

object JdbcRunStore {

  val SchemaResource = "run-store/postgres-schema.sql"

  def schemaDdl(): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(SchemaResource)
    if (stream == null)
      throw new RunStoreException(s"$SchemaResource is missing from the starlake jar")
    val source = Source.fromInputStream(stream, "UTF-8")
    try source.mkString
    finally source.close()
  }

  def open(
    url: String,
    user: Option[String],
    password: Option[String],
    correlationId: Option[String]
  ): JdbcRunStore = {
    val connection =
      try
        (user, password) match {
          case (Some(u), Some(p)) => DriverManager.getConnection(url, u, p)
          case _                  => DriverManager.getConnection(url)
        }
      catch {
        case e: SQLException =>
          throw new RunStoreException(
            s"Cannot open the run store at $url: ${e.getMessage}. Check the url, the credentials," +
            " and that the JDBC driver for it is on the classpath: the PostgreSQL driver is a" +
            " provided dependency and is not bundled in every starlake distribution.",
            e
          )
      }
    connection.setAutoCommit(true)
    val store = new JdbcRunStore(connection, correlationId)
    // The missing-schema path is reached whenever the api has not run its migration yet, which is
    // routine, not exceptional: leaking the connection there is how a pool runs dry.
    try {
      store.checkSchema()
      store
    } catch {
      case NonFatal(e) =>
        try connection.close()
        catch { case NonFatal(_) => () }
        throw e
    }
  }
}
