# Quack Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `starlake quack` CLI (serve foreground + start/stop/list/stop-all detached) to host a Quack DuckDB query server in the Starlake JVM, plus connection-pool recognition for Quack client connections; also rename `gizmosql --process-name` → `--connection` as a breaking change.

**Architecture:** Quack client connections are plain DuckDB JDBC connections whose `preActions` install/load the `quack` extension and `ATTACH 'quack:host:port'`. The Quack server is a JVM thread that opens a DuckDB JDBC connection (running the user's `preActions` for DuckLake attach + S3 secrets), then issues `CALL quack_serve(...)` and blocks on a shutdown latch. The detached `start` action re-execs `quack serve` as a child process and tracks `{pid, bind, port}` in `$SL_ROOT/.quack/<connection>.json`.

**Tech Stack:** Scala 2.13.18, scopt for CLI, Jackson for state JSON (matches existing patterns), DuckDB JDBC driver (already a project dependency), `java.lang.ProcessBuilder` + `ProcessHandle` for daemon lifecycle.

**Scope note:** This spec covers one feature plus a small piggy-backed rename. Three repos are touched in lock-step: `starlake`, `starlake-docs`, `starlake-skills`. Each repo gets its own commits but they are part of the same logical change.

**Reference spec:** `docs/superpowers/specs/2026-05-21-quack-support-design.md`

---

## Task 0: Upgrade DuckDB to 1.5.3 (prerequisite)

**Why:** Per `docs/quack.md` § Version requirements, `quack` is a **core** DuckDB extension starting at 1.5.3 (in 1.5.2 it lives in `core_nightly`). Upgrading lets `INSTALL quack;` work out of the box without pointing at a non-default repository.

**Files:**
- Modify: `project/Versions.scala:38`
- Modify: `CHANGELOG.md` (add a bullet under `__Improvement__` in the active `-SNAPSHOT` block — see Task 11; group with the Quack changelog entry)

- [ ] **Step 1: Verify the exact 1.5.3 JDBC artifact on Maven Central**

Run:

```bash
curl -s 'https://search.maven.org/solrsearch/select?q=g:%22org.duckdb%22+AND+a:%22duckdb_jdbc%22&rows=10&wt=json' \
  | grep -oE '"v":"1\.5\.3[^"]*"' | sort -u
```

Expected: lists the published 1.5.3 patch versions (typically `1.5.3.0`). Use the latest patch.

- [ ] **Step 2: Bump the version**

In `project/Versions.scala`, change:

```scala
  val duckdb = "1.5.2.1"
```

to (substitute the exact patch version found in Step 1):

```scala
  val duckdb = "1.5.3.0"
```

- [ ] **Step 3: Compile and run a smoke-test of DuckDB-dependent tests**

Run:

```bash
sbt compile
sbt "testOnly *DuckDb* *Duckdb*"
```

Expected: compiles cleanly; existing DuckDB tests pass. If a test fails for a non-obvious reason, check the DuckDB 1.5.3 release notes for any SQL-level breaking changes (column types, default behavior on edge cases). Note any necessary follow-up before continuing the plan.

- [ ] **Step 4: Commit**

```bash
git add project/Versions.scala
git commit -m "$(cat <<'EOF'
Upgrade DuckDB JDBC to 1.5.3 (Quack as core extension)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Note: the CHANGELOG entry for this upgrade is consolidated with the Quack changelog entry in Task 11, so no separate commit there.

---

## Task 1: Add `ConnectionInfo` helper methods (TDD)

**Files:**
- Modify: `src/main/scala/ai/starlake/config/ConnectionInfo.scala` (alongside existing `isDucklake()` at line 33)
- Test: `src/test/scala/ai/starlake/config/QuackConnectionInfoSpec.scala` (new)

- [ ] **Step 1: Write failing test for `isQuackClient`, `isQuackServer`, `quackBind`, `quackPort`, `quackServerToken`**

Create `src/test/scala/ai/starlake/config/QuackConnectionInfoSpec.scala`:

```scala
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
        "url" -> "jdbc:duckdb:",
        "preActions" -> "INSTALL quack; LOAD quack; ATTACH 'quack:host:9494' AS remote;"
      ).isQuackClient() shouldBe true
    }

    it should "be false when preActions only has 'ducklake:quack: (server-attached DuckLake bypass)" in {
      conn(
        "url" -> "jdbc:duckdb:",
        "preActions" -> "ATTACH 'ducklake:quack:host' AS lake;"
      ).isQuackClient() shouldBe false
    }

    it should "be false when no preActions" in {
      conn("url" -> "jdbc:duckdb:").isQuackClient() shouldBe false
    }

    "isQuackServer" should "be true when quackServerToken option is set" in {
      conn(
        "url"               -> "jdbc:duckdb:",
        "quackServerToken"  -> "secret"
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
      conn("url" -> "jdbc:duckdb:", "quackServerToken" -> "tok").quackServerToken() shouldBe Some("tok")
    }

    it should "return None when absent" in {
      conn("url" -> "jdbc:duckdb:").quackServerToken() shouldBe None
    }
  }
}
```

- [ ] **Step 2: Run test, verify failure**

Run: `sbt "testOnly *QuackConnectionInfoSpec*"`
Expected: compile error or `value isQuackClient is not a member of ConnectionInfo`.

- [ ] **Step 3: Implement the helpers**

Edit `src/main/scala/ai/starlake/config/ConnectionInfo.scala`. Insert immediately after `isDucklake()` (currently at lines 32-34):

```scala
  @JsonIgnore
  def isQuackClient(): Boolean = {
    val pa = this.options.getOrElse("preActions", "")
    pa.contains("'quack:") && !pa.contains("'ducklake:quack:")
  }

  @JsonIgnore
  def isQuackServer(): Boolean =
    this.options.contains("quackServerToken")

  @JsonIgnore
  def quackBind(): String =
    this.options.getOrElse("quackBind", "127.0.0.1")

  @JsonIgnore
  def quackPort(): Int =
    this.options.get("quackPort").map(_.toInt).getOrElse(9494)

  @JsonIgnore
  def quackServerToken(): Option[String] =
    this.options.get("quackServerToken")
```

- [ ] **Step 4: Run test, verify pass**

Run: `sbt "testOnly *QuackConnectionInfoSpec*"`
Expected: all 10 assertions pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/config/ConnectionInfo.scala \
        src/test/scala/ai/starlake/config/QuackConnectionInfoSpec.scala
git commit -m "$(cat <<'EOF'
Add ConnectionInfo helpers for Quack client and server detection

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Generalize `StarlakeConnectionPool` ATTACH dedup (TDD)

**Files:**
- Modify: `src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala:119-151`

This is a pure refactor — the existing DuckLake path already keys the pool by the ATTACH line. We add `quack:` to the predicate and the line picker. No new test file: the existing DuckLake pool tests cover the path, and we add a small unit assertion alongside.

**Note:** This task has no isolated unit test because the pool's behavior is exercised by integration tests in Task 8. The change is a one-line predicate + one-line filter widening with a clear semantic equivalence on existing DuckLake input.

- [ ] **Step 1: Read the current dedup block**

Run: `sed -n '119,151p' src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala`
Expected: shows the `isDucklake` boolean and the `dbKey` if-branch using `.contains("ducklake:")`.

- [ ] **Step 2: Generalize the predicate and the line picker**

In `src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala`, replace:

```scala
    val isDucklake =
      connectionOptions
        .get("preActions")
        .exists(preActions => preActions.contains("ducklake:"))

    val driver = connectionOptions("driver")
    val url =
      if (isDucklake)
        "jdbc:duckdb:"
      else
        connectionOptions("url")

    if (url.contains(":duckdb:")) {
      val duckOptions = JdbcDbUtils.removeNonDuckDbProperties(connectionOptions)
      val properties = new Properties()
      duckOptions
        .foreach { case (k, v) =>
          properties.setProperty(k, v)
        }

      val dbKey =
        if (connectionOptions.get("preActions").exists(_.contains("ducklake:"))) {
          val ducklakeAttachment =
            connectionOptions("preActions")
              .split(";")
              .find(_.contains("ducklake:"))
              .getOrElse("Should never happen")
          ducklakeAttachment.replaceAll("\\s+", " ")
        } else {
          // No connection pool for duckdb. This is a single user database on write.
          // We need to release the connection asap
          url + "?" + properties.toString
        }
```

with:

```scala
    val isAttachBacked =
      connectionOptions
        .get("preActions")
        .exists(pa => pa.contains("ducklake:") || pa.contains("quack:"))

    val driver = connectionOptions("driver")
    val url =
      if (isAttachBacked)
        "jdbc:duckdb:"
      else
        connectionOptions("url")

    if (url.contains(":duckdb:")) {
      val duckOptions = JdbcDbUtils.removeNonDuckDbProperties(connectionOptions)
      val properties = new Properties()
      duckOptions
        .foreach { case (k, v) =>
          properties.setProperty(k, v)
        }

      val dbKey =
        if (isAttachBacked) {
          val attachLine =
            connectionOptions("preActions")
              .split(";")
              .find(line => line.contains("ducklake:") || line.contains("quack:"))
              .getOrElse("Should never happen")
          attachLine.replaceAll("\\s+", " ")
        } else {
          // No connection pool for duckdb. This is a single user database on write.
          // We need to release the connection asap
          url + "?" + properties.toString
        }
```

- [ ] **Step 3: Compile, run existing pool-adjacent tests**

Run: `sbt compile && sbt "testOnly *DuckDbMergeStrategy*"`
Expected: compiles; existing tests still pass (no semantic change for DuckLake input).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/ai/starlake/extract/StarlakeConnectionPool.scala
git commit -m "$(cat <<'EOF'
Widen StarlakeConnectionPool dedup to recognize Quack ATTACH lines

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Add `QuackState` and `QuackConfig` (TDD)

**Files:**
- Create: `src/main/scala/ai/starlake/job/quack/QuackState.scala`
- Create: `src/main/scala/ai/starlake/job/quack/QuackConfig.scala`
- Test: `src/test/scala/ai/starlake/job/quack/QuackStateSpec.scala`

- [ ] **Step 1: Write failing test for QuackState round-trip + state directory ops**

Create `src/test/scala/ai/starlake/job/quack/QuackStateSpec.scala`:

```scala
package ai.starlake.job.quack

import ai.starlake.TestHelper
import better.files.{File => BFile}

class QuackStateSpec extends TestHelper {
  new WithSettings() {

    "QuackState" should "round-trip through JSON" in {
      val st = QuackState(
        connection = "warehouse",
        pid        = 12345L,
        bind       = "127.0.0.1",
        port       = 9494,
        logFile    = "/tmp/warehouse.log",
        startedAt  = 1716297600000L
      )
      val json    = QuackState.toJson(st)
      val decoded = QuackState.fromJson(json)
      decoded shouldBe st
    }

    "QuackState.stateDir" should "be $SL_ROOT/.quack" in {
      QuackState.stateDir(settings).pathAsString should endWith("/.quack")
    }

    "QuackState.stateFile" should "live under stateDir keyed by connection name" in {
      QuackState.stateFile("warehouse")(settings).name shouldBe "warehouse.json"
    }

    "QuackState.list" should "skip files whose pid is dead and remove them" in {
      val dir = QuackState.stateDir(settings).createDirectoryIfNotExists(createParents = true)
      val stale = QuackState(
        connection = "ghost",
        pid        = 1L, // PID 1 (init) exists, so use a clearly-impossible value
        bind       = "127.0.0.1",
        port       = 9999,
        logFile    = "/tmp/ghost.log",
        startedAt  = 0L
      ).copy(pid = Int.MaxValue.toLong) // very unlikely to be alive
      val stalePath = dir / "ghost.json"
      stalePath.overwrite(QuackState.toJson(stale))

      val live = QuackState.list()(settings)
      live.exists(_.connection == "ghost") shouldBe false
      stalePath.exists shouldBe false
    }
  }
}
```

- [ ] **Step 2: Run test, verify failure**

Run: `sbt "testOnly *QuackStateSpec*"`
Expected: compile error — `QuackState` does not exist.

- [ ] **Step 3: Implement `QuackState`**

Create `src/main/scala/ai/starlake/job/quack/QuackState.scala`:

```scala
package ai.starlake.job.quack

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.utils.YamlSerde
import better.files.{File => BFile}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class QuackState(
  connection: String,
  pid: Long,
  bind: String,
  port: Int,
  logFile: String,
  startedAt: Long
)

object QuackState {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  def toJson(state: QuackState): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(state)

  def fromJson(json: String): QuackState =
    mapper.readValue(json, classOf[QuackState])

  def stateDir(implicit settings: Settings): BFile =
    BFile(new java.net.URI(settings.appConfig.root).getPath) / ".quack"

  def logsDir(implicit settings: Settings): BFile =
    stateDir / "logs"

  def stateFile(connection: String)(implicit settings: Settings): BFile =
    stateDir / s"$connection.json"

  def logFile(connection: String)(implicit settings: Settings): BFile =
    logsDir / s"$connection.log"

  def write(state: QuackState)(implicit settings: Settings): Unit = {
    stateDir.createDirectoryIfNotExists(createParents = true)
    stateFile(state.connection).overwrite(toJson(state))
  }

  def read(connection: String)(implicit settings: Settings): Option[QuackState] = {
    val f = stateFile(connection)
    if (f.exists) Some(fromJson(f.contentAsString)) else None
  }

  def delete(connection: String)(implicit settings: Settings): Unit = {
    val f = stateFile(connection)
    if (f.exists) f.delete()
  }

  /** Hydrate every state file and prune those whose PID is no longer alive. */
  def list()(implicit settings: Settings): List[QuackState] = {
    val dir = stateDir
    if (!dir.exists) Nil
    else {
      dir
        .glob("*.json")
        .toList
        .flatMap { f =>
          val st = fromJson(f.contentAsString)
          if (java.lang.ProcessHandle.of(st.pid).isPresent) Some(st)
          else {
            f.delete()
            None
          }
        }
    }
  }
}
```

Note on `settings.appConfig.root`: this is the configured SL_ROOT (a URI like `file:/tmp/...` or `/abs/path`). Strip the scheme via `new java.net.URI(...).getPath`. The unused `YamlSerde` import is intentionally omitted from this file (Jackson is used directly here — matches the existing pattern in `GizmoModels.scala`).

Remove the unused import line if scalafmt or compiler warns.

- [ ] **Step 4: Implement `QuackConfig`**

Create `src/main/scala/ai/starlake/job/quack/QuackConfig.scala`:

```scala
package ai.starlake.job.quack

import ai.starlake.job.ReportFormatConfig

case class QuackConfig(
  action: String = "",
  connectionName: Option[String] = None,
  bind: Option[String] = None,
  port: Option[Int] = None,
  token: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
```

- [ ] **Step 5: Run test, verify pass**

Run: `sbt "testOnly *QuackStateSpec*"`
Expected: all 4 assertions pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/job/quack/QuackState.scala \
        src/main/scala/ai/starlake/job/quack/QuackConfig.scala \
        src/test/scala/ai/starlake/job/quack/QuackStateSpec.scala
git commit -m "$(cat <<'EOF'
Add QuackState and QuackConfig models

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Implement `QuackServer` (foreground serve loop)

**Files:**
- Create: `src/main/scala/ai/starlake/job/quack/QuackServer.scala`
- Test: covered indirectly by `QuackCmdSpec` (Task 6) and the opt-in integration spec (Task 8). No isolated unit test — the entire surface is "open JDBC, issue SQL, block on latch", which is meaningful only end-to-end.

- [ ] **Step 1: Create `QuackServer.scala`**

```scala
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
```

Notes:
- `quack_shutdown` is best-effort. If the extension version doesn't expose it, closing the JDBC connection tears down the DuckDB instance and stops the server.
- Token is single-quote-escaped before SQL interpolation. Tokens with embedded quotes are rare but supported.
- The `WARN` for `0.0.0.0` echoes `docs/quack.md`.

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: compiles cleanly.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/ai/starlake/job/quack/QuackServer.scala
git commit -m "$(cat <<'EOF'
Add QuackServer: in-JVM serve loop with shutdown hook

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Implement `QuackCmd` (CLI parser + action dispatch)

**Files:**
- Create: `src/main/scala/ai/starlake/job/quack/QuackCmd.scala`

This task wires the CLI surface. Action handlers (`serve`, `start`, `stop`, `list`, `stop-all`) are implemented as private methods.

- [ ] **Step 1: Create `QuackCmd.scala`**

```scala
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
      if (logFile.exists) logFile.renameTo(s"${name}.log.$startedAt")

      val javaCmd = java.lang.ProcessHandle.current().info().command().toScala.getOrElse("java")
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
      val child   = pb.start()
      val pid     = child.pid()

      if (!waitForBind(bind, port, 10000L, 100L)) {
        child.destroyForcibly()
        child.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)
        val tail = Try(logFile.lines.toList.takeRight(50).mkString("\n")).getOrElse("")
        return Failure(
          new RuntimeException(
            s"Quack server failed to bind $bind:$port:\n$tail"
          )
        )
      }

      val state = QuackState(
        connection = name,
        pid        = pid,
        bind       = bind,
        port       = port,
        logFile    = logFile.pathAsString,
        startedAt  = startedAt
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
      return Failure(new IllegalArgumentException("--connection is required for the stop action"))
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
          if (!ph.onExit().toCompletableFuture.thenApply(_ => true).completeOnTimeout(false, 5, java.util.concurrent.TimeUnit.SECONDS).join()) {
            ph.destroyForcibly()
          }
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
    val bind  = config.bind.getOrElse(conn.quackBind())
    val port  = config.port.getOrElse(conn.quackPort())
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
```

- [ ] **Step 2: Compile**

Run: `sbt compile`
Expected: compiles cleanly. If `scala.jdk.OptionConverters._` or `better-files` imports are flagged, leave them; both are already on the project's dependency graph.

- [ ] **Step 3: Commit**

```bash
git add src/main/scala/ai/starlake/job/quack/QuackCmd.scala
git commit -m "$(cat <<'EOF'
Add QuackCmd: serve, start, stop, list, stop-all actions

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Unit-test `QuackCmd` input resolution (TDD)

**Files:**
- Test: `src/test/scala/ai/starlake/job/quack/QuackCmdSpec.scala`

The whole `serve` SQL path is integration territory (it needs a real DuckDB + Quack extension). The CLI **input resolution** is pure logic and worth unit-testing.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/job/quack/QuackCmdSpec.scala`:

```scala
package ai.starlake.job.quack

import ai.starlake.TestHelper

class QuackCmdSpec extends TestHelper {
  new WithSettings() {

    "QuackCmd.parse" should "parse 'serve --connection foo --port 9999'" in {
      val cfg = QuackCmd.parse(Seq("serve", "--connection", "foo", "--port", "9999")).get
      cfg.action shouldBe "serve"
      cfg.connectionName shouldBe Some("foo")
      cfg.port shouldBe Some(9999)
    }

    it should "parse 'stop --connection bar'" in {
      val cfg = QuackCmd.parse(Seq("stop", "--connection", "bar")).get
      cfg.action shouldBe "stop"
      cfg.connectionName shouldBe Some("bar")
    }

    "QuackCmd.run" should "fail when --connection is missing on serve" in {
      val result = QuackCmd.run(
        QuackConfig(action = "serve"),
        schemaHandler = schemaHandler()
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("--connection is required")
    }

    it should "fail when connection name is unknown" in {
      val result = QuackCmd.run(
        QuackConfig(action = "serve", connectionName = Some("does-not-exist")),
        schemaHandler = schemaHandler()
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("Connection not found: does-not-exist")
    }

    it should "list as an empty result when no servers are running" in {
      // ensure clean state dir
      val dir = QuackState.stateDir(settings)
      if (dir.exists) dir.list.toList.foreach(_.delete(swallowIOExceptions = true))

      val result = QuackCmd.run(QuackConfig(action = "list"), schemaHandler = schemaHandler())
      result.isSuccess shouldBe true
      result.get.asInstanceOf[QuackJobResult].rows shouldBe Nil
    }

    it should "succeed silently on 'stop' for an unknown connection" in {
      val result = QuackCmd.run(
        QuackConfig(action = "stop", connectionName = Some("never-started")),
        schemaHandler = schemaHandler()
      )
      result.isSuccess shouldBe true
      result.get
        .asInstanceOf[QuackJobResult]
        .rows
        .head
        .last should include("no Quack server")
    }
  }
}
```

Note on `schemaHandler()`: this is a helper already available via `TestHelper`/`WithSettings`. If the IDE flags it, the equivalent is `new SchemaHandler(settings.storageHandler())(settings)`.

- [ ] **Step 2: Run, verify pass**

Run: `sbt "testOnly *QuackCmdSpec*"`
Expected: all 6 assertions pass.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/ai/starlake/job/quack/QuackCmdSpec.scala
git commit -m "$(cat <<'EOF'
Add QuackCmdSpec covering CLI parsing and input resolution

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Register `QuackCmd` in `Main.scala`

**Files:**
- Modify: `src/main/scala/ai/starlake/job/Main.scala:19,130`

- [ ] **Step 1: Add import**

In `src/main/scala/ai/starlake/job/Main.scala`, find the line:

```scala
import ai.starlake.job.gizmo.GizmoCmd
```

(currently at line 19). Add immediately after:

```scala
import ai.starlake.job.quack.QuackCmd
```

- [ ] **Step 2: Add to commands list**

In the same file, the `commands` list ends with `GizmoCmd` at line 130. Change:

```scala
    JobCmd,
    GizmoCmd
  )
```

to:

```scala
    JobCmd,
    GizmoCmd,
    QuackCmd
  )
```

- [ ] **Step 3: Compile and smoke-check the CLI surface**

Run: `sbt compile`
Expected: compiles cleanly.

Run: `sbt "runMain ai.starlake.job.Main quack"`
Expected: prints scopt usage text mentioning `serve, start, stop, list, stop-all`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/ai/starlake/job/Main.scala
git commit -m "$(cat <<'EOF'
Register QuackCmd in Main command list

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Opt-in end-to-end integration spec

**Files:**
- Test: `src/test/scala/ai/starlake/job/quack/QuackServerIntegrationSpec.scala`

This spec exercises the real `quack` extension. It is **gated** by `SL_QUACK_E2E=1` and skipped otherwise (CI default) so that the absence of the extension never fails the build.

- [ ] **Step 1: Create the gated integration test**

```scala
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

      val port  = freePort()
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
      val clientConn = java.sql.DriverManager.getConnection("jdbc:duckdb:", new java.util.Properties())
      val stmt       = clientConn.createStatement()
      stmt.execute("INSTALL quack;")
      stmt.execute("LOAD quack;")
      stmt.execute(s"CREATE SECRET (TYPE quack, TOKEN '$token');")
      stmt.execute(s"ATTACH 'quack:127.0.0.1:$port' AS remote;")
      val rs = stmt.executeQuery("SELECT 1 AS x")
      rs.next() shouldBe true
      rs.getInt("x") shouldBe 1
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
        socket.close()
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
```

- [ ] **Step 2: Verify the test is skipped by default**

Run: `sbt "testOnly *QuackServerIntegrationSpec*"`
Expected: 1 test canceled (the `assume(enabled, ...)` cancels it without failure).

- [ ] **Step 3: (Optional, local only) run with the gate on**

Run: `SL_QUACK_E2E=1 sbt "testOnly *QuackServerIntegrationSpec*"`
Expected: PASS if the DuckDB version on the classpath ships Quack; otherwise the test fails with a clear `INSTALL quack` SQL error — that's the signal to upgrade the DuckDB driver.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/ai/starlake/job/quack/QuackServerIntegrationSpec.scala
git commit -m "$(cat <<'EOF'
Add opt-in Quack end-to-end integration spec (SL_QUACK_E2E=1)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Rename `gizmosql --process-name` → `--connection` (breaking)

**Files:**
- Modify: `src/main/scala/ai/starlake/job/gizmo/GizmoConfig.scala`
- Modify: `src/main/scala/ai/starlake/job/gizmo/GizmoCmd.scala`

- [ ] **Step 1: Drop `processName` from `GizmoConfig`**

In `src/main/scala/ai/starlake/job/gizmo/GizmoConfig.scala`, change:

```scala
case class GizmoConfig(
  action: String = "",
  connectionName: Option[String] = None,
  processName: Option[String] = None,
  port: Option[Int] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
```

to:

```scala
case class GizmoConfig(
  action: String = "",
  connectionName: Option[String] = None,
  port: Option[Int] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
```

- [ ] **Step 2: Remove `--process-name` from the parser, repoint `stop` to `--connection`**

In `src/main/scala/ai/starlake/job/gizmo/GizmoCmd.scala`:

1. Delete the scopt block (currently lines 39-43):

```scala
      builder
        .opt[String]("process-name")
        .action((x, c) => c.copy(processName = Some(x)))
        .optional()
        .text("Process name (required for stop)"),
```

2. Update the `--connection` text (currently lines 34-38) to mention it's required for both `start` and `stop`:

```scala
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connectionName = Some(x)))
        .optional()
        .text("Connection name (required for start and stop)"),
```

3. In `stopProcess`, replace:

```scala
    val processName = config.processName.getOrElse {
      return Failure(
        new IllegalArgumentException("--process-name is required for the stop action")
      )
    }
```

with:

```scala
    val processName = config.connectionName.getOrElse {
      return Failure(
        new IllegalArgumentException("--connection is required for the stop action")
      )
    }
```

4. The hard-coded `val processName = s"$connectionName"` at line 142 in `startProcess` remains valid (it's just naming for the orchestrator); no change there.

- [ ] **Step 3: Compile**

Run: `sbt compile`
Expected: compiles cleanly.

- [ ] **Step 4: Verify CLI help reflects the change**

Run: `sbt "runMain ai.starlake.job.Main gizmosql"`
Expected: usage text shows `--connection`; no `--process-name` line.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/job/gizmo/GizmoConfig.scala \
        src/main/scala/ai/starlake/job/gizmo/GizmoCmd.scala
git commit -m "$(cat <<'EOF'
Rename gizmosql --process-name to --connection (breaking change)

The processName argument was always equal to the connection name (see prior
GizmoCmd.scala line where val processName = s"$connectionName"). Collapsing
to a single flag removes the two-names-for-one-thing confusion. No deprecation
alias: scripts using --process-name must switch to --connection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Append CLI section to `docs/quack.md`

**Files:**
- Modify: `docs/quack.md`

- [ ] **Step 1: Append the section**

Append to `docs/quack.md`:

```markdown
---

## Using `starlake quack` from Starlake

Starlake ships a CLI that hosts a Quack server in the JVM (no Docker, no external orchestrator) and recognizes Quack client connections on the read side.

### Client connection (consumer)

```yaml
connections:
  warehouse-quack:
    type: JDBC
    options:
      url: "jdbc:duckdb:"
      driver: "org.duckdb.DuckDBDriver"
      preActions: |
        INSTALL quack; LOAD quack;
        CREATE SECRET (TYPE quack, TOKEN '{{quackToken}}');
        ATTACH 'quack:warehouse-host:9494' AS remote;
      quote: "\""
```

### Server connection (producer, hosts DuckLake + token)

```yaml
connections:
  warehouse-server:
    type: JDBC
    options:
      url: "jdbc:duckdb:"
      driver: "org.duckdb.DuckDBDriver"
      preActions: |
        INSTALL ducklake; LOAD ducklake; INSTALL quack; LOAD quack;
        CREATE SECRET (TYPE s3, KEY_ID '{{s3Key}}', SECRET '{{s3Secret}}', REGION 'eu-west-1');
        ATTACH 'ducklake:my_catalog.ducklake' AS lake (DATA_PATH 's3://my-bucket/data/');
      quackServerToken: "{{quackToken}}"
      quackBind: "127.0.0.1"   # optional, default 127.0.0.1
      quackPort: "9494"         # optional, default 9494
      quote: "\""
```

### CLI

```bash
starlake quack serve    --connection warehouse-server                   # foreground
starlake quack start    --connection warehouse-server                   # detached daemon
starlake quack stop     --connection warehouse-server
starlake quack list
starlake quack stop-all
```

Flags `--bind`, `--port`, `--token` override the corresponding connection options. State for detached servers lives under `$SL_ROOT/.quack/`.

> **Security:** the default `quackBind` is `127.0.0.1`. Bind to `0.0.0.0` only behind a reverse proxy terminating TLS (see the production-considerations table above).
```

- [ ] **Step 2: Commit**

```bash
git add docs/quack.md
git commit -m "$(cat <<'EOF'
Document starlake quack CLI in docs/quack.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Update `CHANGELOG.md`

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add Improvement and Breaking change entries under 1.5.16-SNAPSHOT**

In `CHANGELOG.md`, the current top block is:

```markdown
# 1.5.16-SNAPSHOT:
__Bug fix__:
- **Domain lookup with shared physical schema**: ...
```

Insert two new sub-sections immediately under the `# 1.5.16-SNAPSHOT:` heading, **before** `__Bug fix__`:

```markdown
__Improvement__:
- **Quack server CLI**: New `starlake quack` command — host a Quack DuckDB query server (exposing a DuckLake without sharing object-storage credentials with clients) directly from the Starlake JVM. Actions: `serve` (foreground), `start` (detached daemon, state under `$SL_ROOT/.quack/`), `stop`, `list`, `stop-all`. Companion client-side recognition: connections whose `preActions` contain a `'quack:` ATTACH are pooled distinctly so multiple Quack clients don't collide. Token + bind + port can be set via connection options (`quackServerToken`, `quackBind`, `quackPort`) or per invocation (`--token`, `--bind`, `--port`). Default bind is `127.0.0.1` — use a reverse proxy with TLS to expose on the network.
- **Upgrade DuckDB**: Update DuckDB JDBC to 1.5.3 so the `quack` extension is available as a core extension (it lives in `core_nightly` on 1.5.2).

__Breaking change__:
- **`gizmosql stop` flag rename**: `--process-name` is removed. Use `--connection <name>` instead (the process identifier was always equal to the connection name). Scripts using the old flag must be updated.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "$(cat <<'EOF'
Document Quack CLI and gizmosql flag rename in CHANGELOG

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: `starlake-docs` — investigate doc-gen, then add CLI page

**Repo:** `/Users/hayssams/git/public/starlake-docs/`

- [ ] **Step 1: Investigate whether CLI pages are autogenerated**

Run from the `starlake` repo root:

```bash
grep -rln 'gizmosql\|pageDescription\|generateCli\|0800-cli' src/ scripts/ 2>/dev/null
```

Expected: lists scala files (`pageDescription` is defined in `Cmd.scala`) and possibly a generator script.

Run from the `starlake-docs` repo root:

```bash
grep -rln 'gizmosql' docs/0800-cli/ docs/.scripts/ scripts/ 2>/dev/null
```

Expected outcome: either there is a generator script (in which case skip Step 2 — registering `QuackCmd` in Task 7 already wired everything), or `0800-cli/gizmosql.md` is a hand-edited file (in which case proceed with Step 2).

- [ ] **Step 2: If no generator exists, create `0800-cli/quack.md`**

Create `/Users/hayssams/git/public/starlake-docs/docs/0800-cli/quack.md`:

```markdown
---
sidebar_position: 210
title: quack
description: "Host a Quack DuckDB query server exposing a DuckLake without sharing object-storage credentials with clients."
keywords: [starlake quack, quack server, DuckLake, DuckDB remote, Quack extension]
---


## Synopsis

**starlake quack [options]**

## Description
Manage Quack DuckDB query servers.
Actions: serve, start, stop, list, stop-all

## Parameters

Parameter|Cardinality|Description
---|---|---
`action`|*Required*|Action to perform: serve, start, stop, list, stop-all
--connection `<value>`|*Optional*|Connection name (required for serve, start, stop)
--bind `<value>`|*Optional*|Bind address (default: 127.0.0.1; overrides quackBind connection option)
--port `<value>`|*Optional*|Port (default: 9494; overrides quackPort connection option)
--token `<value>`|*Optional*|Server token (overrides quackServerToken connection option)
--reportFormat `<value>`|*Optional*|Report format: console, json, html
```

- [ ] **Step 3: Update `0800-cli/gizmosql.md` for the rename**

In `/Users/hayssams/git/public/starlake-docs/docs/0800-cli/gizmosql.md`, replace:

```markdown
--process-name `<value>`|*Optional*|Process name (required for stop)
```

with:

```markdown
--connection `<value>`|*Optional*|Connection name (required for start and stop)
```

Remove the now-duplicated `--connection` row if one already exists.

- [ ] **Step 4: Add a Quack subsection to `0500-configuration/0110-connections.mdx`**

Open `/Users/hayssams/git/public/starlake-docs/docs/0500-configuration/0110-connections.mdx`. Find the existing DuckDB / DuckLake section (search for `duckdb` or `ducklake`). Add a new subsection after it:

```mdx
### Quack (DuckDB remote)

[Quack](/docs/superpowers/quack) is a DuckDB extension that turns a DuckDB instance into a query server. The recommended way to expose a DuckLake lakehouse without sharing object-storage credentials with clients.

**Client connection** (consumer, no DuckLake, no S3):

```yaml
connections:
  warehouse-quack:
    type: JDBC
    options:
      url: "jdbc:duckdb:"
      driver: "org.duckdb.DuckDBDriver"
      preActions: |
        INSTALL quack; LOAD quack;
        CREATE SECRET (TYPE quack, TOKEN '{{quackToken}}');
        ATTACH 'quack:warehouse-host:9494' AS remote;
      quote: "\""
```

**Server connection** (producer, owns DuckLake + S3 + token):

```yaml
connections:
  warehouse-server:
    type: JDBC
    options:
      url: "jdbc:duckdb:"
      driver: "org.duckdb.DuckDBDriver"
      preActions: |
        INSTALL ducklake; LOAD ducklake; INSTALL quack; LOAD quack;
        CREATE SECRET (TYPE s3, KEY_ID '{{s3Key}}', SECRET '{{s3Secret}}', REGION 'eu-west-1');
        ATTACH 'ducklake:my_catalog.ducklake' AS lake (DATA_PATH 's3://my-bucket/data/');
      quackServerToken: "{{quackToken}}"
      quackBind: "127.0.0.1"
      quackPort: "9494"
      quote: "\""
```

See [`starlake quack`](../0800-cli/quack.md) for the server-lifecycle CLI.
```

Update the page frontmatter `keywords` line to include `quack, ducklake` if not already present.

- [ ] **Step 5: Commit (in `starlake-docs` repo)**

```bash
cd /Users/hayssams/git/public/starlake-docs
git add docs/0800-cli/quack.md docs/0800-cli/gizmosql.md docs/0500-configuration/0110-connections.mdx
git commit -m "$(cat <<'EOF'
Document starlake quack CLI; rename gizmosql --process-name to --connection

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: `starlake-skills` — add Quack skill, update Gizmo skill

**Repo:** `/Users/hayssams/git/public/starlake-skills/`

- [ ] **Step 1: Create the Quack skill manifest**

Create `/Users/hayssams/git/public/starlake-skills/.agents/skills/quack/SKILL.md`:

```markdown
---
name: quack
description: Manage Quack DuckDB query servers exposing DuckLake over a thin remote protocol — serve (foreground), start/stop/list/stop-all (background)
---

# Quack Skill

Hosts a [Quack](https://duckdb.org/quack) DuckDB query server from the Starlake JVM. Quack lets a DuckDB instance expose its tables (typically backed by a DuckLake lakehouse) to remote clients **without** sharing object-storage credentials with those clients. The server owns DuckLake + S3 secrets; clients connect with a thin token and run ordinary SQL.

## Usage

```bash
starlake quack <action> [options]
```

## Actions

| Action     | Description                                                                |
|------------|----------------------------------------------------------------------------|
| `serve`    | Run a Quack server in the foreground (blocks until SIGTERM)                |
| `start`    | Spawn a Quack server as a detached daemon, tracked under `$SL_ROOT/.quack/` |
| `stop`     | Stop the server for a given connection                                     |
| `list`     | List all running Quack servers                                             |
| `stop-all` | Stop every running Quack server                                            |

## Options

- `action` (required): `serve`, `start`, `stop`, `list`, or `stop-all`
- `--connection <value>`: Connection name (required for `serve`, `start`, `stop`)
- `--bind <value>`: Bind address (default `127.0.0.1`; overrides `quackBind` connection option)
- `--port <value>`: Port (default `9494`; overrides `quackPort` connection option)
- `--token <value>`: Server token (overrides `quackServerToken` connection option)
- `--reportFormat <value>`: `console`, `json`, or `html`

## Configuration Context

### Client connection (consumer)

Reads/writes go through the server. The client connection has no DuckLake, no S3 secret.

```yaml
version: 1
application:
  connections:
    warehouse-quack:
      type: JDBC
      options:
        url: "jdbc:duckdb:"
        driver: "org.duckdb.DuckDBDriver"
        preActions: |
          INSTALL quack; LOAD quack;
          CREATE SECRET (TYPE quack, TOKEN '{{quackToken}}');
          ATTACH 'quack:warehouse-host:9494' AS remote;
        quote: "\""
```

### Server connection (producer)

Owns the DuckLake catalog, the S3 (or GCS/Azure) credentials, and the shared token.

```yaml
version: 1
application:
  connections:
    warehouse-server:
      type: JDBC
      options:
        url: "jdbc:duckdb:"
        driver: "org.duckdb.DuckDBDriver"
        preActions: |
          INSTALL ducklake; LOAD ducklake; INSTALL quack; LOAD quack;
          CREATE SECRET (TYPE s3,
              KEY_ID '{{AWS_ACCESS_KEY_ID}}',
              SECRET '{{AWS_SECRET_ACCESS_KEY}}',
              REGION '{{AWS_REGION}}');
          ATTACH 'ducklake:my_catalog.ducklake' AS lake (DATA_PATH 's3://{{S3_BUCKET}}/data/');
        quackServerToken: "{{quackToken}}"
        quackBind: "127.0.0.1"
        quackPort: "9494"
        quote: "\""
```

### Connection options consumed by `quack serve|start`

| Option              | Description                                | Default     |
|---------------------|--------------------------------------------|-------------|
| `quackServerToken`  | Required. Token clients must present.       | (required)  |
| `quackBind`         | Bind address                                | `127.0.0.1` |
| `quackPort`         | Port                                        | `9494`      |
| `preActions`        | SQL run before `quack_serve` (DuckLake attach, S3 secrets, etc.) | (recommended) |

## Examples

### Start a Quack server in the foreground

```bash
starlake quack serve --connection warehouse-server
```

### Start as a detached daemon

```bash
starlake quack start --connection warehouse-server
```

### List running servers

```bash
starlake quack list
```

Output columns: `connection`, `pid`, `bind`, `port`, `startedAt`, `log`

### Stop one server

```bash
starlake quack stop --connection warehouse-server
```

### Stop everything

```bash
starlake quack stop-all
```

### Connect from a client

```bash
starlake transform --name my_job --connection warehouse-quack
```

## Security notes

- Default bind is `127.0.0.1`. To expose on the network, set `quackBind: "0.0.0.0"` and put a TLS-terminating reverse proxy (nginx, Caddy) in front. Quack does not provide TLS on its own.
- The token never appears in log files. It does appear briefly in the child process's command line when using `start` (`ps` exposure). For stricter deployments, prefer `serve` under systemd/k8s with the token in an env-substituted YAML.

## Related Skills

- [connection](../connection/SKILL.md): Database connection configuration
- [gizmosql](../gizmosql/SKILL.md): Sibling command for the GizmoSQL/Flight server
- [settings](../settings/SKILL.md): Print settings and test connections
- [config](../config/SKILL.md): Configuration reference
```

- [ ] **Step 2: Update `gizmosql` skill — remove `--process-name`, point at `--connection`**

In `/Users/hayssams/git/public/starlake-skills/.agents/skills/gizmosql/SKILL.md`:

1. In the **Options** section, remove the line:

```markdown
- `--process-name <value>`: Process name (required for `stop`)
```

2. Update the `--connection` line to:

```markdown
- `--connection <value>`: Connection name (required for `start` and `stop`). Must reference a DuckLake connection
```

3. In the **Examples** section, replace:

```markdown
### Stop a Specific GizmoSQL Process

```bash
starlake gizmosql stop --process-name my-ducklake
```
```

with:

```markdown
### Stop a Specific GizmoSQL Process

```bash
starlake gizmosql stop --connection my-ducklake
```
```

4. Search the whole file for any remaining `--process-name` mention; remove or rewrite.

- [ ] **Step 3: Commit (in `starlake-skills` repo)**

```bash
cd /Users/hayssams/git/public/starlake-skills
git add .agents/skills/quack/SKILL.md .agents/skills/gizmosql/SKILL.md
git commit -m "$(cat <<'EOF'
Add quack skill; align gizmosql skill with --connection rename

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Final verification

- [ ] **Step 1: Compile and scalafmt-check the `starlake` repo**

Run: `sbt compile scalafmtCheck`
Expected: PASS.

- [ ] **Step 2: Run the new unit-test suites**

Run: `sbt "testOnly *QuackConnectionInfoSpec* *QuackStateSpec* *QuackCmdSpec*"`
Expected: all assertions pass; `QuackServerIntegrationSpec` is canceled (env gate).

- [ ] **Step 3: Verify CLI listing**

Run: `sbt "runMain ai.starlake.job.Main"` (or any arg-less invocation that prints the command list)
Expected: `quack` appears alongside `gizmosql` in the usage output.

- [ ] **Step 4: Verify `gizmosql stop` rejects the old flag**

Run: `sbt "runMain ai.starlake.job.Main gizmosql stop --process-name foo"`
Expected: scopt fails with `Unknown option --process-name`.

Run: `sbt "runMain ai.starlake.job.Main gizmosql stop --connection foo"`
Expected: command proceeds (will probably fail because no Gizmo orchestrator is running, but the flag is accepted).

- [ ] **Step 5: Cross-repo sanity**

```bash
ls /Users/hayssams/git/public/starlake-docs/docs/0800-cli/quack.md
ls /Users/hayssams/git/public/starlake-skills/.agents/skills/quack/SKILL.md
grep -L 'process-name' /Users/hayssams/git/public/starlake-docs/docs/0800-cli/gizmosql.md
grep -L 'process-name' /Users/hayssams/git/public/starlake-skills/.agents/skills/gizmosql/SKILL.md
```

Expected: both files exist; both `grep -L` calls print their filename (the `-L` flag lists files **without** the pattern — that's the success condition).

---

## Open implementation-time questions

These were marked in the spec as "verify at implementation time" and should be resolved by the implementer:

1. **`CALL quack_shutdown()`** — does the version of the `quack` DuckDB extension on the project's classpath expose this call? If not, the QuackServer shutdown hook should fall back to just `Try(jdbc.close())`. The spec already calls this out; Task 4's shutdown hook wraps the call in `Try` so the fallback is automatic.
2. **Doc-gen for CLI pages** — Task 12 Step 1 instructs the implementer to grep for a generator. If one exists, Step 2 of Task 12 is unnecessary because Task 7's registration of `QuackCmd` (which extends `Cmd[T]` with `pageDescription` + `pageKeywords`) feeds the generator automatically.