# Quack support in Starlake — design

**Status:** approved, ready for implementation plan
**Date:** 2026-05-21
**Owner:** Hayssam Saleh

## Goal

Let Starlake act as a Quack **client** (DuckDB JDBC reaching a remote `quack:host:port` server) and host a Quack **server** in its own JVM (analogous to the existing `gizmosql` command). Quack itself is a thin DuckDB extension that turns a DuckDB instance into a query server, and is the recommended way to expose a DuckLake lakehouse without sharing object-storage credentials with clients (see `docs/quack.md`, `docs/quack-auth.md`).

## Prerequisite: DuckDB ≥ 1.5.3

`docs/quack.md` § "Version requirements" lists DuckDB 1.5.3+ as the version where Quack ships as a core extension. The current pin (`project/Versions.scala`) must be bumped from `1.5.2.1` to `1.5.3.x` before `INSTALL quack;` works without pointing at `core_nightly`. The implementation plan starts with this bump.

## Non-goals

- Managing per-user tokens (`quack_authentication_function` with a `quack_tokens` table). Token = single shared secret per server.
- Authorization callbacks (`quack_authorization_function`). Out of scope for v1.
- Docker / containerized server lifecycle (rejected in favor of in-JVM serve).
- A new `ConnectionType.QUACK`. Reuse the existing JDBC + DuckDB driver.

## Connection model (YAML surface)

A Quack connection is a normal DuckDB JDBC connection. The intent is encoded in `preActions` (client) and a new `quackServerToken` option (server).

**Client connection** — consumer, no DuckLake, no S3 secret:

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

**Server connection** — producer, owns DuckLake + S3 secret + token:

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

## `ConnectionInfo` additions

All `@JsonIgnore`, no field changes:

| Method | Definition |
|---|---|
| `isQuackClient(): Boolean` | `preActions` contains `'quack:` and **does not** contain `'ducklake:quack:` (the docs warn against the second — it bypasses the server). |
| `isQuackServer(): Boolean` | `options.contains("quackServerToken")`. A connection can be both `isDucklake()` and `isQuackServer()`. |
| `quackBind(): String` | `options.getOrElse("quackBind", "127.0.0.1")` |
| `quackPort(): Int` | `options.get("quackPort").map(_.toInt).getOrElse(9494)` |
| `quackServerToken(): Option[String]` | `options.get("quackServerToken")` |

## `StarlakeConnectionPool` change

`getConnection` already special-cases DuckLake by deriving a stable pool key from the `'ducklake:` ATTACH line in `preActions` (`StarlakeConnectionPool.scala:119-151`). Quack client connections need the same dedup so two distinct Quack clients don't share one pool entry.

**Change:** generalize the predicate and the line picker:

```scala
val isAttachBacked =
  connectionOptions.get("preActions")
    .exists(pa => pa.contains("ducklake:") || pa.contains("quack:"))

// ...

val dbKey =
  if (isAttachBacked) {
    val attachLine =
      connectionOptions("preActions")
        .split(";")
        .find(line => line.contains("ducklake:") || line.contains("quack:"))
        .getOrElse("Should never happen")
    attachLine.replaceAll("\\s+", " ")
  } else { url + "?" + properties.toString }
```

No behavior change for DuckLake; Quack client connections now get a stable, distinct pool entry keyed by the ATTACH string.

## Server lifecycle — package layout

```
src/main/scala/ai/starlake/job/quack/
├── QuackCmd.scala         # CLI parser + dispatcher (serve/start/stop/list/stop-all)
├── QuackConfig.scala      # scopt config case class
├── QuackServer.scala      # In-JVM serve loop (quack_serve + shutdown hook)
└── QuackState.scala       # JSON state file read/write for the daemon registry
```

Registered in `Main.scala`'s command list next to `GizmoCmd`.

## `serve` — foreground in-JVM server

`starlake quack serve --connection <name> [--bind 127.0.0.1] [--port 9494] [--token <t>] [--reportFormat <fmt>]`

CLI flags override values from the connection options.

1. Resolve `ConnectionInfo` via `settings.appConfig.connection(name)`. Require `isDuckDb()`; otherwise fail with `Connection <name> must be a DuckDB connection (got <engine>) to host a Quack server`.
2. Resolve effective `bind`, `port`, `token`. Token is **required** — empty token fails with `quackServerToken is required (set in connection options or pass --token)` *before* opening JDBC.
3. Open a DuckDB JDBC connection through `StarlakeConnectionPool.getConnection(None, conn.options)`. This runs the user's `preActions` (DuckLake attach, S3 secret).
4. On that connection, execute:
   ```sql
   INSTALL quack; LOAD quack;
   CALL quack_serve('quack:<bind>:<port>', token => '<token>');
   ```
   `quack_serve` returns immediately on success (background server thread inside DuckDB). JVM must stay alive.
5. Block the main thread on a `CountDownLatch`. Install a JVM shutdown hook that:
   - Runs `CALL quack_shutdown()` (exact extension function verified at implementation time; fallback: drop the connection).
   - Closes the JDBC connection.
   - Counts down the latch.
6. SIGTERM (and Ctrl-C) trigger the shutdown hook cleanly.
7. Returns `Success(QuackJobResult(...))` only after the latch trips.

## `start` — detached daemon

`starlake quack start --connection <name> [--bind 127.0.0.1] [--port 9494] [--token <t>] [--reportFormat <fmt>]`

**State directory:** `$SL_ROOT/.quack/` (created on demand). One JSON file per running server:

```
$SL_ROOT/.quack/<connection-name>.json
$SL_ROOT/.quack/logs/<connection-name>.log
```

`QuackState`:

```scala
case class QuackState(
  connection: String,
  pid: Long,
  bind: String,
  port: Int,
  logFile: String,
  startedAt: Long
)
```

The state-file basename is the connection name (`<connection>.json`); there is no separate `name` field — see "One server per connection" below.

**Flow:**

1. Parse args (same as `serve`).
2. If `<state-dir>/<name>.json` exists and `ProcessHandle.of(pid).isPresent`, fail with `Quack server '<name>' already running on <bind>:<port> (pid <pid>)`. Otherwise remove the stale file.
3. Build the child command:
   ```
   <java> -cp <classpath> ai.starlake.job.Main quack serve \
     --connection <name> --bind <bind> --port <port> --token <token>
   ```
   `java` from `ProcessHandle.current().info().command()`; classpath from `System.getProperty("java.class.path")`.
4. `Redirect.to(logFile)` for stdout and stderr. If the prior log file exists, rename to `<name>.log.<startedAt>` before redirect.
5. Spawn detached with `val child = new ProcessBuilder(...).start()`. Capture `pid = child.pid()` for the state file and the bind probe.
6. Poll a TCP connect on `bind:port` every 100ms for up to 10s. If the port never binds, call `child.destroyForcibly()`, wait up to 2s, then fail with `Quack server failed to bind <bind>:<port>:` followed by the tail (50 lines) of the log file. Do **not** write a state file in this failure path.
7. On success, write `<state-dir>/<name>.json` with `{connection, pid, bind, port, logFile, startedAt}`. The parent exits; the `Process` handle goes out of scope but the OS process keeps running because we never called `child.waitFor()`. Return `QuackJobResult` with `connection`, `pid`, `port`, `log`.

## `stop`, `list`, `stop-all`

```
starlake quack stop --connection <name> [--reportFormat <fmt>]
starlake quack list                     [--reportFormat <fmt>]
starlake quack stop-all                 [--reportFormat <fmt>]
```

**`stop --connection <name>`:**
1. Read `<state-dir>/<name>.json`. If missing, succeed silently (`no Quack server for connection <name>`) — matches `gizmosql stop` behavior.
2. `ProcessHandle.of(pid).ifPresent(_.destroy())`. Wait up to 5s; if still alive, `destroyForcibly()`.
3. Delete the state file.

**`list`:** glob `<state-dir>/*.json`, hydrate each, drop any whose pid is dead (and remove their state files), print rows `[connection, pid, bind, port, started, log]`.

**`stop-all`:** `list` then iterate `stop --connection <name>` for each entry.

## One server per connection (process identity)

The state-file key, log filename, and `stop` argument all use the **connection name** — there is no separate "process name" concept. `GizmoCmd` introduced a `--process-name` flag but then hard-coded `processName = connectionName` at `GizmoCmd.scala:142`, leaving two names for one thing. This spec collapses that to a single `--connection` flag everywhere (`start`, `stop`, `serve`), keeping the mental model: *one Quack server per connection at a time*. Users wanting two servers from one DuckLake catalog define two connections (one per port).

## Scope addendum: align `gizmosql` flags (breaking change)

The same "two names for one thing" problem exists in `gizmosql`. This spec scopes the rename in as a **breaking change** — no deprecated alias, no backward-compat shim.

**Changes to `GizmoCmd`:**

- Replace `--process-name` with `--connection` on `stop` (and any other action that today takes `--process-name`). The old flag stops being recognized.
- Drop the separate `processName` field — `GizmoCmd.scala:142` (`val processName = s"$connectionName"`) becomes pointless. Wire `stop` directly off the connection name.
- `GizmoProcessClient`'s wire types keep using the existing `processName` field name internally if changing it would ripple into the orchestrator service; that's an implementation detail, not user-facing.

**Release-note framing:** CHANGELOG entry under `__Breaking change__` (new subsection if none exists in the current `-SNAPSHOT` block):
> `gizmosql stop` now takes `--connection <name>` instead of `--process-name <name>`. Scripts using the old flag must be updated.

**Cross-repo follow-through:**

- `starlake-docs/docs/0800-cli/gizmosql.md` — update the Parameters table: replace the `--process-name` row with `--connection` (required for `stop`).
- `starlake-skills/.agents/skills/gizmosql/SKILL.md` — change the Options section to list `--connection` for `stop`, remove every `--process-name` mention, update Examples (`starlake gizmosql stop --connection my-ducklake`).
- No installer change required in `starlake-skills`.

## Error matrix

| Failure | Detection point | Message |
|---|---|---|
| Connection name unknown | `serve`/`start` arg resolution | `Connection not found: <name>` |
| Connection is not DuckDB | `serve` after lookup | `Connection <name> must be a DuckDB connection (got <engine>) to host a Quack server` |
| No token resolved | `serve` before opening JDBC | `quackServerToken is required (set in connection options or pass --token)` |
| `INSTALL quack` fails | inside `serve` | propagate JDBC exception, suggest DuckDB ≥ 1.5.2 |
| Port already in use | `start` post-spawn TCP probe | kill child, fail with `Quack server failed to bind <bind>:<port>:` + last 50 log lines |
| `start` with stale state file | `start` precheck | remove stale file, debug-log, continue |
| `stop` with missing/dead pid | `stop` | succeed silently, remove state file |

## Observability

- `LazyLogging` for lifecycle events: `INFO` for start/stop, `DEBUG` for SQL executed (token redacted via the same predicate already used at `StarlakeConnectionPool.scala:94-98` — keys containing `password`, `token`, `sl_access_token` are masked).
- `list` returns a `QuackJobResult extends JobResult` with `prettyPrint`/`asList`, so `--reportFormat csv|json|table` works for free.

## Security defaults & guardrails

- `quackBind` defaults to `127.0.0.1`. Invocations with `bind = 0.0.0.0` emit a `WARN` recommending a reverse proxy + TLS (echoes `docs/quack.md` production-considerations table).
- Token never echoed to stdout or logfile.
- `serve` fails fast before opening JDBC if no token is resolved.
- Child process spawned by `start` inherits the parent's environment, mirroring `GizmoCmd`'s carry-forward pattern. State files in `$SL_ROOT/.quack/` contain **no** token (token is only in the child's command line; `ps` exposure is a known limitation of the foreground/detached split and is documented in `docs/quack.md`).

## Tests

- `QuackCmdSpec` (unit, no Docker): mock the JDBC connection, assert `serve` issues `INSTALL quack; LOAD quack; CALL quack_serve(...)` with the expected `bind`/`port`/`token` and installs a shutdown hook.
- `QuackStateSpec` (unit): round-trip JSON, stale-pid pruning, glob behavior.
- `QuackServerIntegrationSpec` (opt-in, gated by `SL_QUACK_E2E=1` env flag): start an actual server on `127.0.0.1:<random-free-port>`, attach a second in-JVM DuckDB as a Quack client, run `SELECT 1` through the remote, stop, assert state file removed. Skipped if the `quack` extension is unavailable on the CI runner.

## Cross-repository updates

Three repos to touch in lock-step:

### 1. `starlake` (this repo)

- `docs/quack.md` (existing): append a "Using `starlake quack` from Starlake" section pointing at the new CLI and showing the client/server connection-options examples from this spec.
- `CHANGELOG.md`: new entry under the active `-SNAPSHOT` block, under both `__Improvement__` (the CLI + client recognition) and any follow-up bug fixes.

### 2. `starlake-docs` — `/Users/hayssams/git/public/starlake-docs/docs/`

- **`0800-cli/quack.md`** — new CLI reference, matching the shape of the existing `0800-cli/gizmosql.md` (frontmatter: `sidebar_position`, `title: quack`, `description`, `keywords`). One Synopsis, one Description, one Parameters table covering: `action`, `--connection` (required for `serve`/`start`/`stop`), `--bind`, `--port`, `--token`, `--reportFormat`.
- **`0500-configuration/0110-connections.mdx`** — add a "Quack (DuckDB remote)" subsection alongside the existing DuckDB / DuckLake guidance, with the two YAML snippets (client and server) from the Connection model section of this spec. Update `keywords` to include `quack, ducklake`.

**Verify before writing the CLI page by hand:** check whether `0800-cli/gizmosql.md` is autogenerated. Run `grep -rl 'gizmosql' starlake-docs/scripts/ starlake-docs/sidebars*` and `grep -rl 'gizmosql\.md\|cliDoc\|generateCli\|pageDescription' starlake/src/main/scala/`. If a generator exists, register `QuackCmd` the same way (via `pageDescription`/`pageKeywords` already implied by extending `Cmd[T]`) and the page will appear automatically. Otherwise create `0800-cli/quack.md` manually with the shape above.

### 3. `starlake-skills` — `/Users/hayssams/git/public/starlake-skills/.agents/skills/`

- **`quack/SKILL.md`** — new skill directory + manifest. Mirror the `gizmosql/SKILL.md` shape:
  - Frontmatter: `name: quack`, `description: Manage Quack DuckDB query servers exposing DuckLake over a thin remote protocol — serve (foreground), start/stop/list/stop-all (background)`.
  - Sections: Usage, Actions (`serve`, `start`, `stop`, `list`, `stop-all`), Options, Configuration Context (client connection example + server connection example), Cloud Storage Data Path Examples (server side: GCS/S3/Azure DuckLake + Quack), Examples, Related Skills (link `connection`, `gizmosql`, `settings`, `config`).
- **No installer change required** — the existing `scripts/install.sh` / `scripts/install.ps1` iterate `.agents/skills/*/` and symlink the new `quack/` directory automatically.

## Out-of-scope / future work

- Per-user tokens (`quack_tokens` table + `quack_authentication_function`).
- Authorization callbacks (`quack_authorization_function`, read-only mode, table-scoped ACLs).
- TLS termination inside Starlake (production guidance stays: terminate TLS at a reverse proxy).
- Multi-server-per-connection (a separate connection per server is the workaround).
