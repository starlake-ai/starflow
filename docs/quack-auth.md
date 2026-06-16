# Quack authentication & authorization callbacks

How to lock down a Quack server by replacing its default authentication and
authorization hooks. Relevant when a Quack server exposes a DuckLake (or any
database) to remote clients.

> A Quack server exposes the **full SQL surface** of the underlying DuckDB
> instance — read and write access to every table the server's session can
> see. The callbacks below are how you restrict that.

---

## The two hooks

Quack runs two separate decisions:

| Hook | Question | Fires when |
|---|---|---|
| **Authentication** | *Who* is the caller? | A new client connects (`CONNECTION_REQUEST`) |
| **Authorization** | *May they run this query?* | A client issues a query (`PREPARE_REQUEST`) |

Each is exposed as an overridable callback. Defaults are fine for local /
single-user use; override them for stricter deployments.

---

## The callback contract

Two global settings hold the **name** of the function to call:

| Setting | Default | Signature |
|---|---|---|
| `quack_authentication_function` | `quack_check_token` | `(VARCHAR, VARCHAR, VARCHAR) -> BOOLEAN` |
| `quack_authorization_function`  | `quack_nop_authorization` | `(VARCHAR, VARCHAR) -> BOOLEAN` |

Rules that apply to both:

- The function must return `BOOLEAN`. `true` admits the request; anything else
  — including a query error — rejects it (`Authentication failed` /
  `Authorization failed`). The hooks are **fail-closed**: a buggy callback
  locks everyone out.
- Set them with **`SET GLOBAL`**. The callbacks run on a fresh server-side
  connection every time, so these are global-scoped. A plain `SET` is
  forwarded to the global slot automatically. Use **`RESET GLOBAL`** to restore
  the default — a plain `RESET` only clears the session view and the auth path
  keeps reading the stale global value.
- Callbacks run in a **fresh, transient server-side connection**. They can read
  tables, call UDFs, and reference extensions, but cannot rely on
  session-local state.
- Any function with the matching arity and a `BOOLEAN` return works: built-in
  scalar functions, scalar UDFs from an extension, or SQL macros.

---

## Authentication hook

The server invokes, on every `CONNECTION_REQUEST`:

```sql
SELECT quack_authentication_function(session_id, client_token, server_token);
```

- `session_id` — server-generated random 32-char string. Becomes the
  `quack_connection_id` for that client.
- `client_token` — the token the client sent.
- `server_token` — the token configured on the server.

### Example: multi-token table

Authenticate against a table of allowed tokens, one per user:

```sql
CREATE TABLE quack_tokens (auth_token VARCHAR, user_name VARCHAR);
INSERT INTO quack_tokens VALUES
    ('alice-key-123', 'alice'),
    ('bob-key-456',   'bob');

CREATE MACRO check_token(sid, client_token, server_token) AS (
    EXISTS (SELECT 1 FROM quack_tokens WHERE auth_token = client_token)
);

SET GLOBAL quack_authentication_function = 'check_token';
```

Adding or removing users becomes a plain `INSERT` / `DELETE`.

### Example: developer mode (always allow)

For a sandboxed local environment only:

```sql
CREATE MACRO developer_mode_auth(sid, client_token, server_token) AS true;
SET GLOBAL quack_authentication_function = 'developer_mode_auth';
```

---

## Authorization hook

The server invokes, on every `PREPARE_REQUEST`:

```sql
SELECT quack_authorization_function(connection_id, query);
```

- `connection_id` — the `quack_connection_id` of the calling client (the same
  id the authentication hook saw as `session_id`).
- `query` — the full SQL text the client wants to execute.

### Example: read-only

The most common need — clients may read but not write:

```sql
CREATE MACRO read_only(sid, query) AS
    regexp_matches(upper(trim(query)), '^(SELECT|FROM|WITH|EXPLAIN|DESCRIBE|SHOW)\b');

SET GLOBAL quack_authorization_function = 'read_only';
```

`SELECT`s pass; `INSERT` / `UPDATE` are rejected at authorization time.

### Example: per-user access control list

To restrict per user, the authorization hook must know *who* is calling. The
`connection_id` argument links back to authentication's `session_id`, so the
pattern is: the auth hook records `sid -> user`, the authorization hook looks
it up.

Note: a macro body is a single expression and **cannot do DML** (no `INSERT` /
`UPDATE` / `DELETE`). So the part that *records* the session must be a scalar
UDF (see below); the lookup side can stay a macro.

```sql
-- sid -> user, populated by your auth UDF when a client connects
CREATE TABLE quack_sessions (sid VARCHAR PRIMARY KEY, user_name VARCHAR);

-- your per-user policy: which query kinds each user may run
CREATE TABLE quack_user_acls (user_name VARCHAR, query_kind VARCHAR);

CREATE MACRO acl_check(sid, query) AS (
    EXISTS (
        SELECT 1
        FROM quack_sessions s
        JOIN quack_user_acls a ON a.user_name = s.user_name
        WHERE s.sid = sid
          AND regexp_matches(upper(trim(query)), '^' || a.query_kind || '\b')
    )
);

SET GLOBAL quack_authorization_function = 'acl_check';
```

---

## Full example: per-user tokens + read-only

A server requiring per-user tokens and limiting everyone to read-only queries:

```sql
CREATE TABLE quack_tokens (auth_token VARCHAR, user_name VARCHAR);
INSERT INTO quack_tokens VALUES ('analytics-team-token', 'analytics');

CREATE MACRO check_token(sid, client_token, server_token) AS (
    EXISTS (SELECT 1 FROM quack_tokens WHERE auth_token = client_token)
);

CREATE MACRO read_only(sid, query) AS (
    regexp_matches(upper(trim(query)), '^(SELECT|FROM|WITH|EXPLAIN)\b')
);

CALL quack_serve('quack:localhost', token => 'analytics-team-token');

SET GLOBAL quack_authentication_function = 'check_token';
SET GLOBAL quack_authorization_function  = 'read_only';
```

---

## Beyond SQL macros

Macros cover most cases, but a macro is a single expression and cannot execute
DML or hold state. For policies that must record every call, maintain
in-process state, or drive imperative logic, register a scalar function via a
**DuckDB extension** instead.

- Extensions can be written in C++ (primary), or Rust / C / Go via the C
  extension API.
- The registered function must expose the same `(VARCHAR, ...) -> BOOLEAN`
  signature. Point the setting at the function name once the extension loads.
- **Python UDFs registered with `con.create_function` do not work** as
  callbacks — they are scoped to the creating connection, and Quack invokes
  each callback on a fresh server-side connection. Use an extension to make
  the function globally visible.

---

## Practical notes

- **Fail-closed.** Any error in the callback rejects the request. Test before
  relying on it, or a bug locks out all clients.
- **Know the exact `query` text.** Your regex/logic only works if it matches
  the real SQL the hook receives. Enable logging and inspect the `query` field
  on `PREPARE_REQUEST` entries:
  ```sql
  CALL enable_logging('Quack');
  -- run a client query, then:
  SELECT * FROM duckdb_logs_parsed('Quack');
  ```
  This matters when clients use `remote.query('...')` — verify whether the
  hook sees the inner SQL or a wrapper.
- **Regex-on-SQL is coarse.** Matching query *kind* (SELECT vs INSERT) is
  reliable; true table-level control by parsing arbitrary SQL with regex is
  fragile. For genuine table-level isolation, prefer limiting what the
  server's session can see (e.g. expose only specific views) and use the
  authorization hook for the read/write distinction.
- **Beta.** Quack is in beta until DuckDB v2.0; the callback API may change.
