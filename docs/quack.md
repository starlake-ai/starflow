# Quack server over DuckLake, with file-isolated ODBC clients

A setup guide for serving a DuckLake lakehouse through a Quack server so that
ODBC clients can query and write data **without ever accessing the underlying
data files** (Parquet / object storage).

---

## Goal

- A **Quack server** owns a **DuckLake** (catalog + Parquet data).
- **Clients** connect over **ODBC** and run ordinary SQL.
- Clients **never** touch the Parquet files, never hold object-storage
  credentials, and never even know DuckLake is involved.

---

## The key decision

DuckLake normally splits work in two: the *catalog* holds metadata, but each
*client* reads and writes the Parquet **data files directly** from object
storage. That stays true even when the catalog is reached over Quack.

> **Do NOT** attach the DuckLake from the client with
> `ATTACH 'ducklake:quack:host'`. That makes the client a full DuckLake
> participant — it *will* open the Parquet files itself.

Instead, attach the DuckLake **server-side** and expose it as a plain Quack
query server. The client attaches as a plain `quack:` remote and sees only
ordinary remote tables. DuckLake, Parquet, and credentials become a server-side
implementation detail.

---

## Architecture

```
ODBC application
      |  (ODBC)
      v
DuckDB ODBC driver  ......  Quack client, NO file access
      |
      |  Quack / HTTP   <-- the only connection leaving the client
      v
+-----------------------------------------------+
|  Quack server host  (sole access to files)    |
|                                               |
|   DuckDB server  -- runs quack_serve()         |
|        |                                      |
|        v                                      |
|   DuckLake       -- attached here, catalog     |
|        |                                      |
|        v                                      |
|   Object storage -- Parquet files + S3 creds   |
+-----------------------------------------------+
```

Because the client attaches with `quack:` (not `ducklake:quack:`), it never
learns that the remote tables are backed by DuckLake or Parquet.

---

## Server setup (the Quack server host)

One DuckDB instance owns the DuckLake and serves it. Requires a DuckDB engine
with Quack available (1.5.2+, or 1.5.3 where Quack is a core extension).

```sql
INSTALL ducklake; INSTALL quack;
LOAD ducklake;  LOAD quack;

-- object-storage credentials live ONLY on the server
CREATE SECRET (
    TYPE s3,
    KEY_ID '...',
    SECRET '...',
    REGION '...'
);

-- attach the DuckLake here, server-side
ATTACH 'ducklake:my_catalog.ducklake' AS lake
    (DATA_PATH 's3://my-bucket/data/');

-- start serving (bind beyond localhost only behind a proxy — see below)
CALL quack_serve('quack:0.0.0.0', token => 'super_secret');
```

The DuckLake catalog can be a local DuckDB file (`my_catalog.ducklake` above),
because only this one server attaches it. A networked catalog such as
PostgreSQL is only needed if you later run multiple Quack servers.

### Alternative: PostgreSQL catalog

Swap the local `.ducklake` file for a Postgres connection string when you need
a shared catalog (e.g. multiple Quack servers fronting the same lake):

```sql
INSTALL ducklake; INSTALL postgres; INSTALL quack;
LOAD ducklake;    LOAD postgres;    LOAD quack;

-- catalog credentials stay on the server alongside the S3 secret
CREATE SECRET pg_catalog (
    TYPE postgres,
    HOST 'pg-host',
    PORT 5432,
    DATABASE 'ducklake_catalog',
    USER 'lake',
    PASSWORD '...'
);
CREATE SECRET s3_lake (
    TYPE s3,
    KEY_ID '...',
    SECRET '...',
    REGION '...'
);

ATTACH 'ducklake:postgres:dbname=ducklake_catalog' AS lake
    (DATA_PATH 's3://my-bucket/data/');

CALL quack_serve('quack:0.0.0.0', token => 'super_secret');
```

Clients are unchanged — they still attach as `quack:server-host:9494` and never
learn the catalog backend.

> **On secret names:** ATTACH never references `pg_catalog` or `s3_lake`.
> DuckDB resolves secrets by *scope* (matching `TYPE` + URL/host) — the names
> are management labels for `DROP SECRET` and `duckdb_secrets()`. Add an
> explicit `SCOPE` clause only when two secrets of the same type would
> otherwise both match.

#### Disambiguating with `SCOPE`

If the server fronts more than one DuckLake — say the catalog metadata lives
in one bucket and the data files in another — define one S3 secret per bucket
prefix:

```sql
CREATE SECRET s3_catalog (
    TYPE s3,
    SCOPE 's3://catalog-bucket',
    KEY_ID 'AKIA...CAT',
    SECRET '...',
    REGION 'eu-west-1'
);

CREATE SECRET s3_lake (
    TYPE s3,
    SCOPE 's3://data-bucket',
    KEY_ID 'AKIA...DAT',
    SECRET '...',
    REGION 'eu-west-1'
);

ATTACH 'ducklake:postgres:dbname=ducklake_catalog' AS lake
    (DATA_PATH 's3://data-bucket/lake/');
```

Reads from `s3://catalog-bucket/...` resolve via `s3_catalog`; reads/writes
under `s3://data-bucket/...` resolve via `s3_lake`. The longest matching
`SCOPE` prefix wins, so an unscoped secret of the same type acts as a
catch-all default. Postgres secrets resolve the same way against the
`HOST`/`PORT`/`DATABASE` triple of the connection.

---

## Client setup (through ODBC)

There is no native Quack ODBC driver. Use the **DuckDB ODBC driver** in front
of a local DuckDB engine; that engine acts as a thin Quack client. It needs the
`quack` extension only — **no** `ducklake` extension, **no** S3 secret, **no**
catalog connection.

Through the ODBC connection, run:

```sql
INSTALL quack; LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:server-host:9494' AS remote;

-- executes on the server, which reads the Parquet files
SELECT * FROM remote.query('SELECT * FROM lake.my_table WHERE id = 42');
```

`remote.query('...')` ships a verbatim query to the server and gives explicit
control over what executes remotely — the recommended form for non-trivial
queries on large data. A direct `SELECT ... FROM remote.lake.my_table` also
works; either way the scan and all file I/O happen on the server. Writes work
the same way and commit server-side.

---

## How a query flows

1. The ODBC application sends SQL to its local DuckDB engine via ODBC.
2. The local engine (a Quack client) forwards the query to the Quack server
   over HTTP.
3. The server runs the query against the attached DuckLake: it consults the
   catalog and reads/writes the Parquet files in object storage.
4. Only the result set travels back over Quack to the client.
5. The client returns rows to the ODBC application.

The client never opens a Parquet file and never sees a storage credential.

---

## Production considerations

| Topic | Note |
|---|---|
| Server sizing | The server does all compute and is the single point of file access — size it for the concurrent client load. |
| Concurrency | Many ODBC clients = concurrent queries on one DuckDB server. Reads scale well; concurrent writes to the *same table* have limits. |
| Catalog | A local DuckDB-file catalog is fine for one server. Use PostgreSQL (or Quack-as-catalog) only if running multiple Quack servers. |
| Credentials | Object-storage credentials never leave the server — a security benefit of this design. |
| Authorization | Quack's authorization callback can inspect each query and restrict which tables a client may touch. |
| TLS | Quack has no SSL by default. For non-local clients, terminate TLS with a reverse proxy (e.g. nginx). |
| Default port | Quack listens on port `9494`. |
| Maturity | Quack is in beta; a production-ready release is planned with DuckDB v2.0 (fall 2026). Validate carefully before production use. |
| ODBC driver | Use a DuckDB ODBC driver whose bundled engine is 1.5.2 or newer. |

---

## Version requirements

- **DuckDB engine:** 1.5.2+ (Quack from `core_nightly`) or 1.5.3+ (Quack as a
  core extension; DuckLake supports a Quack catalog).
- **DuckDB ODBC driver:** bundled engine 1.5.2 or newer.
- **Extensions:** `ducklake` + `quack` on the server; `quack` only on the
  client.

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
