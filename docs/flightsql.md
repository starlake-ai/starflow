# Connecting Starlake Starflow to an Arrow Flight SQL endpoint

How to use any Arrow Flight SQL server (quack-on-demand gateway, GizmoSQL,
Dremio, Doris, ...) as a regular Starflow connection: extract, load, transform
and audit all run over the Flight SQL wire. The primary scenario is a DuckDB or
DuckLake lakehouse exposed through a Flight SQL server, with the same
file-isolation model as `quack.md`: the server owns the catalog and the
object-storage credentials, the client only ever speaks SQL.

---

## Declaring the connection

A Flight SQL connection is ordinary JDBC YAML. This is a complete
quack-on-demand example:

```yaml
connections:
  qod_bi:
    type: jdbc
    options:
      url: "jdbc:arrow-flight-sql://localhost:31338?useEncryption=true&disableCertificateVerification=true&tenant=acme&pool=bi&superuser=true"
      user: "..."
      password: "..."
      # dialect: duckdb        # optional, duckdb is the default
      # driver: "..."          # optional, defaults to the Arrow Flight SQL JDBC driver
```

Everything after `host:port` is opaque to Starflow: the query string is handed
to the Arrow driver untouched.

| URL parameter | Consumed by | Meaning |
|---|---|---|
| `useEncryption`, `disableCertificateVerification` | Arrow driver | TLS on the gRPC channel |
| `tenant`, `pool`, `superuser` | forwarded to the server | quack-on-demand routing |

Connections whose query strings differ (another tenant, another pool) get
distinct connection pools, so they never share sessions.

---

## The `dialect` option

Flight SQL is a transport, not a dialect. The endpoint fronts a real engine,
and Starflow must generate that engine's SQL. The `dialect` option selects the
engine profile (the `jdbcEngines` entry) used for DDL, merge strategies, audit
tables and identifier quoting:

```yaml
      dialect: postgresql   # duckdb (default), postgresql, snowflake, mysql, ...
```

Notes:

- The default is `duckdb`, which is what quack-on-demand and GizmoSQL front.
- `dialect: mariadb` normalizes to the `mysql` profile, and `databricks` to
  `spark`, following Starflow's usual engine aliasing.
- An unknown dialect fails at first use with a key-not-found error naming the
  missing `jdbcEngines` entry.

---

## Installing the driver

The Arrow Flight SQL JDBC driver is not bundled in the Starflow assembly. The
setup tool downloads it into `bin/deps` when the FlightSQL dependency is
enabled (it is by default):

```bash
ENABLE_FLIGHTSQL=true          # default true, part of ENABLE_ALL
FLIGHT_SQL_JDBC_VERSION=19.0.0 # override to pin another driver version
```

The driver class defaults to
`org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`; set the `driver` option
only to use a custom build.

---

## The client stays fully remote

In the quack-on-demand / GizmoSQL model the server attaches the DuckLake and
holds the object-storage secrets. Starflow honors that:

- The client never runs `ATTACH 'ducklake:...'`. Even if a connection's
  `preActions` mention `ducklake:` or `quack:`, a Flight SQL connection is
  never rewritten to a local DuckDB database.
- Local DuckDB session setup (`home_directory`, `secret_directory`, S3
  endpoint and credential `SET`s) is skipped. Credentials live server-side.
- `preActions` and `postActions`, if present, still run as plain session SQL
  over the Flight connection (`USE lake`, `SET schema ...`).

One consequence for loading: with a duckdb dialect, load SQL such as
`read_csv(...)` executes on the server, so the file paths in your load jobs
must be visible to the server (object storage it has secrets for, or a shared
filesystem). Paths that only exist on the client machine fail with the
server's error.
