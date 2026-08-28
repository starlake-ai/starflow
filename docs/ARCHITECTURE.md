# Starlake Starflow - Architecture Document

**Version:** 1.5.x
**Last Updated:** 2026-03-25

---

## 1. System Overview

Starflow is a Scala-based declarative data pipeline platform. Users define Extract, Load, Transform, and Orchestration pipelines as YAML configuration files. Starflow compiles these declarations into executable jobs that run against any supported warehouse.

```
                          ┌──────────────────────────────────────────────┐
                          │              YAML Configuration              │
                          │  (domains, tables, transforms, connections)  │
                          └──────────────┬───────────────────────────────┘
                                         │
                          ┌──────────────▼───────────────────────────────┐
                          │            Starflow Engine                   │
                          │  ┌─────────┐ ┌──────┐ ┌─────────┐ ┌──────┐   │
                          │  │ Extract │ │ Load │ │Transform│ │  DAG │   │
                          │  └────┬────┘ └──┬───┘ └────┬────┘ └──┬───┘   │
                          │       │         │          │         │       │
                          │  ┌────▼─────────▼──────────▼─────────▼───┐   │
                          │  │        Execution Layer                │   │
                          │  │  Native Loaders │ Spark │ JDBC │ API  │   │
                          │  └────┬─────────┬──────────┬─────────┬───┘   │
                          └───────┼─────────┼──────────┼─────────┼───────┘
                                  │         │          │         │
                    ┌─────────────▼──┐ ┌────▼────┐ ┌──▼───┐ ┌──▼──────────┐
                    │  BigQuery      │ │Snowflake│ │DuckDB│ │PostgreSQL   │
                    │  Redshift      │ │  Spark  │ │Delta │ │Iceberg/Hive │
                    └────────────────┘ └─────────┘ └──────┘ └─────────────┘
```

---

## 2. Module Architecture

### 2.1 Module Map

```
ai.starlake/
├── config/           # Configuration model and settings management
├── schema/
│   ├── model/        # Domain model (89 files): domains, tables, attributes, jobs, tasks
│   ├── handlers/     # Schema loading, validation, storage abstraction
│   └── generator/    # Code generation: DDL, Excel, DAGs
├── job/
│   ├── bootstrap/    # Project initialization
│   ├── ingest/       # File ingestion pipeline
│   ├── transform/    # SQL transform execution
│   ├── metrics/      # Data quality metrics
│   ├── sink/         # Warehouse-specific writers (BigQuery, JDBC, Kafka, ES)
│   ├── validator/    # Row validation framework
│   └── loaders/      # Native loaders (BigQuery, Snowflake, DuckDB)
├── extract/          # Source system extraction (JDBC, BigQuery, OpenAPI)
├── lineage/          # Data dependency tracking
├── migration/        # YAML schema version migration
├── workflow/         # Pipeline orchestration
├── serve/            # REST API server
├── sql/              # SQL utilities and dialect management
├── tests/            # Declarative test framework
├── console/          # Interactive SQL shell
├── utils/            # YAML serialization, Jinja2 rendering, Spark helpers
└── exceptions/       # Error types
```

### 2.2 Dependency Flow

```
CLI (Main.scala)
 └─▶ Command Layer (47 Cmd[T] implementations)
      └─▶ Workflow Layer (IngestionWorkflow)
           ├─▶ Schema Layer (SchemaHandler, model classes)
           │    └─▶ Config Layer (Settings, DatasetArea, Connections)
           ├─▶ Job Layer (IngestionJob, AutoTask, ExtractDataJob)
           │    ├─▶ Validator Layer (GenericRowValidator)
           │    ├─▶ Sink Layer (BigQuery, JDBC, Kafka, ES writers)
           │    └─▶ Native Loaders (BigQuery, Snowflake, DuckDB)
           ├─▶ Lineage Layer (table, column, ACL dependencies)
           └─▶ Generator Layer (DAGs, DDL, Excel)
```

---

## 3. Core Components

### 3.1 Configuration System

**Entry Point:** `Settings.scala`

The configuration system merges multiple sources in priority order:

```
reference.conf (defaults)
  ▼ overridden by
application.sl.yml (project config)
  ▼ overridden by
env.sl.yml (default environment variables)
  ▼ overridden by
env.{SL_ENV}.sl.yml (profile-specific variables)
  ▼ overridden by
System properties / OS environment variables
  ▼ overridden by
CLI arguments (--options)
```

**Key Configuration Objects:**

| Class | Purpose |
|---|---|
| `AppConfig` | Root config: connections, loader, validation, audit, DAG refs, schedule presets |
| `ConnectionInfo` | Database connection: type (JDBC/BQ/FS), options, credentials, spark format |
| `Area` | Pipeline stage directory names: incoming, stage, archive, etc. |
| `Audit` | Audit sink configuration and error thresholds |
| `ExpectationsConfig` | Data quality expectations path and failure behavior |
| `Metrics` | Metrics computation settings |

**Variable Resolution:**
- `{{VAR_NAME}}` Mustache syntax in YAML files
- Resolved via `richFormat` (simple substitution) and `parseJinja` (Jinja2 engine)
- Variables sourced from: env files → OS environment → system properties → CLI args

**Caching:**
- `CaffeineSettingsManager` provides in-memory settings caching for server mode
- Cache keyed by (root, env) tuple
- Invalidated on project reload or bootstrap

### 3.2 Schema Model

**Entry Point:** `SchemaHandler.scala`

The schema model represents the complete project metadata:

```
SchemaHandler
 ├─▶ domains(): List[DomainInfo]          # Load domain definitions
 │    └─▶ DomainInfo
 │         ├── name, database, comment
 │         ├── metadata: Metadata          # Default metadata for tables
 │         └── tables: List[SchemaInfo]
 │              └─▶ SchemaInfo (table)
 │                   ├── pattern (file regex)
 │                   ├── attributes: List[TableAttribute]
 │                   │    └── name, type, required, privacy, rename, default, foreign key
 │                   ├── metadata: Metadata
 │                   │    ├── format, encoding, separator, writeStrategy
 │                   │    ├── sink, partition, clustering
 │                   │    └── schedule, materialization
 │                   ├── primaryKey, acl, rls
 │                   └── expectations
 │
 ├─▶ jobs(): List[AutoJobInfo]             # Transform job definitions
 │    └─▶ AutoJobInfo
 │         ├── name, comment
 │         ├── default: AutoTaskInfo       # Default task settings
 │         └── tasks: List[AutoTaskInfo]
 │              └─▶ AutoTaskInfo
 │                   ├── name, domain, table
 │                   ├── sql (or separate .sql file)
 │                   ├── writeStrategy, connectionRef
 │                   ├── presql, postsql
 │                   ├── schedule, dagRef
 │                   └── expectations
 │
 ├─▶ types(): List[Type]                   # Data type definitions
 ├─▶ refs(): RefDesc                       # Reference data
 └─▶ activeEnvVars(): Map[String, String]  # Resolved environment
```

**Schema Validation:**
- All YAML files validated against `starlake.json` (JSON Schema draft 2019-09)
- On-the-fly migration for older schema versions via `YamlMigrator`
- Default values injected via `ApplyDefaultsStrategy`

### 3.3 Storage Abstraction

**Entry Point:** `StorageHandler.scala` (trait)

```
StorageHandler (trait)
 ├─▶ HdfsStorageHandler    # Hadoop filesystem (HDFS, GCS, S3, Azure, local)
 └─▶ LocalStorageHandler   # Direct local filesystem (testing, DuckDB)
```

All file operations (read, write, list, delete, move, copy) go through this abstraction, enabling transparent cloud storage support.

### 3.4 Dataset Lifecycle

**Entry Point:** `DatasetArea.scala`

Files flow through well-defined pipeline stages:

```
incoming/{domain}/          # Raw files land here
    │
    ▼  (stage command)
stage/{domain}/             # Staged for processing
    │
    ▼  (load command)
    ├─▶ accepted            # Successfully loaded to warehouse
    ├─▶ rejected            # Failed validation
    ├─▶ unresolved          # No matching schema
    └─▶ archive/{domain}/   # Archived after processing
         └─▶ replay/        # Available for reprocessing
```

**Metadata Directory Structure:**
```
metadata/
├── application.sl.yml       # Project configuration
├── env.sl.yml               # Default environment variables
├── env.{PROFILE}.sl.yml     # Profile-specific variables
├── load/                    # Domain and table definitions
│   └── {domain}/
│       ├── _config.sl.yml   # Domain-level config
│       └── {table}.sl.yml   # Table schema
├── transform/               # Transform definitions
│   └── {job}/
│       ├── _config.sl.yml   # Job-level config
│       ├── {task}.sl.yml    # Task metadata
│       └── {task}.sql       # SQL query
├── extract/                 # Extraction configurations
├── types/                   # Data type definitions
├── expectations/            # Data quality macros (Jinja2)
├── dags/                    # DAG generation templates
├── external/                # External table definitions
├── refs/                    # Reference data
└── tests/                   # Test definitions
    ├── load/{domain}/{table}/
    └── transform/{job}/{task}/
```

---

## 4. Execution Pipelines

### 4.1 Ingestion Pipeline

```
LoadCmd
 └─▶ IngestionWorkflow.loadData()
      └─▶ For each file in incoming/:
           1. Pattern match → find SchemaInfo
           2. Select IngestionJob by format:
              ├── DsvIngestionJob (CSV/TSV/DSV)
              ├── JsonIngestionJob
              ├── XmlIngestionJob
              ├── ParquetIngestionJob
              ├── PositionIngestionJob (fixed-width)
              └── KafkaIngestionJob
           3. Parse file → DataFrame
           4. Validate rows (GenericRowValidator)
              ├── FlatRowValidator → flat structures
              └── TreeRowValidator → nested JSON/XML
           5. Split: accepted / rejected
           6. Apply write strategy:
              ├── APPEND          → INSERT INTO
              ├── OVERWRITE       → TRUNCATE + INSERT
              ├── UPSERT_BY_KEY   → MERGE ON key
              ├── UPSERT_BY_KEY_AND_TIMESTAMP → MERGE ON key + ts
              ├── SCD2            → Slowly Changing Dimension Type 2
              ├── DELETE_THEN_INSERT → DELETE matching + INSERT
              └── OVERWRITE_BY_PARTITION → partition-level overwrite
           7. Sink to warehouse:
              ├── BigQueryNativeLoader (Load API)
              ├── SnowflakeNativeLoader (COPY INTO)
              ├── DuckDbNativeLoader (direct)
              ├── SparkJdbcWriter (JDBC)
              └── BigQuerySparkWriter (Spark → BQ)
           8. Compute metrics
           9. Execute expectations
          10. Log audit record
          11. Archive source file
```

**Load Strategies:**
- `IngestionNameStrategy` — process files matching a name pattern
- `IngestionTimeStrategy` — process files by arrival time window

### 4.2 Transform Pipeline

```
TransformCmd
 └─▶ IngestionWorkflow.transform()
      └─▶ For each AutoTaskInfo:
           1. Resolve SQL (file or inline)
           2. Apply Jinja2 templating with env vars
           3. Extract SQL dependencies (table references)
           4. Execute pre-SQL statements
           5. Run main SQL query
           6. Apply write strategy (same as ingestion)
           7. Execute post-SQL statements
           8. Execute expectations
           9. Log audit record
```

**Transform Actions:**
- `run` — execute the transform
- `create` — create target table without data
- `compile` — validate SQL without execution

### 4.3 Extraction Pipeline

```
ExtractSchemaCmd / ExtractDataCmd
 └─▶ ExtractSchema / ExtractDataJob
      ├─▶ JDBC Extraction:
      │    1. Connect via JDBC
      │    2. Read table metadata (columns, types, keys)
      │    3. Generate Starflow YAML schema
      │    4. Optionally pull data with:
      │       - Partition-based parallelism
      │       - Incremental via timestamp column
      │       - Configurable fetch size
      │
      ├─▶ BigQuery Extraction:
      │    1. Read BigQuery table metadata
      │    2. Generate schema YAML
      │
      └─▶ OpenAPI Extraction:
           1. Parse OpenAPI/JSON Schema definition
           2. Map to Starflow attribute types
           3. Generate table YAML
```

### 4.4 DAG Generation Pipeline

```
DagGenerateCmd
 └─▶ DagGenerateJob
      1. Load all domains and transform jobs
      2. Extract SQL dependencies for each task
      3. Build dependency graph
      4. Load DAG templates (Jinja2):
         ├── Airflow shell/Cloud Run/Fargate templates
         ├── Dagster templates
         └── Snowflake Task templates
      5. Render templates with:
         - Task metadata
         - Dependencies
         - Schedule presets
         - Connection info
      6. Write DAG files to output directory

DagDeployCmd
 └─▶ DagDeployJob
      1. Copy generated DAGs to orchestrator path
      2. Validate deployment
```

---

## 5. Write Strategy Engine

The write strategy engine is central to both Load and Transform pipelines. It translates declarative intent into warehouse-specific DML.

| Strategy | SQL Generated | Use Case |
|---|---|---|
| `APPEND` | `INSERT INTO target SELECT ...` | Append-only event logs |
| `OVERWRITE` | `TRUNCATE target; INSERT INTO target SELECT ...` | Full refresh dimensions |
| `UPSERT_BY_KEY` | `MERGE INTO target USING source ON key WHEN MATCHED UPDATE WHEN NOT MATCHED INSERT` | Deduplicated upsert |
| `UPSERT_BY_KEY_AND_TIMESTAMP` | `MERGE ... ON key WHEN MATCHED AND source.ts > target.ts UPDATE ...` | Latest-wins upsert |
| `SCD2` | `MERGE ... WHEN MATCHED AND changed SET end_date; INSERT new rows` | Historical tracking |
| `DELETE_THEN_INSERT` | `DELETE FROM target WHERE key IN (source); INSERT ...` | Idempotent reload |
| `OVERWRITE_BY_PARTITION` | `INSERT OVERWRITE PARTITION(...)` | Partition-level refresh |

Each strategy is implemented per target warehouse dialect via `StarlakeJdbcDialects`.

---

## 6. Data Quality Framework

### 6.1 Expectations

Expectations are Jinja2 macros defined in `metadata/expectations/` and referenced in table/task YAML:

```yaml
table:
  expectations:
    - "{{ completeness('email', 0.95) }}"
    - "{{ uniqueness('order_id', 1.0) }}"
    - "{{ accepted_values('status', ['active','inactive']) }}"
```

Results are persisted to the audit database. Pipeline behavior on failure is configurable (warn vs. fail).

### 6.2 Row Validation

Every row passes through a `GenericRowValidator` during load:
- Type checking against declared schema
- Required field enforcement
- Format validation (dates, timestamps, etc.)

Rejected rows are written to a separate rejection table with error details.

### 6.3 Metrics

Automatic computation of per-column statistics:
- **Discrete:** cardinality, mode, frequency distribution
- **Continuous:** min, max, mean, stddev, percentiles
- Results stored in the metrics audit table

---

## 7. Lineage System

Starflow provides multi-level lineage tracking:

| Level | Source | Output |
|---|---|---|
| **Table-level** | SQL dependency parsing | Which tables feed which tables |
| **Column-level** | SQL column tracing | Which columns flow where |
| **ACL** | Role/permission declarations | Which roles access which tables |

**Output Formats:**
- Mermaid diagrams (embeddable in docs)
- GraphViz DOT (for visualization tools)
- Reactflow JSON (for web UI)
- Programmatic JSON/YAML

Lineage is extracted automatically from SQL transforms — no manual annotation required.

---

## 8. Migration System

Starflow supports automatic migration of YAML configuration between schema versions.

```
ProjectMigrator
 └─▶ MetadataFolderMigrator (per area: extract, load, transform, env, etc.)
      └─▶ YamlFileMigrator (per file type)
           1. Read raw YAML
           2. Check if migration needed (canMigrate)
           3. Apply YamlMigrator transformations (rename fields, restructure)
           4. Validate against JSON schema
           5. Serialize back to disk
```

**Migration Chain:** PreV1 → V1 → ScalaClass (latest)

Key migrations:
- `schema` → `table`, `schemas` → `tables` (PreV1)
- `write` mode → `writeStrategy` object (V1)
- `merge` config → `writeStrategy` consolidation (V1)
- `sink` restructuring (V1)
- Container wrapping: `load`, `transform`, `extract`, `env`, etc. (V1)

---

## 9. Server Architecture

### 9.1 Embedded Server

```
MainServerCmd
 └─▶ SingleUserMainServer (Jetty)
      └─▶ SingleUserRequestHandler (Servlet)
           ├── /api/v1/cli  (POST/GET)
           │    ├── domains     → SchemaHandler.domains()
           │    ├── jobs        → SchemaHandler.jobs()
           │    ├── types       → SchemaHandler.types()
           │    ├── reload      → Cache invalidation
           │    ├── bootstrap   → Bootstrap.bootstrap()
           │    └── {command}   → Main.run(args)
           └── CaffeineSettingsManager
                └── Per-(root, env) settings cache
```

### 9.2 External API (starlake-api)

The separate `starlake-api` project wraps starlake-core with:
- Multi-tenant project management
- Authentication and authorization
- UI-facing REST endpoints
- Docker deployment with path remapping

---

## 10. Technology Stack

| Component | Technology | Version |
|---|---|---|
| Language | Scala | 2.13.18 |
| Runtime | Java | 17+ |
| Build | SBT | - |
| Compute | Apache Spark | 4.1.3 |
| Lake Formats | Delta Lake | 4.3.1 |
| SQL Parser | JSQLParser | 5.4.260-SNAPSHOT |
| SQL Transpiler | JSQLTranspiler | 1.8 |
| Config | PureConfig + Typesafe Config | 0.17.9 |
| YAML | Jackson YAML (SnakeYAML) | 2.21.2 |
| Schema Validation | networknt/json-schema-validator | 2019-09 |
| Template Engine | Jinjava (Jinja2) | - |
| HTTP Server | Jetty (embedded) | - |
| Caching | Caffeine | - |
| BigQuery | google-cloud-bigquery | 2.49.0 |
| Snowflake | snowflake-jdbc | 3.28.0 |
| DuckDB | duckdb_jdbc | 1.5.0 |
| Kafka | Confluent Platform | 7.7.5 |
| CLI Parsing | scopt | - |
| Testing | ScalaTest + TestContainers | - |

---

## 11. Resource Management & Caching

### 11.1 JDBC Resource Management

All JDBC statements and result sets must be wrapped in `scala.util.Using.resource` to guarantee cleanup:

```scala
// Project convention — always use Using.resource for AutoCloseable
Using.resource(connection.createStatement()) { stmt =>
  Using.resource(stmt.executeQuery(sql)) { rs =>
    // process results
  }
}
```

This pattern is used consistently in `JdbcMetadata`, `JdbcDbUtils`, `StarlakeJdbcOps`, `ExpectationAssertionHandler`, and `ExtractDataJob`.

### 11.2 Connection Pool

**Entry Point:** `StarlakeConnectionPool.scala`

```
StarlakeConnectionPool
 ├─▶ HikariCP pools        # For JDBC connections (optional, via SL_USE_CONNECTION_POOLING=true)
 └─▶ DuckDB pool           # Single-writer pool with idle cleanup
      ├── Pool key: URL + properties hash (SHA-256)
      ├── Idle timeout: 15 seconds
      ├── Background cleanup via ScheduledExecutorService
      └── Synchronized compound operations (get/put/remove)
```

DuckDB connections use `synchronized` blocks around compound get-then-put operations to prevent race conditions in concurrent access.

### 11.3 Spark DataFrame Caching Strategy

The ingestion pipeline caches DataFrames at specific points to prevent redundant disk reads:

```
dataset (input)                      ← persist before validation
    │
    ▼
validate() → errors, rejected, accepted  ← each persisted by validator
    │
    ├─▶ saveRejected(errors, rejected)   reads from cached input
    │
    ├─▶ saveAccepted(accepted)           reads from cached input
    │       └─▶ computeFinalDF()
    │            └─▶ finalAcceptedDF     ← persist for sink + metrics
    │                                      unpersist after sink completes
    │
    ▼
input.unpersist()                        ← freed after both saves complete
    │
    ▼
caller .count() on errors, accepted      ← hits validator caches
    │
    ▼
session.catalog.clearCache()             ← clears remaining validator caches
```

**Key invariant:** the input dataset is persisted before the validator splits it, so both `saveRejected` and `saveAccepted` compute from cache rather than re-reading source files. Each cache is unpersisted as soon as its last consumer finishes.

The default storage level is `MEMORY_AND_DISK`, configurable via `cacheStorageLevel` in `AppConfig`.

---

## 12. Security Patterns

### 12.1 SQL Safety

- **Identifier quoting:** Table and column names are quoted via `ConnectionInfo.quoteIdentifier()` using the appropriate dialect (`"col"` for standard, `` `col` `` for MySQL/DuckDB).
- **String literal escaping:** Values interpolated into DuckDB SET commands are escaped via `escapeSqlStringLiteral()` (single-quote doubling) to prevent SQL injection.
- **Parameterized queries:** Data extraction uses `PreparedStatement` with typed setters for all user-facing values (see `ExtractDataJob`, `LastExportUtils`).

### 12.2 Credential Handling

- **Redaction utilities:** `Utils.redact()` and `Utils.obfuscate()` mask sensitive map values before logging.
- **Log hygiene:** Connection options containing `password`, `token`, or `access` keys are masked in log output. DuckDB SET commands for credentials log only the setting name, not the value.
- **Pool key hashing:** Connection pool keys are hashed with SHA-256 to avoid exposing credentials in pool identifiers.
- **AES encryption:** `AESEncryption` uses AES/CBC with random IV per operation via `SecureRandom`.

### 12.3 Process Execution

External process invocation (e.g., GraphViz) uses `Process(Seq(...))` with argument lists instead of string interpolation to prevent shell metacharacter injection.

---

## 13. Design Principles

### 13.1 Declarative Over Imperative

All pipeline behavior is driven by YAML configuration. No custom Scala/Python code is required for standard ELT operations. The engine translates declarations into warehouse-specific operations.

### 13.2 Convention Over Configuration

Sensible defaults minimize required configuration:
- File format auto-detection
- Schema inference from data
- Default write strategy (APPEND)
- Standard directory layout

### 13.3 Warehouse Abstraction

A single pipeline definition targets multiple warehouses. Dialect-specific SQL generation is handled internally via `StarlakeJdbcDialects` and engine-specific type mappings.

### 13.4 Governance as Configuration

Data quality, privacy, access control, and lineage are declared alongside the pipeline — not as separate systems. This ensures governance is version-controlled and reviewed with every change.

### 13.5 Zero Lock-in

- YAML configuration is portable across warehouses
- SQL transforms use standard SQL (transpiled per target)
- No proprietary runtime — runs on Spark, JDBC, or native APIs
- Open source (Apache 2.0)
