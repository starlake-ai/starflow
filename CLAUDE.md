# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Starlake

Starlake is a declarative data pipeline tool (Extract, Load, Transform, Orchestrate) written in Scala. It replaces custom ETL scripts with YAML configurations, supporting BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL, Delta Lake, and Iceberg.

## Build Commands

**Requires:** JDK 17, SBT 1.11.5

```bash
sbt compile                    # Compile
sbt cc                         # Clean + compile (alias)
sbt test                       # Run all tests (sequential, forked)
sbt "testOnly *ClassName*"     # Run a single test class
sbt "testOnly *ClassName* -- -z \"test name\""  # Run a single test
sbt assembly                   # Build fat JAR (without Spark)
sbt assemblyWithSpark          # Build fat JAR (with Spark embedded)
sbt pl                         # Clean + publishLocal (alias)
sbt scalafmtCheck              # Check formatting (CI runs this)
sbt scalafmt                   # Auto-format code
```

Code formatting (`scalafmt`) runs automatically on compile. Tests run sequentially (`parallelExecution := false`) and are forked (`Test / fork := true`). Tests require ~4GB heap (`-Xmx4g`).

## Project Structure

Single SBT module. Scala 2.13.18. Main class: `ai.starlake.job.Main`.

```
src/main/scala/ai/starlake/
├── job/             # CLI commands and job execution
│   ├── Main.scala   # Entry point, routes CLI commands to Cmd implementations
│   ├── Cmd.scala    # Base trait for all commands: Cmd[T <: ReportFormatConfig]
│   ├── ingest/      # Data loading (CSV, JSON, XML, Parquet, Kafka)
│   ├── transform/   # SQL transformations (Spark, BigQuery, JDBC, Snowflake)
│   ├── sink/        # Output to BigQuery, JDBC, Kafka, Elasticsearch
│   ├── bootstrap/   # Project scaffolding
│   ├── infer/       # Schema inference from data files
│   └── metrics/     # Data quality metrics
├── schema/
│   ├── model/       # Domain model case classes (Domain, Table, Attribute, WriteStrategy, etc.)
│   └── handlers/    # SchemaHandler (orchestrator), StorageHandler (filesystem abstraction)
├── config/          # Settings, DatasetArea, ConnectionInfo, SparkEnv
├── extract/         # JDBC/BigQuery data extraction
├── workflow/        # IngestionWorkflow (composes Transform, Sink, Test, Metrics, Infer workflows)
├── lineage/         # Table/column dependency tracking
├── sql/             # SQL parsing, formatting, type mappings, dialect handling
├── serve/           # REST API server mode
├── tests/           # Data quality test framework
├── migration/       # Schema migration tooling
├── console/         # Interactive console
└── utils/           # Shared utilities (YAML, Jinja, Spark, GCP, JDBC helpers)
```

## Architecture

### Command Pattern
Every CLI operation is a `Cmd[T]` object (trait in `job/Cmd.scala`). Each command:
- Parses args via `scopt` into a config case class `T`
- Implements `run(config: T, schemaHandler: SchemaHandler): Try[JobResult]`
- Is registered in `Main.scala`'s command list

There are 45+ commands (LoadCmd, TransformCmd, ExtractSchemaCmd, BootstrapCmd, etc.).

### Data Flow
```
Extract (JDBC/BQ) → Stage (landing) → Ingest (parse + validate + merge) → Transform (SQL) → Sink (output)
```

### Workflow Composition
`IngestionWorkflow` is the main coordinator, composed via trait mixing:
`TransformWorkflow + TestWorkflow + SinkWorkflow + MetricsSecurityWorkflow + InferWorkflow`

### Schema Model
YAML configs drive everything. Key model classes in `schema/model/`:
- **DomainInfo**: Groups tables (like a DB schema)
- **SchemaInfo/TableInfo**: Table definition with attributes and metadata
- **AutoTaskInfo/AutoJobInfo**: SQL transformation definitions
- **WriteStrategy**: Merge logic (UPSERT, APPEND, OVERWRITE, etc.)
- **Metadata**: Format, inference, delimiter settings
- **Sink/AllSinks**: Output destination config

### Storage Abstraction
`StorageHandler` trait with `LocalStorageHandler` and `HdfsStorageHandler` implementations.

### Multi-Engine Support
Transform jobs can target different engines via `Engine`: Spark, BigQuery, Snowflake, JDBC, DuckDB. Engine-specific implementations live in `job/transform/` (e.g., `SparkAutoTask`, `BigQueryAutoTask`, `JdbcAutoTask`).

## Key Dependencies

- **Spark 3.5.8** (provided scope — not bundled in standard assembly)
- **Jackson 2.15.2** for JSON/YAML serialization
- **scopt** for CLI parsing
- **PureConfig** for typesafe config
- **better-files** for file I/O
- **jinjava** for Jinja2 template processing
- **jsqlparser/jsqltranspiler** for SQL analysis
- **DuckDB 1.5.0** for local engine support
- **TestContainers** (PostgreSQL, MariaDB, Kafka) for integration tests

## Testing

Base class: `TestHelper` (extends ScalaTest `AnyFlatSpec`). Key traits:
- `WithSettings`: Provides test `Settings` with isolated temp directories
- `SpecTrait`: Manages domain/job YAML delivery and workflow testing
- `PgContainerHelper`: PostgreSQL container for JDBC tests

Tests create isolated `starlake-test-{uuid}` temp directories and configure test-specific Spark sessions. Integration tests use TestContainers for PostgreSQL, MariaDB, and Kafka.

## YAML Metadata Layout (for user projects)

```
metadata/
├── types/default.sl.yml          # Type definitions
├── load/{domain}/_config.sl.yml  # Domain config
├── load/{domain}/{table}.sl.yml  # Table schemas
├── transform/{job}/*.sl.yml      # SQL transform tasks
├── dags/*.sl.yml                 # DAG definitions
└── expectations/*.sl.yml         # Data quality rules
```