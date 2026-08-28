# Starlake Starflow - Product Requirements Document

**Version:** 1.5.x
**Last Updated:** 2026-03-18

---

## 1. Product Overview

Starflow is a declarative data pipeline platform that enables data teams to extract, load, transform, and orchestrate their data pipelines using YAML configuration instead of code. It replaces hundreds of lines of warehouse-specific boilerplate (BigQuery, Snowflake, Redshift, Spark, DuckDB) with self-describing configuration files that are version-controlled, reviewed, and deployed like infrastructure-as-code.

### 1.1 Problem Statement

Data engineering teams face recurring challenges:

- **Boilerplate proliferation** - Every warehouse requires its own MERGE, UPSERT, SCD2, and COPY logic. Teams rewrite the same patterns across projects.
- **Fragile orchestration** - DAGs are hand-coded, dependencies are implicit, and pipeline changes require both SQL and orchestrator updates.
- **No governance by default** - Data quality, lineage, privacy, and access controls are bolted on after the fact, often inconsistently.
- **Warehouse lock-in** - Migrating between BigQuery, Snowflake, and Redshift means rewriting ETL code.

### 1.2 Solution

Starflow provides a single YAML-based DSL that:

1. **Abstracts warehouse differences** - One configuration, multiple target warehouses
2. **Automates write strategies** - Declare APPEND, OVERWRITE, UPSERT, SCD2, or DELETE_THEN_INSERT; Starflow generates the correct SQL
3. **Infers and enforces schemas** - Auto-detect formats, types, and separators; validate every row at load time
4. **Generates orchestration DAGs** - Extract SQL dependencies and produce Airflow/Dagster/Snowflake Task DAGs automatically
5. **Embeds governance** - Data quality expectations, column-level encryption, row-level security, and lineage are first-class configuration

### 1.3 Target Users

| Persona | Use Case |
|---|---|
| **Data Engineers** | Define and maintain ELT pipelines, manage schema evolution, configure write strategies |
| **Analytics Engineers** | Write SQL transforms, define data quality expectations, build aggregation layers |
| **Data Platform Teams** | Standardize pipeline patterns, enforce governance, manage multi-warehouse deployments |
| **Data Analysts** | Use bootstrap templates, explore lineage, run ad-hoc transforms |

---

## 2. Core Capabilities

### 2.1 Extract

Pull data from source systems into Starflow-managed landing areas.

**Requirements:**
- Extract from any JDBC source (PostgreSQL, MySQL, SQL Server, Oracle, etc.)
- Extract from BigQuery (native API)
- Support incremental extraction via timestamp columns
- Parallel extraction with configurable partition columns and fetch sizes
- Auto-infer Starflow schemas from source database metadata
- OpenAPI schema extraction for REST API sources
- Data freshness tracking and alerting

**Configuration:**
```yaml
extract:
  connectionRef: "source-db"
  jdbcSchemas:
    - schema: "sales"
      tables:
        - name: "orders"
          partitionColumn: "order_id"
          fetchSize: 100
          timestamp: updated_at
```

### 2.2 Load

Ingest files from landing areas into the target warehouse with schema validation and write strategy enforcement.

**Requirements:**
- Support file formats: CSV/DSV, JSON, XML, fixed-width, Parquet, ORC
- Auto-detect format, encoding, headers, and separators
- Pattern-based file-to-table routing via regex
- Row-level validation against declared schemas
- Rejected record handling with audit trail
- Write strategies: APPEND, OVERWRITE, UPSERT_BY_KEY, UPSERT_BY_KEY_AND_TIMESTAMP, SCD2, DELETE_THEN_INSERT, OVERWRITE_BY_PARTITION
- Native loaders for BigQuery (LOAD API), Snowflake (COPY), DuckDB (direct)
- Spark-based loading for complex transformations
- Column-level privacy/encryption at load time
- Metrics computation (cardinality, min, max, stddev, frequency)
- Pre/post load SQL hooks

**Configuration:**
```yaml
table:
  pattern: "orders.*.csv"
  metadata:
    format: "DSV"
    separator: ","
    writeStrategy:
      type: "UPSERT_BY_KEY_AND_TIMESTAMP"
      key: [order_id]
      timestamp: updated_at
  attributes:
    - name: "order_id"
      type: "long"
      required: true
    - name: "customer_id"
      type: "long"
    - name: "amount"
      type: "decimal"
      privacy: "HASH"
```

### 2.3 Transform

Execute SQL-based transformations with automatic write strategy application.

**Requirements:**
- SQL-based transform definitions with separate `.sql` and `.sl.yml` files
- Automatic MERGE INTO / INSERT OVERWRITE generation based on write strategy
- Pre-SQL and post-SQL hooks
- Dependency extraction from SQL (table references)
- Schema sync: detect column additions/removals between SQL output and YAML definition
- Interactive mode with CSV/JSON/table output
- Dry-run support (BigQuery cost estimation)
- Python UDF support
- Jinja2 templating in SQL with environment variable substitution
- Materialization options: TABLE, VIEW, ICEBERG, DELTA

**Configuration:**
```yaml
transform:
  tasks:
    - name: customer_lifetime_value
      writeStrategy:
        type: "OVERWRITE"
      schedule: "daily"
```
```sql
SELECT
  customer_id,
  SUM(amount) AS lifetime_value,
  COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
```

### 2.4 Orchestrate

Generate and deploy DAGs from declared dependencies.

**Requirements:**
- Automatic dependency inference from SQL table references
- DAG generation for Airflow (v2 and v3), Dagster, and Snowflake Tasks
- Customizable DAG templates (Jinja2)
- Schedule presets (daily, hourly, when_available, custom cron)
- DAG deployment to orchestrator filesystem or API
- Visual dependency graph output (Mermaid, GraphViz DOT, Reactflow)

### 2.5 Data Quality

Validate data at every stage of the pipeline.

**Requirements:**
- Expectations framework with Jinja2 macros (completeness, uniqueness, volume, range, etc.)
- Per-table and per-column validation rules
- Configurable failure behavior (warn vs. fail)
- Results persisted to audit database
- Row-level rejection tracking with reason codes

### 2.6 Data Governance

Embed governance into pipeline configuration.

**Requirements:**
- Column-level encryption (pluggable privacy algorithms)
- Row-level security (RLS) with role-based grants
- Access control lists (ACL) per table/column
- IAM policy tags (Google Cloud)
- Data lineage: table-level, column-level, and ACL dependency graphs
- Audit logging of all pipeline executions

### 2.7 Schema Management

**Requirements:**
- Auto-infer schemas from data files (CSV, JSON, XML, Parquet)
- Auto-infer from JDBC source metadata
- Auto-infer from JSON Schema / OpenAPI definitions
- Excel import/export for collaborative schema editing
- DDL generation for all supported warehouses
- Schema validation against JSON Schema (starlake.json)
- Migration system for schema version upgrades

### 2.8 Testing

**Requirements:**
- Declarative test definitions for load and transform steps
- Expected output comparison
- Domain/table/test-level filtering
- Test result reporting (table, JSON, site)
- Integration with CI/CD pipelines

---

## 3. Supported Platforms

### 3.1 Target Warehouses

| Warehouse | Load | Transform | Native Loader |
|---|---|---|---|
| BigQuery | Yes | Yes | Yes (Load API) |
| Snowflake | Yes | Yes | Yes (COPY) |
| Redshift | Yes | Yes | No (JDBC) |
| DuckDB | Yes | Yes | Yes (Direct) |
| PostgreSQL | Yes | Yes | No (JDBC) |
| Spark/Hive | Yes | Yes | N/A |
| Delta Lake | Yes | Yes | N/A |
| Apache Iceberg | Yes | Yes | N/A |

### 3.2 Source Systems

| Source | Method |
|---|---|
| JDBC Databases | Extract via JDBC (PostgreSQL, MySQL, SQL Server, Oracle, etc.) |
| BigQuery | Native API extraction |
| REST APIs | OpenAPI schema extraction |
| File Systems | CSV, JSON, XML, fixed-width, Parquet, ORC |
| Kafka | Streaming ingestion |
| Cloud Storage | GCS, S3, Azure Blob, HDFS |

### 3.3 Orchestrators

| Orchestrator | DAG Generation | Deployment |
|---|---|---|
| Airflow v2 | Yes | Filesystem / API |
| Airflow v3 | Yes | Filesystem / API |
| Dagster | Yes | Filesystem |
| Snowflake Tasks | Yes | SQL |

---

## 4. Deployment Models

### 4.1 CLI

Standalone command-line tool. Requires Java 17+. Distributed as a self-contained JAR or via shell installer.

### 4.2 Docker

Official Docker image `starlakeai/starlake:latest` with all dependencies pre-installed.

### 4.3 Embedded Server

REST API server mode (`starlake serve`) for integration with UIs and external systems. Default port 9900.

### 4.4 Library

Published to Maven Central as `ai.starlake:starlake-core_2.13`. Embeddable in Spark applications.

---

## 5. Non-Functional Requirements

### 5.1 Performance

- Native loader paths for BigQuery, Snowflake, and DuckDB bypass Spark overhead
- Parallel file processing and extraction
- Configurable Spark tuning
- Connection pooling for JDBC targets
- Caffeine-based settings caching for server mode

### 5.2 Reliability

- Transactional write strategies (atomic MERGE/UPSERT)
- Rejected record isolation (no partial loads pollute target tables)
- Retry logic for transient cloud failures (BigQuery retryable exceptions)
- Audit trail for every pipeline execution

### 5.3 Security

- Column-level encryption with pluggable algorithms
- Row-level security policies
- AES secret key support for connection password encryption
- Proxy configuration support
- No credentials in YAML (environment variable and Jinja2 substitution)

### 5.4 Extensibility

- SPI-based SchemaExtractor for custom source systems
- Pluggable row validators
- Custom Jinja2 macros for expectations
- Template-based DAG generation (bring your own templates)
- Custom type definitions with per-engine DDL mappings

### 5.5 Compatibility

- Java 17+
- All major cloud providers (GCP, AWS, Azure)
- Local development with DuckDB (no cloud required)
- Ducklake Support