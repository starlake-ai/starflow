<p align="center">
  <img src="docs/static/img/starlake-draw.png" alt="Starlake" width="600"/>
</p>

<h3 align="center">Declarative Data Pipelines. Extract. Load. Transform. Orchestrate.</h3>

<p align="center">
  <a href="https://github.com/starlake-ai/starlake/workflows/Build/badge.svg"><img src="https://github.com/starlake-ai/starlake/workflows/Build/badge.svg" alt="Build Status"/></a>
  <a href="https://central.sonatype.com/artifact/ai.starlake/starlake-core_2.13"><img src="https://img.shields.io/maven-central/v/ai.starlake/starlake-core_2.13?label=Maven%20Central" alt="Maven Central"/></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"/></a>
</p>

<p align="center">
  <a href="https://docs.starlake.ai/">Documentation</a> &bull;
  <a href="https://docs.starlake.ai/setup/starlake-core-setup">Installation</a> &bull;
  <a href="https://github.com/starlake-ai/starlake-data-stack">Data Stacks</a> &bull;
  <a href="https://docs.starlake.ai/devguide/contribute">Contributing</a>
</p>

---

Starlake replaces hundreds of lines of BigQuery/Snowflake/Redshift/Spark/SQL boilerplate with simple YAML declarations. Define **what** your data pipeline should do — Starlake figures out **how**.

Inspired by Terraform and Ansible, Starlake brings declarative programming to data engineering: schema inference, merge strategies, data quality checks, lineage tracking, and DAG generation — all from configuration files.

## Why Starlake?

- **No code, just config** - YAML declarations replace custom ETL scripts
- **Any warehouse** - BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL, Delta Lake, Iceberg
- **Any orchestrator** - Airflow, Dagster, Snowflake Tasks with auto-generated DAGs
- **Any source** - JDBC databases, CSV, JSON, XML, fixed-width, Parquet, Kafka
- **Schema inference** - Auto-detect formats, headers, separators, and data types
- **Built-in data quality** - Expectations and validation at load time
- **Data lineage** - Automatic dependency tracking across your entire pipeline
- **Privacy controls** - Column-level encryption and access policies

## Quick Start

**macOS / Linux:**
```bash
curl -sSL https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.sh | bash
```

**Windows (PowerShell):**
```powershell
Invoke-Expression (Invoke-WebRequest -Uri "https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.ps1" -UseBasicParsing).Content
```

**Docker:**
```bash
docker run -it starlakeai/starlake:latest starlake bootstrap
```

Then:
```bash
# Create a new project from a template
starlake bootstrap

# Load data
starlake load

# Run transformations
starlake transform --name my_domain.my_table
```

For pre-built production-ready data stacks, see [Starlake Pragmatic Data Stacks](https://github.com/starlake-ai/starlake-data-stack).

## How It Works

<img src="docs/static/img/intent.png" alt="Starlake pipeline flow"/>

### 1. Extract

Pull data from any JDBC source with a few lines of YAML:

```yaml
extract:
  connectionRef: "pg-adventure-works-db"
  jdbcSchemas:
    - schema: "sales"
      tables:
        - name: "salesorderdetail"
          partitionColumn: "salesorderdetailid"  # parallel extraction
          timestamp: salesdatetime               # incremental
```

### 2. Load

Define schemas, merge strategies, and data quality rules:

```yaml
table:
  pattern: "salesorderdetail.*.psv"
  metadata:
    writeStrategy:
      type: "UPSERT_BY_KEY_AND_TIMESTAMP"
      timestamp: signup
      key: [id]
  attributes:
    - name: "id"
      type: "string"
      required: true
    - name: "signup"
      type: "timestamp"
```

### 3. Transform

Write SQL, Starlake generates the correct MERGE/INSERT/OVERWRITE logic:

```yaml
transform:
  tasks:
    - name: most_profitable_products
      writeStrategy:
        type: "UPSERT_BY_KEY_AND_TIMESTAMP"
        timestamp: signup
        key: [id]
```
```sql
SELECT
  productid,
  SUM(unitprice * orderqty) AS total_revenue
FROM salesorderdetail
GROUP BY productid
ORDER BY total_revenue DESC
```

### 4. Orchestrate

Starlake extracts SQL dependencies and generates DAGs automatically:

<p align="center"><img src="docs/static/img/transform-viz.svg" alt="Dependency graph" width="500"/></p>
<p align="center"><img src="docs/static/img/transform-dags.png" alt="Generated DAG" width="500"/></p>

Reference built-in templates for Airflow, Dagster, or Snowflake Tasks in your YAML. No custom DAG code required.

## Supported Platforms

<p align="center">
  <img src="docs/static/img/data-star.png" alt="Supported platforms"/>
</p>

| Category | Supported |
|---|---|
| **Warehouses** | BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL, Spark/Hive |
| **Lake Formats** | Delta Lake, Apache Iceberg, Parquet |
| **File Formats** | CSV/DSV, JSON, XML, Fixed-width, Parquet |
| **Orchestrators** | Airflow (v2 & v3), Dagster, Snowflake Tasks |
| **Streaming** | Kafka |
| **Cloud Storage** | GCS, S3, Azure Blob, HDFS, Local |

## IDE & AI Support

### VS Code Extension

The [Starlake VS Code Extension](https://marketplace.visualstudio.com/items?itemName=Starlake.starlake) brings the full power of Starlake into your editor: schema inference, SQL transformations, ER diagrams, lineage visualization, and workflow orchestration, all without leaving VS Code.

### Starlake Skills

The extension ships with [Starlake Skills](https://github.com/starlake-ai/starlake-skills): MCP-based skills that supercharge AI coding assistants like **Claude Code** and **GitHub Copilot** with deep knowledge of the Starlake platform. Your AI assistant can help you build, debug, and optimize data pipelines using Starlake best practices.

## Documentation

Full documentation at **[docs.starlake.ai](https://docs.starlake.ai/)**

- [Installation Guide](https://docs.starlake.ai/setup/starlake-core-setup)
- [Concepts & Architecture](https://docs.starlake.ai/)
- [Configuration Reference](https://docs.starlake.ai/)
- [Contributing](https://docs.starlake.ai/devguide/contribute)

## Contributing

Contributions are welcome! See our [Contributing Guide](https://docs.starlake.ai/devguide/contribute) and [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
