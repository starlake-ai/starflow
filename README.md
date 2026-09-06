<p align="center">
  <img src="docs/static/img/starlake-draw.png" alt="Starflow" width="600"/>
</p>

<h1 align="center">Starflow</h1>
<h3 align="center">Declarative data pipelines by Starlake: Extract. Load. Transform. Orchestrate.</h3>

<p align="center">
  <a href="https://github.com/starlake-ai/starflow/actions/workflows/test-only.yml"><img src="https://github.com/starlake-ai/starflow/actions/workflows/test-only.yml/badge.svg?event=pull_request" alt="Build Status"/></a>
  <a href="https://github.com/starlake-ai/starflow/releases/latest"><img src="https://img.shields.io/github/v/release/starlake-ai/starflow" alt="GitHub Release"/></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"/></a>
  <a href="https://github.com/starlake-ai/starflow/stargazers"><img src="https://img.shields.io/github/stars/starlake-ai/starflow" alt="GitHub Stars"/></a>
  <a href="https://discord.com/invite/6tNa7yCNqw"><img src="https://img.shields.io/badge/Discord-join%20us-5865F2?logo=discord&logoColor=white" alt="Discord"/></a>
</p>

<p align="center">
  <a href="https://docs.starlake.ai/">Documentation</a> &bull;
  <a href="https://docs.starlake.ai/setup/starlake-core-setup">Installation</a> &bull;
  <a href="https://discord.com/invite/6tNa7yCNqw">Discord</a> &bull;
  <a href="https://github.com/starlake-ai/starlake-data-stack">Data Stacks</a> &bull;
  <a href="https://docs.starlake.ai/devguide/contribute">Contributing</a>
</p>

---

**Your warehouse, described, not scripted.** Starflow turns hundreds of lines of BigQuery/Snowflake/Redshift/Spark boilerplate into a few lines of YAML: declare **what** your pipeline does, and Starflow works out **how**: schemas, merges, quality checks, lineage, and the DAGs to run it all.

## A pipeline in 30 seconds

Describe the table once: file pattern, merge strategy, validation:

```yaml
# metadata/load/crm/customers.sl.yml
table:
  pattern: "customers.*.csv"              # files landing in your incoming folder
  metadata:
    writeStrategy:
      type: UPSERT_BY_KEY_AND_TIMESTAMP   # incremental merge, no MERGE SQL to write
      key: [id]
      timestamp: signup
  attributes:
    - name: id
      type: string
      required: true
    - name: signup
      type: timestamp
    - name: email
      type: string                        # or one of your own semantic types, validated at load time
```

Then let Starflow do the engineering:

```bash
starlake bootstrap                        # scaffold a project
starlake load                             # infer, validate and merge into your warehouse
starlake transform --name kpi.revenue     # run SQL with the right MERGE/INSERT logic
```

<p align="center"><img src="docs/static/img/transform-dags.png" alt="Generated DAG" width="500"/></p>
<p align="center"><i>The Airflow DAG above was generated from the SQL dependencies. Nobody wrote it.</i></p>

## Why teams pick Starflow

- **Config, not code**: YAML and plain SQL replace bespoke ETL scripts and orchestration glue.
- **Any source, any warehouse, any orchestrator**: files, JDBC databases and Kafka into BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL, Delta Lake or Iceberg, scheduled on Airflow, Dagster or Snowflake Tasks with generated DAGs.
- **Quality and lineage built in**: expectations run at load time, and table- and column-level lineage falls out of your SQL automatically.
- **Privacy by declaration**: column-level encryption, row- and column-level security policies applied from the same YAML.
- **AI-assistant-ready**: MCP-based [Starlake Skills](https://github.com/starlake-ai/starlake-skills) teach Claude Code and GitHub Copilot to build and debug pipelines with you.

## Quick Start

**macOS / Linux:**
```bash
bash <(curl -sL https://starlake.ai/setup.sh)
```

**Windows (PowerShell):**
```powershell
Invoke-Expression (Invoke-WebRequest -Uri "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/setup.ps1" -UseBasicParsing).Content
```

**Docker:**
```bash
docker run -it starlakeai/starlake:latest starlake bootstrap
```

> The product is Starflow; the CLI keeps its historical name `starlake`.

For pre-built production-ready data stacks, see [Starlake Pragmatic Data Stacks](https://github.com/starlake-ai/starlake-data-stack).

## How it works

<img src="docs/static/img/intent.png" alt="Starflow pipeline flow"/>

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

Point Starflow at your files: it infers schemas, validates every row against the declared types and expectations, and applies the merge strategy you declared. Malformed lines are quarantined into an audit trail and a replay file instead of failing the load.

### 3. Transform

Write plain SQL; Starflow wraps it in the correct MERGE/INSERT/OVERWRITE logic for your warehouse:

```sql
SELECT
  productid,
  SUM(unitprice * orderqty) AS total_revenue
FROM salesorderdetail
GROUP BY productid
ORDER BY total_revenue DESC
```

```yaml
transform:
  tasks:
    - name: most_profitable_products
      writeStrategy:
        type: "UPSERT_BY_KEY_AND_TIMESTAMP"
        timestamp: order_date
        key: [productid]
```

### 4. Orchestrate

Starflow extracts the dependencies between your loads and transforms and generates the DAGs:

<p align="center"><img src="docs/static/img/transform-viz.svg" alt="Dependency graph" width="500"/></p>

Reference built-in templates for Airflow, Dagster, or Snowflake Tasks in your YAML. No custom DAG code required.

## Supported platforms

| Category | Supported |
|---|---|
| **Warehouses** | BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL, Spark/Hive |
| **Lake Formats** | Delta Lake, Apache Iceberg, Parquet |
| **File Formats** | CSV/DSV, JSON, XML, Fixed-width, Parquet |
| **Orchestrators** | Airflow (v2 & v3), Dagster, Snowflake Tasks |
| **Streaming** | Kafka |
| **Cloud Storage** | GCS, S3, Azure Blob, HDFS, Local |

## IDE & AI support

The [Starlake VS Code Extension](https://marketplace.visualstudio.com/items?itemName=Starlake.starlake) brings Starflow into your editor: schema inference, SQL transformations, ER diagrams, lineage visualization, and workflow orchestration without leaving VS Code.

It ships with [Starlake Skills](https://github.com/starlake-ai/starlake-skills), MCP-based skills that give AI coding assistants like **Claude Code** and **GitHub Copilot** deep knowledge of the platform, so your assistant builds, debugs, and optimizes pipelines using Starflow best practices.

## Community & documentation

- **Discord**: questions, feedback and release news. [Join us](https://discord.com/invite/6tNa7yCNqw)
- **Docs**: guides, concepts, and the full configuration reference at [docs.starlake.ai](https://docs.starlake.ai/)
- **Contributing**: see the [Contributing Guide](https://docs.starlake.ai/devguide/contribute) and [Code of Conduct](CODE_OF_CONDUCT.md)

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
