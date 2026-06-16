# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Starlake** data pipeline project ("StarBake") — a sample bakery analytics system demonstrating data ingestion, transformation, and KPI computation. The default engine is **DuckDB** (configurable via environment files).

## Starlake CLI

Before running any `starlake` command, verify the CLI is available in the PATH by running `starlake --version`. If not found, ask the user for the path to the starlake executable. On Windows, the command is `starlake.cmd` instead of `starlake`.

The CLI is silent by default — exit code 0 with empty stdout means success, not a no-op. To see execution logs (SQL run, write strategies, audit inserts), prefix the command with `SL_LOG_LEVEL=info`.

## Key Commands

```bash
# Validate project configuration
starlake validate

# Load source data (CSV/JSON) into the warehouse
# Always specify --domains, --tables, and --files explicitly
starlake load --domains starbake --tables customers --files "${SL_ROOT}/datasets/incoming/starbake/customers.csv"
starlake load --domains starbake --tables orders --files "${SL_ROOT}/datasets/incoming/starbake/orders.json"
starlake load --domains starbake --tables products --files "${SL_ROOT}/datasets/incoming/starbake/products.json"


# Run a specific transformation
starlake transform --name starbake_analytics.customer_purchase_history
starlake transform --name starbake_analytics.order_items_analysis
starlake transform --name starbake_kpis.overall_kpis
# Run a transformation (use --recursive to execute all upstream dependencies in order)
starlake transform --name starbake_kpis.overall_kpis --recursive

# Auto-infer schema and load
starlake autoload

# Generate DAGs for orchestration
starlake dag-generate

# Print settings / test a connection
starlake settings

# Run integration tests
starlake test

# Switch environment (default: DuckDB)
export SL_ENV=BQ        # BigQuery
export SL_ENV=PG        # PostgreSQL
export SL_ENV=SNOW      # Snowflake
export SL_ENV=REDSHIFT  # Redshift
```

## Architecture

### Data Pipeline Flow

```
datasets/incoming/starbake/     →  Load (metadata/load/)
    customers.*.csv                    ↓
    orders.*.json               →  starbake domain tables
    products.*.json                    ↓
                                   Transform (metadata/transform/)
                                       ↓
                               starbake_analytics/
                                 ├─ customer_purchase_history
                                 └─ order_items_analysis
                                       ↓
                               starbake_kpis/
                                 └─ overall_kpis
```

### Project Structure

- **`metadata/application.sl.yml`** — Main config: connections (DuckDB, BigQuery, Snowflake, PostgreSQL, Redshift), DAG references, schedule presets
- **`metadata/env.sl.yml`** — Default env vars (`activeConnection: duckdb`). Override with `metadata/env.{BQ,PG,SNOW,...}.sl.yml`
- **`metadata/load/starbake/`** — Source table schemas (3 tables). Each `.sl.yml` defines pattern, format, attributes, write strategy
- **`metadata/transform/`** — SQL transformations with paired `.sl.yml` (task metadata) + `.sql` (query) files
- **`metadata/types/`** — Custom data types with DDL mappings for each engine
- **`metadata/expectations/`** — Jinja2 data quality templates (completeness, uniqueness, volume, etc.)
- **`metadata/dags/`** — DAG definitions for Airflow (shell/Cloud Run/Fargate), Dagster, and Snowflake native
- **`metadata/external/`** — External table definitions for reading outputs
- **`datasets/`** — Sample data files and DuckDB database

### Transformation Dependency Chain

Transforms have an implicit dependency order based on table references in SQL:
1. `starbake_analytics.customer_purchase_history` and `starbake_analytics.order_items_analysis` read from `starbake.*` (loaded tables)
2. `starbake_kpis.overall_kpis` reads from both `starbake_analytics.*` tables (must run after step 1)

### Configuration Conventions

- All metadata files use `.sl.yml` extension and start with `version: 1`
- Variables use `{{VAR_NAME}}` Mustache-style templating, resolved from env files or shell environment
- Load tables define `pattern`, `metadata.format` (DSV/JSON_FLAT), and `writeStrategy`
- Transform tasks pair a `.sl.yml` (columns, domain, write strategy) with a `.sql` file (query logic)
- SQL uses DuckDB syntax by default; alternative Snowflake/Redshift syntax is often commented in `.sql` files
- `metadata/starlake.json` provides the JSON Schema for all `.sl.yml` files — used for IDE validation
- Data quality expectations in `metadata/expectations/` are Jinja2 macros invoked with `{{ macro_name(table, column, ...) }}`

### Tooling

- **VSCode extensions**: Starlake (`Starlake.starlake`), Jinja HTML (`samuelcolvin.jinjahtml`), Output Colorizer (`IBM.output-colorizer`)
- **Claude Code skills**: 45+ Starlake-specific skills installed at `.claude/skills/starlake-skills/` covering all CLI commands, config patterns, and best practices