# Starlake vs. dbt, Fivetran/Airbyte, Matillion/Talend/Informatica

A side-by-side look at where Starlake fits in the modern data stack.
This page is maintained by hand and reviewed quarterly. If a cell is
wrong, open a PR — every claim is meant to be defensible.

## Legend

| Symbol | Meaning |
|---|---|
| ✅ | First-class support, available out of the box |
| ⚠️ | Partial support, requires an extra tool, or has a significant caveat (footnoted below) |
| ❌ | Not supported |

Numbered superscripts (¹, ², …) refer to footnotes at the bottom of the page.

## 1. ELT lifecycle coverage

| Capability | Starlake | dbt | Fivetran/Airbyte | Matillion/Talend/Informatica |
|---|---|---|---|---|
| Extract from operational DBs (JDBC) | ✅ | ❌ | ✅ | ✅ |
| Load raw files (CSV/JSON/XML/Parquet) | ✅ | ❌ | ⚠️¹ | ✅ |
| Declarative schema validation on ingest | ✅ | ❌ | ⚠️² | ⚠️³ |
| Merge strategies (UPSERT / SCD2 / APPEND) | ✅ | ⚠️⁴ | ❌ | ✅ |
| SQL transforms | ✅ | ✅ | ❌ | ✅ |
| Python transforms | ⚠️⁵ | ⚠️⁶ | ❌ | ✅ |
| Orchestration / DAG generation | ✅ | ⚠️⁷ | ❌ | ✅ |

## 2. Engines & warehouses

| Engine | Starlake | dbt | Fivetran/Airbyte | Matillion/Talend |
|---|---|---|---|---|
| BigQuery / Snowflake / Redshift / Databricks / Postgres | ✅ | ✅ | ✅ | ✅ |
| DuckDB (local dev) | ✅ | ✅ | ❌ | ❌ |
| Spark (self-managed) | ✅ | ⚠️⁸ | ❌ | ⚠️⁹ |
| Delta Lake / Iceberg | ✅ | ⚠️¹⁰ | ❌ | ⚠️ |

## 3. Quality & governance

| Capability | Starlake | dbt | Fivetran/Airbyte | Matillion |
|---|---|---|---|---|
| Declarative expectations / tests | ✅ | ✅ | ❌ | ⚠️ |
| Table-level lineage | ✅ | ✅ | ⚠️ | ⚠️ |
| Column-level lineage | ✅ | ⚠️¹¹ | ❌ | ⚠️ |
| Row-level security (RLS) | ✅ | ❌ | ❌ | ⚠️ |
| Column-level security (CLS) | ✅ | ❌ | ❌ | ⚠️ |
| IAM / policy tags | ✅ | ❌ | ❌ | ⚠️ |

## 4. Developer experience

| Trait | Starlake | dbt | Fivetran/Airbyte | Matillion |
|---|---|---|---|---|
| Open source | ✅ | ✅¹² | ✅¹³ | ❌ |
| Self-hosted option | ✅ | ✅ | ⚠️ | ⚠️ |
| Declarative YAML config | ✅ | ⚠️¹⁴ | ❌ | ❌ |
| Local dev (no warehouse needed) | ✅ | ✅¹⁵ | ❌ | ❌ |
| CI-friendly (text-based, diffable) | ✅ | ✅ | ❌ | ⚠️ |
| GUI editor | ⚠️¹⁶ | ⚠️ | ✅ | ✅ |

## Footnotes

1. Airbyte supports file connectors; Fivetran has limited file ingestion.
2. Schema-drift detection only; not a declarative contract.
3. GUI-driven schema definition rather than as-code.
4. Via incremental models and snapshots (dbt-utils / dbt snapshots for SCD2).
5. Python transforms supported via Spark jobs; not a general Python ETL runtime.
6. Python models supported on a subset of warehouses (BigQuery, Snowflake, Databricks).
7. dbt Cloud has built-in scheduling; dbt Core requires an external orchestrator.
8. Via dbt-spark adapter.
9. Talend has Spark execution; Matillion / Informatica do not natively.
10. Via warehouse-side or adapter-side support; varies per adapter.
11. dbt Cloud Explorer offers column lineage; Core requires extra tooling.
12. dbt Core is OSS (Apache 2.0); dbt Cloud is proprietary SaaS.
13. Airbyte OSS is open source; Fivetran is proprietary.
14. dbt uses YAML for config and Jinja-templated SQL for models.
15. Via the dbt-duckdb adapter.
16. Starlake provides an Excel-driven config workflow and a VSCode plugin, but no full visual drag-and-drop editor.