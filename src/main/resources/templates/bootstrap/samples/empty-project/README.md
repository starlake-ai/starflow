# A new Starlake project

This project was created from the `empty-project` template. It holds a
`metadata/` skeleton and no data model yet: **no domains, no tables, no
transforms, no tests** — you add those.

## What is already here

| Path | What it is |
| --- | --- |
| `metadata/application.sl.yml` | connections (DuckDB, BigQuery, Snowflake, PostgreSQL, Redshift), the audit sink, DAG references, schedule presets |
| `metadata/env.sl.yml` | the default environment — `activeConnection: duckdb`. Add `metadata/env.<NAME>.sl.yml` and select it with `SL_ENV=<NAME>` |
| `metadata/types/` | the built-in types and their DDL mapping per engine |
| `metadata/expectations/` | the Jinja2 data-quality macros you can call from a table's `expectations:` |
| `metadata/dags/` | DAG configuration templates for Airflow, Dagster and Snowflake native |
| `metadata/starlake.json` | the JSON Schema behind every `.sl.yml` — this is what gives your editor validation and completion |
| `datasets/` | where the DuckDB database and your incoming files live |

The default engine is **DuckDB**, so the project runs with no cloud account and
no configuration: both your tables and the `audit.*` tables land in
`datasets/duckdb.db`.

## Getting started

```bash
export SL_ROOT=$PWD          # the variable that tells the CLI which project to act on
starlake validate            # should report no errors

# describe a source file as a table, then load it
starlake infer-schema --domain <domain> --table <table> \
         --input <path/to/file.csv> --outputDir metadata/load
starlake load --domains <domain> --tables <table> --files <path/to/file.csv>

# or let the CLI do both for everything under datasets/incoming/<domain>/
starlake autoload
```

The CLI is **silent on success**: exit code 0 with no output means it worked, not
that it did nothing. Prefix any command with `SL_LOG_LEVEL=info` to see the SQL it
ran, the write strategy it applied and the audit rows it inserted.

## Working with an AI assistant

`AGENTS.md` is the contract your assistant reads: what this project contains,
where to look each thing up, and the rules it must follow — read the project
rather than assume it, never invent a YAML key or a macro name, run
`starlake validate` instead of predicting it. `CLAUDE.md`, `GEMINI.md` and
`.github/copilot-instructions.md` point at it, each in the file its own assistant
loads. Keep them in step with the project as it grows.

## Where to go next

- [starlake.ai](https://starlake.ai) — product site and documentation
- `starlake --help`, and `starlake <command> --help` for any command
