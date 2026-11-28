# AGENTS.md — how to work in this Starlake project

You are assisting inside a **Starlake / Starflow** data pipeline project created
from the `empty-project` template. This file is the contract for how you work
here. It describes **method**: what it says about the project's *contents* is
true of the template as shipped and stops being true the moment someone adds
something, so the filesystem is always the authority.

---

## What this project is

A `metadata/` skeleton and nothing else: **no domains, no tables, no transforms,
no tests**. Each of those exists only once someone has created it. At any moment
the project contains exactly what is on disk.

What the template does ship:

- `metadata/application.sl.yml` — connections (DuckDB, BigQuery, Snowflake,
  PostgreSQL, Redshift), the audit sink, DAG references and schedule presets
- `metadata/env.sl.yml` — the default environment (`activeConnection: duckdb`);
  override it with `metadata/env.<NAME>.sl.yml` and `SL_ENV=<NAME>`
- `metadata/types/` — the built-in types with their DDL mapping per engine
- `metadata/expectations/` — the Jinja2 data-quality macros this project ships
- `metadata/dags/` — DAG configuration templates for Airflow, Dagster and
  Snowflake native
- `metadata/starlake.json` — the JSON Schema for every `.sl.yml`

The engine is **DuckDB** by default; both the tables you load and the `audit.*`
tables land in `datasets/duckdb.db`.

Starting points, once you know what the project should contain:

```bash
starlake infer-schema --domain <domain> --table <table> --input <file> --outputDir metadata/load
starlake autoload            # infer AND load everything under datasets/incoming/<domain>/
starlake validate
```

---
## Ground rules

**1 — Never assume. Read the project first.**
Do not answer a question about this project from a template, from a sample you
have seen elsewhere, or from an earlier turn in the conversation. List the
directory, open the file, run the command. A name that looks familiar because
another Starlake project uses it is a reason to check, not a reason to trust.

**2 — Never invent syntax.** YAML keys, CLI flags, expectation macro names and
write strategies either exist in this project's authorities or they do not
exist. YAML keys → `metadata/starlake.json`, the JSON Schema for every `.sl.yml`.
Expectation macros → `ls metadata/expectations/`, and that listing is the whole
list. CLI flags → `starlake <command> --help`. A key you half-remember from
documentation is not evidence: if you cannot point to where a name comes from,
do not write it.

**3 — Cite where each answer came from.** For every non-trivial claim, name the
file you read or the command you ran. "According to `metadata/load/x/y.sl.yml`"
is an answer; "typically, Starlake uses…" is a guess wearing an answer's clothes.

**4 — Run the gate. Do not predict it.** After any change to `metadata/`, run
`starlake validate` — and `starlake test` / `starlake dag-generate` when they
apply — and report the real output. Never write "this should validate".

**5 — Change only what was asked.** One table, one task, one file. Do not
reformat, reorder attributes, "fix" neighbouring files, or delete comments you
did not write. Attribute order is part of a table's contract.

**6 — "I checked, and it is not there" is a correct answer.** So is "I don't
know". A plausible answer that turns out to be invented costs more than a
question.

---

## Before you answer — look here first

| The question is about… | Read or run this before answering |
|---|---|
| which domains / tables exist | `ls metadata/load/` then `ls metadata/load/<domain>/` |
| a table's columns, types, write strategy | `metadata/load/<domain>/<table>.sl.yml` |
| which transforms exist | `ls metadata/transform/` and its `*.sl.yml` + `*.sql` pairs |
| which expectation macros are available | `ls metadata/expectations/` — these `.j2` files are the whole list |
| which types are available | `metadata/types/types.sl.yml` and `default.sl.yml` |
| which connection / engine is active | `metadata/env.sl.yml`, then `starlake settings` |
| what a load or transform actually did | the `audit.audit` table |
| why a row was rejected | the `audit.rejected` table |
| whether a declared rule passed | the `audit.expectations` table |
| what feeds what | `starlake lineage`, `starlake col-lineage`, `starlake table-dependencies` |
| which DAG templates ship here | `ls metadata/dags/` |
| any `.sl.yml` key you are unsure of | `metadata/starlake.json` |
| any CLI flag you are unsure of | `starlake <command> --help` |

---

## The authorities, in order

When they disagree, the earlier one wins.

1. **This project's files** — `metadata/**`, `metadata/starlake.json`,
   `metadata/expectations/*.j2`, `metadata/types/*.sl.yml`.
2. **`starlake <command> --help`** — the flags this CLI version actually accepts.
3. **The installed Starlake skills** — one per CLI command, plus configuration
   and best-practice skills. Consult the skill for a command *before* writing
   that command.
4. **Nothing else.** Not your memory of the documentation, not a blog post, not
   another Starlake project.

---

## The gate

```bash
starlake validate                                  # the YAML is well-formed and coherent
starlake test                                      # the unit tests, when the project has any
starlake dag-generate --clean --outputDir out/dags # the orchestration still generates
```

Two things about this CLI you must not misread:

- **It is silent on success.** Exit code 0 with empty stdout means it worked, not
  that it did nothing. Prefix a command with `SL_LOG_LEVEL=info` to see the SQL it
  ran, the write strategy it applied and the audit rows it inserted.
- **`validate` judges the YAML, not the result.** A configuration can validate
  perfectly and still load the wrong rows. Only the data and the `audit.*` tables
  tell you what happened.

Before running any `starlake` command, check the CLI is on the PATH with
`starlake --version`; if it is not, ask for its path rather than guessing one. On
Windows the command is `starlake.cmd`.

---

## Reading what actually happened

The audit tables live in the same warehouse as the data.

| Table | Answers |
|---|---|
| `audit.audit` | what ran: domain, table, step (`LOAD` / `TRANSFORM`), counts, success |
| `audit.rejected` | which rule stopped a row, and which file it came from |
| `audit.expectations` | which declared data-quality rule passed or failed, and its count |

Never describe what one of these tables holds from memory — read its columns
first. An audit table that does not store what you are about to hand back is a
trap you can only avoid by looking:

```bash
starlake transform --name __ignore__.__ignore__ --query "describe audit.rejected" --interactive csv
```

`__ignore__.__ignore__` is the CLI's sentinel for "no task, just run this SQL";
any other name must resolve to a task that exists.

---

## The skills are your manual

A Starlake skill is installed for every CLI command and for the main
configuration topics. They are the reference for syntax and options — read the
relevant one instead of recalling it.

- Claude Code: `~/.claude/skills/`
- Gemini CLI: `~/.gemini/skills/`
- GitHub Copilot: `~/.copilot/skills/`

**The directory listing is the authoritative catalogue** — run `ls` on it rather
than trusting the list below, which names only the ones a project like this one
uses most:

- **Project & config** — `config`, `connection`, `settings`, `validate`,
  `bootstrap`, `migrate`, `console`, `serve`
- **Schema discovery** — `infer-schema`, `autoload`, `preload`, `extract-schema`,
  `extract-data`, `extract-bq-schema`, `extract-rest-schema`, `extract-rest-data`
- **Loading** — `stage`, `load`, `ingest`, `cnxload`, `esload`, `kafkaload`
- **Data quality** — `expectations`, `test`, `metrics`, `summarize`, `freshness`
- **Transforms & lineage** — `transform`, `lineage`, `col-lineage`,
  `table-dependencies`, `acl-dependencies`
- **Semantic & catalog** — `semantic`, `semantic-export`, `site`, `secure`,
  `iam-policies`
- **Orchestration** — `dag-generate`, `dag-create`, `dag-template-generate`,
  `dag-deploy`

**Starflow** adds a guided methodology on top: expert personas
(`starflow-data-architect`, `starflow-data-engineer`,
`starflow-data-quality-engineer`, `starflow-data-analyst`,
`starflow-platform-engineer`) and workflow skills
(`starflow-create-pipeline-spec`, `starflow-dev-pipeline`,
`starflow-schema-design`, `starflow-transform-design`,
`starflow-semantic-model-design`, `starflow-orchestration-design`,
`starflow-code-review`, `starflow-data-quality-review`,
`starflow-lineage-review`, `starflow-help`).

They are installed from [starlake-ai/starlake-skills](https://github.com/starlake-ai/starlake-skills);
if the directory is empty, say so rather than guessing a command's options.

---

## Configuration conventions

- All metadata files use the `.sl.yml` extension and start with `version: 1`
- Variables use `{{VAR_NAME}}` Mustache-style templating, resolved from the env
  files or the shell environment
- Load tables define `pattern`, `metadata.format` (DSV / JSON / JSON_FLAT…) and a
  `writeStrategy`
- Transform tasks pair a `.sl.yml` (columns, domain, write strategy) with a `.sql`
  file holding the query
- Data-quality expectations in `metadata/expectations/` are Jinja2 macros invoked
  as `{{ macro_name(table, column, ...) }}`
- `metadata/starlake.json` is the JSON Schema for every `.sl.yml`, which is what
  gives an editor its validation and completion

---

## How to hand back a change

1. Say what you read to decide (files, commands).
2. Show the diff, minimal, one concern at a time.
3. Run the gate and paste its **real** output.
4. State plainly what you did not verify, and what you would check next.

If a request is ambiguous — two tables could be meant, two write strategies would
both work — ask, or state the assumption in one line and proceed. Do not silently
pick one and present it as the only reading.
