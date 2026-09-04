# Replay File Support for DuckDB Native Loads

Date: 2026-09-04

## Problem

Replay files are produced only by the Spark ingestion path. `IngestionJob.saveRejected`
(`src/main/scala/ai/starlake/job/ingest/IngestionJob.scala:227`), guarded by the
`sinkReplayToFile` setting, writes `{domain}.{table}.{yyyyMMddHHmmss}.replay` into
`DatasetArea.replay(domain)`. Its only call site is `SparkIngestionPipeline.scala:51`.

The DuckDB native loader has no rejection story at all:

* `NativeValidator` is a marker that returns an empty rejected dataset and is never called.
* `DuckDbNativeLoader.run()` reports `rejectedCount = -1`
  (`src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala:217`).
* `singleStepLoad` issues `INSERT INTO <target> SELECT * FROM read_csv(..., columns = {...})`
  with no error tolerance, so a single malformed row aborts the entire load.

Because `rejectedCount` is always `-1`, `IngestionAudit.scala:78`
(`success = !rejectAllOnError || rejectedCount == 0`) is effectively inert for native
DuckDB loads.

## Goal

Give DuckDB native loads the same rejection story as Spark loads: bad input lines are
skipped, counted, written to a replay file that can be dropped back into the landing
area, and recorded in the audit `rejected` table.

## Verified DuckDB behavior

Checked against the DuckDB CLI 1.5.4 (the project pins `duckdb_jdbc` 1.5.5.1 in
`project/Versions.scala:50`):

* `store_rejects = true` works alongside the explicit `columns = {...}` type spec that
  Starlake already generates.
* `reject_errors.csv_line` holds the byte faithful original input line.
* `reject_errors` yields one row per bad column, so a line with two bad columns produces
  two rows. Deduplication by `(file_id, line)` is required.
* `reject_scans` maps `scan_id` and `file_id` to `file_path`.
* `reject_scans` and `reject_errors` are session scoped temporary tables, so they must be
  queried on the same JDBC connection that ran the `read_csv`.
* `ROLLBACK` also discards `reject_errors`. Rejects must be materialized into Scala values
  before any rollback.
* DDL (`CREATE SCHEMA`, `DROP TABLE`, `CREATE TABLE`), `INSTALL httpfs` / `LOAD httpfs`
  and the `read_csv` INSERT all coexist inside one transaction, and `ROLLBACK` correctly
  undoes the DDL.
* `read_json` has no equivalent rejects mechanism.

## Decisions

| Question | Decision |
| --- | --- |
| Error semantics | `read_csv` always runs with `store_rejects`. Bad rows are skipped and counted. The load fails when the reject count exceeds `rejectMaxRecords`, or when `rejectAllOnError` is set and there is at least one reject. This mirrors the BigQuery native loader, which already calls `setMaxBadRecords` (`BigQueryNativeJob.scala:220`). |
| Format scope | DSV and POSITION. JSON keeps today's behavior and reports `rejectedCount = -1`. |
| Audit `rejected` table | Populated, without Spark, using the literal `SELECT ... UNION ALL` AutoTask pattern from `AuditLog.createTask` (`AuditLog.scala:242`). |
| POSITION reject rule | A line is rejected when `TRY_CAST` returns NULL on a non empty slice, or when `length(value)` is shorter than the last declared position. |
| Replay file content | Original raw lines verbatim, with the source header line prepended when `mergedMetadata.resolveWithHeader()` is true, so the file can be re-ingested as is. POSITION gets raw lines with no header, matching `PositionIngestionJob.defineOutputAsOriginalFormat`. |
| Threshold breach | Materialize the rejects, then `ROLLBACK` so the target table is left untouched, then fail the job. |
| Structure | Three small collaborators rather than inlining in `DuckDbNativeLoader` or generalizing across all native loaders. |

### Rejected alternative: SQL side type pattern validation

Enforcing each attribute's Starlake `Type` pattern in SQL was considered and rejected. It
would make the same file load differently under Spark and under DuckDB, sometimes
silently:

* `TypesInfo.scala:127` validates with `matcher(value).matches()`, a full match, while
  DuckDB's `regexp_matches` is a partial match. With the shipped `integer` pattern
  `[-|\+|0-9][0-9]*`, the value `12abc` is rejected by Spark and accepted by DuckDB.
  Naive anchoring with `^...$` breaks patterns with a top level alternation, and the
  patterns are compiled with `Pattern.MULTILINE` (`TypesInfo.scala:78`), which changes
  what `^` and `$` mean.
* All date and timestamp types carry a Java `DateTimeFormatter` pattern rather than a
  regex: `yyyy-MM-dd HH:mm:ss`, `BASIC_ISO_DATE`, `RFC_1123_DATE_TIME`,
  `ISO_ZONED_DATE_TIME`. `ISO_ZONED_DATE_TIME` (sample `2011-12-03T10:15:30+01:00[Europe/Paris]`)
  has no `strptime` equivalent.
* The `boolean` type uses a Starlake private encoding, two patterns joined by `<-TF->`
  and split at `TypesInfo.scala:91`. There is no single regex to hand DuckDB.
* DuckDB uses RE2, which has no lookahead, lookbehind, backreferences, atomic groups or
  possessive quantifiers. A user type such as `pattern: "(?!000)\\d{3}"` compiles in Java
  and makes DuckDB fail the whole load with an invalid regex error.

Validation in the DuckDB native path is therefore limited to what the engine itself can
decide: cast success, column count, and declared field positions. Type pattern validation
remains a Spark mode guarantee.

## Components

### `DuckDbRejectCapture` (new, `job/ingest/loaders/`)

```scala
case class RejectedLine(file: String, line: Long, rawLine: String, error: String)
```

`captureCsvRejects(conn: Connection): List[RejectedLine]` runs immediately after the
`read_csv` INSERT, on the same connection:

```sql
SELECT s.file_path, e.line, e.csv_line,
       string_agg(e.error_type || ': ' || e.error_message, ' | ') AS error
FROM reject_errors e JOIN reject_scans s USING (scan_id, file_id)
WHERE e.scan_id = (SELECT max(scan_id) FROM reject_scans)
GROUP BY 1, 2, 3
```

The `GROUP BY` collapses DuckDB's one row per bad column into one row per input line, so
the resulting count is comparable to `acceptedCount`.

`capturePositionRejects(conn, tempTable, schema, ddlTypes): List[RejectedLine]` builds a
boolean predicate from the attribute positions and DDL types:

```sql
length(value) < <maxLastPosition + 1>
OR (TRIM(SUBSTR(value, p.first + 1, p.last - p.first + 1)) <> ''
    AND TRY_CAST(SUBSTR(value, p.first + 1, p.last - p.first + 1) AS <ddlType>) IS NULL)
OR ...   -- one clause per non string attribute
```

Positions are zero based and inclusive, consistent with `positionProjection` in
`SchemaInfo.buildSecondStepSqlSelectOnLoad`, which emits
`SUBSTR(value, pos.first + 1, pos.last - pos.first + 1)`. The short line clause therefore
compares against `max(pos.last) + 1` over all attributes, which is the minimum line length
a complete record must have. Attributes whose DDL type is string like are not cast, so
they contribute no cast clause.

It runs `SELECT value, ... FROM <tempTable> WHERE <predicate>` to collect the lines, then
`DELETE FROM <tempTable> WHERE <predicate>` so the existing second step runs unchanged.
`SchemaInfo.buildSecondStepSqlSelectOnLoad` is deliberately left untouched, because it is
shared with the BigQuery and Snowflake loaders.

### `ReplayFileWriter` (new, engine neutral)

Writes `DatasetArea.replay(domain)/{domain}.{table}.{yyyyMMddHHmmss}.replay` through
`StorageHandler`, in the encoding from `mergedMetadata.resolveEncoding()`: the source
header line first when `resolveWithHeader()` is true, then each `rawLine` verbatim. No
Spark, and no part file merge, unlike `IngestionJob.scala:236` which calls
`moveSparkPartFile`.

### `NativeRejectedSink` (new, engine neutral)

Writes one row per `RejectedLine` into the audit `rejected` table via a literal
`SELECT ... UNION ALL` AutoTask on the audit connection, following `AuditLog.createTask`.
Columns match `IngestionUtil.rejectedCols`: `jobid`, `timestamp`, `domain`, `schema`,
`error`, `path`. Capped at `settings.appConfig.audit.maxErrors`.

Both `ReplayFileWriter` and `NativeRejectedSink` are engine neutral so the Snowflake
native loader can reuse them later, when its own `COPY ... VALIDATE` reject mechanism is
wired up.

## Data flow

DSV, single step (`singleStepLoad` writing directly to the target):

```
BEGIN -> DDL -> INSERT ... read_csv(..., store_rejects = true) -> captureCsvRejects
      -> threshold check -> COMMIT, or materialize rejects then ROLLBACK
```

DSV, two steps: `store_rejects` fires on each per path temp table load. Each call returns
its rejects to `run()`, which accumulates them across paths, applies the threshold before
the second step INSERT, and only then runs the transform task.

POSITION (always two steps, per `requireTwoSteps`): the temp load cannot fail because each
line is read as a single VARCHAR. `capturePositionRejects` then collects and deletes the
offending lines from each temp table before the second step.

JSON: unchanged.

After the load, `run()` writes the replay file when `sinkReplayToFile` is true, writes the
audit rejected rows when audit is active, and reports the counters.

## Error handling

* Threshold: `rejected > settings.appConfig.rejectMaxRecords`, or
  `settings.appConfig.rejectAllOnError && rejected > 0`, aborts the load.
* Rollback: `singleStepLoad` switches to `conn.setAutoCommit(false)` with explicit commit
  and rollback, mirroring `JdbcAutoTask.scala:332`. Rejects are read into Scala values
  before the rollback, because the rollback destroys `reject_errors`.
* Two step mode aborts before the second step INSERT. The existing `finally` block already
  drops the temp tables.
* Ordering on failure: the replay file and the audit rejected rows are written first, then
  the exception is thrown, so `IngestionAudit.logLoadFailureInAudit` records a failed load
  and the user still gets full diagnostics.
* A failure inside `ReplayFileWriter` or `NativeRejectedSink` fails the load, matching
  Spark's `saveRejected`, which fails when `IngestionUtil.sinkRejected` fails.
* `sinkReplayToFile = false` skips only the file. Counting and the audit rejected rows
  still happen.

## Counters

`IngestionCounters` for DuckDB native DSV and POSITION loads becomes:

* `rejectedCount`: number of distinct rejected input lines, replacing the hardcoded `-1`.
* `acceptedCount`: unchanged, the `currentRowCount - initialRowCount` delta.
* `inputCount`: `acceptedCount + rejectedCount`, instead of being set equal to
  `acceptedCount`.

This also activates `IngestionAudit.scala:78`, which is currently inert.

Known adjacent issue, explicitly out of scope: `initialRowCount` is read before
`singleStepLoad` drops the table for an OVERWRITE strategy, so
`currentRowCount - initialRowCount` goes negative when overwriting a non empty table.
Fixing it properly means capturing `getUpdateCount` from the INSERT, which is unrelated to
rejects.

## Configuration

No new settings. The feature reuses `sinkReplayToFile`, `rejectMaxRecords`,
`rejectAllOnError` and `audit.maxErrors`. The `sinkReplayToFile` comment at
`src/main/resources/reference-general.conf:198` is updated to state that it now covers
DuckDB native DSV and POSITION loads.

## Behavior changes

1. A malformed DSV line no longer aborts a DuckDB native load. It is skipped and counted,
   and the load fails only when a threshold is breached.
2. A POSITION line whose slice fails `TRY_CAST` is no longer loaded with NULL in that
   column. It is rejected and dropped. This contradicts the current contract asserted by
   `DuckDbNativePositionLoadSpec.scala:38` ("slice lines with SUBSTR and NULL malformed
   cells via TRY_CAST"), which loads `BadRow    abcde` as a third row with a NULL amount.
   That test is updated as part of this work.
3. DuckDB native loads now write rows into the audit `rejected` table, which previously
   only Spark loads did.

## Testing

New `DuckDbNativeRejectSpec`:

* DSV with a cast error and a missing column line: good rows loaded, `inputCount`,
  `acceptedCount` and `rejectedCount` correct, replay file contains the header plus both
  raw lines byte identical to the source, audit `rejected` holds one row per rejected line
  with the DuckDB error text.
* `rejectMaxRecords = 1` with two bad lines: the load fails, the target row count is
  unchanged, which proves the rollback, and the replay file is still written.
* `rejectAllOnError = true` with one bad line: the load fails.
* Two input files with one bad line each: a single replay file holding both lines, which
  proves two step accumulation across paths.
* `sinkReplayToFile = false`: no replay file, counters still correct.

Updated `DuckDbNativePositionLoadSpec`: the `BadRow    abcde` case now expects two loaded
rows and one reject, with the test name and the line 71 comment updated. A truncated line
case is added.

Unit tests for the `capturePositionRejects` predicate generation and for
`ReplayFileWriter` header and encoding handling.

`DuckDbNativeJsonLoadSpec` must pass unchanged.
