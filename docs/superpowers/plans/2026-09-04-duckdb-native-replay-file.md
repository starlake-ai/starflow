# DuckDB Native Replay File Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture rejected input lines on DuckDB native DSV and POSITION loads, write them to a `.replay` file and to the audit `rejected` table, and report an accurate `rejectedCount`.

**Architecture:** DuckDB's `read_csv` gains `store_rejects = true`, so malformed lines are skipped instead of aborting the load, and their byte faithful original text is read back from the session scoped `reject_errors` table on the same JDBC connection. POSITION files never fail at read time, so their rejects are found by a cast/length predicate run against the first step temp table, whose offending rows are then deleted before the second step. Three small collaborators do the work: `DuckDbRejectCapture` (engine specific), `ReplayFileWriter` and `NativeRejectedSink` (engine neutral, reusable by the Snowflake loader later).

**Tech Stack:** Scala 2.13.18, SBT 1.11.5, JDK 17, DuckDB JDBC 1.5.5.1, ScalaTest via `ai.starlake.TestHelper`.

**Spec:** `docs/superpowers/specs/2026-09-04-duckdb-replay-file-design.md`

## Global Constraints

- Never use an em dash in any prose, comment or scaladoc you write.
- `sbt scalafmt` runs automatically on compile. Do not hand format around it.
- Tests are sequential and forked. Run a single spec with `sbt "testOnly *SpecName*"`.
- Do not modify `SchemaInfo.buildSecondStepSqlSelectOnLoad`. It is shared with the BigQuery and Snowflake native loaders.
- JSON loads keep today's behavior and keep reporting `rejectedCount = -1`.
- No new settings. Reuse `sinkReplayToFile`, `rejectMaxRecords`, `rejectAllOnError`, `audit.maxErrors`.
- All new files go under `src/main/scala/ai/starlake/job/ingest/loaders/`.
- Positions in `TableAttribute.position` are zero based and inclusive: attribute slice is `SUBSTR(value, first + 1, last - first + 1)`.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/main/scala/ai/starlake/job/ingest/loaders/RejectedLine.scala` (create) | `RejectedLine` case class and `RejectThresholdExceededException`. No logic. |
| `src/main/scala/ai/starlake/job/ingest/loaders/ReplayFileWriter.scala` (create) | Writes raw rejected lines to the replay area. Engine neutral, no Spark. |
| `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala` (create) | Reads DuckDB `reject_errors`, and builds/runs the POSITION reject predicate. |
| `src/main/scala/ai/starlake/job/ingest/loaders/NativeRejectedSink.scala` (create) | Writes rejected rows into the audit `rejected` table over JDBC, no Spark. |
| `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala` (modify) | Orchestrates: `store_rejects` in the DSV SQL, transaction, threshold, reporting, counters. |
| `src/main/resources/reference-general.conf` (modify) | Update the `sinkReplayToFile` comment. |

---

### Task 1: RejectedLine model and ReplayFileWriter

**Files:**
- Create: `src/main/scala/ai/starlake/job/ingest/loaders/RejectedLine.scala`
- Create: `src/main/scala/ai/starlake/job/ingest/loaders/ReplayFileWriter.scala`
- Test: `src/test/scala/ai/starlake/job/ingest/loaders/ReplayFileWriterSpec.scala`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `case class RejectedLine(file: String, line: Option[Long], rawLine: String, error: String)`
  - `class RejectThresholdExceededException(val rejected: List[RejectedLine], message: String) extends RuntimeException(message)`
  - `ReplayFileWriter.write(domainName: String, tableName: String, rejectedLines: List[RejectedLine], header: Option[String], encoding: String, timestamp: java.sql.Timestamp)(implicit settings: Settings, storageHandler: StorageHandler): Option[org.apache.hadoop.fs.Path]`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/job/ingest/loaders/ReplayFileWriterSpec.scala`:

```scala
package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.handlers.StorageHandler

import java.nio.charset.Charset
import java.sql.Timestamp

class ReplayFileWriterSpec extends TestHelper {

  new WithSettings() {

    implicit val implicitStorageHandler: StorageHandler = storageHandler

    private val timestamp = Timestamp.valueOf("2026-09-04 10:11:12")

    private def rejected(rawLines: String*): List[RejectedLine] =
      rawLines.toList.zipWithIndex.map { case (raw, idx) =>
        RejectedLine("file:///incoming/XTBL", Some(idx.toLong + 2), raw, "CAST: boom")
      }

    "ReplayFileWriter" should "write the header then every raw line verbatim" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "orders",
        rejectedLines = rejected("2;bob;NOTANUM", "badline;dave"),
        header = Some("id;name;amount"),
        encoding = "UTF-8",
        timestamp = timestamp
      )

      path.map(_.getName) shouldBe Some("sales.orders.20260904101112.replay")
      path.map(_.getParent) shouldBe Some(DatasetArea.replay("sales"))
      storageHandler.read(path.get) shouldBe
      "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
    }

    it should "omit the header when none is given" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "positions",
        rejectedLines = rejected("BadRow    abcde"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp
      )

      storageHandler.read(path.get) shouldBe "BadRow    abcde\n"
    }

    it should "write nothing when there is no rejected line" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "empty",
        rejectedLines = Nil,
        header = Some("id;name"),
        encoding = "UTF-8",
        timestamp = timestamp
      )

      path shouldBe None
    }

    it should "honor the requested encoding" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "latin",
        rejectedLines = rejected("1;café"),
        header = None,
        encoding = "ISO-8859-1",
        timestamp = timestamp
      )

      storageHandler.read(path.get, Charset.forName("ISO-8859-1")) shouldBe "1;café\n"
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly *ReplayFileWriterSpec*"`
Expected: FAIL at compilation with "not found: value ReplayFileWriter" and "not found: type RejectedLine".

- [ ] **Step 3: Create the model file**

Create `src/main/scala/ai/starlake/job/ingest/loaders/RejectedLine.scala`:

```scala
package ai.starlake.job.ingest.loaders

/** A single input line rejected by a native loader.
  *
  * @param file
  *   the input file the line comes from
  * @param line
  *   the 1 based line number inside that file, when the engine reports one. POSITION
  *   rejects are found by scanning a temporary table, which carries no line number, so
  *   they leave this empty.
  * @param rawLine
  *   the original line, byte faithful, as it appears in the input file
  * @param error
  *   a human readable description of every error found on that line
  */
case class RejectedLine(file: String, line: Option[Long], rawLine: String, error: String)

/** Thrown when the number of rejected lines breaches `rejectMaxRecords`, or when
  * `rejectAllOnError` is set and there is at least one reject. It carries the rejected
  * lines so the caller can still write the replay file and the audit rejected rows before
  * failing the load.
  */
class RejectThresholdExceededException(val rejected: List[RejectedLine], message: String)
    extends RuntimeException(message)
```

- [ ] **Step 4: Create the replay file writer**

Create `src/main/scala/ai/starlake/job/ingest/loaders/ReplayFileWriter.scala`:

```scala
package ai.starlake.job.ingest.loaders

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import java.sql.Timestamp
import java.text.SimpleDateFormat

/** Writes rejected input lines to a replay file that can be dropped back into the landing
  * area and ingested again. Engine neutral on purpose: it takes raw lines rather than a
  * DataFrame, so native loaders can use it without pulling in Spark.
  */
object ReplayFileWriter extends LazyLogging {

  /** @return
    *   the path written, or None when there is nothing to replay
    */
  def write(
    domainName: String,
    tableName: String,
    rejectedLines: List[RejectedLine],
    header: Option[String],
    encoding: String,
    timestamp: Timestamp
  )(implicit settings: Settings, storageHandler: StorageHandler): Option[Path] = {
    if (rejectedLines.isEmpty) {
      None
    } else {
      val replayArea = DatasetArea.replay(domainName)
      storageHandler.mkdirs(replayArea)
      val formattedDate = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)
      val targetPath = new Path(replayArea, s"$domainName.$tableName.$formattedDate.replay")
      val content = (header.toList ++ rejectedLines.map(_.rawLine)).mkString("", "\n", "\n")
      storageHandler.write(content, targetPath)(Charset.forName(encoding))
      logger.info(s"Wrote ${rejectedLines.size} rejected line(s) to $targetPath")
      Some(targetPath)
    }
  }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `sbt "testOnly *ReplayFileWriterSpec*"`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/RejectedLine.scala \
        src/main/scala/ai/starlake/job/ingest/loaders/ReplayFileWriter.scala \
        src/test/scala/ai/starlake/job/ingest/loaders/ReplayFileWriterSpec.scala
git commit -m "feat: add engine neutral replay file writer for native loaders"
```

---

### Task 2: Capture DSV rejects and report them in the counters

Today a single malformed DSV line aborts the whole DuckDB native load. This task makes `read_csv` skip and record bad lines, reads them back, and reports them in `IngestionCounters`. Note that DSV loads go through the two step path in practice, because `requireTwoSteps` returns true for any non variant schema, so per path accumulation matters.

**Files:**
- Create: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala`
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala` (`run()`, `singleStepLoad`)
- Create: `src/test/resources/sample/dsvduckreject/dsvduckreject.sl.yml`
- Create: `src/test/resources/sample/dsvduckreject/account_dsvduckreject.sl.yml`
- Create: `src/test/resources/sample/dsvduckreject/XDSVREJECTTBL`
- Test: `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala`

**Interfaces:**
- Consumes: `RejectedLine` from Task 1.
- Produces:
  - `DuckDbRejectCapture.captureCsvRejects(conn: java.sql.Connection): List[RejectedLine]`
  - `DuckDbNativeLoader.singleStepLoad(...): List[RejectedLine]` (was `Unit`)

- [ ] **Step 1: Create the test fixtures**

Create `src/test/resources/sample/dsvduckreject/dsvduckreject.sl.yml`:

```yaml
---
version: 1
load:
  name: "dsvduckreject"
  metadata:
    directory: "__SL_TEST_ROOT__/dsvduckreject"
    format: "DSV"
    separator: ";"
    withHeader: true
    loader: "native"
    writeStrategy:
      type: "APPEND"
```

Create `src/test/resources/sample/dsvduckreject/account_dsvduckreject.sl.yml`:

```yaml
---
version: 1
table:
  name: "account"
  pattern: "XDSVREJECT.*"
  attributes:
    - name: "id"
      type: "long"
      required: true
    - name: "name"
      type: "string"
      required: true
    - name: "amount"
      type: "double"
      required: false
```

Create `src/test/resources/sample/dsvduckreject/XDSVREJECTTBL` with exactly these six lines, the last one ending with a newline:

```
id;name;amount
1;alice;10.5
2;bob;NOTANUM
3;carol;7
badline;dave
5;eve;3.3
```

Line 3 has a value that cannot cast to DOUBLE. Line 5 has only two columns and an id that cannot cast to BIGINT. The other three lines are valid.

- [ ] **Step 2: Write the failing test**

Create `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala`:

```scala
package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}

class DuckDbNativeRejectSpec extends TestHelper {

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_reject_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbConfiguration) {

    private def queryDuckDb[T](sql: String)(f: java.sql.ResultSet => T): T = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn.createStatement().executeQuery(sql)
        f(rs)
      }
    }

    "Native DuckDB load of a DSV file with malformed lines" should
    "load the good lines and count the rejected ones" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb(
          "SELECT name FROM dsvduckreject.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }
        names shouldBe List("alice", "carol", "eve")

        val counters = result.get.counters
        counters.size shouldBe 1
        counters.head.acceptedCount shouldBe 3
        counters.head.rejectedCount shouldBe 2
        counters.head.inputCount shouldBe 5
      }
    }
  }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: FAIL. The load aborts on the malformed line, so `result.isSuccess shouldBe true` fails with a DuckDB conversion error.

- [ ] **Step 4: Create the reject capture**

Create `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala`:

```scala
package ai.starlake.job.ingest.loaders

import com.typesafe.scalalogging.LazyLogging

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import scala.util.Using

/** Reads back the lines DuckDB refused while loading.
  *
  * `reject_scans` and `reject_errors` are session scoped temporary tables populated by
  * `read_csv(..., store_rejects = true)`. They only exist on the connection that ran the
  * scan, and a ROLLBACK discards them, so they must be read on that same connection and
  * before any rollback.
  */
object DuckDbRejectCapture extends LazyLogging {

  /** DuckDB records one row per bad column, so a line with two bad columns yields two
    * rows. The GROUP BY collapses them into one entry per input line, which keeps the
    * resulting count comparable to the accepted row count.
    */
  private val captureCsvRejectsSql =
    """SELECT s.file_path AS file, e.line AS line, e.csv_line AS raw_line,
      |       string_agg(CAST(e.error_type AS VARCHAR) || ': ' || e.error_message, ' | ') AS error
      |FROM reject_errors e JOIN reject_scans s USING (scan_id, file_id)
      |WHERE e.scan_id = (SELECT max(scan_id) FROM reject_scans)
      |GROUP BY 1, 2, 3
      |ORDER BY 1, 2""".stripMargin

  def captureCsvRejects(conn: Connection): List[RejectedLine] = {
    val rejected = ListBuffer[RejectedLine]()
    Using.resource(conn.createStatement()) { statement =>
      Using.resource(statement.executeQuery(captureCsvRejectsSql)) { rs =>
        while (rs.next()) {
          rejected += RejectedLine(
            file = rs.getString("file"),
            line = Some(rs.getLong("line")),
            rawLine = rs.getString("raw_line"),
            error = rs.getString("error")
          )
        }
      }
    }
    if (rejected.nonEmpty) {
      logger.warn(s"DuckDB rejected ${rejected.size} line(s) while reading the input files")
    }
    rejected.toList
  }
}
```

- [ ] **Step 5: Add store_rejects to the DSV read and return the rejects**

In `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala`, change `singleStepLoad` to return the rejected lines. Change its signature line from:

```scala
  private def singleStepLoad(
    domain: String,
    table: String,
    schema: SchemaInfo,
    path: List[Path]
  ) = {
```

to:

```scala
  private def singleStepLoad(
    domain: String,
    table: String,
    schema: SchemaInfo,
    path: List[Path]
  ): List[RejectedLine] = {
```

In the `Format.DSV` branch, add `store_rejects = true,` to the `read_csv` options and capture the rejects. Replace:

```scala
            val sql = s"""INSERT INTO $domainAndTableName SELECT
               | * FROM read_csv(
               | ${paths},
               | delim = '${mergedMetadata.resolveSeparator()}',
               | header = ${mergedMetadata.resolveWithHeader()},
               | quote = '${mergedMetadata.resolveQuote()}',
               | escape = '${mergedMetadata.resolveEscape()}',
               | $nullstr
               | $extraOptions
               | columns = { $columnsString});""".stripMargin
            JdbcDbUtils.execute(sql, conn)
```

with:

```scala
            // store_rejects makes DuckDB skip and record malformed lines instead of
            // aborting the whole INSERT. The rejected lines are read back below, on this
            // same connection, because reject_errors is a session scoped temp table.
            val sql = s"""INSERT INTO $domainAndTableName SELECT
               | * FROM read_csv(
               | ${paths},
               | delim = '${mergedMetadata.resolveSeparator()}',
               | header = ${mergedMetadata.resolveWithHeader()},
               | quote = '${mergedMetadata.resolveQuote()}',
               | escape = '${mergedMetadata.resolveEscape()}',
               | store_rejects = true,
               | $nullstr
               | $extraOptions
               | columns = { $columnsString});""".stripMargin
            JdbcDbUtils.execute(sql, conn)
            DuckDbRejectCapture.captureCsvRejects(conn)
```

Make every other branch of the `mergedMetadata.resolveFormat() match` return `List.empty[RejectedLine]`, so the match is the connection block's result:
- at the end of the `Format.POSITION` branch, after `JdbcDbUtils.execute(sql, conn)`, add `List.empty[RejectedLine]`
- in the `Format.JSON_FLAT | Format.JSON` branch, add `List.empty[RejectedLine]` as the last expression of the branch, after the inner `if/else`
- change the final `case _ =>` to `case _ => List.empty[RejectedLine]`

- [ ] **Step 6: Accumulate the rejects in run() and report them in the counters**

In `run()`, the two step block currently builds `tempTables` with `path.map { p => ... tempTable }`. Change that `val tempTables = path.map {...}` block so it also carries the rejects. Replace:

```scala
        val tempTables =
          path.map { p =>
            logger.info(s"Loading $p to temporary table")
            val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
            singleStepLoad(domain.finalName, tempTable, schemaWithMergedMetadata, List(p))
```

with:

```scala
        val tempTablesWithRejects =
          path.map { p =>
            logger.info(s"Loading $p to temporary table")
            val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
            val rejects =
              singleStepLoad(domain.finalName, tempTable, schemaWithMergedMetadata, List(p))
```

and change the last expression of that `map` body from:

```scala
            tempTable
          }
```

to:

```scala
            (tempTable, rejects)
          }
        val tempTables = tempTablesWithRejects.map(_._1)
        val rejectedLines = tempTablesWithRejects.flatMap(_._2)
```

The `try { ... } finally { ... }` block that follows keeps using `tempTables` unchanged. Make the whole `if (twoSteps) { ... } else { ... }` expression yield the rejected lines: end the `try` block with `rejectedLines` after `job.run()`, and change the `else` branch from:

```scala
      } else {
        singleStepLoad(domain.finalName, schema.finalName, schemaWithMergedMetadata, path)
      }
      initialRowCount
```

to:

```scala
      } else {
        singleStepLoad(domain.finalName, schema.finalName, schemaWithMergedMetadata, path)
      }
      (initialRowCount, rejected)
```

where `rejected` is the value of the `if (twoSteps) ... else ...` expression, so assign it: change `val twoSteps = requireTwoSteps(effectiveSchema)` to be followed by `val rejected: List[RejectedLine] = if (twoSteps) { ... } else { ... }`.

Then change the `.map { initialRowCount => ... }` that follows the `Try` to destructure the pair and compute the counters:

```scala
    }.map { case (initialRowCount, rejected) =>
      val countSql =
        s"SELECT COUNT(*) AS cnt FROM ${domain.finalName}.${schema.finalName};"

      val currentRowCount =
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
          conn =>
            val res = JdbcDbUtils.executeQueryAsMap(countSql, conn)
            val count = res.head("cnt").toInt
            logger.info(
              s"Table ${domain.finalName}.${schema.finalName} now has $count records"
            )
            count
        }
      val acceptedCount = (currentRowCount - initialRowCount).toLong
      // JSON has no reject capture in DuckDB, so it keeps reporting an unknown count.
      val rejectsSupported = mergedMetadata.resolveFormat() == Format.DSV
      val rejectedCount = if (rejectsSupported) rejected.size.toLong else -1L
      val inputCount = if (rejectsSupported) acceptedCount + rejectedCount else acceptedCount
      List(
        IngestionCounters(
          inputCount = inputCount,
          acceptedCount = acceptedCount,
          rejectedCount = rejectedCount,
          paths = path.map(_.toString),
          jobid = ingestionJob.applicationId()
        )
      )
    }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: PASS, 1 test.

- [ ] **Step 8: Verify the existing native specs still pass**

Run: `sbt "testOnly *DuckDbNativeJsonLoadSpec* *DuckDbNativePositionLoadSpec*"`
Expected: PASS. The JSON and POSITION paths are untouched at this point.

- [ ] **Step 9: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala \
        src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala \
        src/test/resources/sample/dsvduckreject
git commit -m "feat: skip and count malformed lines on DuckDB native DSV loads"
```

---

### Task 3: Write the replay file

**Files:**
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala` (add `replayHeaderLine()` and `reportRejects()`, call it from `run()`)
- Test: `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala` (add two tests)

**Interfaces:**
- Consumes: `ReplayFileWriter.write(...)` and `RejectedLine` from Task 1, the accumulated `rejected` list from Task 2.
- Produces: `DuckDbNativeLoader.reportRejects(rejected: List[RejectedLine]): Unit`, called on both the success and the failure path.

- [ ] **Step 1: Write the failing tests**

Add these two tests to `DuckDbNativeRejectSpec`, inside the same `new WithSettings(duckDbConfiguration)` block. The first one needs its own settings because `sinkReplayToFile` defaults to false, so add a second `WithSettings` block at the end of the file, after the existing one:

```scala
  lazy val duckDbReplayConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_replay_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbReplayConfiguration) {

    "Native DuckDB load with sinkReplayToFile" should
    "write the rejected lines verbatim under the replay area, header first" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe true

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe
        "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
      }
    }
  }
```

Add to the first `WithSettings` block, whose config leaves `sinkReplayToFile` at its default of false:

```scala
    "Native DuckDB load without sinkReplayToFile" should "write no replay file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe true

        storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path) shouldBe Nil
      }
    }
```

Add the import `ai.starlake.config.DatasetArea` at the top of the spec file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: The replay test FAILS with `replayFiles.size` being 0 instead of 1. The no-replay test passes already, which is expected: it is a regression guard.

- [ ] **Step 3: Add the header reader and the reporting hook**

In `DuckDbNativeLoader.scala`, add these imports:

```scala
import java.io.BufferedReader
```

and add these two private methods to the class, right above `computeEffectiveInputSchema()`:

```scala
  /** The first physical line of the first input file, so the replay file can be ingested
    * again by a table that declares a header. Read verbatim rather than rebuilt from the
    * attribute names, so it survives renamed or reordered source columns.
    */
  private def replayHeaderLine(): Option[String] = {
    val isDsvWithHeader =
      mergedMetadata.resolveFormat() == Format.DSV &&
      mergedMetadata.resolveWithHeader().booleanValue()
    if (isDsvWithHeader) {
      path.headOption.flatMap { p =>
        Try {
          storageHandler.readAndExecute(p, Charset.forName(mergedMetadata.resolveEncoding())) {
            reader => Option(new BufferedReader(reader).readLine())
          }
        }.recover { case e =>
          logger.warn(s"Could not read the header line of $p for the replay file", e)
          None
        }.get
      }
    } else {
      None
    }
  }

  /** Writes the rejected lines where the user can act on them: the replay file when
    * sinkReplayToFile is set, and the audit rejected table.
    */
  private def reportRejects(rejected: List[RejectedLine]): Unit = {
    if (rejected.nonEmpty && settings.appConfig.sinkReplayToFile) {
      ReplayFileWriter.write(
        domainName = domain.finalName,
        tableName = schema.finalName,
        rejectedLines = rejected,
        header = replayHeaderLine(),
        encoding = mergedMetadata.resolveEncoding(),
        timestamp = ingestionJob.now
      )(settings, storageHandler)
    }
  }
```

- [ ] **Step 4: Call it from run()**

In the `.map { case (initialRowCount, rejected) => ... }` block added in Task 2, insert `reportRejects(rejected)` as the first statement of the block, before `val countSql`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: PASS, 3 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala
git commit -m "feat: write a replay file for DuckDB native DSV rejects"
```

---

### Task 4: Enforce rejectMaxRecords and rejectAllOnError

**Files:**
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala` (threshold predicate, transaction in `singleStepLoad`, `recoverWith` in `run()`)
- Test: `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala` (add one `WithSettings` block with two tests)

**Interfaces:**
- Consumes: `RejectThresholdExceededException` from Task 1, `reportRejects` from Task 3.
- Produces: nothing new for later tasks.

- [ ] **Step 1: Write the failing tests**

Append a third `WithSettings` block to `DuckDbNativeRejectSpec`:

```scala
  lazy val duckDbThresholdConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |rejectMaxRecords: 1
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_threshold_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbThresholdConfiguration) {

    private def tableExists(domainName: String, tableName: String): Boolean = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn
          .createStatement()
          .executeQuery(
            s"SELECT count(*) AS cnt FROM duckdb_tables() " +
            s"WHERE schema_name = '$domainName' AND table_name = '$tableName'"
          )
        rs.next()
        rs.getInt("cnt") > 0
      }
    }

    "Native DuckDB load breaching rejectMaxRecords" should
    "fail, leave the target untouched and still write the replay file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe false

        tableExists("dsvduckreject", "account") shouldBe false

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        storageHandler.read(replayFiles.head) shouldBe
        "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
      }
    }
  }
```

And a fourth block for `rejectAllOnError`:

```scala
  lazy val duckDbRejectAllConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |rejectAllOnError: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_rejectall_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbRejectAllConfiguration) {

    "Native DuckDB load with rejectAllOnError" should "fail on the first rejected line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe false
      }
    }
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: both new tests FAIL with `loadPending.isSuccess` being true, because no threshold is enforced yet.

- [ ] **Step 3: Add the threshold predicate**

In `DuckDbNativeLoader.scala`, add this private method next to `reportRejects`:

```scala
  /** A load is aborted when it produces more rejects than allowed, or any reject at all
    * when the user asked for all or nothing.
    */
  private def rejectThresholdBreached(rejectedCount: Int): Boolean =
    rejectedCount > settings.appConfig.rejectMaxRecords ||
    (settings.appConfig.rejectAllOnError && rejectedCount > 0)
```

- [ ] **Step 4: Wrap singleStepLoad in a transaction**

In `singleStepLoad`, the body is a single `JdbcDbUtils.withJDBCConnection(...) { conn => ... }` call. Keep that body exactly as it is, assign its result to a `val`, and surround it with the transaction handling. The block becomes:

```scala
      conn =>
        val previousAutoCommit = conn.getAutoCommit
        conn.setAutoCommit(false)
        val rejected =
          try {
            // ... the entire existing body, unchanged, ending with the
            // mergedMetadata.resolveFormat() match expression ...
          } catch {
            case e: Throwable =>
              Try(conn.rollback())
              Try(conn.setAutoCommit(previousAutoCommit))
              throw e
          }
        if (!isTemporary && rejectThresholdBreached(rejected.size)) {
          // Read the rejects before rolling back: a ROLLBACK also discards the session
          // scoped reject_errors table.
          conn.rollback()
          conn.setAutoCommit(previousAutoCommit)
          throw new RejectThresholdExceededException(
            rejected,
            s"${rejected.size} rejected record(s) exceeds the allowed threshold"
          )
        }
        conn.commit()
        conn.setAutoCommit(previousAutoCommit)
        rejected
```

Note the known limitation, worth a comment in the code: `SparkUtils.updateJdbcTableSchema` calls `JdbcDbUtils.executeAlterTable`, which commits on its own. Schema changes are therefore not rolled back. Data changes are.

- [ ] **Step 5: Enforce the threshold for the two step path**

In `run()`, inside the two step branch, right after `val rejectedLines = tempTablesWithRejects.flatMap(_._2)` and before the `try {` that builds the second step task, add:

```scala
        if (rejectThresholdBreached(rejectedLines.size)) {
          // The target table has not been written yet at this point, so aborting here
          // leaves it untouched. The temp tables are dropped by the finally block below.
          throw new RejectThresholdExceededException(
            rejectedLines,
            s"${rejectedLines.size} rejected record(s) exceeds the allowed threshold"
          )
        }
```

Move that check inside the existing `try { ... } finally { ... }` so the temp tables are still dropped: put it as the first statement of the `try` block.

- [ ] **Step 6: Report the rejects on the failure path**

At the end of `run()`, after the closing `}` of the `.map { case (initialRowCount, rejected) => ... }`, chain a recovery that reports the rejects before failing:

```scala
    }.recoverWith { case e: RejectThresholdExceededException =>
      // The load is going to fail, but the user still gets the replay file and the audit
      // rejected rows so they can fix the input and load it again.
      reportRejects(e.rejected)
      Failure(e)
    }
```

Add `import scala.util.Failure` to the existing `scala.util` import, which becomes:

```scala
import scala.util.{Failure, Try, Using}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: PASS, 5 tests.

- [ ] **Step 8: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala
git commit -m "feat: fail DuckDB native loads that breach the reject threshold"
```

---

### Task 5: Write the rejected lines to the audit rejected table

**Files:**
- Create: `src/main/scala/ai/starlake/job/ingest/loaders/NativeRejectedSink.scala`
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala` (call it from `reportRejects`)
- Test: `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala` (add one test)

**Interfaces:**
- Consumes: `RejectedLine` from Task 1.
- Produces: `NativeRejectedSink.sink(applicationId: String, domainName: String, tableName: String, rejected: List[RejectedLine], paths: List[Path], timestamp: java.sql.Timestamp, scheduledDate: Option[String], accessToken: Option[String])(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler): Try[Unit]`

The audit sink connection falls back to `settings.appConfig.connectionRef` when `audit.sink.connectionRef` is unset (`Settings.Audit.getConnectionRef`), so in these tests the audit rows land in the same DuckDB file, in schema `audit`, table `rejected`.

- [ ] **Step 1: Write the failing test**

Add this test to the first `WithSettings(duckDbConfiguration)` block of `DuckDbNativeRejectSpec`:

```scala
    "Native DuckDB load with malformed lines" should
    "record one audit rejected row per rejected line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        loadPending.isSuccess shouldBe true

        val errors = queryDuckDb(
          "SELECT error FROM audit.rejected WHERE domain = 'dsvduckreject' ORDER BY error"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("error")
          buf.toList
        }

        errors.size shouldBe 2
        errors.count(_.contains("NOTANUM")) shouldBe 1
        errors.count(_.contains("MISSING COLUMNS")) shouldBe 1
      }
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: FAIL with a DuckDB catalog error, because `audit.rejected` does not exist.

- [ ] **Step 3: Create the sink**

Create `src/main/scala/ai/starlake/job/ingest/loaders/NativeRejectedSink.scala`:

```scala
package ai.starlake.job.ingest.loaders

import ai.starlake.config.Settings
import ai.starlake.job.transform.TransformContext
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, Engine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

/** Writes rejected input lines into the audit `rejected` table without Spark, by running a
  * literal SELECT ... UNION ALL through an AutoTask. Same approach as AuditLog, so it works
  * for every audit sink a native load can target.
  */
object NativeRejectedSink extends LazyLogging {

  /** Values are inlined into the SQL, so quotes and newlines are neutralized the same way
    * AuditLog does it.
    */
  private def literal(value: String): String =
    value.replaceAll("'", "-").replaceAll("\n", " ")

  def sink(
    applicationId: String,
    domainName: String,
    tableName: String,
    rejected: List[RejectedLine],
    paths: List[Path],
    timestamp: Timestamp,
    scheduledDate: Option[String],
    accessToken: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): Try[Unit] = Try {
    if (rejected.nonEmpty && settings.appConfig.audit.isActive()) {
      val limited = rejected.take(settings.appConfig.audit.maxErrors)
      if (limited.size < rejected.size) {
        logger.warn(
          s"Only ${limited.size} of ${rejected.size} rejected lines were written to the " +
          s"audit rejected table, capped by audit.maxErrors"
        )
      }
      val rejectedPathName = paths.map(_.toString).mkString(",")
      val auditTimestamp = new Timestamp(timestamp.getTime)
      auditTimestamp.setNanos(0)
      val selectSql = limited
        .map { line =>
          val location = line.line.map(l => s"${line.file}:$l").getOrElse(line.file)
          val error = literal(s"$location: ${line.error}")
          s"""SELECT '${literal(applicationId)}' AS JOBID,
             |  CAST('${auditTimestamp.toString}' AS TIMESTAMP) AS TIMESTAMP,
             |  '${literal(domainName)}' AS DOMAIN,
             |  '${literal(tableName)}' AS SCHEMA,
             |  '$error' AS ERROR,
             |  '${literal(rejectedPathName)}' AS PATH""".stripMargin
        }
        .mkString("\nUNION ALL\n")

      val taskDesc = AutoTaskInfo(
        name = s"rejected-$applicationId-$domainName-$tableName",
        sql = Some(selectSql),
        database = settings.appConfig.audit.getDatabase(),
        domain = settings.appConfig.audit.getDomain(),
        table = "rejected",
        presql = Nil,
        postsql = Nil,
        sink = Some(settings.appConfig.audit.sink),
        _auditTableName = Some("rejected"),
        connectionRef = settings.appConfig.audit.sink.connectionRef,
        parseSQL = Some(true),
        taskTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
      )

      val context = TransformContext(
        appId = Option(applicationId),
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        accessToken = accessToken,
        resultPageSize = 200,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = scheduledDate,
        syncSchema = false
      )(settings, storageHandler, schemaHandler)

      val engine =
        if (taskDesc.getSinkConnection().isJdbcUrl()) Engine.JDBC
        else taskDesc.getSinkConnection().getEngine()

      context.toTask(engine).run() match {
        case Success(_) => ()
        case Failure(e) =>
          logger.error("Failed to write the audit rejected rows", e)
          throw e
      }
    }
  }
}
```

- [ ] **Step 4: Call it from reportRejects**

In `DuckDbNativeLoader.reportRejects`, after the `ReplayFileWriter.write` block, add:

```scala
    if (rejected.nonEmpty) {
      NativeRejectedSink
        .sink(
          applicationId = ingestionJob.applicationId(),
          domainName = domain.finalName,
          tableName = schema.finalName,
          rejected = rejected,
          paths = path,
          timestamp = ingestionJob.now,
          scheduledDate = scheduledDate,
          accessToken = ingestionJob.accessToken
        )(settings, storageHandler, schemaHandler)
        .get
    }
```

Calling `.get` makes a failed audit write fail the load, which matches Spark's `saveRejected`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/NativeRejectedSink.scala \
        src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala
git commit -m "feat: record DuckDB native rejects in the audit rejected table"
```

---

### Task 6: Reject malformed POSITION lines

DuckDB cannot fail on a POSITION read, because the first step loads each line as a single VARCHAR column named `value`. Rejects are found by running a predicate against that temp table, and the offending rows are deleted so the existing second step runs unchanged.

This changes documented behavior: `DuckDbNativePositionLoadSpec` currently asserts that a malformed numeric cell is loaded as NULL. It is now a reject.

**Files:**
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala`
- Modify: `src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala`
- Create: `src/test/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCaptureSpec.scala`
- Modify: `src/test/scala/ai/starlake/job/ingest/DuckDbNativePositionLoadSpec.scala`
- Create: `src/test/resources/sample/positionduckshort/positionduckshort.sl.yml`
- Create: `src/test/resources/sample/positionduckshort/account_positionduckshort.sl.yml`
- Create: `src/test/resources/sample/positionduckshort/XPOSSHORTTBL`

**Interfaces:**
- Consumes: `RejectedLine` from Task 1, `reportRejects` from Task 3, `rejectThresholdBreached` from Task 4.
- Produces:
  - `DuckDbRejectCapture.positionRejectClauses(schema: SchemaInfo, ddlTypesByAttribute: Map[String, String]): List[(String, String)]` returning `(predicate, message)` pairs
  - `DuckDbRejectCapture.capturePositionRejects(conn: java.sql.Connection, tableName: String, filePath: String, schema: SchemaInfo, ddlTypesByAttribute: Map[String, String]): List[RejectedLine]`

- [ ] **Step 1: Write the failing unit test for the predicate**

Create `src/test/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCaptureSpec.scala`:

```scala
package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.{Position, SchemaInfo, TableAttribute}

import java.util.regex.Pattern

class DuckDbRejectCaptureSpec extends TestHelper {

  private val schema = SchemaInfo(
    name = "account",
    pattern = Pattern.compile("XPOS.*"),
    attributes = List(
      TableAttribute("name", "string", position = Some(Position(0, 9))),
      TableAttribute("amount", "long", position = Some(Position(10, 14)))
    ),
    metadata = None,
    comment = None
  )

  private val ddlTypes = Map("name" -> "VARCHAR", "amount" -> "BIGINT")

  "positionRejectClauses" should "guard the line length and cast every non string slice" in {
    val clauses = DuckDbRejectCapture.positionRejectClauses(schema, ddlTypes)

    clauses.map(_._1) shouldBe List(
      "length(value) < 15",
      "(TRIM(SUBSTR(value, 11, 5)) <> '' AND TRY_CAST(SUBSTR(value, 11, 5) AS BIGINT) IS NULL)"
    )
    clauses.map(_._2) shouldBe List(
      "line is shorter than the 15 characters required by the declared positions",
      "amount: cannot cast to BIGINT"
    )
  }

  it should "produce no clause when there is no positioned attribute" in {
    DuckDbRejectCapture.positionRejectClauses(
      schema.copy(attributes = List(TableAttribute("name", "string"))),
      Map("name" -> "VARCHAR")
    ) shouldBe Nil
  }
}
```

`SchemaInfo` requires `name`, `pattern` and `attributes`; every other field defaults. `TableAttribute` requires only `name`. Both are declared in `src/main/scala/ai/starlake/schema/model/SchemaInfo.scala:70` and `TableAttribute.scala:67`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `sbt "testOnly *DuckDbRejectCaptureSpec*"`
Expected: FAIL at compilation with "value positionRejectClauses is not a member of object DuckDbRejectCapture".

- [ ] **Step 3: Implement the predicate and the capture**

Add to `DuckDbRejectCapture.scala` these imports:

```scala
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.SchemaInfo
```

and these members:

```scala
  /** A cast is only emitted for attributes whose DuckDB type is not string like, matching
    * the projection built by SchemaInfo.buildSecondStepSqlSelectOnLoad.
    */
  private def isStringLikeDdlType(ddlType: String): Boolean = {
    val upper = ddlType.trim.toUpperCase
    upper == "STRING" || upper.startsWith("VARCHAR") || upper.startsWith("CHAR") ||
    upper == "TEXT"
  }

  /** Builds the (predicate, message) pairs that decide whether a fixed width line is
    * rejected. Positions are zero based and inclusive, so an attribute at [first, last]
    * is sliced as SUBSTR(value, first + 1, last - first + 1) and a complete record needs
    * at least last + 1 characters.
    */
  def positionRejectClauses(
    schema: SchemaInfo,
    ddlTypesByAttribute: Map[String, String]
  ): List[(String, String)] = {
    val positioned =
      schema.exceptIgnoreScriptAndTransformAttributes().filter(_.position.isDefined)
    if (positioned.isEmpty) {
      Nil
    } else {
      val minimumLength = positioned.flatMap(_.position).map(_.last).max + 1
      val shortLine =
        (
          s"length(value) < $minimumLength",
          s"line is shorter than the $minimumLength characters required by the declared positions"
        )
      val castClauses = positioned.flatMap { attr =>
        val position = attr.position.get
        ddlTypesByAttribute.get(attr.name).filterNot(isStringLikeDdlType).map { ddlType =>
          val slice =
            s"SUBSTR(value, ${position.first + 1}, ${position.last - position.first + 1})"
          (
            s"(TRIM($slice) <> '' AND TRY_CAST($slice AS $ddlType) IS NULL)",
            s"${attr.name}: cannot cast to $ddlType"
          )
        }
      }
      shortLine :: castClauses
    }
  }

  /** Collects the fixed width lines that cannot be projected into the target columns, then
    * deletes them from the temporary table so the second step inserts only good rows.
    */
  def capturePositionRejects(
    conn: Connection,
    tableName: String,
    filePath: String,
    schema: SchemaInfo,
    ddlTypesByAttribute: Map[String, String]
  ): List[RejectedLine] = {
    val clauses = positionRejectClauses(schema, ddlTypesByAttribute)
    if (clauses.isEmpty) {
      Nil
    } else {
      val where = clauses.map(_._1).mkString(" OR ")
      val errorExpression = clauses
        .map { case (predicate, message) =>
          s"CASE WHEN $predicate THEN '${message.replace("'", "''")}' END"
        }
        .mkString("concat_ws(' | ', ", ", ", ")")
      val selectSql =
        s"SELECT value, $errorExpression AS sl_reject_error FROM $tableName WHERE $where"
      val rejected = ListBuffer[RejectedLine]()
      Using.resource(conn.createStatement()) { statement =>
        Using.resource(statement.executeQuery(selectSql)) { rs =>
          while (rs.next()) {
            rejected += RejectedLine(
              file = filePath,
              line = None,
              rawLine = rs.getString("value"),
              error = rs.getString("sl_reject_error")
            )
          }
        }
      }
      if (rejected.nonEmpty) {
        logger.warn(s"Rejecting ${rejected.size} fixed width line(s) from $filePath")
        JdbcDbUtils.execute(s"DELETE FROM $tableName WHERE $where", conn)
      }
      rejected.toList
    }
  }
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `sbt "testOnly *DuckDbRejectCaptureSpec*"`
Expected: PASS, 2 tests.

- [ ] **Step 5: Wire it into the POSITION branch**

In `DuckDbNativeLoader.singleStepLoad`, replace the tail of the `Format.POSITION` branch. It currently ends with:

```scala
            JdbcDbUtils.execute(sql, conn)
```

followed by the `List.empty[RejectedLine]` added in Task 2. Replace that `List.empty[RejectedLine]` with:

```scala
            DuckDbRejectCapture.capturePositionRejects(
              conn = conn,
              tableName = domainAndTableName,
              filePath = path.map(_.toString).mkString(","),
              schema = schema,
              ddlTypesByAttribute = attrsWithDDLTypes.toMap
            )
```

- [ ] **Step 6: Report POSITION rejects in the counters**

In `run()`, widen the supported formats. Change:

```scala
      val rejectsSupported = mergedMetadata.resolveFormat() == Format.DSV
```

to:

```scala
      val format = mergedMetadata.resolveFormat()
      val rejectsSupported = format == Format.DSV || format == Format.POSITION
```

- [ ] **Step 7: Update the existing POSITION expectations**

In `src/test/scala/ai/starlake/job/ingest/DuckDbNativePositionLoadSpec.scala`, the first test currently expects the `BadRow` line to be loaded with a NULL amount. Rename it and change the expectations. Replace:

```scala
    "Native DuckDB load of a POSITION file" should "slice lines with SUBSTR and NULL malformed cells via TRY_CAST" in {
```

with:

```scala
    "Native DuckDB load of a POSITION file" should "slice lines with SUBSTR and reject cells that cannot be cast" in {
```

and replace the assertion block:

```scala
        rows.size shouldBe 3
        // fixed-width slices keep their trailing spaces, as on the BigQuery native path
        rows.map(_._1) shouldBe List("BadRow    ", "Jane      ", "John      ")
        // malformed numeric cell yields NULL instead of failing the load
        rows.find(_._1.trim == "BadRow").flatMap(_._2) shouldBe None
        rows.find(_._1.trim == "John").flatMap(_._2) shouldBe Some(12345L)
        rows.find(_._1.trim == "Jane").flatMap(_._2) shouldBe Some(67890L)
```

with:

```scala
        rows.size shouldBe 2
        // fixed-width slices keep their trailing spaces, as on the BigQuery native path
        rows.map(_._1) shouldBe List("Jane      ", "John      ")
        // the BadRow line holds abcde where a number is declared, so it is rejected
        // rather than loaded with a NULL amount
        rows.find(_._1.trim == "BadRow") shouldBe None
        rows.find(_._1.trim == "John").flatMap(_._2) shouldBe Some(12345L)
        rows.find(_._1.trim == "Jane").flatMap(_._2) shouldBe Some(67890L)
        result.get.counters.head.rejectedCount shouldBe 1
```

- [ ] **Step 8: Add the truncated line fixtures**

Create `src/test/resources/sample/positionduckshort/positionduckshort.sl.yml`:

```yaml
---
version: 1
load:
  name: "positionduckshort"
  metadata:
    directory: "__SL_TEST_ROOT__/positionduckshort"
    format: "POSITION"
    withHeader: false
    loader: "native"
    writeStrategy:
      type: "APPEND"
```

Create `src/test/resources/sample/positionduckshort/account_positionduckshort.sl.yml`:

```yaml
---
version: 1
table:
  name: "account"
  pattern: "XPOSSHORT.*"
  attributes:
    - name: "name"
      type: "string"
      required: true
      position:
        first: 0
        last: 9
    - name: "amount"
      type: "long"
      required: false
      position:
        first: 10
        last: 14
```

Create `src/test/resources/sample/positionduckshort/XPOSSHORTTBL` with exactly these three lines:

```
John      12345
Short
Jane      67890
```

- [ ] **Step 9: Add the truncated line test**

Add to `DuckDbNativePositionLoadSpec`, inside the same `WithSettings` block:

```scala
    "Native DuckDB load of a POSITION file with a truncated line" should
    "reject the short line" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/positionduckshort/positionduckshort.sl.yml",
        datasetDomainName = "positionduckshort",
        sourceDatasetPathName = "/sample/positionduckshort/XPOSSHORTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "positionduckshort",
          "/sample/positionduckshort/account_positionduckshort.sl.yml",
          Some("account.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val names = queryDuckDb(
          "SELECT name FROM positionduckshort.account ORDER BY name"
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[String]()
          while (rs.next()) buf += rs.getString("name")
          buf.toList
        }

        names shouldBe List("Jane      ", "John      ")
        result.get.counters.head.rejectedCount shouldBe 1
      }
    }
```

- [ ] **Step 10: Run the POSITION specs to verify they pass**

Run: `sbt "testOnly *DuckDbNativePositionLoadSpec* *DuckDbRejectCaptureSpec*"`
Expected: PASS, 6 tests total.

- [ ] **Step 11: Commit**

```bash
git add src/main/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCapture.scala \
        src/main/scala/ai/starlake/job/ingest/loaders/DuckDbNativeLoader.scala \
        src/test/scala/ai/starlake/job/ingest/loaders/DuckDbRejectCaptureSpec.scala \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativePositionLoadSpec.scala \
        src/test/resources/sample/positionduckshort
git commit -m "feat: reject malformed and truncated lines on DuckDB native POSITION loads"
```

---

### Task 7: Multi file accumulation, documentation, full verification

**Files:**
- Create: `src/test/resources/sample/dsvduckreject/XDSVREJECTTBL2`
- Modify: `src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala`
- Modify: `src/main/resources/reference-general.conf:198`

**Interfaces:**
- Consumes: everything from Tasks 1 to 6.
- Produces: nothing.

- [ ] **Step 1: Create the second input file**

Create `src/test/resources/sample/dsvduckreject/XDSVREJECTTBL2` with exactly these three lines:

```
id;name;amount
6;frank;1.1
7;grace;NOTANUM
```

- [ ] **Step 2: Write the failing test**

`IngestionWorkflow` builds one ingestion job per file unless `grouped` is true (`IngestionWorkflow.scala:474`, `grouped = false` at `reference-general.conf:187`). The accumulation this test exercises is across the paths of a single ingestion, so the test needs its own settings with `grouped: true`. Append a fifth `WithSettings` block to `DuckDbNativeRejectSpec`:

```scala
  lazy val duckDbGroupedConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |sinkReplayToFile: true
         |grouped: true
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_grouped_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbGroupedConfiguration) {

    "Native DuckDB load of two files in one ingestion" should
    "accumulate the rejected lines of every file into one replay file" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )
        // delivered before loadPending, which delivers the first file and then loads
        // every staged file matching the table pattern in one ingestion
        withSettings.deliverTestFile(
          "/sample/dsvduckreject/XDSVREJECTTBL2",
          new Path(DatasetArea.stage("dsvduckreject"), "XDSVREJECTTBL2")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        result.get.counters.head.rejectedCount shouldBe 3

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        val content = storageHandler.read(replayFiles.head)
        content.linesIterator.toList.size shouldBe 4
        content.contains("2;bob;NOTANUM") shouldBe true
        content.contains("badline;dave") shouldBe true
        content.contains("7;grace;NOTANUM") shouldBe true
      }
    }
  }
```

Add `import org.apache.hadoop.fs.Path` and `import ai.starlake.config.DatasetArea` to the spec file if they are not already there.

- [ ] **Step 3: Run the test**

Run: `sbt "testOnly *DuckDbNativeRejectSpec*"`
Expected: PASS, 7 tests.

- [ ] **Step 4: Update the configuration comment**

In `src/main/resources/reference-general.conf`, change line 198 from:

```
# sinkReplayToFile / SL_SINK_REPLAY_TO_FILE: Sink replay to file
```

to:

```
# sinkReplayToFile / SL_SINK_REPLAY_TO_FILE: Write rejected input lines to a replay file
# under the domain replay area, so they can be fixed and ingested again. Supported by the
# Spark loader and by the DuckDB native loader for DSV and POSITION.
```

- [ ] **Step 5: Run the whole DuckDB and ingestion test surface**

Run: `sbt "testOnly *DuckDb* *IngestJobSpec* *LoadEngineSpec*"`
Expected: PASS. Report any failure with its output rather than adjusting expectations to make it green.

- [ ] **Step 6: Check formatting and compile cleanly**

Run: `sbt scalafmtCheck compile`
Expected: no formatting diff, no warnings introduced by the new files.

- [ ] **Step 7: Commit**

```bash
git add src/test/resources/sample/dsvduckreject/XDSVREJECTTBL2 \
        src/test/scala/ai/starlake/job/ingest/DuckDbNativeRejectSpec.scala \
        src/main/resources/reference-general.conf
git commit -m "test: cover multi file reject accumulation and document sinkReplayToFile"
```

---

## Out of scope, do not fix here

`DuckDbNativeLoader.run()` reads `initialRowCount` before `singleStepLoad` drops the table for an OVERWRITE strategy, so `currentRowCount - initialRowCount` goes negative when overwriting a non empty table. It is pre-existing, unrelated to rejects, and fixing it properly means capturing `getUpdateCount` from the INSERT. Leave it alone.
