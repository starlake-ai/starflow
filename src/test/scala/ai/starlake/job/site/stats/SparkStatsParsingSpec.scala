package ai.starlake.job.site.stats

import Rows.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant

class SparkStatsParsingSpec extends AnyFlatSpec with Matchers {
  import SparkStatsParsing._

  "statistics" should "parse the DESCRIBE EXTENDED Statistics value" in {
    statistics("1234 bytes, 10 rows") shouldBe ((Some(1234L), Some(10L)))
    statistics("1234 bytes") shouldBe ((Some(1234L), None))
    statistics("") shouldBe ((None, None))
  }

  "describeExtended" should "keep the first value of each non-empty key, lowercased" in {
    describeExtended(
      List(
        "id"                           -> "bigint",
        ""                             -> "",
        "# Detailed Table Information" -> "",
        "Provider"                     -> "delta",
        "Type"                         -> "MANAGED",
        "provider"                     -> "ignored"
      )
    ) shouldBe Map("id" -> "bigint", "provider" -> "delta", "type" -> "MANAGED")
  }

  "detailSection" should "drop the column rows so a column named type or provider cannot leak" in {
    val rows = List(
      "type"                         -> "string",
      "provider"                     -> "string",
      "# Detailed Table Information" -> "",
      "Type"                         -> "MANAGED",
      "Provider"                     -> "parquet"
    )
    describeExtended(detailSection(rows)) shouldBe Map(
      "type"     -> "MANAGED",
      "provider" -> "parquet"
    )
    detailSection(List("id" -> "bigint")) shouldBe List("id" -> "bigint")
  }

  "tableType" should "map catalog types" in {
    tableType(Some("MANAGED")) shouldBe "TABLE"
    tableType(Some("EXTERNAL")) shouldBe "EXTERNAL"
    tableType(Some("VIEW")) shouldBe "VIEW"
    tableType(None) shouldBe "TABLE"
  }

  "fromDeltaDetail" should "read sizeInBytes and lastModified" in {
    val ts = Timestamp.from(Instant.parse("2026-11-22T03:10:00Z"))
    fromDeltaDetail(Map("sizeinbytes" -> 4096L, "lastmodified" -> ts)) shouldBe
    ((Some(4096L), Some("2026-11-22T03:10:00Z")))
  }

  "fromIcebergSnapshot" should "read total records, size and commit time" in {
    val ts = Timestamp.from(Instant.parse("2026-11-22T03:10:00Z"))
    fromIcebergSnapshot(
      Map("total_records" -> "120", "total_files_size" -> "8192", "committed_at" -> ts)
    ) shouldBe ((Some(120L), Some(8192L), Some("2026-11-22T03:10:00Z")))
  }

  "icebergHasDeletes" should "detect position or equality deletes" in {
    icebergHasDeletes(
      Map("total_position_deletes" -> "0", "total_equality_deletes" -> "0")
    ) shouldBe
    false
    icebergHasDeletes(Map("total_position_deletes" -> "3")) shouldBe true
    icebergHasDeletes(Map("total_equality_deletes" -> 1L)) shouldBe true
    icebergHasDeletes(Map.empty) shouldBe false
  }

  "quote" should "backtick every part" in {
    quote(PhysicalTable(Some("cat"), "db", "t")) shouldBe "`cat`.`db`.`t`"
    quote(PhysicalTable(None, "db", "t`x")) shouldBe "`db`.`t``x`"
  }

  private val base =
    RawTableStats(true, Some("TABLE"), None, None, false, None, None, Nil)
  private val ts = Timestamp.from(Instant.parse("2026-11-22T03:10:00Z"))
  private val q = "`cat`.`db`.`t`"

  /** A fake Spark SQL backend for the per-format stats queries. */
  private def fake(answers: PartialFunction[String, List[Row]])(sql: String): List[Row] =
    answers.applyOrElse(sql, (s: String) => sys.error(s"unexpected $s"))

  "SparkStatsCollector.enrich" should "read the latest Iceberg snapshot summary" in {
    val (stats, failures) = SparkStatsCollector.enrich(
      base.copy(format = Some("iceberg")),
      Map("provider" -> "iceberg"),
      q,
      "cat.db.t",
      fake {
        case s if s == icebergSnapshotSql(q) =>
          List(
            Map(
              "total_records"    -> "120",
              "total_files_size" -> "8192",
              "committed_at"     -> ts
            )
          )
      }
    )
    stats.rowCount shouldBe Some(120L)
    stats.rowCountEstimated shouldBe false
    stats.bytes shouldBe Some(8192L)
    stats.lastModified shouldBe Some("2026-11-22T03:10:00Z")
    failures shouldBe Nil
  }

  it should "fall back to the Iceberg files metadata table when the summary has no totals" in {
    val (stats, failures) = SparkStatsCollector.enrich(
      base.copy(format = Some("iceberg")),
      Map("provider" -> "iceberg"),
      q,
      "cat.db.t",
      fake {
        case s if s == icebergSnapshotSql(q) => List(Map("committed_at" -> ts))
        case s if s == icebergFilesSql(q) =>
          List(Map("total_records" -> 50L, "total_files_size" -> 2048L, "delete_files" -> 1L))
      }
    )
    stats.rowCount shouldBe Some(50L)
    stats.rowCountEstimated shouldBe true // a delete file exists: the data files overcount
    stats.bytes shouldBe Some(2048L)
    stats.lastModified shouldBe Some("2026-11-22T03:10:00Z")
    failures shouldBe Nil
    icebergFilesSql(q) should not include "count("
  }

  it should "mark the Iceberg row count estimated when the snapshot carries deletes" in {
    val (stats, _) = SparkStatsCollector.enrich(
      base.copy(format = Some("iceberg")),
      Map("provider" -> "iceberg"),
      q,
      "cat.db.t",
      fake {
        case s if s == icebergSnapshotSql(q) =>
          List(
            Map(
              "total_records"          -> "120",
              "total_files_size"       -> "8192",
              "total_position_deletes" -> "4",
              "committed_at"           -> ts
            )
          )
      }
    )
    stats.rowCount shouldBe Some(120L)
    stats.rowCountEstimated shouldBe true
  }

  it should "read the snapshot the main branch points to, not the latest commit" in {
    val sql = icebergSnapshotSql(q)
    sql should include(s"FROM $q.refs r JOIN $q.snapshots s ON s.snapshot_id = r.snapshot_id")
    sql should include("r.name = 'main'")
    sql should not include "ORDER BY committed_at"
  }

  it should "report 0 rows and 0 bytes for an Iceberg table without snapshot" in {
    val (stats, failures) = SparkStatsCollector.enrich(
      base.copy(format = Some("iceberg")),
      Map("provider" -> "iceberg"),
      q,
      "cat.db.t",
      fake { case s if s == icebergSnapshotSql(q) => Nil }
    )
    stats.rowCount shouldBe Some(0L)
    stats.rowCountEstimated shouldBe false
    stats.bytes shouldBe Some(0L)
    stats.lastModified shouldBe None
    failures shouldBe Nil
  }

  it should "report no Iceberg row count when the snapshot query fails" in {
    val (stats, failures) = SparkStatsCollector.enrich(
      base.copy(format = Some("iceberg")),
      Map("provider" -> "iceberg"),
      q,
      "cat.db.t",
      _ => throw new IllegalStateException("no refs table")
    )
    stats.rowCount shouldBe None
    failures shouldBe List("cat.db.t stats failed: no refs table")
  }

  it should "keep the table and its columns when a format stats query fails" in {
    val withColumns = base.copy(
      format = Some("delta"),
      columns = List(ColumnStat("id", "bigint", nullable = true))
    )
    val (stats, failures) = SparkStatsCollector.enrich(
      withColumns,
      Map("provider" -> "delta"),
      q,
      "cat.db.t",
      _ => throw new IllegalStateException("corrupt\n  delta log")
    )
    stats shouldBe withColumns
    failures shouldBe List("cat.db.t stats failed: corrupt")
  }

  it should "read the catalog statistics of other providers, as estimates" in {
    val (stats, failures) = SparkStatsCollector.enrich(
      base.copy(format = Some("parquet")),
      Map("provider" -> "parquet", "statistics" -> "1234 bytes, 3 rows"),
      q,
      "db.t",
      fake(PartialFunction.empty)
    )
    stats.rowCount shouldBe Some(3L)
    stats.rowCountEstimated shouldBe true
    stats.bytes shouldBe Some(1234L)
    failures shouldBe Nil
  }
}
