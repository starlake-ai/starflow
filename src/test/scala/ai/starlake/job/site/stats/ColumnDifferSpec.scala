package ai.starlake.job.site.stats

import ai.starlake.job.site.SiteColumn
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnDifferSpec extends AnyFlatSpec with Matchers {
  private val declared = List(
    SiteColumn("id", sqlType = Some("BIGINT")),
    SiteColumn("amount", sqlType = Some("DECIMAL(38,9)")),
    SiteColumn("src_name", rename = Some("name"), sqlType = Some("VARCHAR")),
    SiteColumn("payload", sqlType = None),
    SiteColumn("gone", sqlType = Some("DATE"))
  )
  private val actual = List(
    ColumnStat("ID", "bigint", nullable = false),
    ColumnStat("amount", "numeric(18,2)", nullable = true),
    ColumnStat("name", "text", nullable = true),
    ColumnStat("payload", "jsonb", nullable = true),
    ColumnStat("extra", "integer", nullable = true)
  )

  "diff" should "classify every column" in {
    ColumnDiffer.diff(declared, actual, "postgresql") shouldBe List(
      ColumnDiff("id", Some("BIGINT"), Some("bigint"), "match"),
      ColumnDiff("amount", Some("DECIMAL(38,9)"), Some("numeric(18,2)"), "mismatch"),
      ColumnDiff("name", Some("VARCHAR"), Some("text"), "match"),
      ColumnDiff("payload", None, Some("jsonb"), "match"),
      ColumnDiff("gone", Some("DATE"), None, "missingInWarehouse"),
      ColumnDiff("extra", None, Some("integer"), "extraInWarehouse")
    )
  }

  it should "return nothing when no column is declared" in {
    ColumnDiffer.diff(Nil, actual, "postgresql") shouldBe Nil
  }

  it should "identify a renamed column by its rename, not its source name" in {
    val d = List(SiteColumn("src_name", rename = Some("name"), sqlType = Some("VARCHAR")))
    ColumnDiffer.diff(
      d,
      List(ColumnStat("src_name", "text", nullable = true)),
      "postgresql"
    ) shouldBe List(
      ColumnDiff("name", Some("VARCHAR"), None, "missingInWarehouse"),
      ColumnDiff("src_name", None, Some("text"), "extraInWarehouse")
    )
  }

  it should "skip ignored columns, which core drops from the table" in {
    val d = List(
      SiteColumn("id", sqlType = Some("BIGINT")),
      SiteColumn("raw_line", sqlType = Some("VARCHAR"), ignore = Some(true))
    )
    ColumnDiffer.diff(d, List(ColumnStat("id", "bigint", nullable = true)), "duckdb") shouldBe List(
      ColumnDiff("id", Some("BIGINT"), Some("bigint"), "match")
    )
    ColumnDiffer.diff(
      List(SiteColumn("raw_line", ignore = Some(true))),
      List(ColumnStat("id", "bigint", nullable = true)),
      "duckdb"
    ) shouldBe Nil
  }

  it should "report nothing when the warehouse lists no column for an existing table" in {
    // Redshift late-binding views, column privileges: no evidence that any column is missing
    ColumnDiffer.diff(declared, Nil, "redshift") shouldBe Nil
    StatsRunner
      .toTableStats(
        "rs",
        "redshift",
        ai.starlake.job.site.SiteNode("load:a.v", "load", "v", "a", "v", columns = declared),
        RawTableStats(true, Some("VIEW"), None, None, false, None, None, Nil)
      )
      .columnDiff shouldBe Nil
  }
}
