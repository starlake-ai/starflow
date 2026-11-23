package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanitizeSpec extends AnyFlatSpec with Matchers {
  "Sanitize.message" should "keep the first non blank line only, with collapsed whitespace" in {
    Sanitize.message("\n  ERROR:  permission denied\n  Hint: ask the admin") shouldBe
    "ERROR: permission denied"
  }

  it should "hide a Delta style multi-line plan dump containing a local path" in {
    val dump =
      "[DELTA_TABLE_NOT_FOUND] Delta table `default`.`orders` doesn't exist at " +
      "file:/Users/jane/projects/acme/datasets/sales/orders.\n" +
      "== Physical Plan ==\n" +
      "*(1) Scan parquet [id#1L] Location: InMemoryFileIndex[file:/Users/jane/acme/x]\n"
    Sanitize.message(dump) shouldBe
    "[DELTA_TABLE_NOT_FOUND] Delta table `default`.`orders` doesn't exist at <path>"
  }

  it should "replace absolute Unix, file: and Windows paths" in {
    Sanitize.message("cannot open /home/bob/data/db.duckdb: denied") shouldBe
    "cannot open <path> denied"
    Sanitize.message("IO Error: Cannot open file \"/tmp/x/duckdb.db\"") shouldBe
    "IO Error: Cannot open file \"<path>\""
    Sanitize.message("bad url jdbc:duckdb:/Users/x/duck.db") shouldBe "bad url jdbc:duckdb:<path>"
    Sanitize.message("at file:///var/lib/spark/warehouse/t, retrying") shouldBe
    "at <path>, retrying"
    Sanitize.message("missing C:\\Users\\bob\\db.duckdb now") shouldBe "missing <path> now"
    Sanitize.message("missing (D:/data/db.duckdb)") shouldBe "missing (<path>)"
    Sanitize.message("table `/tmp/delta/orders` not found") shouldBe "table `<path>` not found"
  }

  it should "keep URLs, SQL operators and dotted names" in {
    Sanitize.message("Connection refused: jdbc:postgresql://db.example.com:5432/sales") shouldBe
    "Connection refused: jdbc:postgresql://db.example.com:5432/sales"
    Sanitize.message("https://bigquery.googleapis.com/bigquery/v2/projects/p failed") shouldBe
    "https://bigquery.googleapis.com/bigquery/v2/projects/p failed"
    Sanitize.message("Division by zero in a / b and/or c /* hint */") shouldBe
    "Division by zero in a / b and/or c /* hint */"
    Sanitize.message("sales.orders: access denied") shouldBe "sales.orders: access denied"
  }

  it should "cap the message at 300 characters with a trailing ellipsis" in {
    val long = "x" * 500
    val capped = Sanitize.message(long)
    capped.length shouldBe 300
    capped should endWith("...")
    capped.dropRight(3) shouldBe "x" * 297
    Sanitize.message("y" * 300) shouldBe "y" * 300
  }

  it should "be idempotent" in {
    val inputs = List(
      "a\nb",
      "open /tmp/a/b failed",
      "x" * 400,
      "file:/x/y and C:\\z",
      ""
    )
    inputs.foreach { s =>
      val once = Sanitize.message(s)
      Sanitize.message(once) shouldBe once
    }
  }

  "StatsCollector.message" should "sanitize exception messages and fall back to the class name" in {
    StatsCollector.message(
      new IllegalStateException("corrupt /tmp/delta/_delta_log\n  dump")
    ) shouldBe
    "corrupt <path>"
    StatsCollector.message(new RuntimeException(null: String)) shouldBe
    "java.lang.RuntimeException"
    StatsCollector.message(new RuntimeException(" \n ")) shouldBe "java.lang.RuntimeException"
  }
}
