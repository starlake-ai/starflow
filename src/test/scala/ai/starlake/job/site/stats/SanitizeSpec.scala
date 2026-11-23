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

  it should "redact a DuckLake style Postgres DSN password" in {
    val msg = Sanitize.message(
      "IO Error: Unable to connect to Postgres at host=db.internal user=u password=s3cr3t dbname=d"
    )
    msg should not include "s3cr3t"
    msg shouldBe
    "IO Error: Unable to connect to Postgres at host=db.internal user=u password=<redacted> dbname=d"
    Sanitize.message("""failed: 'host=h password="p w" port=5432'""") should not include "p w"
    Sanitize.message("PWD: hunter2; retry") shouldBe "PWD=<redacted>; retry"
  }

  it should "redact the user info of a URL" in {
    val msg = Sanitize.message("Connection refused: jdbc:postgresql://alice:pa55word@db:5432/sales")
    msg should not include "pa55word"
    msg should not include "alice"
    msg shouldBe "Connection refused: jdbc:postgresql://<redacted>@db:5432/sales"
  }

  it should "redact the signature and credential of an S3 presigned URL" in {
    val msg = Sanitize.message(
      "403 on https://b.s3.amazonaws.com/k.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
      "&X-Amz-Credential=AKIAEXAMPLE%2F20261123%2Fus-east-1%2Fs3%2Faws4_request" +
      "&X-Amz-Signature=deadbeef0123&X-Amz-Expires=60"
    )
    msg should not include "AKIAEXAMPLE"
    msg should not include "deadbeef0123"
    msg should include("X-Amz-Signature=<redacted>")
    msg should include("X-Amz-Credential=<redacted>")
  }

  it should "redact a token header and keyed secrets" in {
    Sanitize.message("token: abc123") shouldBe "token=<redacted>"
    val msg = Sanitize.message(
      "bad config secret_key=K1 access_key: K2 private_key_id = K3 sig=K4 signature='K 5'"
    )
    List("K1", "K2", "K3", "K4", "K 5").foreach(s => msg should not include s)
  }

  it should "stay idempotent once secrets are redacted" in {
    List(
      "user=u password=secret dbname=d",
      "jdbc:postgresql://a:b@h/db",
      "https://h/x?X-Amz-Signature=abc"
    ).foreach { s =>
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
