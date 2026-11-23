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

  it should "redact keys that contain a secret word, keeping the other pairs" in {
    val sf = Sanitize.message("Snowflake: private_key_file_pwd: s1, user: bob")
    sf should not include "s1"
    sf should include("user: bob")
    sf should include("private_key_file_pwd=<redacted>")
    List("sfPassword=x1", "PGPASSWORD=x2", "client_secret=x3", "access_token=x4").foreach { s =>
      Sanitize.message(s"bad $s here") shouldBe
      s"bad ${s.takeWhile(_ != '=')}=<redacted> here"
    }
    Sanitize.message("api_key=k1") shouldBe "api_key=<redacted>"
    Sanitize.message("apikey: k2") shouldBe "apikey=<redacted>"
  }

  it should "redact a service account JSON key" in {
    val json =
      """{"type": "service_account", "private_key_id": "abc123", "private_key": """ +
      """"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADAN\n-----END PRIVATE KEY-----\n", """ +
      """"client_email": "x@y.iam.gserviceaccount.com"}"""
    val msg = Sanitize.message(s"invalid credentials $json")
    List("abc123", "MIIEvQIBADAN", "BEGIN PRIVATE KEY", "END PRIVATE KEY").foreach(
      msg should not include _
    )
    msg should include(""""client_email": "x@y.iam.gserviceaccount.com"""")
  }

  it should "redact a PEM block up to its END line, or to the end of the message" in {
    val multiLine =
      "bad key private_key: -----BEGIN RSA PRIVATE KEY-----\nMIIEpAIB\nAAKCAQ\n" +
      "-----END RSA PRIVATE KEY----- for user bob"
    val msg = Sanitize.message(multiLine)
    msg shouldBe "bad key private_key=<redacted> for user bob"
    val cut = Sanitize.message("bad key private_key=-----BEGIN PRIVATE KEY-----\nMIIEvQ")
    cut shouldBe "bad key private_key=<redacted>"
  }

  it should "redact JSON style quoted keys" in {
    List(
      """{"password":"x"}""",
      """{"password": "x"}""",
      """{"access_token":"ya29.abc"}""",
      """{'password': 'x'}"""
    ).foreach { s =>
      val msg = Sanitize.message(s"bad payload $s")
      msg should not include "\"x\""
      msg should not include "'x'"
      msg should not include "ya29"
      msg should include("=<redacted>}")
    }
  }

  it should "redact Authorization headers and bare bearer tokens" in {
    Sanitize.message("401 with Authorization: Bearer xyz.abc") shouldBe
    "401 with Authorization: <redacted>"
    Sanitize.message("401 with Authorization: Basic dXNlcjpwYXNz") shouldBe
    "401 with Authorization: <redacted>"
    Sanitize.message("Proxy-Authorization=Digest abc") shouldBe "Proxy-Authorization: <redacted>"
    Sanitize.message("""headers {"Authorization": "Bearer xyz"}""") should not include "xyz"
    Sanitize.message("sent Bearer eyJhbGciOi.payload.sig to the API") shouldBe
    "sent Bearer <redacted> to the API"
  }

  it should "redact an Azure storage connection string account key" in {
    val msg = Sanitize.message(
      "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=K3y==;EndpointSuffix=core.windows.net"
    )
    msg should not include "K3y=="
    msg shouldBe
    "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=<redacted>;EndpointSuffix=core.windows.net"
  }

  it should "redact values with commas, parentheses, braces, escapes or URL encoding" in {
    Sanitize.message("password=ab)cd next") shouldBe "password=<redacted> next"
    Sanitize.message("password=a,b next") shouldBe "password=<redacted> next"
    Sanitize.message("Driver={ODBC};Pwd={a;b};Uid=u") shouldBe "Driver={ODBC};Pwd=<redacted>;Uid=u"
    Sanitize.message("""password='it\'s' next""") shouldBe "password=<redacted> next"
    Sanitize.message("""password="say \"hi\"" next""") shouldBe "password=<redacted> next"
    Sanitize.message("q?user=u&password%3Dhunter2&x=1") shouldBe "q?user=u&password=<redacted>&x=1"
  }

  it should "redact a URL password containing an @" in {
    val msg = Sanitize.message("cannot connect to jdbc:postgresql://alice:p@ss@h:5432/db")
    msg should not include "p@ss"
    msg should not include "alice"
    msg shouldBe "cannot connect to jdbc:postgresql://<redacted>@h:5432/db"
  }

  it should "keep keys that are not followed by a separator" in {
    Sanitize.message("table password_history not found") shouldBe
    "table password_history not found"
    Sanitize.message("Column private_key not found: expected type STRING") shouldBe
    "Column private_key not found: expected type STRING"
    Sanitize.message("Secret Manager access denied") shouldBe "Secret Manager access denied"
  }

  it should "match short secret words only as whole key segments" in {
    List("Assignment: done", "design: v2", "signal=3", "upwdx=1").foreach { s =>
      Sanitize.message(s) shouldBe s
    }
    List(
      "sig=abc",
      "url_sig=abc",
      "X-Amz-Signature=abc",
      "PWD=x",
      "db.pwd: y",
      "x-amz-sig=abc"
    ).foreach { s =>
      val msg = Sanitize.message(s)
      msg should include("=<redacted>")
      msg should not include "abc"
      msg should not include "=x"
      msg should not include " y"
    }
  }

  it should "redact a parser token (accepted false positive)" in {
    // JSqlParser reports `token: "FROM"`; the key/value rule cannot tell it from a token pair
    Sanitize.message("""Encountered unexpected token: "FROM" at line 1""") shouldBe
    "Encountered unexpected token=<redacted> at line 1"
  }

  it should "stay idempotent on the new redactions" in {
    List(
      """{"password":"x"}""",
      "Authorization: Bearer xyz",
      "Bearer abc def",
      "AccountKey=K3y==;EndpointSuffix=e",
      "Pwd={a;b};Uid=u",
      "jdbc:postgresql://alice:p@ss@h:5432/db",
      "private_key: -----BEGIN PRIVATE KEY-----\nX\n-----END PRIVATE KEY----- end"
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
