package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionStatusSpec extends AnyFlatSpec with Matchers {
  private def result(failures: List[String], ok: Boolean = false) =
    CollectResult(
      if (ok) Map(PhysicalTable(None, "s", "t") -> RawTableStats.Missing) else Map.empty,
      failures
    )

  "ConnectionStatus.from" should "publish the first failure followed by (+k more), capped at 300" in {
    val failures = (1 to 50).map(i => s"schema$i: permission denied").toList
    val cs = ConnectionStatus.from("duckdb", result(failures), 1L)
    cs.status shouldBe "error"
    val error = cs.error.get
    error.length should be <= Sanitize.MaxLength
    error should startWith("schema1: permission denied")
    error should endWith("(+49 more)")
  }

  it should "cut a long first failure with an ellipsis and keep the (+k more) suffix" in {
    val failures = "x" * 1000 :: List.fill(5)("other")
    val error = ConnectionStatus.from("duckdb", result(failures), 1L).error.get
    error.length should be <= Sanitize.MaxLength
    error should startWith("xxx")
    error should endWith("... (+5 more)")
  }

  it should "cut a single long failure at 300 characters" in {
    val error = ConnectionStatus.from("duckdb", result(List("y" * 1000)), 1L).error.get
    error.length shouldBe Sanitize.MaxLength
    error should endWith("...")
  }

  it should "add no suffix for a single failure" in {
    val cs = ConnectionStatus.from("duckdb", result(List("s: denied"), ok = true), 1L)
    cs.status shouldBe "ok"
    cs.error shouldBe Some("s: denied")
    ConnectionStatus.from("duckdb", result(Nil, ok = true), 1L).error shouldBe None
  }
}
