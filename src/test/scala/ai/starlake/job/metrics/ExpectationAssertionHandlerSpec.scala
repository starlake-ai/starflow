package ai.starlake.job.metrics

import ai.starlake.TestHelper

import java.sql.DriverManager
import scala.util.Using

/** The result of an expectation query is an integer, a count of offending rows for example (#1879)
  */
class ExpectationAssertionHandlerSpec extends TestHelper {
  "The result of an expectation" should "be read from any integral value" in {
    ExpectationAssertionHandler.result(java.lang.Integer.valueOf(3)) shouldBe 3
    ExpectationAssertionHandler.result(java.lang.Long.valueOf(3L)) shouldBe 3
    ExpectationAssertionHandler.result(java.lang.Short.valueOf(3.toShort)) shouldBe 3
    ExpectationAssertionHandler.result(java.lang.Byte.valueOf(3.toByte)) shouldBe 3
    ExpectationAssertionHandler.result(java.math.BigInteger.valueOf(3)) shouldBe 3
    ExpectationAssertionHandler.result(new java.math.BigDecimal("3.00")) shouldBe 3
    ExpectationAssertionHandler.result(new java.math.BigDecimal("0.0")) shouldBe 0
    ExpectationAssertionHandler.result(java.lang.Double.valueOf(0.0)) shouldBe 0
  }

  it should "fail on a NULL, a fraction or a value that is not a number" in {
    List[Any](
      null,
      java.lang.Double.valueOf(0.5),
      java.lang.Double.valueOf(Double.NaN),
      new java.math.BigDecimal("0.5"),
      "0",
      java.lang.Boolean.FALSE
    ).foreach { value =>
      withClue(String.valueOf(value)) {
        a[MatchError] should be thrownBy ExpectationAssertionHandler.result(value)
      }
    }
  }

  it should "never wrap a count to 0, which means success" in {
    ExpectationAssertionHandler.result(java.lang.Long.valueOf(1L << 32)) shouldBe Int.MaxValue
    ExpectationAssertionHandler.result(java.lang.Long.valueOf(Long.MinValue)) shouldBe Int.MinValue
    ExpectationAssertionHandler.result(
      java.math.BigInteger.ONE.shiftLeft(64)
    ) shouldBe Int.MaxValue
  }

  new WithSettings() {
    "The JDBC assertion handler" should "count a NULL or a fraction as a failure" in {
      Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { connection =>
        val handler = new SQLConnectionExpectationAssertionHandler(connection)
        handler.handle("SELECT COUNT(*) FROM (VALUES (1), (2)) AS t(x)") shouldBe 2
        handler.handle("SELECT CAST(2 AS HUGEINT)") shouldBe 2
        handler.handle("SELECT 0.0") shouldBe 0
        handler.handle("SELECT NULL") shouldBe Integer.MIN_VALUE
        handler.handle("SELECT 0.5") shouldBe Integer.MIN_VALUE
      }
    }

    "The Spark assertion handler" should "read a COUNT(*), a BIGINT" in {
      val handler = new SparkExpectationAssertionHandler(sparkSession)
      handler.handle("SELECT COUNT(*) FROM VALUES (1), (2) AS t(x)") shouldBe 2
      handler.handle("SELECT CAST(NULL AS INT)") shouldBe Integer.MIN_VALUE
    }
  }
}
