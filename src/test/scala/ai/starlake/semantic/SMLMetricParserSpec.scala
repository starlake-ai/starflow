package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SMLMetricParserSpec extends AnyFlatSpec with Matchers {

  import SMLMetricParser._

  "parseCall" should "map every supported aggregate to its SML calculation method" in {
    val cases = List(
      "SUM(x)"                   -> "sum",
      "SUM(DISTINCT x)"          -> "sum distinct",
      "avg(x)"                   -> "average",
      "MIN(x)"                   -> "minimum",
      "MAX(x)"                   -> "maximum",
      "COUNT(x)"                 -> "count non-null",
      "COUNT(DISTINCT x)"        -> "count distinct",
      "APPROX_COUNT_DISTINCT(x)" -> "estimated count distinct",
      "STDDEV(x)"                -> "stddev_samp",
      "STDDEV_SAMP(x)"           -> "stddev_samp",
      "STDDEV_POP(x)"            -> "stddev_pop",
      "VARIANCE(x)"              -> "var_samp",
      "VAR_SAMP(x)"              -> "var_samp",
      "VAR_POP(x)"               -> "var_pop"
    )
    cases.foreach { case (expr, method) =>
      withClue(expr) { parseCall(expr) shouldBe Some(AggregateCall(method, "x")) }
    }
  }

  it should "accept COUNT(*) and arguments with nested parentheses" in {
    parseCall("COUNT(*)") shouldBe Some(AggregateCall("count non-null", "*"))
    parseCall(" SUM( (price * qty) - COALESCE(discount, 0) ) ") shouldBe Some(
      AggregateCall("sum", "(price * qty) - COALESCE(discount, 0)")
    )
  }

  it should "reject unsupported DISTINCT combinations, window functions and non-aggregates" in {
    parseCall("AVG(DISTINCT x)") shouldBe None
    parseCall("COUNT(DISTINCT *)") shouldBe None
    parseCall("SUM(*)") shouldBe None
    parseCall("SUM(x) OVER (PARTITION BY y)") shouldBe None
    parseCall("MEDIAN(x)") shouldBe None
    parseCall("SUM(x) / COUNT(y)") shouldBe None
    parseCall("SUM()") shouldBe None
  }

  it should "accept DISTINCT followed by a parenthesized argument" in {
    parseCall("COUNT(DISTINCT(x))") shouldBe Some(AggregateCall("count distinct", "x"))
    parseCall("SUM(DISTINCT(x))") shouldBe Some(AggregateCall("sum distinct", "x"))
    parseCall("COUNT(DISTINCT (a) + (b))") shouldBe Some(
      AggregateCall("count distinct", "(a) + (b)")
    )
    parseCall("AVG(DISTINCT(x))") shouldBe None
    parseCall("COUNT(DISTINCTx)") shouldBe Some(AggregateCall("count non-null", "DISTINCTx"))
  }

  "decompose" should "split arithmetic over aggregates into calls and render MDX" in {
    val tokens = decompose("(SUM(orders.total) - 10.5) / COUNT(DISTINCT customers.id)").get
    tokens.collect { case AggToken(c) => c } shouldBe List(
      AggregateCall("sum", "orders.total"),
      AggregateCall("count distinct", "customers.id")
    )
    render(tokens, List("a", "b")) shouldBe "([Measures].[a] - 10.5) / [Measures].[b]"
  }

  it should "reject CASE, window functions, unbalanced parentheses and expressions without aggregates" in {
    decompose("CASE WHEN SUM(x) > 0 THEN 1 ELSE 0 END") shouldBe None
    decompose("SUM(x) OVER (PARTITION BY y)") shouldBe None
    decompose("(SUM(x)") shouldBe None
    decompose("1 + 2") shouldBe None
  }

  "render" should "refuse to render with a wrong number of measure names" in {
    val tokens = decompose("SUM(a) + SUM(b)").get
    an[IllegalArgumentException] should be thrownBy render(tokens, List("only_one"))
  }

  "substitute" should "replace field identifiers outside strings and keep function names" in {
    val fields = Map("price" -> "PRICE", "qty" -> "(QUANTITY * 1)", "orders.price" -> "PRICE")
    substitute(
      "SUM_IF(price) + qty * orders.price + 'price' + \"qty\" + COALESCE(unknown, 0)",
      token => fields.get(token.toLowerCase)
    ) shouldBe "SUM_IF(PRICE) + (QUANTITY * 1) * PRICE + 'price' + \"qty\" + COALESCE(unknown, 0)"
  }
}
