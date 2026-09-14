package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SelectorSpec extends AnyFlatSpec with Matchers {

  private def parsed(expr: String): Selector =
    Selector.parse(expr) match {
      case Right(selector) => selector
      case Left(message)   => fail(s"expected '$expr' to parse, got: $message")
    }

  private def rejected(expr: String): String =
    Selector.parse(expr) match {
      case Left(message) => message
      case Right(value)  => fail(s"expected '$expr' to be rejected, got: $value")
    }

  "Selector" should "parse an exact two-part name" in {
    val selector = parsed("sales.revenue")
    selector.matcher shouldBe Matcher.Exact("sales.revenue")
    selector.upstream shouldBe false
    selector.downstream shouldBe false
    selector.expr shouldBe "sales.revenue"
  }

  it should "parse a domain wildcard" in {
    parsed("sales.*").matcher shouldBe Matcher.DomainAll("sales")
  }

  it should "parse a tag selector" in {
    parsed("tag:daily").matcher shouldBe Matcher.Tag("daily")
  }

  it should "read a leading plus as upstream and a trailing plus as downstream" in {
    val up = parsed("+sales.revenue")
    up.upstream shouldBe true
    up.downstream shouldBe false

    val down = parsed("sales.revenue+")
    down.upstream shouldBe false
    down.downstream shouldBe true

    val both = parsed("+sales.revenue+")
    both.upstream shouldBe true
    both.downstream shouldBe true
    both.matcher shouldBe Matcher.Exact("sales.revenue")
  }

  it should "apply the operators to wildcard and tag forms too" in {
    // The spec's table shows '+' only on domain.table, but a uniform parser is less code and a
    // better CLI. dbt behaves the same way.
    parsed("+tag:daily").upstream shouldBe true
    parsed("sales.*+").downstream shouldBe true
  }

  it should "lowercase every matcher value" in {
    parsed("Sales.Revenue").matcher shouldBe Matcher.Exact("sales.revenue")
    parsed("SALES.*").matcher shouldBe Matcher.DomainAll("sales")
    parsed("TAG:Daily").matcher shouldBe Matcher.Tag("daily")
  }

  it should "trim surrounding whitespace but keep the original text for messages" in {
    val selector = parsed("  +sales.revenue  ")
    selector.matcher shouldBe Matcher.Exact("sales.revenue")
    selector.expr shouldBe "+sales.revenue"
  }

  it should "reject every malformed expression" in {
    List(
      "",
      "   ",
      "*",
      "*.table",
      "sales",
      "a.b.c",
      "a.*.c",
      "a.*b",
      "a..b",
      "a.b.",
      ".b",
      "++sales.revenue",
      "sales.revenue++",
      ".*"
    ).foreach { expr =>
      withClue(s"expression '$expr' should be rejected: ") {
        Selector.parse(expr).isLeft shouldBe true
      }
    }
  }

  it should "name the offending expression in the rejection message" in {
    rejected("a.b.c") should include("a.b.c")
  }

  it should "explain that a tag selector needs a value" in {
    val message = rejected("tag:")
    message.toLowerCase should include("tag")
    message should include("value")
  }
}
