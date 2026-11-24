package ai.starlake.dbt

import com.hubspot.jinjava.Jinjava
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IncrementalDiffSpec extends AnyFlatSpec with Matchers {

  "IncrementalDiff" should "keep identical renders as they are" in {
    IncrementalDiff.merge("select 1\nfrom t", "select 1\nfrom t") shouldBe "select 1\nfrom t"
  }

  it should "wrap the lines only in the incremental render" in {
    IncrementalDiff.merge(
      "select a\nfrom t",
      "select a\nfrom t\nwhere a > (select max(a) from x)"
    ) shouldBe
    """select a
      |from t
      |{% if sl_incremental %}
      |where a > (select max(a) from x)
      |{% endif %}""".stripMargin
  }

  it should "wrap the lines only in the full render" in {
    IncrementalDiff.merge("select a\nfrom t\nunion all select 0", "select a\nfrom t") shouldBe
    """select a
      |from t
      |{% if not sl_incremental %}
      |union all select 0
      |{% endif %}""".stripMargin
  }

  it should "write a replaced line as if / else" in {
    IncrementalDiff.merge("select a\nfrom full_t", "select a\nfrom inc_t") shouldBe
    """select a
      |{% if sl_incremental %}
      |from inc_t
      |{% else %}
      |from full_t
      |{% endif %}""".stripMargin
  }

  it should "write each hunk of several" in {
    IncrementalDiff.merge(
      "select a\nfrom t\ngroup by a",
      "select a, max(b)\nfrom t\nwhere b > 1\ngroup by a"
    ) shouldBe
    """{% if sl_incremental %}
      |select a, max(b)
      |{% else %}
      |select a
      |{% endif %}
      |from t
      |{% if sl_incremental %}
      |where b > 1
      |{% endif %}
      |group by a""".stripMargin
  }

  // Rendering the merged text adds only newlines (the tag lines), so both sides are compared
  // after dropping blank lines.
  private val jinjava = new Jinjava()

  private def nonBlank(text: String): String =
    text.split("\n", -1).filter(_.trim.nonEmpty).mkString("\n")

  private def render(merged: String, incremental: Boolean): String =
    jinjava.render(
      merged,
      java.util.Map.of[String, Object]("sl_incremental", Boolean.box(incremental))
    )

  private def roundTrip(full: String, incremental: String): Unit = {
    val merged = IncrementalDiff.merge(full, incremental)
    withClue(s"merged:\n$merged\n") {
      nonBlank(render(merged, incremental = false)) shouldBe nonBlank(full)
      nonBlank(render(merged, incremental = true)) shouldBe nonBlank(incremental)
    }
  }

  it should "round trip an empty full render" in {
    roundTrip("", "select 1\nfrom t")
  }

  it should "round trip an empty incremental render" in {
    roundTrip("select 1\nfrom t", "")
  }

  it should "round trip a trailing newline on one side only" in {
    roundTrip("select 1\nfrom t\n", "select 1\nfrom t\nwhere a > 1")
    roundTrip("select 1\nfrom t", "select 1\nfrom t\nwhere a > 1\n")
    roundTrip("select 1\nfrom t\n", "select 1\nfrom t")
  }

  it should "round trip repeated identical lines" in {
    roundTrip(
      "select 1\nfrom (\nselect 2\nfrom (\nselect 3\n)\n)\n)",
      "select 1\nfrom (\nselect 2\nwhere x\nfrom (\nselect 3\n)\n)"
    )
    roundTrip("select\nselect\nselect\nx", "select\nx\nselect\nselect\nselect")
    roundTrip(")\n)\n)\n)", ")\n)")
    roundTrip("a\nb\na\nb\na", "b\na\nb\nb\na\na")
  }

  it should "round trip a hunk at the very start and one at the very end" in {
    roundTrip("a\nb\nc", "x\nb\nc")
    roundTrip("a\nb\nc", "a\nb\nx")
    roundTrip("a\nb\nc", "x\nb\ny")
    roundTrip("b\nc", "a\nb\nc\nd")
  }

  it should "round trip many hunks" in {
    val full = (1 to 40).map(i => s"line $i").mkString("\n")
    val incremental = (1 to 40)
      .flatMap {
        case i if i % 7 == 0 => Seq.empty
        case i if i % 5 == 0 => Seq(s"line $i", s"extra $i")
        case i if i % 3 == 0 => Seq(s"changed $i")
        case i               => Seq(s"line $i")
      }
      .mkString("\n")
    roundTrip(full, incremental)
  }

  it should "round trip a realistic incremental model" in {
    val full =
      """with src as (
        |    select id, amount, updated_at
        |    from raw.orders
        |)
        |select id, amount, updated_at
        |from src
        |""".stripMargin
    val incremental =
      """with src as (
        |    select id, amount, updated_at
        |    from raw.orders
        |)
        |select id, amount, updated_at
        |from src
        |where updated_at > (select max(updated_at) from analytics.orders)
        |""".stripMargin
    roundTrip(full, incremental)
  }

  it should "pin the hunk layout for a tie between equally long common subsequences" in {
    // "a b" against "b a": one line is common either way. On a tie the code consumes the
    // incremental line first, so `a` is the common line and `b` moves.
    IncrementalDiff.merge("a\nb", "b\na") shouldBe
    """{% if sl_incremental %}
      |b
      |{% endif %}
      |a
      |{% if not sl_incremental %}
      |b
      |{% endif %}""".stripMargin
  }
}
