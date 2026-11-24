package ai.starlake.utils

import ai.starlake.TestHelper
import org.apache.hadoop.fs.Path

class JinjaDeferredIncrementalSpec extends TestHelper {
  new WithSettings() {
    private def render(sql: String, incremental: Boolean): String =
      Utils.parseJinja(sql, Map("sl_incremental" -> incremental))

    "parseJinjaDeferringIncremental" should "render the other variables and keep the sl_incremental blocks" in {
      val deferred = Utils.parseJinjaDeferringIncremental(
        """SELECT id * {{ mult }} FROM t
          |{% if sl_incremental %}
          |WHERE id > {{ mult }}
          |{% endif %}""".stripMargin,
        Map("mult" -> "10")
      )
      deferred should not include "mult"
      deferred should include("sl_incremental")
      render(deferred, incremental = false) should not include "WHERE"
      render(deferred, incremental = false) should include("SELECT id * 10 FROM t")
      render(deferred, incremental = true) should include("WHERE id > 10")
    }

    it should "keep the else branch and the negated test" in {
      val deferred = Utils.parseJinjaDeferringIncremental(
        "{% if not sl_incremental %}FULL{% else %}DELTA {{ x }}{% endif %}",
        Map("x" -> "y")
      )
      render(deferred, incremental = false).trim shouldBe "FULL"
      render(deferred, incremental = true).trim shouldBe "DELTA y"
    }

    it should "keep the variable when it is printed" in {
      val deferred =
        Utils.parseJinjaDeferringIncremental("SELECT {{ sl_incremental }} AS inc", Map.empty)
      deferred should include("sl_incremental")
      render(deferred, incremental = true).trim shouldBe "SELECT true AS inc"
    }

    it should "ignore a value given for the variable" in {
      val deferred = Utils.parseJinjaDeferringIncremental(
        "{% if sl_incremental %}DELTA{% endif %}",
        Map("sl_incremental" -> "true")
      )
      deferred should include("sl_incremental")
      render(deferred, incremental = false).trim shouldBe ""
    }

    it should "render unknown variables, the doc function and includes as parseJinja does" in {
      storageHandler.write(
        "INCLUDED {{ mult }}",
        new Path(starlakeMetadataPath + "/included_for_deferred.sql")
      )
      val text =
        """{{ unknown_var }}|{{ doc("my_doc") }}|{% include "included_for_deferred.sql" %}"""
      val vars = Map("mult" -> "3")
      Utils.parseJinjaDeferringIncremental(text + "|{{ sl_incremental }}", vars) should
      startWith(Utils.parseJinja(text, vars) + "|")
      Utils.parseJinja(text, vars) shouldBe "|<!--sl-doc:my_doc-->|INCLUDED 3"
    }

    it should "fail on an unknown macro as parseJinja does" in {
      an[Exception] should be thrownBy Utils.parseJinja("A {{ unknown_macro('x') }}", Map.empty)
      an[Exception] should be thrownBy
      Utils.parseJinjaDeferringIncremental(
        "A {{ unknown_macro('x') }} {{ sl_incremental }}",
        Map.empty
      )
    }

    it should "evaluate the known part of a test that also reads the variable" in {
      val deferred = Utils.parseJinjaDeferringIncremental(
        """{% macro m(a) %}M{{ a }}{% endmacro %}{{ m('x') }} {% if sl_incremental and mode == 'x' %}D "q"{% endif %}""",
        Map("mode" -> "x")
      )
      render(deferred, incremental = false).trim shouldBe "Mx"
      render(deferred, incremental = true).trim shouldBe "Mx D \"q\""
    }

    it should "render loops, sets and filters as parseJinja does" in {
      val text =
        """{% set cols = ["a", "b"] %}SELECT {% for c in cols %}{{ c | upper }}{% if not loop.last %}, {% endif %}{% endfor %}
          |FROM t""".stripMargin
      val vars = Map("mult" -> "3")
      val deferred =
        Utils.parseJinjaDeferringIncremental(text + "\n{% if sl_incremental %}D{% endif %}", vars)
      render(deferred, incremental = false) shouldBe Utils.parseJinja(text, vars)
      render(deferred, incremental = true) shouldBe Utils.parseJinja(text, vars) + "\nD"
    }

    it should "keep the variable inside an included file" in {
      storageHandler.write(
        "{% if sl_incremental %}DELTA {{ mult }}{% endif %}",
        new Path(starlakeMetadataPath + "/included_incremental.sql")
      )
      val deferred = Utils.parseJinjaDeferringIncremental(
        """SELECT 1 {% include "included_incremental.sql" %}""",
        Map("mult" -> "4")
      )
      render(deferred, incremental = false).trim shouldBe "SELECT 1"
      render(deferred, incremental = true).trim shouldBe "SELECT 1 DELTA 4"
    }

    it should "keep a value holding Jinja for the run, inside and outside the deferred tags" in {
      // an env value left for the run, as "{{SL_CONNECTION}}" in an env file
      val deferred = Utils.parseJinjaDeferringIncremental(
        "A {{ conn }} {% if sl_incremental %}B {{ conn }}{% endif %}",
        Map("conn" -> "{{SL_X}}")
      )
      deferred should not include "raw"
      Utils.parseJinja(deferred, Map("SL_X" -> "prod", "sl_incremental" -> true)) shouldBe
      "A prod B prod"
      Utils.parseJinja(deferred, Map("SL_X" -> "prod", "sl_incremental" -> false)) shouldBe
      "A prod"
    }

    it should "render a raw block as parseJinja does" in {
      val text = "R {% raw %}{{ x }}{% endraw %}"
      val deferred =
        Utils.parseJinjaDeferringIncremental(
          text + " {% if sl_incremental %}D{% endif %}",
          Map.empty
        )
      val master = Utils.parseJinja(text, Map.empty)
      master shouldBe "R {{ x }}"
      deferred should startWith(master + " ")
      Utils.parseJinja(deferred, Map("x" -> "1", "sl_incremental" -> true)) shouldBe
      Utils.parseJinja(master, Map("x" -> "1")) + " D"
    }

    it should "leave the shared instance rendering the variable as undefined" in {
      Utils.parseJinja("[{% if sl_incremental %}X{% endif %}]", Map.empty) shouldBe "[]"
    }
  }
}
