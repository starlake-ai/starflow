package ai.starlake.dbt

import ai.starlake.dbt.jinja.DbtTemplateText
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DbtTemplateTextSpec extends AnyFlatSpec with Matchers {
  private def rewrite(text: String): String = DbtTemplateText.rewrite(text, "__l_").text

  "DbtTemplateText" should "turn a test block into a test_ macro" in {
    rewrite("{% test not_null(model, column_name) %}x{% endtest %}") shouldBe
    "{% macro test_not_null(model, column_name) %}x{% endmacro %}"
    rewrite(
      "{%- test t(model) -%}x{%- endtest -%}"
    ) shouldBe "{%- macro test_t(model) -%}x{%- endmacro -%}"
  }

  it should "turn adapter.dispatch(A)(B) into one call" in {
    rewrite("{{ return(adapter.dispatch('hash', 'dbt') (field)) }}") shouldBe
    "{{ return(__sl_dispatch(['hash', 'dbt'], field)) }}"
    rewrite("{{ return(adapter.dispatch('type_string', 'dbt')()) }}") shouldBe
    "{{ return(__sl_dispatch(['type_string', 'dbt'])) }}"
    rewrite("{{ adapter.dispatch(macro_name='x', macro_namespace='p')(a) }}") shouldBe
    "{{ __sl_dispatch(['x', 'p'], a) }}"
  }

  it should "turn a dispatch kept in a variable into calls, and a bare one into its macro" in {
    rewrite(
      "{% set m = adapter.dispatch('test_not_null', 'dbt') %}{{ m(model, column_name) }}"
    ) shouldBe
    "{{ __sl_dispatch(['test_not_null', 'dbt'], model, column_name) }}"
    rewrite("{% set f = adapter.dispatch('x') %}") shouldBe ""
    rewrite("{{ adapter.dispatch('x') }}") shouldBe "{{ __sl_dispatch_macro(['x']) }}"
  }

  it should "drop trailing commas inside tags only" in {
    rewrite("{{ config(materialized = 'table',\n) }} select a, b,\nc") shouldBe
    "{{ config(materialized = 'table'\n) }} select a, b,\nc"
  }

  it should "lift the string defaults that MacroTag cannot split" in {
    val r = DbtTemplateText.rewrite(
      "{% macro m(sep=\"','\", order_by='count(*) desc', n=none) %}x{% endmacro %}",
      "__l_"
    )
    r.text shouldBe "{% macro m(sep=__l_0, order_by=__l_1, n=none) %}x{% endmacro %}"
    r.literals shouldBe Map("__l_0" -> "','", "__l_1" -> "count(*) desc")
  }

  it should "recognize materializations" in {
    DbtTemplateText.isMaterialization("{% materialization table, adapter='duckdb' %}") shouldBe true
    DbtTemplateText.isMaterialization("{% macro m() %}{% endmacro %}") shouldBe false
  }
}
