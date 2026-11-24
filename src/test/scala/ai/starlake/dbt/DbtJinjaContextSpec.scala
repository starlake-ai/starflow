package ai.starlake.dbt

import ai.starlake.dbt.jinja.{DbtAdapter, DbtConfigObject, DbtExceptions}
import ai.starlake.dbt.manifest.DbtMacro
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

/** dbt's context in DbtJinja, on the macros of the fixture manifest */
class DbtJinjaContextSpec extends AnyFlatSpec with Matchers {
  private lazy val manifest = DbtFixtures.shopManifest
  private lazy val jinja =
    DbtFixtures.jinja(cliVars = Map("cli_only" -> "c"), env = Map("SET_VAR" -> "s"))
  private lazy val orders = DbtFixtures.node(manifest, "orders")

  private def sql(template: String, incremental: Boolean = false): String =
    jinja.render(orders, template, incremental).sql.get

  private def skipped(template: String): String =
    jinja.render(orders, template, incremental = false).sql.failed.get match {
      case DbtSkip(reason) => reason
      case other           => s"not a skip: ${other.getMessage}"
    }

  "ref, source and this" should "render Starlake's domain.table" in {
    sql(
      "{{ ref('stg_orders') }}|{{ ref('shop', 'raw_orders') }}|{{ source('crm', 'crm_accounts') }}|{{ this }}|{{ this.schema }}.{{ this.name }}"
    ) shouldBe
    "staging.stg_orders|raw.raw_orders|legacy.accounts|marts.orders|marts.orders"
  }

  "a ref to nothing" should "fail the render with the name" in {
    jinja
      .render(orders, "{{ ref('nope') }}", incremental = false)
      .sql
      .failed
      .get
      .getMessage should include("ref('nope')")
  }

  "config" should "read the node config and capture config(...) calls" in {
    val rendered = jinja.render(
      orders,
      "{{ config(unique_key='id') }}{{ config.get('materialized') }}/{{ config.get('unique_key') }}/{{ config.get('nope', 'd') }}",
      incremental = false
    )
    rendered.sql.get shouldBe "table/id/d"
    rendered.config("unique_key") shouldBe "id"
  }

  "is_incremental()" should "follow the render" in {
    sql("{% if is_incremental() %}inc{% else %}full{% endif %}", incremental = true) shouldBe "inc"
    sql("{% if is_incremental() %}inc{% else %}full{% endif %}") shouldBe "full"
  }

  "var()" should "read --vars, then the package vars, then the global ones, then the default" in {
    val rendered = jinja.render(
      orders,
      "{{ var('cli_only') }}{{ var('currency') }}{{ var('min_amount') }}{{ var('absent', 'd') }}",
      incremental = false
    )
    rendered.sql.get shouldBe "cEUR0d"
    rendered.vars shouldBe Map(
      "cli_only"   -> "c",
      "currency"   -> "EUR",
      "min_amount" -> "0",
      "absent"     -> "d"
    )
  }

  it should "skip the model when a var has no value and no default" in {
    skipped("{{ var('absent') }}") shouldBe "var('absent') has no value and no default"
  }

  "env_var()" should "bake the value, or the default, and skip when neither exists" in {
    val rendered = jinja.render(
      orders,
      "{{ env_var('SET_VAR') }}{{ env_var('UNSET_VAR', 'u') }}",
      incremental = false
    )
    rendered.sql.get shouldBe "su"
    rendered.envVars shouldBe Map("SET_VAR" -> "s", "UNSET_VAR" -> "u")
    skipped(
      "{{ env_var('UNSET_VAR') }}"
    ) shouldBe "env_var('UNSET_VAR') is not set and has no default"
  }

  "what needs the warehouse" should "skip the model" in {
    skipped(
      "{% set r = run_query('select 1') %}"
    ) shouldBe "needs the warehouse at import (run_query)"
    skipped(
      "{% call statement('x', fetch_result=True) %}select 1{% endcall %}"
    ) shouldBe "needs the warehouse at import (statement)"
    skipped(
      "{% set r = adapter.get_relation(this.database, this.schema, this.name) %}"
    ) shouldBe "needs the warehouse at import (adapter.get_relation)"
    skipped(
      "{% set cols = adapter.get_columns_in_relation(this) %}"
    ) shouldBe "needs the warehouse at import (adapter.get_columns_in_relation)"
    skipped(
      "{% for n in graph.nodes.values() %}{% endfor %}"
    ) shouldBe "needs the warehouse at import (graph)"
  }

  "exceptions" should "skip on raise_compiler_error and report warn" in {
    skipped("{{ exceptions.raise_compiler_error('bad config') }}") shouldBe "bad config"
    jinja
      .render(orders, "{{ exceptions.warn('careful') }}x", incremental = false)
      .warnings shouldBe List("careful")
  }

  "adapter.dispatch" should "call the project's default__ implementation" in {
    sql("{{ cents_to_dollars('c') }}").trim shouldBe "(c / 100)::numeric(16, 2)"
  }

  it should "resolve a dispatch kept in a variable" in {
    sql(
      "{% set m = adapter.dispatch('date_trunc', 'dbt') %}{{ m('day', 'd') }}"
    ).trim shouldBe "date_trunc('day', d)"
  }

  "package namespaces" should "call package macros, with kwargs" in {
    sql("{{ dbt_utils.generate_surrogate_key(field_list=['a']) }}") shouldBe
    "md5(cast(coalesce(cast(a as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))"
    sql("{{ dbt.date_trunc('month', 'd') }}").trim shouldBe "date_trunc('month', d)"
  }

  "generic tests" should "be callable as test_ macros" in {
    sql("{{ dbt_utils.test_expression_is_true(model=ref('orders'), expression='amount >= 0') }}")
      .split("\\s+")
      .filter(_.nonEmpty)
      .mkString(" ") shouldBe "select 1 from marts.orders where not(amount >= 0)"
  }

  "the target" should "come from the profile, the type from the manifest" in {
    sql(
      "{{ target.name }}/{{ target.type }}/{{ target.schema }}/{{ target.database }}"
    ) shouldBe "dev/duckdb/main/shop"
  }

  "return()" should "end the effects of the macro: a raise after it does not skip" in {
    sql(
      "{% macro f() %}{{ return('v') }}{{ exceptions.raise_compiler_error('boom') }}{% endmacro %}{{ f() }}"
    ) shouldBe "v"
  }

  it should "end the effects of the macro: run_query after it does not skip" in {
    sql(
      "{% macro f() %}{{ return('v') }}{% do run_query('select 1') %}{% endmacro %}{{ f() }}"
    ) shouldBe "v"
  }

  it should "end the effects of the macro: no warning after it" in {
    val rendered = jinja.render(
      orders,
      "{% macro f() %}{{ return('v') }}{{ exceptions.warn('late') }}{% endmacro %}{{ f() }}",
      incremental = false
    )
    rendered.sql.get shouldBe "v"
    rendered.warnings shouldBe Nil
  }

  it should "end the effects of the macro: no config set after it" in {
    val rendered = jinja.render(
      orders,
      "{% macro f() %}{{ return('v') }}{{ config(materialized='x') }}{% endmacro %}{{ f() }}/{{ config.get('materialized') }}",
      incremental = false
    )
    rendered.sql.get shouldBe "v/table"
    rendered.config shouldBe Map.empty
  }

  it should "end the effects inside the block that returned, and of the macros called after it" in {
    sql(
      "{% macro f() %}{% for i in [1, 2] %}{% if i == 1 %}{{ return('v') }}{% endif %}{{ exceptions.raise_compiler_error('boom') }}{% endfor %}{% endmacro %}{{ f() }}"
    ) shouldBe "v"
    sql(
      "{% macro g() %}{{ exceptions.raise_compiler_error('g') }}{% endmacro %}{% macro f() %}{% if true %}{{ return('v') }}{{ g() }}{% endif %}{% endmacro %}{{ f() }}"
    ) shouldBe "v"
  }

  it should "not fail on a dispatch without implementation after it" in {
    sql(
      "{% macro f() %}{% if true %}{{ return('v') }}{{ adapter.dispatch('nope', 'dbt')() }}{% endif %}{% endmacro %}{{ f() }}"
    ) shouldBe "v"
  }

  it should "keep the effects of the outer macro when only the inner one returned" in {
    skipped(
      "{% macro inner() %}{{ return(1) }}{{ exceptions.raise_compiler_error('inner') }}{% endmacro %}{% macro outer() %}{% set x = inner() %}{{ exceptions.raise_compiler_error('outer ' ~ x) }}{% endmacro %}{{ outer() }}"
    ) shouldBe "outer 1"
  }

  it should "return none from dbt_duckdb's get_partitioned_by when partitioned_by is unset" in {
    sql("{% set p = dbt.duckdb__get_partitioned_by(this, false) %}{{ p is none }}") shouldBe "true"
  }

  "a default= keyword" should "be the default of config.get, dict.get, var and env_var" in {
    sql(
      "{{ config.get('nope', default='d') }}/{{ config.get('materialized', default='d') }}"
    ) shouldBe "d/table"
    sql("{{ {'a': 1}.get('b', default=7) }}/{{ {'a': 1}.get('a', default=7) }}") shouldBe "7/1"
    val rendered = jinja.render(
      orders,
      "{{ var('absent', default='d') }}/{{ env_var('UNSET_VAR', default='u') }}",
      incremental = false
    )
    rendered.sql.get shouldBe "d/u"
    rendered.vars shouldBe Map("absent" -> "d")
    rendered.envVars shouldBe Map("UNSET_VAR" -> "u")
  }

  it should "let dbt's default__create_indexes render without indexes" in {
    sql("{{ dbt.default__create_indexes(this) }}").trim shouldBe ""
  }

  "the dbt objects" should "do nothing outside a render" in {
    new DbtExceptions().invoke("warn", List("w")) shouldBe Some("")
    new DbtExceptions().invoke("raise_compiler_error", List("e")) shouldBe Some("")
    new DbtAdapter("duckdb").invoke("execute", Nil) shouldBe Some("")
    new DbtConfigObject(new util.HashMap[String, Object]()).require("k") shouldBe ""
  }

  "a macro that fails to define" should "be a definition error, the other macros and models still render" in {
    val manifest = DbtFixtures
      .emptyManifest()
      .copy(macros =
        List(
          DbtMacro("macro.shop.bad", "shop", "bad", null),
          DbtMacro("macro.shop.good", "shop", "good", "{% macro good() %}g{% endmacro %}")
        )
      )
    val jinja = DbtFixtures.jinja(manifest)
    jinja.definitionErrors.map(_.takeWhile(_ != ':')) shouldBe List("macro.shop.bad")
    jinja.render(DbtFixtures.model("m"), "{{ good() }}", incremental = false).sql.get shouldBe "g"
  }

  "a render that cannot start" should "be a Failure" in {
    jinja.render(orders, null, incremental = false).sql shouldBe a[scala.util.Failure[_]]
  }
}
