package ai.starlake.dbt

import ai.starlake.dbt.jinja.DbtJinja
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

/** The dbt Jinja constructs of the assessment probe, through DbtJinja. Where Jinjava 2.7.4 prints a
  * value differently from Python and the difference is harmless in SQL (booleans print `true`,
  * `none` prints nothing, tojson has no spaces), the expected value is Jinjava's, marked KNOWN.
  */
class DbtJinjaSpec extends AnyFlatSpec with Matchers {
  private lazy val jinja: DbtJinja = DbtFixtures.jinja(DbtFixtures.emptyManifest())
  private val node = DbtFixtures.model("probe")

  private def render(template: String, incremental: Boolean = false): String =
    jinja.render(node, template, incremental).sql match {
      case Success(sql) => sql
      case Failure(e)   => s"<failed: ${e.getMessage}>"
    }

  private val cases: List[(String, String, String)] = List(
    (
      "return() from macro",
      "{% macro f() %}{{ return(42) }}{% endmacro %}{% set x = f() %}{{ x + 1 }}",
      "43"
    ),
    (
      "return() list from macro",
      "{% macro f() %}{{ return([1,2]) }}{% endmacro %}{% for i in f() %}{{ i }}{% endfor %}",
      "12"
    ),
    (
      "namespace() KNOWN",
      "{% set ns = namespace(found=false) %}{% for i in [1,2] %}{% set ns.found = true %}{% endfor %}{{ ns.found }}",
      "true"
    ),
    ("do list.append", "{% set l = [] %}{% do l.append(1) %}{{ l }}", "[1]"),
    ("do dict.update", "{% set d = {} %}{% do d.update({'a': 1}) %}{{ d.a }}", "1"),
    ("block set", "{% set q %}select 1{% endset %}{{ q }}", "select 1"),
    (
      "call/caller",
      "{% macro wrap() %}[{{ caller() }}]{% endmacro %}{% call wrap() %}x{% endcall %}",
      "[x]"
    ),
    (
      "macro kwargs/varargs KNOWN",
      "{% macro m(a, b=2) %}{{ a }}{{ b }}{{ kwargs.c }}{{ varargs|length }}{% endmacro %}{{ m(1, 9, b=3, c=4) }}",
      "1340"
    ),
    ("str.split", "{{ 'a,b'.split(',')|length }}", "2"),
    ("str.split literal", "{{ 'a.b'.split('.')|length }}", "2"),
    ("str.upper/lower", "{{ 'x'.upper() }}{{ 'Y'.lower() }}", "Xy"),
    ("str.join", "{{ ', '.join(['a','b']) }}", "a, b"),
    ("str.startswith KNOWN", "{{ 'abc'.startswith('a') }}", "true"),
    ("str.replace", "{{ 'abc'.replace('a','z') }}", "zbc"),
    ("str.strip", "{{ '  a '.strip() }}", "a"),
    ("str.format", "{{ '{}-{}'.format(1,2) }}", "1-2"),
    ("% format filter", "{{ '%s-%s' | format(1,2) }}", "1-2"),
    ("string slicing", "{{ 'abc'[1:] }}", "bc"),
    ("negative index", "{{ [1,2,3][-1] }}", "3"),
    ("is none KNOWN", "{% set x = none %}{{ x is none }}", "true"),
    ("is string / mapping KNOWN", "{{ 'a' is string }}{{ {} is mapping }}", "truetrue"),
    ("is iterable/sequence KNOWN", "{{ [1] is iterable }}{{ [1] is sequence }}", "truetrue"),
    (
      "selectattr/map",
      "{% set l = [{'n':'a','k':1},{'n':'b','k':0}] %}{{ l|selectattr('k')|map(attribute='n')|join(',') }}",
      "a"
    ),
    ("reject/unique/list", "{{ [1,1,2]|unique|list|length }}", "2"),
    ("tojson KNOWN", "{{ {'a':1}|tojson }}", "{\"a\":1}"),
    ("dict.items unpack", "{% for k, v in {'a':1}.items() %}{{ k }}={{ v }}{% endfor %}", "a=1"),
    ("dict.get default", "{{ {'a':1}.get('b', 7) }}", "7"),
    ("dict.keys", "{{ {'a':1}.keys()|list|length }}", "1"),
    ("dict.pop", "{% set d = {'a':1} %}{{ d.pop('a') }}", "1"),
    ("~ concat", "{{ 'a' ~ 1 }}", "a1"),
    ("in / not in KNOWN", "{{ 'a' in ['a'] }}{{ 'b' not in ['a'] }}", "truetrue"),
    ("// and **", "{{ 7 // 2 }}{{ 2 ** 3 }}", "38"),
    ("range|list", "{{ range(3)|list|length }}", "3"),
    ("list + list", "{{ ([1]+[2])|length }}", "2"),
    ("dict() ctor", "{{ dict(a=1).a }}", "1"),
    ("inline if", "{{ 'a' if true else 'b' }}", "a"),
    (
      "loop.last",
      "{% for i in [1,2] %}{{ i }}{% if not loop.last %},{% endif %}{% endfor %}",
      "1,2"
    ),
    ("whitespace control", "a {%- if true -%} b {%- endif -%} c", "abc"),
    ("raw", "{% raw %}{{ x }}{% endraw %}", "{{ x }}"),
    ("int/float/string filters", "{{ '3'|int + 1 }}{{ 3|string }}", "43"),
    ("zip", "{% for a, b in zip([1],[2]) %}{{ a }}{{ b }}{% endfor %}", "12"),
    (
      "set in if leaks to outer scope",
      "{% set x = 1 %}{% if true %}{% set x = 2 %}{% endif %}{{ x }}",
      "2"
    ),
    (
      "set in for does not leak",
      "{% set x = 1 %}{% for i in [1] %}{% set x = 2 %}{% endfor %}{{ x }}",
      "1"
    ),
    (
      "macro returns sql text",
      "{% macro cols() %}a, b{% endmacro %}select {{ cols() }} from t",
      "select a, b from t"
    ),
    (
      "nested quotes in kwargs",
      "{% macro c(materialized=none, unique_key=none) %}{{ materialized }}/{{ unique_key }}{% endmacro %}{{ c(materialized='incremental', unique_key=['id','dt']) }}",
      "incremental/['id', 'dt']"
    ),
    ("list repr", "{{ ['a', 'b'] }}", "['a', 'b']"),
    ("bool repr KNOWN", "{{ true }}", "true"),
    ("none repr KNOWN", "{{ none }}", ""),
    ("string escape filter", "{{ \"it's\"|replace(\"'\", \"''\") }}", "it''s"),
    (
      "items on namespace kwargs",
      "{% macro m() %}{% for k in kwargs|sort %}{{ k }}{% endfor %}{% endmacro %}{{ m(b=1, a=2) }}",
      "ab"
    ),
    ("is defined KNOWN", "{{ undefined_x is defined }}", "false"),
    ("default filter", "{{ undefined_x|default('d') }}", "d"),
    (
      "return() inside if after output",
      "{% macro f(a) %}junk{% if a %}{{ return('yes') }}{% endif %}{{ return('no') }}{% endmacro %}{{ f(true) }}/{{ f(false) }}",
      "yes/no"
    ),
    (
      "nested macro returns",
      "{% macro inner() %}{{ return(1) }}{% endmacro %}{% macro outer() %}{% set x = inner() %}{{ return(x + 1) }}{% endmacro %}{{ outer() }}",
      "2"
    ),
    (
      "return() of dict used as object",
      "{% macro f() %}{{ return({'k': 'v'}) }}{% endmacro %}{{ f().k }}",
      "v"
    ),
    ("do return", "{% macro f() %}{% do return('r') %}{% endmacro %}{{ f() }}", "r"),
    ("trailing comma in a call", "{{ dict(a=1,).a }}", "1"),
    (
      "default holding a comma",
      "{% macro m(sep=\"','\") %}{{ sep }}{% endmacro %}{{ m() }}",
      "','"
    ),
    (
      "default holding parentheses",
      "{% macro m(order_by='count(*) desc', n=none) %}{{ order_by }}{% endmacro %}{{ m() }}",
      "count(*) desc"
    )
  )

  cases.foreach { case (name, template, expected) =>
    "DbtJinja" should s"render $name" in {
      render(template) shouldBe expected
    }
  }

  // Unsupported by Jinjava, rewritten for adapter.dispatch only
  it should "fail on calling the result of a call outside adapter.dispatch" in {
    render(
      "{% macro a(x) %}A{{ x }}{% endmacro %}{% macro pick() %}{{ return(a) }}{% endmacro %}{{ pick()(1) }}"
    ) should startWith("<failed")
  }
}
