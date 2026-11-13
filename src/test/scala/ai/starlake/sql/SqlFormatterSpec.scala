package ai.starlake.sql

import ai.starlake.TestHelper
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
import com.manticore.jsqlformatter.JSQLFormatter

/** Formatting keeps the underscores of the SQL: it encoded the Jinja delimiters as `___` and
  * `______`, then decoded every run of three or six underscores (#1898)
  */
class SqlFormatterSpec extends TestHelper {

  private def format(sql: String): String = SQLUtils.format(sql, JSQLFormatter.OutputFormat.PLAIN)

  private def count(text: String, part: String): Int = text.sliding(part.length).count(_ == part)

  "format" should "keep the underscores of identifiers and literals" in {
    val formatted = format(
      "SELECT src.id, 'a__b' AS two, 'a___b' AS three, 'a____b' AS four, 'a______b' AS six, " +
      "src.col NOT LIKE '__/__/____ __:__:__' AS shape FROM acme___shared_data.vw_orders AS src"
    )
    withClue(formatted) {
      List(
        "acme___shared_data",
        "'a__b'",
        "'a___b'",
        "'a____b'",
        "'a______b'",
        "'__/__/____ __:__:__'"
      ).foreach(text => formatted should include(text))
      formatted should not include "{{"
      formatted should not include "}}"
      // Formatted: the input is on one line
      formatted should include("\n")
    }
  }

  it should "restore the Jinja delimiters, and keep the underscores next to them" in {
    val formatted =
      format("SELECT a___b FROM {{table}} WHERE d = '{{day}}' AND e = 'x___y'")
    withClue(formatted) {
      count(formatted, "{{") shouldBe 2
      count(formatted, "}}") shouldBe 2
      formatted should include("{{table}}")
      formatted should include("'{{day}}'")
      formatted should include("a___b")
      formatted should include("'x___y'")
      formatted should not include regex("_[0-9]{13}_[01]")
      formatted should include("\n")
    }
  }

  it should "keep the braces of literals and comments as they are" in {
    val formatted = format(
      "SELECT a -- note {{x}}\nFROM {{table}}\n\n" +
      "WHERE j = '{\"a\":{\"b\":1}}' AND k = 'a}}' AND l = '{{' AND d = '{{ sl_start_date }}'"
    )
    withClue(formatted) {
      List(
        "{{x}}",
        "{{table}}",
        "'{\"a\":{\"b\":1}}'",
        "'a}}'",
        "'{{'",
        "'{{ sl_start_date }}'"
      ).foreach(text => formatted should include(text))
      formatted should not include "failed to format"
      formatted should not include regex("_[0-9]{13}_[01]")
    }
  }

  it should "keep the SQL as it is when formatting would change a Jinja expression" in {
    // The case of a Jinja variable, which is formatted as an identifier, and a marker in a function
    // name, which JSQLFormatter splits on underscores
    List(
      "SELECT {{myCol}} FROM {{MyTable}}",
      "SELECT a::{{typ}} FROM t",
      "SELECT fn{{x}}(a) FROM t"
    ).foreach(sql => withClue(sql)(format(sql) shouldBe sql))
  }

  it should "keep the underscores in its HTML output" in {
    val formatted =
      SQLUtils.format("SELECT 'a___b' AS three FROM t", JSQLFormatter.OutputFormat.HTML)
    withClue(formatted)(formatted should include("a___b"))
  }

  "transpile" should "keep the underscores of a BigQuery dataset name" in {
    new WithSettings() {
      val duckdb = ConnectionInfo(
        ConnectionType.JDBC,
        options = Map("url" -> "jdbc:duckdb:"),
        _transpileDialect = Some("GOOGLE_BIG_QUERY")
      )
      val transpiled = SQLUtils.transpile(
        "SELECT src.id FROM `my-project.acme___shared_data.vw_orders` AS src",
        duckdb,
        Map.empty
      )
      withClue(transpiled) {
        // Transpiled: the BigQuery name is quoted as DuckDB names are
        transpiled should include("\"acme___shared_data\"")
        transpiled should not include "`"
        transpiled should not include "{{"
      }
    }
  }
}
