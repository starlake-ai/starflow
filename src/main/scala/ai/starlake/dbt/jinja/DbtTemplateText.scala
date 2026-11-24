package ai.starlake.dbt.jinja

import java.util.regex.Matcher
import scala.collection.mutable
import scala.util.matching.Regex

/** Rewrites dbt Jinja that Jinjava cannot parse into Jinja that it can, before rendering:
  *   - `{% test name(...) %}...{% endtest %}` becomes the macro `test_name`;
  *   - `adapter.dispatch(A)(B)` becomes `__sl_dispatch([A], B)`: Jinjava cannot call the result of
  *     a call, and `{% set m = adapter.dispatch(A) %}...m(B)` the same way;
  *   - a trailing comma before `)`, `]` or `}` inside a tag is dropped;
  *   - a string default of a macro argument that holds `,`, `(` or `)` (MacroTag splits the
  *     arguments on commas and ends them at the first `)`) becomes a variable, returned in
  *     `literals`, to put in the context that defines the macro.
  */
object DbtTemplateText {
  final case class Rewritten(text: String, literals: Map[String, String])

  private val TestOpen: Regex = """\{%(-?)\s*test\s+(\w+)\s*\(""".r
  private val TestClose: Regex = """\{%(-?)\s*endtest\s*(-?)%\}""".r
  private val DispatchKwargs: Regex =
    """adapter\.dispatch\(\s*macro_name\s*=\s*([^,()]+?)\s*(?:,\s*macro_namespace\s*=\s*([^,()]+?)\s*)?\)""".r
  private val DispatchSet: Regex =
    """\{%(-?)\s*set\s+(\w+)\s*=\s*adapter\.dispatch\(([^()]*)\)\s*(-?)%\}""".r
  private val DispatchEmptyCall: Regex = """adapter\.dispatch\(([^()]*)\)\s*\(\s*\)""".r
  private val DispatchCall: Regex = """adapter\.dispatch\(([^()]*)\)\s*\(""".r
  private val Dispatch: Regex = """adapter\.dispatch\(([^()]*)\)""".r
  private val Tag: Regex = """(?s)\{\{.*?\}\}|\{%.*?%\}""".r
  private val TrailingComma: Regex = """,(\s*[)\]}])""".r
  private val MacroHeader: Regex = """(?s)(\{%-?\s*macro\s+\w+\s*\()(.*?)(\)\s*-?%\})""".r
  private val StringLiteral: Regex = """'[^']*'|"[^"]*"""".r
  private val Materialization: Regex = """\{%-?\s*materialization\s""".r

  /** Materializations are adapter code that the import never calls */
  def isMaterialization(macroSql: String): Boolean =
    Materialization.findFirstIn(macroSql).isDefined

  /** @param literalPrefix
    *   prefix of the variables that hold the lifted defaults, unique per defining context
    */
  def rewrite(text: String, literalPrefix: String): Rewritten = {
    var s = TestOpen.replaceAllIn(
      text,
      m => Matcher.quoteReplacement(s"{%${m.group(1)} macro test_${m.group(2)}(")
    )
    s = TestClose.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(s"{%${m.group(1)} endmacro ${m.group(2)}%}")
    )
    s = DispatchKwargs.replaceAllIn(
      s,
      m =>
        Matcher.quoteReplacement(
          s"adapter.dispatch(${(Option(m.group(1)) ++ Option(m.group(2))).mkString(", ")})"
        )
    )
    s = rewriteDispatchSets(s)
    s = DispatchEmptyCall.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(s"__sl_dispatch([${m.group(1)}])")
    )
    s = DispatchCall.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(s"__sl_dispatch([${m.group(1)}], ")
    )
    s = Dispatch.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(s"__sl_dispatch_macro([${m.group(1)}])")
    )
    s = Tag.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(TrailingComma.replaceAllIn(m.matched, "$1"))
    )
    liftDefaults(s, literalPrefix)
  }

  /** `{% set m = adapter.dispatch(A) %}` then `m(B)`: the set goes, each call becomes a dispatch */
  private def rewriteDispatchSets(text: String): String = {
    var s = text
    DispatchSet.findAllMatchIn(text).toList.foreach { m =>
      val variable = m.group(2)
      val spec = m.group(3)
      s = s.replace(m.matched, "")
      s = s"""\\b${Regex.quote(variable)}\\(\\s*\\)""".r
        .replaceAllIn(s, Matcher.quoteReplacement(s"__sl_dispatch([$spec])"))
      s = s"""\\b${Regex.quote(variable)}\\(""".r
        .replaceAllIn(s, Matcher.quoteReplacement(s"__sl_dispatch([$spec], "))
    }
    s
  }

  private def liftDefaults(text: String, prefix: String): Rewritten = {
    val literals = mutable.LinkedHashMap.empty[String, String]
    val out = MacroHeader.replaceAllIn(
      text,
      { header =>
        val args = StringLiteral.replaceAllIn(
          header.group(2),
          { lit =>
            val value = lit.matched.substring(1, lit.matched.length - 1)
            if (value.exists(c => c == ',' || c == '(' || c == ')')) {
              val name = s"$prefix${literals.size}"
              literals.put(name, value)
              Matcher.quoteReplacement(name)
            } else Matcher.quoteReplacement(lit.matched)
          }
        )
        Matcher.quoteReplacement(header.group(1) + args + header.group(3))
      }
    )
    Rewritten(out, literals.toMap)
  }
}
