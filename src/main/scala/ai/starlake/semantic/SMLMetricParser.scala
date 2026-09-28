package ai.starlake.semantic

/** Parses the SQL of semantic model metrics for the SML export: single aggregate calls mapped to
  * SML calculation methods, arithmetic over aggregates decomposed for MDX calculations, and field
  * substitution inside SQL expressions.
  */
private[semantic] object SMLMetricParser {

  /** One aggregate call; `arg` is the trimmed argument without DISTINCT ("*" for COUNT(*)). */
  case class AggregateCall(method: String, arg: String)

  sealed trait Token
  case class AggToken(call: AggregateCall) extends Token
  case class TextToken(text: String) extends Token

  private val FunctionStart = """^([A-Za-z_][A-Za-z0-9_]*)\s*\(""".r
  private val DistinctArg = """(?is)^DISTINCT\s+(.+)$""".r
  private val NumberLiteral = """^\d+(\.\d+)?""".r
  private val IdentifierToken = """^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?""".r

  /** The whole expression is exactly one supported aggregate call. */
  def parseCall(expr: String): Option[AggregateCall] = {
    val s = expr.trim
    callAt(s, 0).collect { case (call, end) if end == s.length => call }
  }

  /** Supported aggregate calls, numeric literals, + - * / and parentheses only, with at least one
    * call. Returns the tokens in expression order.
    */
  def decompose(expr: String): Option[List[Token]] = {
    val s = expr.trim
    @annotation.tailrec
    def loop(i: Int, depth: Int, acc: List[Token]): Option[List[Token]] =
      if (i >= s.length) { if (depth == 0) Some(acc.reverse) else None }
      else {
        val c = s.charAt(i)
        if (c.isWhitespace) loop(i + 1, depth, TextToken(c.toString) :: acc)
        else if (c == '(') loop(i + 1, depth + 1, TextToken("(") :: acc)
        else if (c == ')') {
          if (depth == 0) None else loop(i + 1, depth - 1, TextToken(")") :: acc)
        } else if ("+-*/".indexOf(c.toInt) >= 0) loop(i + 1, depth, TextToken(c.toString) :: acc)
        else
          NumberLiteral.findPrefixOf(s.substring(i)) match {
            case Some(n) => loop(i + n.length, depth, TextToken(n) :: acc)
            case None =>
              callAt(s, i) match {
                case Some((call, end)) => loop(end, depth, AggToken(call) :: acc)
                case None              => None
              }
          }
      }
    loop(0, 0, Nil).filter(_.exists(_.isInstanceOf[AggToken]))
  }

  /** MDX text of decomposed tokens, each call replaced by the next measure of `names`. */
  def render(tokens: List[Token], names: List[String]): String = {
    require(
      names.size == tokens.count(_.isInstanceOf[AggToken]),
      s"render needs one measure name per aggregate call, got ${names.size}"
    )
    val measures = names.iterator
    tokens.map {
      case TextToken(text) => text
      case AggToken(_)     => s"[Measures].[${measures.next()}]"
    }.mkString
  }

  /** Replace identifier tokens (bare or qualified) outside string literals and quoted identifiers
    * by `resolve(token)` when defined. Identifiers followed by '(' are function names and are kept.
    */
  def substitute(sql: String, resolve: String => Option[String]): String = {
    val out = new java.lang.StringBuilder
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (c == '\'' || c == '"') {
        val close = sql.indexOf(c.toInt, i + 1)
        val stop = if (close < 0) sql.length else close + 1
        out.append(sql, i, stop)
        i = stop
      } else if (c.isDigit) {
        var j = i
        while (
          j < sql.length && (sql.charAt(j).isLetterOrDigit || "_.".indexOf(
            sql.charAt(j).toInt
          ) >= 0)
        )
          j += 1
        out.append(sql, i, j)
        i = j
      } else
        IdentifierToken.findPrefixOf(sql.substring(i)) match {
          case Some(token) =>
            val isFunction =
              sql.substring(i + token.length).dropWhile(_.isWhitespace).startsWith("(")
            out.append(if (isFunction) token else resolve(token).getOrElse(token))
            i += token.length
          case None =>
            out.append(c)
            i += 1
        }
    }
    out.toString
  }

  /** A supported aggregate call starting at `start`, with the index just after its ')'. */
  private def callAt(s: String, start: Int): Option[(AggregateCall, Int)] =
    FunctionStart.findPrefixMatchOf(s.substring(start)).flatMap { m =>
      val open = start + m.end - 1
      closingParen(s, open).flatMap { close =>
        val inner = s.substring(open + 1, close).trim
        val (distinct, arg) = inner match {
          case DistinctArg(a) => (true, a.trim)
          case other          => (false, other)
        }
        method(m.group(1), distinct, arg).map(meth => (AggregateCall(meth, arg), close + 1))
      }
    }

  /** Index of the ')' matching the '(' at `open`, skipping single-quoted strings. */
  private def closingParen(s: String, open: Int): Option[Int] = {
    var depth = 0
    var inString = false
    var i = open
    while (i < s.length) {
      val c = s.charAt(i)
      if (inString) { if (c == '\'') inString = false }
      else if (c == '\'') inString = true
      else if (c == '(') depth += 1
      else if (c == ')') {
        depth -= 1
        if (depth == 0) return Some(i)
      }
      i += 1
    }
    None
  }

  private def method(function: String, distinct: Boolean, arg: String): Option[String] =
    if (arg.isEmpty || (arg == "*" && (distinct || !function.equalsIgnoreCase("COUNT")))) None
    else
      (function.toUpperCase, distinct) match {
        case ("SUM", false)                    => Some("sum")
        case ("SUM", true)                     => Some("sum distinct")
        case ("AVG", false)                    => Some("average")
        case ("MIN", false)                    => Some("minimum")
        case ("MAX", false)                    => Some("maximum")
        case ("COUNT", false)                  => Some("count non-null")
        case ("COUNT", true)                   => Some("count distinct")
        case ("APPROX_COUNT_DISTINCT", false)  => Some("estimated count distinct")
        case ("STDDEV" | "STDDEV_SAMP", false) => Some("stddev_samp")
        case ("STDDEV_POP", false)             => Some("stddev_pop")
        case ("VARIANCE" | "VAR_SAMP", false)  => Some("var_samp")
        case ("VAR_POP", false)                => Some("var_pop")
        case _                                 => None
      }
}
