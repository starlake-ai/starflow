package ai.starlake.utils.conversion

import net.sf.jsqlparser.expression.{
  CaseExpression,
  CastExpression,
  Expression,
  ExpressionVisitorAdapter,
  Function
}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The strict conversions of the scripts of the attributes, which the second step of a native
  * BigQuery load checks (#1935): `CAST` and the `PARSE_` functions fail the statement on a value
  * that does not convert, where their safe forms, `SAFE_CAST` and `SAFE.PARSE_`, give NULL. A row
  * on which a strict conversion would fail is rejected, and the second step evaluates the scripts
  * in their safe form, so that no evaluation order makes it fail on a rejected row.
  */
object BigQueryScriptChecks {

  private val strictFunctions = Set(
    "PARSE_DATE",
    "PARSE_DATETIME",
    "PARSE_TIMESTAMP",
    "PARSE_TIME",
    "PARSE_NUMERIC",
    "PARSE_BIGNUMERIC"
  )

  /** A strict conversion of a script
    *
    * @param sql
    *   its SQL, as JSqlParser prints it
    * @param check
    *   the SQL condition, never NULL, of the rows on which it does not fail: its branch is not
    *   evaluated, one of its arguments is NULL, or its safe form is not NULL. The branches are
    *   evaluated lazily, as in the script.
    * @param input
    *   the SQL of the value that it converts, in safe form
    * @param columns
    *   the columns that this value reads
    */
  case class Conversion(sql: String, check: String, input: String, columns: List[String])

  /** A script with strict conversions
    *
    * @param safe
    *   its SQL as it is written, before Jinja, with `SAFE_` or `SAFE.` before its strict
    *   conversions
    * @param conversions
    *   its strict conversions, none if it has none
    */
  case class Script(safe: String, conversions: List[Conversion])

  /** @return
    *   the strict conversions of a script without Jinja, and its safe form (see [[analyze]])
    */
  def analyze(script: String): Option[Script] = analyze(script, script)

  /** @param rendered
    *   the script, rendered
    * @param written
    *   the script as it is written: its safe form is too, so that the second step renders it once
    * @return
    *   the strict conversions of the script, and its safe form, None if it does not parse, or if it
    *   has strict conversions that are not found, in a subquery for example, or that Jinja adds or
    *   removes. A script without strict conversion is kept as it is.
    */
  def analyze(rendered: String, written: String): Option[Script] =
    parse(rendered).flatMap { expression =>
      val collector = new Collector
      expression.accept(collector, null)
      val found = collector.found.toList
      val (safeScript, safeCount) = safeText(written)
      if (safeCount != found.size || safeText(rendered)._2 != found.size) None
      else if (found.isEmpty) Some(Script(written, Nil))
      else {
        val strict = found.map(_.node.toString)
        found.foreach(found => safe(found.node))
        // Printed once every conversion is safe: their arguments and conditions are too. A guard
        // is evaluated only when the guards before it hold, and the conversion only when they all
        // do, as in the script: BigQuery evaluates the branches of IF lazily, not the operands of
        // OR
        Some(
          Script(
            safeScript,
            found.zip(strict).map { case (found, sql) =>
              val converts = (found.arguments.map(argument => s"($argument) IS NULL") :+
                s"(${found.node}) IS NOT NULL").mkString("(", " OR ", ")")
              val check = found.guards.foldRight(converts) { case (guard, inner) =>
                s"IF(${guard.sql}, $inner, TRUE)"
              }
              Conversion(sql, check, found.input.toString, columns(found.input))
            }
          )
        )
      }
    }

  /** @return
    *   the script parsed, its strings in double quotes single-quoted first: BigQuery reads them as
    *   strings, where JSqlParser reads them as names
    */
  private def parse(script: String): Option[Expression] = {
    def parsed(sql: String) = Try(CCJSqlParserUtil.parseExpression(sql, false)).toOption
    singleQuoted(script).flatMap(parsed).orElse(parsed(script))
  }

  /** @return
    *   the index of the quote that closes the one at `start` in the script, -1 if none: a backslash
    *   escapes the next character in a string, raw or not
    */
  private def closing(script: String, start: Int, quote: String): Int = {
    var index = start + quote.length
    var end = -1
    while (end < 0 && index < script.length) {
      if (script.charAt(index) == '\\' && quote != "`") index += 2
      else if (script.startsWith(quote, index)) end = index
      else index += 1
    }
    end
  }

  /** @return
    *   the script as it is written, with `SAFE_` before its `CAST` calls and `SAFE.` before its
    *   strict `PARSE_` calls, and the number of these calls: those of the strings, quoted names and
    *   comments are not
    */
  private[conversion] def safeText(script: String): (String, Int) = {
    val result = new StringBuilder
    var index = 0
    var count = 0
    // The last character of the script before the current one, but whitespace
    var previous = ' '
    def copy(end: Int): Unit = {
      val text = script.substring(index, end)
      result ++= text
      text.reverseIterator.find(!_.isWhitespace).foreach(previous = _)
      index = end
    }
    while (index < script.length) {
      val char = script.charAt(index)
      if (char == '\'' || char == '"' || char == '`') {
        val quote =
          if (char != '`' && script.startsWith(char.toString * 3, index)) char.toString * 3
          else char.toString
        val end = closing(script, index, quote)
        copy(if (end < 0) script.length else end + quote.length)
      } else if (script.startsWith("--", index) || char == '#') {
        val end = script.indexOf('\n', index)
        copy(if (end < 0) script.length else end)
      } else if (script.startsWith("/*", index)) {
        val end = script.indexOf("*/", index + 2)
        copy(if (end < 0) script.length else end + 2)
      } else if (char.isLetter || char == '_') {
        val end = script.indexWhere(c => !(c.isLetterOrDigit || c == '_'), index) match {
          case -1  => script.length
          case end => end
        }
        val word = script.substring(index, end)
        val call = script.indexWhere(!_.isWhitespace, end) match {
          case -1   => false
          case next => script.charAt(next) == '('
        }
        if (call && previous != '.') {
          val upper = word.toUpperCase
          if (upper == "CAST") {
            result ++= "SAFE_"
            count += 1
          } else if (strictFunctions(upper)) {
            result ++= "SAFE."
            count += 1
          }
        }
        copy(end)
      } else copy(index + 1)
    }
    (result.result(), count)
  }

  /** @return
    *   the script with its double-quoted strings single-quoted, which JSqlParser reads as strings,
    *   None if it has none, or a triple-quoted string, or a raw string with an apostrophe, or a
    *   string or a quoted name that does not end
    */
  private[conversion] def singleQuoted(script: String): Option[String] = {
    val result = new StringBuilder
    var index = 0
    var changed = false
    var valid = true
    while (valid && index < script.length) {
      val char = script.charAt(index)
      if (char == '\'' || char == '`') {
        val end = closing(script, index, char.toString)
        if (end < 0) valid = false
        else {
          result ++= script.substring(index, end + 1)
          index = end + 1
        }
      } else if (char == '"') {
        val end = if (script.startsWith("\"\"\"", index)) -1 else closing(script, index, "\"")
        val raw = index > 0 && "rR".contains(script.charAt(index - 1))
        val content = if (end < 0) "" else script.substring(index + 1, end)
        if (end < 0 || raw && content.contains('\'')) valid = false
        else {
          result += '\''
          var position = 0
          while (position < content.length) {
            val next = content.charAt(position)
            if (next == '\\') {
              result ++= content.substring(position, (position + 2).min(content.length))
              position += 2
            } else {
              if (next == '\'') result += '\\'
              result += next
              position += 1
            }
          }
          result += '\''
          changed = true
          index = end + 1
        }
      } else {
        result += char
        index += 1
      }
    }
    if (valid && changed) Some(result.result()) else None
  }

  /** A condition of the evaluation of a branch, never NULL, printed once the conversions are safe
    */
  private sealed trait Guard {
    def sql: String
  }

  /** The branch is evaluated when the condition holds */
  private case class Holds(condition: () => String) extends Guard {
    def sql: String = s"(${condition()}) IS TRUE"
  }

  /** The branch is evaluated when the condition does not hold: false or NULL */
  private case class Fails(condition: () => String) extends Guard {
    def sql: String = s"(${condition()}) IS NOT TRUE"
  }

  /** The branch is evaluated when the condition is not false: true or NULL */
  private case class NotFalse(condition: () => String) extends Guard {
    def sql: String = s"(${condition()}) IS NOT FALSE"
  }

  /** The branch is evaluated when the expression is NULL */
  private case class IsNull(expression: Expression) extends Guard {
    def sql: String = s"($expression) IS NULL"
  }

  /** A strict conversion found in a script
    *
    * @param arguments
    *   its arguments: the conversion fails only when none is NULL
    * @param input
    *   the value that it converts
    * @param guards
    *   the conditions of the evaluation of its branch, from the outermost one
    */
  private case class Found(
    node: Expression,
    arguments: List[Expression],
    input: Expression,
    guards: List[Guard]
  )

  /** Finds the strict conversions of an expression, with the conditions of their branch: `CASE`,
    * `IF`, `IFNULL` and `COALESCE` evaluate their branches lazily, and the second operand of `AND`
    * and `OR` matters only when the first one does not decide. The other expressions evaluate all
    * their arguments. Subqueries are not visited.
    */
  private class Collector extends ExpressionVisitorAdapter[Void] {
    val found: ListBuffer[Found] = ListBuffer.empty
    private var guards: List[Guard] = Nil

    private def under(added: List[Guard])(expression: Expression): Unit =
      if (expression != null) {
        val saved = guards
        guards = saved ++ added
        try expression.accept(this, null)
        finally guards = saved
      }

    override def visit[S](cast: CastExpression, context: S): Void = {
      // BigQuery has no `::` cast
      if ("CAST".equalsIgnoreCase(cast.keyword))
        found += Found(cast, List(cast.getLeftExpression), cast.getLeftExpression, guards)
      super.visit(cast, context)
    }

    override def visit[S](function: Function, context: S): Void = {
      val parameters = Option(function.getParameters)
        .map(_.asInstanceOf[java.util.List[Expression]].asScala.toList)
        .getOrElse(Nil)
      function.getMultipartName.asScala.toList.map(_.toUpperCase) match {
        case List(name)
            if strictFunctions(
              name
            ) && parameters.nonEmpty && function.getNamedParameters == null =>
          // The text is the first argument of PARSE_NUMERIC, the second of the others
          val input =
            if (name.endsWith("NUMERIC")) parameters.head
            else parameters.lift(1).getOrElse(parameters.head)
          found += Found(function, parameters, input, guards)
          super.visit(function, context)
        case List("IF") if parameters.size == 3 =>
          val condition = parameters.head
          under(Nil)(condition)
          under(List(Holds(() => condition.toString)))(parameters(1))
          under(List(Fails(() => condition.toString)))(parameters(2))
          null
        case List("IFNULL") if parameters.size == 2 =>
          under(Nil)(parameters.head)
          under(List(IsNull(parameters.head)))(parameters(1))
          null
        case List("COALESCE") =>
          parameters.zipWithIndex.foreach { case (parameter, index) =>
            under(parameters.take(index).map(IsNull(_)))(parameter)
          }
          null
        case _ => super.visit(function, context)
      }
    }

    override def visit[S](expression: AndExpression, context: S): Void = {
      val left = expression.getLeftExpression
      under(Nil)(left)
      under(List(NotFalse(() => left.toString)))(expression.getRightExpression)
      null
    }

    override def visit[S](expression: OrExpression, context: S): Void = {
      val left = expression.getLeftExpression
      under(Nil)(left)
      under(List(Fails(() => left.toString)))(expression.getRightExpression)
      null
    }

    override def visit[S](expression: CaseExpression, context: S): Void = {
      val switch = Option(expression.getSwitchExpression)
      switch.foreach(under(Nil))
      // The conditions of the branches before, which did not hold
      val before = ListBuffer.empty[() => String]
      expression.getWhenClauses.asScala.foreach { clause =>
        val when = clause.getWhenExpression
        // Both operands parenthesized: either may be a comparison
        val condition: () => String = switch match {
          case Some(switch) => () => s"($switch) = ($when)"
          case None         => () => when.toString
        }
        val notBefore = before.toList.map(Fails(_))
        under(notBefore)(when)
        under(notBefore :+ Holds(condition))(clause.getThenExpression)
        before += condition
      }
      under(before.toList.map(Fails(_)))(expression.getElseExpression)
      null
    }
  }

  /** Replaces a strict conversion with its safe form */
  private def safe(node: Expression): Unit =
    node match {
      case cast: CastExpression => cast.keyword = "SAFE_CAST"
      case function: Function => function.setName(java.util.Arrays.asList("SAFE", function.getName))
      case _                  =>
    }

  /** @return
    *   the names of the columns that an expression reads, without quotes
    */
  private def columns(expression: Expression): List[String] = {
    val names = ListBuffer.empty[String]
    expression.accept(
      new ExpressionVisitorAdapter[Void] {
        override def visit[S](column: Column, context: S): Void = {
          names += column.getColumnName.stripPrefix("`").stripSuffix("`")
          super.visit(column, context)
        }
      },
      null
    )
    names.toList.distinct
  }
}
