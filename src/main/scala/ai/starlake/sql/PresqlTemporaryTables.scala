package ai.starlake.sql

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/** The temporary tables that the presql of a task creates with `CREATE TEMP TABLE x AS SELECT ...`,
  * as common table expressions of its SELECT (#1950). The sync of the attributes of a task reads
  * the columns of its SELECT without running its presql, which may write.
  *
  * The SQL is read with regular expressions and a small lexer, not parsed: a table is read where a
  * table name can appear, after `FROM` or `JOIN`, or after a comma in a FROM clause.
  */
object PresqlTemporaryTables {

  /** How the SQL of an engine writes its strings and comments
    *
    * @param backslashEscapes
    *   whether a backslash escapes the character that follows it in a string
    * @param hashComments
    *   whether a `#` starts a comment up to the end of the line
    */
  final case class Lexing(backslashEscapes: Boolean, hashComments: Boolean)

  object Lexing {
    val standard: Lexing = Lexing(backslashEscapes = false, hashComments = false)

    /** @param engineName
      *   the name of the engine of a connection, as `ConnectionInfo.getJdbcEngineName` gives it
      */
    def of(engineName: String): Lexing =
      engineName.toLowerCase match {
        case "bigquery" | "mysql"  => Lexing(backslashEscapes = true, hashComments = true)
        case "snowflake" | "spark" => Lexing(backslashEscapes = true, hashComments = false)
        case _                     => standard
      }
  }

  /** A temporary table of the presql, which the SELECT may read as a common table expression
    *
    * @param name
    *   the name of the table, as the presql writes it
    * @param query
    *   the query that fills it, without the parentheses that may enclose it
    */
  final case class TemporaryTable(name: String, query: String) {

    /** The name without its quotes */
    val bareName: String = unquote(name)

    /** @return
      *   true if the SQL reads this table: its name, not qualified by a dataset or a schema, where
      *   a table name can appear
      */
    def isReadBy(sql: String): Boolean =
      tableReferences(sql).exists(reference =>
        reference.size == 1 && reference.head.equalsIgnoreCase(bareName)
      )

    /** @return
      *   true if the SQL defines a common table expression of this name
      */
    def isDefinedBy(sql: String): Boolean =
      commonTableExpressionNames(sql).exists(_.equalsIgnoreCase(bareName))

    /** @return
      *   true if its query reads the table of a task: its name, alone or qualified by the dataset
      *   or schema of the task
      */
    def readsTable(domain: String, table: String): Boolean =
      tableReferences(query).exists { reference =>
        reference.last.equalsIgnoreCase(table) &&
        (reference.size == 1 || reference(reference.size - 2).equalsIgnoreCase(domain))
      }

    def asCommonTableExpression: String = s"$name AS (\n$query\n)"
  }

  private[sql] def unquote(name: String): String = name.filterNot("`\"".contains(_))

  /** A table name, alone or qualified, quoted or not, at the start of the text */
  private val TableName: Regex =
    """(?s)\s*((?:`[^`]+`|"[^"]+"|[\w$-]+)(?:\s*\.\s*(?:`[^`]+`|"[^"]+"|[\w$-]+))*).*""".r

  private val FromOrJoin: Regex = """(?i)\b(FROM|JOIN)\b""".r

  /** The keywords that end a FROM clause */
  private val EndOfFrom: Regex =
    """(?is)(?:WHERE|GROUP|HAVING|QUALIFY|WINDOW|ORDER|LIMIT|UNION|INTERSECT|EXCEPT|SELECT)\b.*""".r

  /** The names written where a table name can appear, without their quotes, split into their parts:
    * `project.dataset.table` gives three parts. They follow `FROM` or `JOIN`, or a comma of a FROM
    * clause, outside the parentheses of the clause.
    */
  private def isNamePart(c: Char): Boolean = c.isLetterOrDigit || c == '_' || c == '$'

  private def tableReferences(sql: String): List[List[String]] = {
    def nameAt(index: Int): Option[List[String]] =
      sql.substring(index) match {
        case TableName(name) => Some(unquote(name).split('.').map(_.trim).toList)
        case _               => None
      }
    val references = ListBuffer.empty[List[String]]
    FromOrJoin.findAllMatchIn(sql).foreach { keyword =>
      references ++= nameAt(keyword.end)
      if (keyword.group(1).equalsIgnoreCase("FROM")) {
        // The commas of the clause, up to its end
        var depth = 0
        var quote: Option[Char] = None
        var i = keyword.end
        var done = false
        while (!done && i < sql.length) {
          val c = sql.charAt(i)
          quote match {
            case Some(q) => if (c == q) quote = None
            case None =>
              c match {
                case '\'' | '"' | '`' => quote = Some(c)
                case '('              => depth += 1
                case ')' =>
                  depth -= 1
                  done = depth < 0
                case ',' if depth == 0 => references ++= nameAt(i + 1)
                case _
                    if depth == 0 && !isNamePart(sql.charAt(i - 1)) &&
                    EndOfFrom.matches(sql.substring(i, math.min(sql.length, i + 20))) =>
                  done = true
                case _ =>
              }
          }
          i += 1
        }
      }
    }
    references.toList
  }

  /** The names of the common table expressions that the SQL defines, with or without a column list
    * or a `MATERIALIZED` keyword
    */
  private[sql] def commonTableExpressionNames(sql: String): List[String] =
    ("""(?i)(?:\bWITH(?:\s+RECURSIVE)?|,)\s*(`[^`]+`|"[^"]+"|[\w$]+)(?:\s*\([^()]*\)\s*|\s+)""" +
      """AS\s*(?:(?:NOT\s+)?MATERIALIZED\s*)?\(""").r
      .findAllMatchIn(sql)
      .map(m => unquote(m.group(1)))
      .toList

  /** `CREATE [OR REPLACE] [LOCAL | GLOBAL] TEMP[ORARY] TABLE [IF NOT EXISTS] name AS query`, for an
    * unqualified name: a temporary table with a column list or options is not one
    */
  private val CreateTemporaryTableAs: Regex =
    ("""(?is)CREATE\s+(?:OR\s+REPLACE\s+)?(?:LOCAL\s+|GLOBAL\s+)?(?:TEMP|TEMPORARY)\s+TABLE\s+""" +
      """(?:IF\s+NOT\s+EXISTS\s+)?(`[^`.]+`|"[^".]+"|[A-Za-z_]\w*)\s+AS\s*((?:\(|SELECT\b|WITH\b).*)""").r

  /** A statement that creates a table, in any form, and its name */
  private val CreateTable: Regex =
    ("""(?is)CREATE\s+(?:OR\s+REPLACE\s+)?(?:LOCAL\s+|GLOBAL\s+)?(?:(?:TEMP|TEMPORARY)\s+)?TABLE\s+""" +
      """(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|"[^"]+"|[\w$.]+)(?:[\s(].*)?""").r

  /** A statement that changes the rows or the columns of a table, and its name */
  private val ChangeTable: Regex =
    ("""(?is)(?:INSERT\s+(?:INTO\s+)?|ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?|UPDATE\s+|DELETE\s+(?:FROM\s+)?|""" +
      """MERGE\s+(?:INTO\s+)?|TRUNCATE\s+(?:TABLE\s+)?|DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?)""" +
      """(`[^`]+`|"[^"]+"|[\w$.]+)(?:[\s(].*)?""").r

  /** A statement that runs other statements conditionally, repeatedly or dynamically, labelled or
    * not: not a `BEGIN` that starts a transaction
    */
  private val ControlFlow: Regex =
    ("""(?is)(?:\w+\s*:\s*)?(?:(?:IF|LOOP|WHILE|REPEAT|FOR|CASE|CALL)\b|EXECUTE\s+IMMEDIATE\b|""" +
      """BEGIN(?!\s+(?:TRANSACTION|TRAN|WORK|ISOLATION|READ|NAME)\b)\s+\S).*""").r

  private val WithData: Regex = """(?is)(.*?)\s+WITH\s+(?:NO\s+)?DATA""".r

  private val With: Regex = """(?is)(WITH(?:\s+RECURSIVE)?\s+)(.*)""".r

  /** @return
    *   the temporary tables that the presql creates from a query, in the order of the presql. A
    *   table created more than once, also in another form, read by its own query, or changed by a
    *   statement of the presql after it is created, is left out: it is not a common table
    *   expression. So are all the tables of a presql that runs statements conditionally, repeatedly
    *   or dynamically (IF, LOOP, EXECUTE IMMEDIATE...).
    */
  def of(presql: List[String], lexing: Lexing = Lexing.standard): List[TemporaryTable] = {
    val all = presql.flatMap(statements(_, lexing))
    if (all.exists(ControlFlow.matches(_))) Nil
    else {
      def named(name: String, table: TemporaryTable) =
        unquote(name).equalsIgnoreCase(table.bareName)
      val created = all.zipWithIndex.flatMap {
        case (CreateTemporaryTableAs(name, query), index) =>
          val withoutDataClause = stripTerminator(query) match {
            case WithData(select) => select
            case select           => select
          }
          Some(TemporaryTable(name, unwrap(withoutDataClause, lexing)) -> index)
        case _ => None
      }
      created.collect {
        case (table, index)
            if all.count {
              case CreateTable(name) => named(name, table)
              case _                 => false
            } == 1 &&
            !all.drop(index + 1).exists {
              case ChangeTable(name) => named(name, table)
              case _                 => false
            } &&
            !table.isReadBy(table.query) =>
          table
      }
    }
  }

  /** @return
    *   the SELECT with the temporary tables of the presql that it reads, directly or through
    *   another of them, as common table expressions in front of its own, and these tables. A table
    *   that the SELECT defines itself in a WITH clause is left out, and so are the tables that read
    *   it. In a recursive WITH clause, where every common table expression sees the others, so is a
    *   table that reads a name that the SELECT defines.
    */
  def withTemporaryTables(
    select: String,
    presql: List[String],
    lexing: Lexing = Lexing.standard
  ): (String, List[TemporaryTable]) = {
    val sql = stripTerminator(stripComments(select, lexing))
    val recursive = """(?is)WITH\s+RECURSIVE\b.*""".r.matches(sql)
    val definedBySelect = commonTableExpressionNames(sql)
    val (tables, _) =
      of(presql, lexing).foldLeft((Vector.empty[TemporaryTable], Vector.empty[TemporaryTable])) {
        case ((kept, leftOut), table) =>
          val readsLeftOut = leftOut.exists(_.isReadBy(table.query))
          val readsSelectName =
            recursive && definedBySelect.exists(name =>
              TemporaryTable(name, "").isReadBy(table.query)
            )
          if (table.isDefinedBy(sql) || readsLeftOut || readsSelectName) (kept, leftOut :+ table)
          else (kept :+ table, leftOut)
      }
    // A table may only read the tables created before it
    val needed = tables.zipWithIndex.foldRight(Set.empty[Int]) { case ((table, index), acc) =>
      val readers = sql :: acc.toList.map(tables(_).query)
      if (readers.exists(table.isReadBy)) acc + index else acc
    }
    val read = tables.indices.filter(needed.contains).map(tables).toList
    if (read.isEmpty) (select, Nil)
    else {
      val ctes = read.map(_.asCommonTableExpression).mkString(",\n")
      val withTables = sql match {
        case With(keyword, rest) => s"$keyword$ctes,\n$rest"
        case _                   => s"WITH $ctes\n$sql"
      }
      (withTables, read)
    }
  }

  /** @return
    *   the SQL without its comments (`--`, `/* */`, and `#` for the engines that have them),
    *   outside the strings and the quoted names
    */
  private[sql] def stripComments(sql: String, lexing: Lexing = Lexing.standard): String = {
    val result = new StringBuilder
    var quote: Option[Char] = None
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      quote match {
        case Some(_) if lexing.backslashEscapes && c == '\\' && i + 1 < sql.length =>
          result.append(c).append(sql.charAt(i + 1))
          i += 1
        case Some(q) =>
          result.append(c)
          if (c == q) quote = None
        case None if (c == '-' && sql.startsWith("--", i)) || (lexing.hashComments && c == '#') =>
          // Up to the end of the line, which is kept
          while (i + 1 < sql.length && sql.charAt(i + 1) != '\n') i += 1
        case None if c == '/' && sql.startsWith("/*", i) =>
          val end = sql.indexOf("*/", i + 2)
          i = if (end < 0) sql.length else end + 1
          result.append(' ')
        case None =>
          if (c == '\'' || c == '"' || c == '`') quote = Some(c)
          result.append(c)
      }
      i += 1
    }
    result.toString.trim
  }

  /** @return
    *   the statements of a presql section, without their comments, split at the semicolons outside
    *   the strings and the quoted names
    */
  private[sql] def statements(sql: String, lexing: Lexing = Lexing.standard): List[String] = {
    val text = stripComments(sql, lexing)
    val result = List.newBuilder[String]
    val current = new StringBuilder
    var quote: Option[Char] = None
    var i = 0
    while (i < text.length) {
      val c = text.charAt(i)
      quote match {
        case Some(_) if lexing.backslashEscapes && c == '\\' && i + 1 < text.length =>
          current.append(c).append(text.charAt(i + 1))
          i += 1
        case Some(q) =>
          current.append(c)
          if (c == q) quote = None
        case None if c == ';' =>
          result += current.toString
          current.clear()
        case None =>
          if (c == '\'' || c == '"' || c == '`') quote = Some(c)
          current.append(c)
      }
      i += 1
    }
    result += current.toString
    result.result().map(_.trim).filter(_.nonEmpty)
  }

  private def stripTerminator(sql: String): String = sql.trim.stripSuffix(";").trim

  /** The query without the parentheses that enclose it whole, if any */
  private def unwrap(query: String, lexing: Lexing): String = {
    val trimmed = stripTerminator(query)
    if (trimmed.startsWith("(") && closingParenthesis(trimmed, lexing) == trimmed.length - 1)
      unwrap(trimmed.substring(1, trimmed.length - 1), lexing)
    else trimmed
  }

  /** The index of the parenthesis that closes the first one, outside the quotes, or -1 */
  private def closingParenthesis(sql: String, lexing: Lexing): Int = {
    var depth = 0
    var quote: Option[Char] = None
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      quote match {
        case Some(_) if lexing.backslashEscapes && c == '\\' => i += 1
        case Some(q)                                         => if (c == q) quote = None
        case None =>
          c match {
            case '\'' | '"' | '`' => quote = Some(c)
            case '('              => depth += 1
            case ')' =>
              depth -= 1
              if (depth == 0) return i
            case _ =>
          }
      }
      i += 1
    }
    -1
  }
}
