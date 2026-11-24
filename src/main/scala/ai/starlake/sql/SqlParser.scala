package ai.starlake.sql

import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.view.CreateView
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.select.{
  PlainSelect,
  Select,
  SelectVisitorAdapter,
  SetOperationList
}
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.{Statement, StatementVisitorAdapter}
import net.sf.jsqlparser.util.TablesNamesFinder

import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object SqlParser extends LazyLogging {

  val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,a-z0-9`./(]*)".r
  val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r

  def extractTableNamesUsingRegEx(sql: String): List[String] = {
    val froms =
      fromsRegex
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList
        .flatMap(_.split(",").map(_.trim))
        .map { table =>
          val space = table.replaceAll("\\s", " ").indexOf(' ')
          if (space > 0) table.substring(0, space) else table
        }
        .filter(!_.contains("("))

    val joins = joinRegex.findAllMatchIn(sql).map(_.group(1)).toList
    (froms ++ joins).map(_.replaceAll("`", "")).distinct
  }

  def extractColumnNames(sql: String): List[String] = {
    var result: List[String] = Nil

    def extractColumnsFromPlainSelect(plainSelect: PlainSelect): Unit = {
      val selectItems = Option(plainSelect.getSelectItems).map(_.asScala).getOrElse(Nil)
      result = selectItems.map { selectItem =>
        selectItem.getASTNode.jjtGetLastToken().image
      }.toList
    }

    val selectVisitorAdapter = new SelectVisitorAdapter[Any]() {
      override def visit[T](plainSelect: PlainSelect, context: T): Any = {
        extractColumnsFromPlainSelect(plainSelect)
      }
      override def visit[T](setOpList: SetOperationList, context: T): Any = {
        val plainSelect = setOpList.getSelect(0).getPlainSelect
        extractColumnsFromPlainSelect(plainSelect)
      }
    }
    val statementVisitor = new StatementVisitorAdapter[Any]() {
      override def visit[T](select: Select, context: T): Any = {
        select.accept(selectVisitorAdapter, null)
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor, null)
    result
  }

  def extractTableNames(sql: String): List[String] = {
    val select = jsqlParse(sql)
    val finder = new TablesNamesFinder()
    val tableList = Option(finder.getTables(select)).map(_.asScala).getOrElse(Nil)
    val unquoted = tableList.map { domainAndTableName =>
      SqlFormatter.unquoteAgressive(domainAndTableName.split("\\.").toList).mkString(".")
    }
    unquoted.toList.distinct
  }

  /** Names declared by a `WITH ... AS (...)` clause, matched textually.
    *
    * Only used when [[jsqlParse]] cannot parse the statement; the parser based path resolves CTE
    * references on its own.
    */
  private val cteNameRegex =
    "(?i)(?:\\bWITH\\b(?:\\s+RECURSIVE\\b)?|,)\\s*([\\w`\"]+)\\s+AS\\s*\\(".r

  def extractCTENamesUsingRegEx(sql: String): List[String] =
    cteNameRegex
      .findAllMatchIn(sql)
      .map(m => SqlFormatter.unquoteAgressive(m.group(1)))
      .toList
      .distinct

  /** Tables the statement *reads*.
    *
    * This is what table level lineage needs: CTE names are resolved away by the parser rather than
    * reported as tables, tables nested in subqueries are reported, names in comments and string
    * literals are not, and the write target of a DML/DDL statement is excluded so that an `INSERT
    * INTO t SELECT ... FROM t` does not turn into a self edge.
    *
    * Throws when the statement cannot be parsed; see
    * [[ai.starlake.sql.SQLUtils.extractInputTableNamesWithFallback]] for the lenient variant.
    */
  def extractInputTableNames(sql: String): List[String] = {
    val statement = parseQuietly(sql)
    val finder = new TablesNamesFinder()
    val allTables =
      Option(finder.getTables(statement)).map(_.asScala.toList).getOrElse(Nil).map(unquote)
    writeTargetOf(statement) match {
      case Some(target) => allTables.filterNot(_.equalsIgnoreCase(target))
      case None         => allTables
    }
  }

  private def unquote(qualifiedName: String): String =
    SqlFormatter.unquoteAgressive(qualifiedName.split("\\.").toList).mkString(".")

  private def writeTargetOf(statement: Statement): Option[String] = {
    val table: Option[Table] = statement match {
      case insert: Insert      => Option(insert.getTable)
      case update: Update      => Option(update.getTable)
      case delete: Delete      => Option(delete.getTable)
      case merge: Merge        => Option(merge.getTable)
      case create: CreateTable => Option(create.getTable)
      case create: CreateView  => Option(create.getView)
      case _                   => None
    }
    table.map(t => unquote(t.getFullyQualifiedName))
  }

  def extractCTENames(sql: String): List[String] = {
    var result: ListBuffer[String] = ListBuffer()
    val statementVisitor = new StatementVisitorAdapter[Any]() {
      override def visit[T](select: Select, context: T): Any = {
        val ctes = Option(select.getWithItemsList()).map(_.asScala).getOrElse(Nil)
        ctes.foreach { withItem =>
          val alias = Option(withItem.getAlias).map(_.getName).getOrElse("")
          if (alias.nonEmpty)
            result += SqlFormatter.unquoteAgressive(alias)
        }
        null
      }
    }
    val select = jsqlParse(sql)
    select.accept(statementVisitor, null)
    result.toList
  }

  /** Parses without logging. Callers that treat a parse failure as an expected outcome (they have a
    * fallback) use this so a benign degradation does not surface as an ERROR.
    */
  private def parseQuietly(sql: String): Statement = {
    val features = new Consumer[CCJSqlParser] {
      override def accept(t: CCJSqlParser): Unit = {
        t.withTimeOut(60 * 1000)
      }
    }
    CCJSqlParserUtil.parse(sql.trim, features)
  }

  def jsqlParse(sql: String): Statement = {
    try {
      parseQuietly(sql)
    } catch {
      case exception: Exception =>
        logger.error(s"Failed to parse $sql")
        throw exception
    }
  }

  def getSelectStatementIndex(sql: String): Int = {
    val trimmedSql = SqlFormatter.stripComments(sql.trim)

    val withPattern = "(?i)^\\s*WITH\\b".r
    if (withPattern.findFirstIn(trimmedSql).isEmpty) {
      0
    } else {
      var depth = 0
      var inSingleQuote = false
      var inDoubleQuote = false
      var i = 0
      var lastCteEnd = -1

      while (i < trimmedSql.length) {
        val c = trimmedSql(i)
        if (c == '\'' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inSingleQuote = !inSingleQuote
        } else if (c == '"' && (i == 0 || trimmedSql(i - 1) != '\\')) {
          inDoubleQuote = !inDoubleQuote
        }
        if (!inSingleQuote && !inDoubleQuote) {
          c match {
            case '(' => depth += 1
            case ')' =>
              depth -= 1
              if (depth == 0) { lastCteEnd = i }
            case _ =>
          }
        }
        i = i + 1
      }

      if (lastCteEnd >= 0) {
        val restOfSql = trimmedSql.substring(lastCteEnd + 1)
        val selectPattern = "(?i)\\bSELECT\\b".r
        selectPattern.findFirstMatchIn(restOfSql) match {
          case Some(m) => return lastCteEnd + 1 + m.start
          case None    =>
        }
      }
      -1
    }
  }
}
