package ai.starlake.sql

import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.expression.Alias
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{PlainSelect, SelectItem}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object SqlFormatter extends LazyLogging {

  def format(input: String, outputFormat: JSQLFormatter.OutputFormat): String = {
    val sql = input.trim
    val jinjaId = jinjaMarkerId(sql)
    val (jinjaOpen, jinjaClose) = (s"_${jinjaId}_0_", s"_${jinjaId}_1_")
    val uppercaseSQL = sql.toUpperCase
    val formatted =
      if (
        uppercaseSQL.startsWith("SELECT") || uppercaseSQL.startsWith("WITH") || uppercaseSQL
          .startsWith("MERGE")
      ) {
        val preformat = sql.replace("}}", jinjaClose).replace("{{", jinjaOpen)
        Try(
          JSQLFormatter.format(
            preformat.trim,
            s"outputFormat=${outputFormat.name()}",
            "statementTerminator=NONE"
          )
        ) match {
          case Success(formatted) => formatted
          case Failure(e) =>
            logger.error(s"Failed to format SQL: $sql")
            s"-- failed to format start\n$sql\n-- failed to format end"
        }
      } else {
        sql
      }

    val result =
      if (formatted.startsWith("-- failed to format start")) {
        sql
      } else {
        val postFormat = formatted.replace(jinjaClose, "}}").replace(jinjaOpen, "{{")
        // JSQLFormatter formats a Jinja expression as part of an identifier: when it changes its
        // case, or a marker (in a function name, which it splits on underscores), the SQL is kept
        if (postFormat.contains(jinjaId) || jinjaExpressions(postFormat) != jinjaExpressions(sql)) {
          sql
        } else if (outputFormat == JSQLFormatter.OutputFormat.HTML) {
          val startIndex = postFormat.indexOf("<body>") + "<body>".length
          val endIndex = postFormat.indexOf("</body>")
          if (startIndex > 0 && endIndex > 0) {
            postFormat.substring(startIndex, endIndex)
          } else {
            postFormat
          }
        } else {
          postFormat
        }
      }
    val trimmedResult = result.trim
    if (!sql.endsWith(";") && trimmedResult.endsWith(";")) {
      trimmedResult.substring(0, trimmedResult.length - 1)
    } else {
      result
    }
  }

  /** @return
    *   the number of the markers that replace the Jinja delimiters `{{` and `}}` while
    *   JSQLFormatter formats the SQL: `_<number>_0_` and `_<number>_1_`, identifiers that it
    *   parses. The number is not in the SQL, so the markers cannot match any of its text, and its
    *   own underscores are kept (#1898).
    */
  private def jinjaMarkerId(sql: String): String =
    Iterator
      .continually(ThreadLocalRandom.current().nextLong(1000000000000L, 10000000000000L).toString)
      .filter(id => !sql.contains(id))
      .next()

  /** @return
    *   the Jinja expressions of the SQL, without their whitespace
    */
  private def jinjaExpressions(sql: String): List[String] =
    "(?s)\\{\\{(.*?)\\}\\}".r.findAllMatchIn(sql).map(_.group(1).replaceAll("\\s", "")).toList

  def stripComments(sql: String): String = SqlCommentStripper.stripComments(sql).trim

  def temporaryTableName(tableName: String): String =
    "zztmp_" + tableName + "_" + UUID.randomUUID().toString.replace("-", "")

  def quoteCols(cols: List[String], quote: String): List[String] = {
    unquoteCols(cols, quote).map(col => s"${quote}$col${quote}")
  }

  def unquoteCols(cols: List[String], quote: String): List[String] = {
    cols.map { col =>
      if (quote.nonEmpty && col.startsWith(quote) && col.endsWith(quote))
        col.substring(1, col.length - 1)
      else
        col
    }
  }

  def unquoteAgressive(cols: List[String]): List[String] = {
    cols.map(col => unquoteAgressive(col))
  }

  def unquoteAgressive(col: String): String = {
    val quotes = List("\"", "'", "`")
    var result = col.trim
    quotes.foreach { quote =>
      if (result.startsWith(quote) && result.endsWith(quote)) {
        result = result.substring(1, result.length - 1)
      }
    }
    result
  }

  def targetColumnsForSelectSql(targetTableColumns: List[String], quote: String): String =
    quoteCols(unquoteCols(targetTableColumns, quote), quote).mkString(",")

  def incomingColumnsForSelectSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    unquoteCols(targetTableColumns, quote)
      .map(col => s"$incomingTable.$quote$col$quote")
      .mkString(",")

  def setForUpdateSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String =
    unquoteCols(targetTableColumns, quote)
      .map(col => s"$quote$col$quote = $incomingTable.$quote$col$quote")
      .mkString("SET ", ",", "")

  def mergeKeyJoinCondition(
    incomingTable: String,
    targetTable: String,
    columns: List[String],
    quote: String
  ): String =
    unquoteCols(columns, quote)
      .map(col => s"$incomingTable.$quote$col$quote = $targetTable.$quote$col$quote")
      .mkString(" AND ")

  def addSelectItem(
    statement: String,
    columnName: String,
    columnExpr: Option[String] = None
  ): String = {
    val select = CCJSqlParserUtil.parse(statement.trim).asInstanceOf[PlainSelect]
    columnExpr match {
      case Some(expr) =>
        val f = new Column(expr)
        val itemAdd = new SelectItem(f, new Alias(columnName, true))
        select.getSelectItems.add(itemAdd)
      case None =>
        val f = new Column(columnName)
        val itemAdd = new SelectItem(f)
        select.getSelectItems.add(itemAdd)
    }
    format(select.toString, JSQLFormatter.OutputFormat.PLAIN)
  }

  def deleteSelectItem(statement: String, columnName: String): String = {
    val select = CCJSqlParserUtil.parse(statement.trim).asInstanceOf[PlainSelect]
    val itemsToRemove = select.getSelectItems.asScala.filter { item =>
      item.getExpression() match {
        case col: Column => col.getColumnName == columnName
        case _ =>
          val alias = item.getAlias()
          alias != null && alias.getName == columnName
      }
    }
    itemsToRemove.foreach(select.getSelectItems.remove)
    format(select.toString, JSQLFormatter.OutputFormat.PLAIN)
  }

  def upsertSelectItem(
    statement: String,
    columnName: String,
    maybePreviousColumnName: Option[String],
    columnExpr: Option[String] = None
  ): String = {
    val select = CCJSqlParserUtil.parse(statement.trim).asInstanceOf[PlainSelect]
    val previousColumnName = maybePreviousColumnName.getOrElse(columnName)
    val items = select.getSelectItems
    val indexToUpdate = items.asScala.indexWhere { item =>
      item.getExpression() match {
        case col: Column => col.getColumnName == previousColumnName
        case _ =>
          val alias = item.getAlias()
          alias != null && alias.getName == previousColumnName
      }
    }
    indexToUpdate match {
      case index if index >= 0 =>
        columnExpr match {
          case Some(expr) =>
            val f = new Column(expr)
            val updatedItem = new SelectItem(f, new Alias(columnName, true))
            items.set(index, updatedItem)
          case None =>
            val f = new Column(columnName)
            val updatedItem = new SelectItem(f)
            items.set(index, updatedItem)
        }
      case _ =>
        addSelectItem(statement, columnName, columnExpr)
    }
    format(select.toString, JSQLFormatter.OutputFormat.PLAIN)
  }
}
