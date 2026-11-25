package ai.starlake.dbt.warehouse

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, SetOperationList}
import net.sf.jsqlparser.statement.show.ShowTablesStatement
import net.sf.jsqlparser.statement.{
  DescribeStatement,
  ExplainStatement,
  ShowColumnsStatement,
  ShowStatement,
  Statement
}

import scala.jdk.CollectionConverters._
import scala.util.Try

/** What a dbt macro may run on the warehouse during the import: statements that only read */
object ReadOnlyGuard {

  def check(sql: String): Either[String, Unit] = {
    val statements = Try(CCJSqlParserUtil.parseStatements(sql).asScala.toList).toOption
    if (statements.exists(s => s.nonEmpty && s.forall(readOnly))) Right(())
    else Left(s"statement is not read-only: ${sql.trim.replaceAll("\\s*\\n\\s*", " ").take(80)}")
  }

  private def readOnly(st: Statement): Boolean = st match {
    case s: Select =>
      // a CTE can hold an INSERT, UPDATE or DELETE, and SELECT ... INTO writes a table
      Option(s.getWithItemsList).forall(
        _.asScala.forall(_.getParenthesedStatement.isInstanceOf[Select])
      ) && noInto(s)
    case _: ShowStatement | _: ShowTablesStatement | _: ShowColumnsStatement |
        _: DescribeStatement | _: ExplainStatement =>
      true
    case _ => false
  }

  private def noInto(select: Select): Boolean = select match {
    case p: PlainSelect =>
      p.getIntoTables == null && p.getIntoTempTable == null && p.getMySqlSelectIntoClause == null
    case u: SetOperationList => u.getSelects.asScala.forall(noInto)
    case _                   => true
  }
}
