package ai.starlake.dbt.warehouse

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.delete.{Delete, ParenthesedDelete}
import net.sf.jsqlparser.statement.insert.{Insert, InsertBulk, ParenthesedInsert}
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.select.{PlainSelect, Select, WithItem}
import net.sf.jsqlparser.statement.show.ShowTablesStatement
import net.sf.jsqlparser.statement.update.{ParenthesedUpdate, Update}
import net.sf.jsqlparser.statement.upsert.Upsert
import net.sf.jsqlparser.statement.{
  DescribeStatement,
  ExplainStatement,
  ShowColumnsStatement,
  ShowStatement,
  Statement
}
import net.sf.jsqlparser.util.TablesNamesFinder

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
    case s: Select => onlyReads(s)
    case _: ShowStatement | _: ShowTablesStatement | _: ShowColumnsStatement |
        _: DescribeStatement =>
      true
    // EXPLAIN ANALYZE executes what it explains
    case e: ExplainStatement => e.getStatement != null && readOnly(e.getStatement)
    case _                   => false
  }

  /** Walks the whole query, nested ones included: a data-modifying CTE or sub-statement or a SELECT
    * ... INTO anywhere refuses the statement, and so does anything the walk fails on.
    */
  private def onlyReads(select: Select): Boolean = {
    val walker = new WriteFinder
    Try(walker.getTables(select: Statement)).isSuccess && !walker.found
  }

  private class WriteFinder extends TablesNamesFinder[Void] {
    var found = false

    private def write: Void = { found = true; null }

    override def visit[S](delete: Delete, context: S): Void = write
    override def visit[S](delete: ParenthesedDelete, context: S): Void = write
    override def visit[S](update: Update, context: S): Void = write
    override def visit[S](update: ParenthesedUpdate, context: S): Void = write
    override def visit[S](insert: Insert, context: S): Void = write
    override def visit[S](insert: ParenthesedInsert, context: S): Void = write
    override def visit[S](insert: InsertBulk, context: S): Void = write
    override def visit[S](merge: Merge, context: S): Void = write
    override def visit[S](upsert: Upsert, context: S): Void = write

    override def visit[S](withItem: WithItem[_], context: S): Void = {
      if (!withItem.getParenthesedStatement.isInstanceOf[Select]) found = true
      super.visit(withItem, context)
    }

    override def visit[S](plain: PlainSelect, context: S): Void = {
      if (
        plain.getIntoTables != null || plain.getIntoTempTable != null ||
        plain.getMySqlSelectIntoClause != null
      ) found = true
      super.visit(plain, context)
    }
  }
}
