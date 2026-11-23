package ai.starlake.impact

import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData}
import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.{
  AllColumns => JAllColumns,
  AllTableColumns,
  ParenthesedSelect,
  PlainSelect,
  Select,
  SelectItem,
  SetOperationList
}

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object TaskShapeBuilder extends LazyLogging {

  private val identifierRegex = "[A-Za-z_][A-Za-z0-9_]*".r

  def normalize(text: String): String =
    Option(text).getOrElse("").toLowerCase.replaceAll("\\s+", " ").trim

  def identifiers(text: String): Set[String] =
    identifierRegex.findAllIn(Option(text).getOrElse("").toLowerCase).toSet

  /** Every PlainSelect whose SELECT list defines columns: the main one, CTE bodies, set operation
    * branches and subqueries in FROM / JOIN. Subqueries in WHERE are left alone on purpose: their
    * select list filters rows, so it belongs to the row-level part.
    */
  private def definingSelects(select: Select): List[PlainSelect] = {
    val withs = Option(select.getWithItemsList).map(_.asScala.toList).getOrElse(Nil).flatMap {
      w =>
        w.getStatement match {
          case ps: ParenthesedSelect => definingSelects(ps)
          case _                     => Nil
        }
    }
    val own = select match {
      case p: PlainSelect =>
        val fromItems =
          Option(p.getFromItem).toList ++
          Option(p.getJoins).map(_.asScala.toList.map(_.getFromItem)).getOrElse(Nil)
        p :: fromItems.collect { case ps: ParenthesedSelect => definingSelects(ps) }.flatten
      case s: SetOperationList  => s.getSelects.asScala.toList.flatMap(definingSelects)
      case ps: ParenthesedSelect => definingSelects(ps.getSelect)
      case _                     => Nil
    }
    withs ++ own
  }

  private def parseSelect(sql: String): Try[Select] =
    Try(CCJSqlParserUtil.parse(sql.trim)).flatMap {
      case s: Select => Success(s)
      case other =>
        Failure(new IllegalArgumentException(s"not a SELECT: ${other.getClass.getSimpleName}"))
    }

  private def rowLevel(sql: String): Try[String] = parseSelect(sql).map { select =>
    definingSelects(select).foreach { p =>
      p.setSelectItems(List[SelectItem[?]](SelectItem.from(new LongValue(1))).asJava)
    }
    normalize(select.toString)
  }

  private def hasStar(sql: String): Boolean =
    parseSelect(sql).toOption.exists { select =>
      definingSelects(select).exists { p =>
        p.getSelectItems.asScala.exists(_.getExpression match {
          case _: JAllColumns     => true
          case _: AllTableColumns => true
          case _                  => false
        })
      }
    }

  private def fingerprint(c: JdbcColumn): String = {
    val self = Option(c.getExpression).map(_.toString).getOrElse(s"${c.tableName}.${c.columnName}")
    val children = c.getChildren.asScala.map(fingerprint)
    if (children.isEmpty) self else s"$self(${children.mkString(",")})"
  }

  private def nonEmpty(s: String): Option[String] = Option(s).map(_.trim).filter(_.nonEmpty)

  def build(
    sql: String,
    resolveName: String => NodeRef,
    knownColumns: NodeRef => Option[List[String]]
  ): TaskShape = {
    val allIds = identifiers(sql)
    val rawNames: List[String] =
      Try {
        val ctes = SQLUtils.extractCTENames(sql).map(_.toLowerCase).toSet
        SQLUtils.extractTableNames(sql).filterNot(n => ctes.contains(n.toLowerCase))
      }.getOrElse(SQLUtils.extractTableNamesUsingRegEx(sql))
    // (schema part as written, table part as written) -> project node
    val raw: List[((String, String), NodeRef)] = rawNames.map { name =>
      val parts = name.split('.').toList
      val schema = if (parts.size >= 2) parts(parts.size - 2) else ""
      ((schema.toLowerCase, parts.last.toLowerCase), resolveName(name))
    }
    val inputs = raw.map(_._2).toSet

    rowLevel(sql) match {
      case Failure(e) =>
        TaskShape.unresolved(s"SQL parse failed: ${e.getMessage}", inputs, allIds)
      case Success(rowFp) =>
        val unresolvedInputs = raw.collect {
          case (_, node) if knownColumns(node).isEmpty => node -> "columns unknown offline"
        }.toMap
        val metadata = new JdbcMetaData("", "")
        raw.foreach { case ((schema, table), node) =>
          knownColumns(node).foreach { cols =>
            metadata.addTable("", schema, table, cols.map(c => new JdbcColumn(c)).asJava)
          }
        }
        val resolved = Try(
          JSQLColumResolver.getResultSetMetaData(
            sql,
            JdbcMetaData.copyOf(metadata.setErrorMode(JdbcMetaData.ErrorMode.LENIENT))
          )
        )
        resolved match {
          case Failure(e) =>
            TaskShape.unresolved(s"column resolution failed: ${e.getMessage}", inputs, allIds)
          case Success(res) =>
            def sourceOf(schema: String, table: String): Option[NodeRef] = {
              val t = Option(table).getOrElse("").toLowerCase
              val s = Option(schema).getOrElse("").toLowerCase
              raw
                .find { case ((rs, rt), _) => rt == t && (rs == s || s.isEmpty || rs.isEmpty) }
                .map(_._2)
            }
            def sources(c: JdbcColumn): Set[SourceCol] = {
              val viaTable = for {
                t   <- nonEmpty(c.tableName)
                col <- nonEmpty(c.columnName)
                n   <- sourceOf(c.tableSchema, t)
              } yield SourceCol(n, col.toLowerCase)
              val viaScope = for {
                t   <- nonEmpty(c.scopeTable)
                col <- nonEmpty(c.scopeColumn).orElse(nonEmpty(c.columnName))
                n   <- sourceOf(c.scopeSchema, t)
              } yield SourceCol(n, col.toLowerCase)
              (viaTable.toSet ++ viaScope.toSet) ++ c.getChildren.asScala.flatMap(sources)
            }
            val columns = res.getColumns.asScala.toList.zip(res.getLabels.asScala.toList).map {
              case (col, label) =>
                val name = nonEmpty(label).getOrElse(col.columnName).toLowerCase
                ColumnDef(name, normalize(fingerprint(col)), sources(col))
            }
            TaskShape(
              inputs = inputs,
              columns = columns,
              starInputs = if (hasStar(sql)) inputs else Set.empty,
              rowFingerprint = rowFp,
              rowIdentifiers = identifiers(rowFp),
              allIdentifiers = allIds,
              unresolvedInputs = unresolvedInputs,
              unresolved = None
            )
        }
    }
  }
}
