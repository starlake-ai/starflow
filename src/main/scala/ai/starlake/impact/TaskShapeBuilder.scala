package ai.starlake.impact

import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData}
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select.{
  AllColumns => JAllColumns,
  AllTableColumns,
  ParenthesedSelect,
  PlainSelect,
  Select,
  SelectItem,
  SetOperationList
}
import net.sf.jsqlparser.util.TablesNamesFinder

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object TaskShapeBuilder {

  private val identifierRegex = "[A-Za-z_][A-Za-z0-9_]*".r

  /** Lowercases and collapses whitespace outside single-quoted string literals. Literals are kept
    * verbatim, so `'FR'` and `'fr'` stay different. Both `''` and backslash escapes are honored
    * inside literals; misreading a backslash can only keep more text verbatim, which over-reports.
    */
  def normalize(text: String): String = {
    val s = Option(text).getOrElse("")
    val out = new StringBuilder
    var inLiteral = false
    var pendingSpace = false
    var i = 0
    while (i < s.length) {
      val ch = s.charAt(i)
      if (inLiteral) {
        out.append(ch)
        if (ch == '\\' && i + 1 < s.length) {
          out.append(s.charAt(i + 1))
          i += 1
        } else if (ch == '\'') {
          if (i + 1 < s.length && s.charAt(i + 1) == '\'') {
            out.append('\'')
            i += 1
          } else inLiteral = false
        }
      } else if (Character.isWhitespace(ch)) {
        pendingSpace = true
      } else {
        if (pendingSpace && out.nonEmpty) out.append(' ')
        pendingSpace = false
        if (ch == '\'') inLiteral = true
        out.append(Character.toLowerCase(ch))
      }
      i += 1
    }
    out.toString
  }

  def identifiers(text: String): Set[String] =
    identifierRegex.findAllIn(Option(text).getOrElse("").toLowerCase).toSet

  /** PlainSelects sharing one output column list: a single SELECT, or every branch of a set
    * operation. `main` is the outermost query, whose select list is the task output.
    */
  private final case class SelectGroup(branches: List[PlainSelect], main: Boolean)

  private def withSelects(select: Select): List[Select] =
    Option(select.getWithItemsList)
      .map(_.asScala.toList)
      .getOrElse(Nil)
      .flatMap(w =>
        w.getStatement match {
          case s: Select => List(s)
          case _         => Nil
        }
      )

  private def fromSubqueries(p: PlainSelect): List[Select] =
    (Option(p.getFromItem).toList ++
      Option(p.getJoins).map(_.asScala.toList.map(_.getFromItem)).getOrElse(Nil)).collect {
      case ps: ParenthesedSelect => ps
    }

  /** (branches defining the output of `select`, groups nested in CTEs and FROM / JOIN subqueries)
    */
  private def walk(select: Select): (List[PlainSelect], List[SelectGroup]) = {
    val withs = withSelects(select).flatMap(groups(_, main = false))
    select match {
      case ps: ParenthesedSelect =>
        val (top, nested) = walk(ps.getSelect)
        (top, withs ++ nested)
      case p: PlainSelect =>
        (List(p), withs ++ fromSubqueries(p).flatMap(groups(_, main = false)))
      case s: SetOperationList =>
        val parts = s.getSelects.asScala.toList.map(walk)
        (parts.flatMap(_._1), withs ++ parts.flatMap(_._2))
      case _ => (Nil, withs)
    }
  }

  /** Every group whose SELECT list defines columns: the main one, CTE bodies, set operation
    * branches and subqueries in FROM / JOIN. Subqueries in WHERE are left alone on purpose: their
    * select list filters rows, so it belongs to the row-level part.
    */
  private def groups(select: Select, main: Boolean): List[SelectGroup] = {
    val (top, nested) = walk(select)
    if (top.isEmpty) nested else SelectGroup(top, main) :: nested
  }

  private def parseSelect(sql: String): Try[Select] =
    Try(CCJSqlParserUtil.parse(sql.trim)).flatMap {
      case s: Select => Success(s)
      case other =>
        Failure(new IllegalArgumentException(s"not a SELECT: ${other.getClass.getSimpleName}"))
    }

  private def unquote(name: String): String =
    SQLUtils.unquoteAgressive(name).toLowerCase

  /** Name a select item is referenced by from an enclosing query: alias, else column name. */
  private def outputName(item: SelectItem[?]): Option[String] =
    Option(item.getAlias)
      .map(_.getName)
      .orElse(item.getExpression match {
        case c: Column => Option(c.getColumnName)
        case _         => None
      })
      .map(unquote)

  /** The SQL with the main select list blanked. Inner select lists (CTE bodies, FROM / JOIN
    * subqueries) keep the items an enclosing query uses in a row-level clause, iterated to a
    * fixpoint since a kept item can reference columns of another CTE. Branches of an inner set
    * operation after the first are kept as written: the column resolver follows only the first
    * branch, so their changes must surface somewhere.
    */
  private def rowLevel(sql: String): Try[String] = {
    def render(keep: Set[String]): Try[String] = parseSelect(sql).map { select =>
      groups(select, main = true).foreach { g =>
        val keptIdx: Set[Int] =
          if (g.main) Set.empty
          else
            g.branches.head.getSelectItems.asScala.toList
              .map(outputName)
              .zipWithIndex
              .collect {
                case (Some(n), i) if keep.contains(n) => i
              }
              .toSet
        val blanked = if (g.main) g.branches else g.branches.take(1)
        blanked.foreach { p =>
          val kept = p.getSelectItems.asScala.toList.zipWithIndex.collect {
            case (item, i) if keptIdx.contains(i) => item
          }
          val items: List[SelectItem[?]] =
            if (kept.isEmpty) List(SelectItem.from(new LongValue(1))) else kept
          p.setSelectItems(items.asJava)
        }
      }
      normalize(select.toString)
    }
    @tailrec
    def fixpoint(keep: Set[String], remaining: Int): Try[String] =
      render(keep) match {
        case Success(fp) =>
          val ids = identifiers(fp)
          if (ids == keep || remaining == 0) Success(fp) else fixpoint(ids, remaining - 1)
        case failure => failure
      }
    fixpoint(Set.empty, 100)
  }

  private def hasStar(select: Select): Boolean =
    groups(select, main = true).exists(_.branches.exists(_.getSelectItems.asScala.exists(isStar)))

  private def hasInnerSetOperation(select: Select): Boolean =
    groups(select, main = true).exists(g => !g.main && g.branches.size > 1)

  /** Every table reference of the statement: (lowercased alias, else table name, name parts). */
  private def tableAliases(select: Select): List[(String, List[String])] = {
    val found = mutable.ListBuffer.empty[(String, List[String])]
    val finder = new TablesNamesFinder[Void]() {
      override def visit[S](table: Table, context: S): Void = {
        val parts = Option(table.getFullyQualifiedName).toList
          .flatMap(_.split('.').toList)
          .map(unquote)
        val alias = Option(table.getAlias).map(_.getName).map(unquote)
        alias.orElse(parts.lastOption).foreach(a => found += (a -> parts))
        super.visit(table, context)
      }
    }
    Try(finder.getTables(select: net.sf.jsqlparser.statement.Statement))
    found.toList
  }

  private def fingerprint(c: JdbcColumn): String = {
    val self = Option(c.getExpression).map(_.toString).getOrElse(s"${c.tableName}.${c.columnName}")
    val children = c.getChildren.asScala.map(fingerprint)
    if (children.isEmpty) self else s"$self(${children.mkString(",")})"
  }

  private def isStar(item: SelectItem[?]): Boolean = item.getExpression match {
    case _: JAllColumns     => true
    case _: AllTableColumns => true
    case _                  => false
  }

  /** The parsed select-item text defining each resolved output column, followed by the text of
    * every inner item (CTE body, FROM / JOIN subquery) it references by output name, recursively.
    * The resolver tree drops wrappers such as CAST, EXTRACT and `::`, the parsed text keeps them.
    * References are matched by identifier, not by scope: a name collision only adds text, which
    * over-reports.
    */
  private def definitions(select: Select, names: List[String]): List[String] = {
    val all = groups(select, main = true)
    val mainItems =
      all.find(_.main).flatMap(_.branches.headOption).map(_.getSelectItems.asScala.toList)
    val inner: List[(String, String)] = all.filterNot(_.main).flatMap { g =>
      val branchItems =
        g.branches.map(b => Option(b.getSelectItems).map(_.asScala.toList).getOrElse(Nil))
      branchItems.headOption.getOrElse(Nil).map(outputName).zipWithIndex.collect {
        case (Some(n), i) => n -> branchItems.flatMap(_.lift(i)).map(_.toString).mkString(" union ")
      }
    }
    def expand(seed: String): String = {
      val seen = mutable.Set.empty[Int]
      val found = mutable.ListBuffer.empty[String]
      var frontier = List(seed)
      while (frontier.nonEmpty) {
        val ids = frontier.flatMap(identifiers).toSet
        val next = inner.zipWithIndex.collect {
          case ((n, text), i) if ids.contains(n) && !seen.contains(i) =>
            seen += i
            text
        }
        found ++= next
        frontier = next
      }
      (seed :: found.toList).mkString(" ; ")
    }
    val items = mainItems.getOrElse(Nil)
    val positional = !items.exists(isStar) && items.size == names.size
    names.zipWithIndex.map { case (name, i) =>
      val seed =
        if (positional) items(i).toString
        else items.find(it => outputName(it).contains(name)).map(_.toString).getOrElse(name)
      expand(seed)
    }
  }

  private def nonEmpty(s: String): Option[String] = Option(s).map(_.trim).filter(_.nonEmpty)

  /** (catalog, schema, table) as written, lowercased, missing parts empty */
  private final case class TableKey(catalog: String, schema: String, table: String)

  private def keyOf(parts: List[String]): TableKey =
    parts.map(_.toLowerCase).takeRight(3) match {
      case c :: s :: t :: Nil => TableKey(c, s, t)
      case s :: t :: Nil      => TableKey("", s, t)
      case t :: Nil           => TableKey("", "", t)
      case _                  => TableKey("", "", "")
    }

  private def lenientEq(a: String, b: String): Boolean = a == b || a.isEmpty || b.isEmpty

  def build(
    sql: String,
    resolveName: String => NodeRef,
    knownColumns: NodeRef => Option[List[String]]
  ): TaskShape = {
    val allIds = identifiers(sql)
    val raw: List[(TableKey, NodeRef)] =
      SQLUtils.extractInputTableNamesWithFallback(sql).map { name =>
        (keyOf(name.split('.').toList), resolveName(name))
      }
    val inputs = raw.map(_._2).toSet

    val analyzed = for {
      select <- parseSelect(sql)
      rowFp  <- rowLevel(sql)
    } yield (select, rowFp)

    analyzed match {
      case Failure(e) =>
        TaskShape.unresolved(s"SQL parse failed: ${e.getMessage}", inputs, allIds)
      case Success((select, rowFp)) =>
        val unknownInputs = raw.collect {
          case (_, node) if knownColumns(node).isEmpty => node -> "columns unknown offline"
        }.toMap
        val metadata = new JdbcMetaData("", "")
        raw.foreach { case (key, node) =>
          knownColumns(node).foreach { cols =>
            metadata.addTable(
              key.catalog,
              key.schema,
              key.table,
              cols.map(c => new JdbcColumn(c)).asJava
            )
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
            def sourceOf(catalog: String, schema: String, table: String): Option[NodeRef] = {
              val t = Option(table).getOrElse("").toLowerCase
              val s = Option(schema).getOrElse("").toLowerCase
              val c = Option(catalog).getOrElse("").toLowerCase
              raw.collectFirst {
                case (k, node)
                    if k.table == t && lenientEq(k.schema, s) && lenientEq(k.catalog, c) =>
                  node
              }
            }
            def own(c: JdbcColumn): Set[SourceCol] = {
              val viaTable = for {
                t   <- nonEmpty(c.tableName)
                col <- nonEmpty(c.columnName)
                n   <- sourceOf(c.tableCatalog, c.tableSchema, t)
              } yield SourceCol(n, col.toLowerCase)
              val viaScope = for {
                t   <- nonEmpty(c.scopeTable)
                col <- nonEmpty(c.scopeColumn).orElse(nonEmpty(c.columnName))
                n   <- sourceOf(c.scopeCatalog, c.scopeSchema, t)
              } yield SourceCol(n, col.toLowerCase)
              viaTable.toSet ++ viaScope.toSet
            }
            def sources(c: JdbcColumn): Set[SourceCol] =
              own(c) ++ c.getChildren.asScala.flatMap(sources)

            // A column reference the resolver could not tie to an input (stale metadata, an
            // unqualified unknown column). It is fine when its qualifier is an input whose
            // columns are unknown: that input is already table level.
            val aliases = tableAliases(select)
            def coveredByUnknownInput(qualifier: String): Boolean =
              aliases.exists { case (alias, parts) =>
                alias == qualifier && raw.exists { case (k, node) =>
                  k == keyOf(parts) && unknownInputs.contains(node)
                }
              }
            def unattributed(c: JdbcColumn): List[String] = {
              val children = c.getChildren.asScala.toList
              if (children.nonEmpty) children.flatMap(unattributed)
              else {
                val isColumnRef = c.getExpression == null || c.getExpression.isInstanceOf[Column]
                val qualifier = nonEmpty(c.tableName).orElse(nonEmpty(c.scopeTable)).map(unquote)
                if (isColumnRef && own(c).isEmpty && !qualifier.exists(coveredByUnknownInput))
                  nonEmpty(c.columnName).orElse(nonEmpty(c.scopeColumn)).toList
                else Nil
              }
            }

            val resolvedCols = res.getColumns.asScala.toList
            val names = resolvedCols.zip(res.getLabels.asScala.toList).map { case (col, label) =>
              nonEmpty(label).getOrElse(col.columnName).toLowerCase
            }
            val defs = definitions(select, names)
            val columns = resolvedCols.zip(names).zip(defs).map { case ((col, name), definition) =>
              ColumnDef(name, normalize(s"${fingerprint(col)} := $definition"), sources(col))
            }
            val setOpInputs =
              if (hasInnerSetOperation(select))
                inputs
                  .map(
                    _ -> "set operation inside a CTE or subquery: column lineage follows its first branch only"
                  )
                  .toMap
              else Map.empty[NodeRef, String]
            val unattributedInputs = resolvedCols.flatMap(unattributed).headOption match {
              case Some(name) =>
                inputs.map(_ -> s"column $name could not be attributed to an input").toMap
              case None => Map.empty[NodeRef, String]
            }
            TaskShape(
              inputs = inputs,
              columns = columns,
              starInputs = if (hasStar(select)) inputs else Set.empty,
              rowFingerprint = rowFp,
              rowIdentifiers = identifiers(rowFp),
              allIdentifiers = allIds,
              // later maps win on a shared key: an input keeps its earlier, more specific reason
              unresolvedInputs = unattributedInputs ++ setOpInputs ++ unknownInputs,
              unresolved = None
            )
        }
    }
  }
}
