package ai.starlake.impact

import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLColumResolver
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData}
import net.sf.jsqlparser.expression.{CastExpression, Expression, LongValue}
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
    * operation. `main` is the outermost query, whose select list is the task output. `name` is the
    * CTE name or the FROM / JOIN subquery alias an enclosing query refers to it by.
    */
  private final case class SelectGroup(
    branches: List[PlainSelect],
    main: Boolean,
    name: Option[String]
  )

  private def withSelects(select: Select): List[(Option[String], Select)] =
    Option(select.getWithItemsList)
      .map(_.asScala.toList)
      .getOrElse(Nil)
      .flatMap(w =>
        w.getStatement match {
          case s: Select => List(Option(w.getAlias).map(_.getName).map(unquote) -> s)
          case _         => Nil
        }
      )

  private def fromSubqueries(p: PlainSelect): List[(Option[String], Select)] =
    (Option(p.getFromItem).toList ++
      Option(p.getJoins).map(_.asScala.toList.map(_.getFromItem)).getOrElse(Nil)).collect {
      case ps: ParenthesedSelect => Option(ps.getAlias).map(_.getName).map(unquote) -> ps
    }

  /** (branches defining the output of `select`, groups nested in CTEs and FROM / JOIN subqueries)
    */
  private def walk(select: Select): (List[PlainSelect], List[SelectGroup]) = {
    val withs = withSelects(select).flatMap { case (n, s) => groups(s, main = false, n) }
    select match {
      case ps: ParenthesedSelect =>
        val (top, nested) = walk(ps.getSelect)
        (top, withs ++ nested)
      case p: PlainSelect =>
        (List(p), withs ++ fromSubqueries(p).flatMap { case (n, s) => groups(s, main = false, n) })
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
  private def groups(
    select: Select,
    main: Boolean,
    name: Option[String] = None
  ): List[SelectGroup] = {
    val (top, nested) = walk(select)
    if (top.isEmpty) nested else SelectGroup(top, main, name) :: nested
  }

  private def parseSelect(sql: String): Try[Select] =
    Try(CCJSqlParserUtil.parse(sql.trim)).flatMap {
      case s: Select => Success(s)
      case other =>
        Failure(new IllegalArgumentException(s"not a SELECT: ${other.getClass.getSimpleName}"))
    }

  private def unquote(name: String): String =
    SQLUtils.unquoteAgressive(name).toLowerCase

  /** Name a select item is referenced by from an enclosing query: alias, else column name, looking
    * through casts (`CAST(x AS T)`, `x::T`, `TRY_CAST`, ...) since they keep the column name.
    */
  private def outputName(item: SelectItem[?]): Option[String] = {
    @tailrec
    def columnName(e: Expression): Option[String] = e match {
      case c: Column         => Option(c.getColumnName)
      case c: CastExpression => columnName(c.getLeftExpression)
      case _                 => None
    }
    Option(item.getAlias)
      .map(_.getName)
      .orElse(columnName(item.getExpression.asInstanceOf[Expression]))
      .map(unquote(_))
  }

  private val tokenRegex = "'(?:[^']|'')*'|\"[^\"]*\"|`[^`]*`|[A-Za-z_][A-Za-z0-9_]*|\\S".r

  /** Column references in an expression text as (qualifier, column), lowercased. String literals
    * and function names (a word followed by `(`) are skipped. Keywords and type names (`as`,
    * `date`, ...) are kept: they are also valid column names, and skipping one could lose a real
    * link.
    */
  private def columnRefs(text: String): List[(Option[String], String)] = {
    val tokens = tokenRegex.findAllIn(Option(text).getOrElse("")).toVector
    def word(t: String): Option[String] =
      if (t.length >= 2 && (t.head == '"' || t.head == '`')) Some(t.substring(1, t.length - 1))
      else if (t.head.isLetter || t.head == '_') Some(t)
      else None
    val refs = mutable.ListBuffer.empty[(Option[String], String)]
    var i = 0
    while (i < tokens.length) {
      word(tokens(i)) match {
        case Some(w) =>
          val parts = mutable.ListBuffer(w.toLowerCase)
          var j = i + 1
          while (j + 1 < tokens.length && tokens(j) == "." && word(tokens(j + 1)).isDefined) {
            parts += word(tokens(j + 1)).get.toLowerCase
            j += 2
          }
          if (!(j < tokens.length && tokens(j) == "("))
            refs += ((if (parts.size >= 2) Some(parts(parts.size - 2)) else None, parts.last))
          i = j
        case None => i += 1
      }
    }
    refs.toList
  }

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

  /** An inner select item an outer reference can resolve to. */
  private final case class InnerDef(
    relation: Option[String],
    name: String,
    text: String,
    expr: String
  )

  /** The parsed expression defining each resolved output column (its alias excluded), followed by
    * the text of every inner item (CTE body, FROM / JOIN subquery) it references, recursively. The
    * resolver tree drops wrappers such as CAST, EXTRACT and `::`, the parsed text keeps them. A
    * qualified reference (`a.col`) is looked up only in the CTE or subquery its qualifier names; an
    * unqualified one, or one whose qualifier is unknown, in every inner item with that name, which
    * can only over-report.
    */
  private def definitions(select: Select, names: List[String]): List[String] = {
    val all = groups(select, main = true)
    val mainItems =
      all.find(_.main).flatMap(_.branches.headOption).map(_.getSelectItems.asScala.toList)
    def exprText(item: SelectItem[?]): String =
      Option(item.getExpression).map(_.toString).getOrElse("")
    val inner: Vector[InnerDef] = all
      .filterNot(_.main)
      .flatMap { g =>
        val branchItems =
          g.branches.map(b => Option(b.getSelectItems).map(_.asScala.toList).getOrElse(Nil))
        branchItems.headOption.getOrElse(Nil).map(outputName).zipWithIndex.collect {
          case (Some(n), i) =>
            val items = branchItems.flatMap(_.lift(i))
            InnerDef(
              g.name,
              n,
              items.map(_.toString).mkString(" union "),
              items.map(exprText).mkString(" union ")
            )
        }
      }
      .toVector
    val aliases = tableAliases(select)
    def targets(ref: (Option[String], String)): Seq[Int] = {
      val (qualifier, column) = ref
      val byName = inner.indices.filter(i => inner(i).name == column)
      qualifier match {
        case None    => byName
        case Some(q) =>
          // the qualifier is a subquery alias or CTE name, or the alias of a table reference
          val relations =
            aliases.collect { case (a, parts) if a == q => parts.lastOption }.flatten.toSet + q
          val knownQualifier =
            inner.exists(_.relation.exists(relations.contains)) || aliases.exists(_._1 == q)
          if (knownQualifier) byName.filter(i => inner(i).relation.exists(relations.contains))
          else byName
      }
    }
    def expand(seedText: String, seedExpr: String): String = {
      val seen = mutable.Set.empty[Int]
      val found = mutable.ListBuffer.empty[String]
      var frontier = List(seedExpr)
      while (frontier.nonEmpty) {
        val next = frontier.flatMap(columnRefs).flatMap(targets).distinct.filterNot(seen.contains)
        seen ++= next
        found ++= next.map(inner(_).text)
        frontier = next.map(inner(_).expr)
      }
      (seedText :: found.toList).mkString(" ; ")
    }
    val items = mainItems.getOrElse(Nil)
    val positional = !items.exists(isStar) && items.size == names.size
    names.zipWithIndex.map { case (name, i) =>
      val seed =
        if (positional) Some(items(i))
        else items.find(it => outputName(it).contains(name))
      // the defining expression only: the item's own alias is the column name, kept out of the
      // fingerprint so a rename can be told apart from a new column
      seed.map(it => expand(exprText(it), exprText(it))).getOrElse(expand(name, name))
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
    // The whole normalized SQL stands in for the row fingerprint: with no column analysis, any
    // edit (a literal, a number, an operator) must still show up as a change.
    def unresolvedShape(reason: String): TaskShape =
      TaskShape.unresolved(reason, inputs, allIds).copy(rowFingerprint = normalize(sql))

    val analyzed = for {
      select <- parseSelect(sql)
      rowFp  <- rowLevel(sql)
    } yield (select, rowFp)

    analyzed match {
      case Failure(e) =>
        unresolvedShape(s"SQL parse failed: ${e.getMessage}")
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
            unresolvedShape(s"column resolution failed: ${e.getMessage}")
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
