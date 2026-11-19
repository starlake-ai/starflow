package ai.starlake.lineage

import net.sf.jsqlparser.expression.{CastExpression, ColumnsTransformer, Expression}
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList
import net.sf.jsqlparser.parser.{CCJSqlParser, CCJSqlParserUtil}
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select.*

import java.util.Locale
import java.util.function.Consumer
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** The origin of each column of a SELECT: the columns of the tables of the project that it copies,
  * as they are, renamed or cast, through common table expressions, subqueries, joins, `*` and the
  * branches of a set operation, or a value computed from them (#1948).
  *
  * The SQL is parsed, not run: the columns of a table come from the catalog, that is from the files
  * of the project.
  */
object SelectColumnOrigins {

  /** A table of the project, lower-cased */
  final case class TableRef(domain: String, table: String) {
    override def toString: String = s"$domain.$table"
  }

  /** A column of a table of the project, lower-cased */
  final case class ColumnRef(table: TableRef, column: String) {
    override def toString: String = s"$table.$column"
  }

  sealed trait Origin

  /** A value computed from columns, or read from a table or a column that is not known */
  case object Computed extends Origin

  /** A copy, renamed or cast, of these columns */
  final case class Copied(columns: List[ColumnRef]) extends Origin

  /** The branches of a UNION, each with the tables that it reads */
  final case class Branches(branches: List[Branch]) extends Origin

  final case class Branch(origin: Origin, reads: Set[TableRef])

  /** The tables of the project, as a SELECT names them */
  trait Catalog {

    /** The table of the project with these name parts, unquoted, without the database */
    def resolve(nameParts: List[String]): Option[TableRef]

    /** The columns of a table of the project, or None when they are not known */
    def columns(table: TableRef): Option[List[String]]

    /** Whether the columns of the table are all known, so that `*` gives all of them */
    def isComplete(table: TableRef): Boolean = true
  }

  /** @return
    *   the columns of the SELECT, in order, with their origin: a name given to several columns is
    *   computed, since it cannot tell them apart
    */
  def of(
    sql: String,
    catalog: Catalog,
    castKeepsName: Boolean = false
  ): Try[List[(String, Origin)]] =
    withCompleteness(sql, catalog, castKeepsName).map(_._1)

  /** @return
    *   the columns of the SELECT, as `of` gives them, and whether they are all known
    * @param castKeepsName
    *   whether the engine names a column cast without an alias after the column, as PostgreSQL,
    *   Redshift and Spark do
    */
  def withCompleteness(
    sql: String,
    catalog: Catalog,
    castKeepsName: Boolean
  ): Try[(List[(String, Origin)], Boolean)] =
    Try(parse(sql)).map {
      case select: Select =>
        val projection = new Resolver(catalog, castKeepsName).projection(select, Map.empty)
        val counts = projection.columns.groupBy(_._1).view.mapValues(_.size).toMap
        projection.columns.map { case (name, origin) =>
          name -> (if (counts(name) > 1) Computed else origin)
        } -> projection.complete
      case statement =>
        throw new IllegalArgumentException(s"Not a SELECT: ${statement.getClass.getSimpleName}")
    }

  /** Parsed without the error log of SQLUtils.jsqlParse: the caller reports the failure */
  private def parse(sql: String) =
    CCJSqlParserUtil.parse(
      sql.trim,
      new Consumer[CCJSqlParser] {
        override def accept(parser: CCJSqlParser): Unit = parser.withTimeOut(60 * 1000)
      }
    )

  private[lineage] def unquote(name: String): String =
    Option(name).getOrElse("").trim.filterNot("`\"[]".contains(_))

  private[lineage] def lower(name: String): String = unquote(name).toLowerCase(Locale.ROOT)

  /** The parts of a table name, unquoted: `project.dataset.table` in one quoted name included */
  private[lineage] def nameParts(table: Table): List[String] =
    Option(table)
      .flatMap(table => Option(table.getFullyQualifiedName))
      .map(unquote(_).split('.').toList.map(_.trim).filter(_.nonEmpty))
      .getOrElse(Nil)

  private type Columns = List[(String, Origin)]

  /** The columns of a SELECT, the tables that it reads, and whether its columns are all known: a
    * `*` on a table whose columns are not known leaves some of them out
    */
  private final case class Projection(columns: Columns, reads: Set[TableRef], complete: Boolean)

  /** A table, a common table expression or a subquery of a FROM clause */
  private final case class Source(
    alias: Option[String],
    nameParts: List[String],
    columns: Option[Columns],
    reads: Set[TableRef],
    table: Option[TableRef],
    complete: Boolean
  ) {
    def isNamed(qualifier: List[String]): Boolean =
      alias match {
        case Some(name) => qualifier == List(name)
        case None       => qualifier.nonEmpty && nameParts.endsWith(qualifier)
      }

    def knows(column: String): Boolean = columns.exists(_.exists(_._1 == column))

    def originOf(column: String): Origin =
      columns
        .flatMap(_.find(_._1 == column).map(_._2))
        // A column of a table that its YAML does not declare is still a column of the table
        .orElse(table.map(ref => Copied(List(ColumnRef(ref, column)))))
        .getOrElse(Computed)
  }

  private final class Resolver(catalog: Catalog, castKeepsName: Boolean) {

    def projection(select: Select, scope: Map[String, Projection]): Projection = {
      val withScope = commonTableExpressions(select, scope)
      select match {
        case parenthesed: ParenthesedSelect => projection(parenthesed.getSelect, withScope)
        case plain: PlainSelect =>
          val sources = this.sources(plain, withScope)
          val (columns, complete) = selectItems(plain, sources)
          Projection(columns, sources.flatMap(_.reads).toSet, complete)
        case setOperation: SetOperationList => setOperationProjection(setOperation, withScope)
        case _                              => Projection(Nil, Set.empty, complete = false)
      }
    }

    private def setOperationProjection(
      setOperation: SetOperationList,
      scope: Map[String, Projection]
    ): Projection = {
      val branches = Option(setOperation.getSelects).map(_.asScala.toList).getOrElse(Nil).map {
        projection(_, scope)
      }
      val operations = Option(setOperation.getOperations).map(_.asScala.toList).getOrElse(Nil)
      val byName = operations.exists { operation =>
        val text = operation.toString.toUpperCase(Locale.ROOT)
        text.contains("BY NAME") || text.contains("CORRESPONDING")
      }
      val reads = branches.flatMap(_.reads).toSet
      branches match {
        case Nil => Projection(Nil, reads, complete = false)
        // The rows of an EXCEPT come from its first branch
        case first :: _
            if !byName && operations.nonEmpty && operations.forall(operation =>
              operation.isInstanceOf[ExceptOp] || operation.isInstanceOf[MinusOp]
            ) =>
          first.copy(reads = reads)
        case first :: _
            if !byName && operations.forall(_.isInstanceOf[UnionOp]) && branches
              .forall(branch => branch.complete && branch.columns.size == first.columns.size) =>
          val columns = first.columns.indices.toList.map { index =>
            first.columns(index)._1 -> (Branches(branches.map { branch =>
              Branch(branch.columns(index)._2, branch.reads)
            }): Origin)
          }
          Projection(columns, reads, complete = true)
        // Matched by name, mixed, or with columns that are not all known
        case first :: _ =>
          Projection(
            first.columns.map { case (name, _) => name -> Computed },
            reads,
            first.complete
          )
      }
    }

    private def commonTableExpressions(
      select: Select,
      scope: Map[String, Projection]
    ): Map[String, Projection] = {
      val items = Option(select.getWithItemsList).map(_.asScala.toList).getOrElse(Nil)
      // WITH RECURSIVE marks its first item only
      val recursive = items.headOption.exists(_.isRecursive)
      items.foldLeft(scope) { case (known, item) =>
        val name = lower(item.getAliasName)
        // A recursive common table expression reads itself: its columns are not known there
        val bodyScope =
          if (recursive && name.nonEmpty)
            known + (name -> Projection(Nil, Set.empty, complete = false))
          else known
        // The body of a common table expression may be a DML statement, which is not a SELECT
        val body = Try(Option(item.getSelect)).toOption.flatten
          .map(projection(_, bodyScope))
          .getOrElse(Projection(Nil, Set.empty, complete = false))
        val names =
          Option(item.getWithItemList).map(_.asScala.toList).getOrElse(Nil).map { column =>
            lower(column.toString)
          }
        val renamed =
          if (names.isEmpty) body
          else if (body.complete && names.size == body.columns.size)
            body.copy(columns = names.zip(body.columns).map { case (name, (_, origin)) =>
              name -> origin
            })
          else Projection(names.map(_ -> Computed), body.reads, complete = true)
        if (name.isEmpty) known else known + (name -> renamed)
      }
    }

    private def sources(select: PlainSelect, scope: Map[String, Projection]): List[Source] = {
      val joins = Option(select.getJoins).map(_.asScala.toList).getOrElse(Nil)
      (Option(select.getFromItem).toList ++ joins.flatMap(join => Option(join.getFromItem)))
        .flatMap(fromItem(_, scope))
    }

    private def fromItem(item: FromItem, scope: Map[String, Projection]): List[Source] = {
      val alias = Option(item.getAlias).map(_.getName).map(lower).filter(_.nonEmpty)
      val sources = item match {
        case table: Table =>
          val parts = nameParts(table).map(lower)
          parts match {
            case List(name) if scope.contains(name) =>
              val known = scope(name)
              List(Source(alias, parts, Some(known.columns), known.reads, None, known.complete))
            case _ =>
              catalog.resolve(parts) match {
                case Some(ref) =>
                  val columns = catalog
                    .columns(ref)
                    .map(_.map { column =>
                      lower(column) -> (Copied(List(ColumnRef(ref, lower(column)))): Origin)
                    })
                  val complete = columns.isDefined && catalog.isComplete(ref)
                  List(Source(alias, parts, columns, Set(ref), Some(ref), complete))
                case None => List(Source(alias, parts, None, Set.empty, None, complete = false))
              }
          }
        case select: ParenthesedSelect =>
          val known = projection(select, scope)
          List(Source(alias, Nil, Some(known.columns), known.reads, None, known.complete))
        case parenthesed: ParenthesedFromItem =>
          val joins = Option(parenthesed.getJoins).map(_.asScala.toList).getOrElse(Nil)
          (Option(parenthesed.getFromItem).toList ++ joins.flatMap(join =>
            Option(join.getFromItem)
          )).flatMap(fromItem(_, scope))
        // UNNEST, a table function...: columns not known
        case _ => List(Source(alias, Nil, None, Set.empty, None, complete = false))
      }
      // A column list after the alias renames the columns by position
      val aliasColumns = Option(item.getAlias)
        .flatMap(alias => Option(alias.getAliasColumns))
        .map(_.asScala.toList.map(column => lower(column.name)))
        .getOrElse(Nil)
      (aliasColumns, sources) match {
        case (Nil, _) => sources
        case (names, List(source))
            if source.complete && source.columns.exists(_.size == names.size) =>
          List(
            source.copy(columns = source.columns.map(names.zip(_).map { case (name, (_, origin)) =>
              name -> origin
            }))
          )
        case (names, _) =>
          List(
            Source(
              alias,
              Nil,
              Some(names.map(_ -> Computed)),
              sources.flatMap(_.reads).toSet,
              None,
              complete = true
            )
          )
      }
    }

    /** @return
      *   the columns of the SELECT, and whether they are all known
      */
    private def selectItems(select: PlainSelect, sources: List[Source]): (Columns, Boolean) = {
      val items = Option(select.getSelectItems).map(_.asScala.toList).getOrElse(Nil).map { item =>
        item.getExpression.asInstanceOf[Expression] match {
          case all: AllTableColumns =>
            val qualifier = nameParts(all.getTable).map(lower)
            expand(sources.filter(_.isNamed(qualifier)), all)
          case all: AllColumns => expand(sources, all)
          case expression =>
            val origin = unwrap(expression) match {
              case column: Column if isColumn(column) => columnOrigin(column, sources)
              case _                                  => Computed
            }
            // Only a column keeps its name, in parentheses or, on some engines, cast: the engine
            // names an expression
            val name = Option(item.getAlias).map(_.getName).map(lower).filter(_.nonEmpty).orElse {
              withoutParentheses(expression) match {
                case column: Column if isColumn(column) => Some(lower(column.getColumnName))
                case cast: CastExpression if castKeepsName =>
                  unwrap(cast) match {
                    case column: Column if isColumn(column) => Some(lower(column.getColumnName))
                    case _                                  => None
                  }
                case _ => None
              }
            }
            (List(name.getOrElse(expression.toString) -> origin), true)
        }
      }
      (items.flatMap(_._1), items.forall(_._2))
    }

    /** A column, not an element of an array or a map */
    private def isColumn(column: Column): Boolean = column.getArrayConstructor == null

    /** The columns of `*` or `t.*`, without its EXCEPT (or EXCLUDE) columns, computed for its
      * REPLACE ones, and under their new name for its RENAME ones. An APPLY computes them all, and
      * an EXCEPT pattern leaves them unknown.
      */
    private def expand(sources: List[Source], all: AllColumns): (Columns, Boolean) = {
      import ColumnsTransformer.ColumnsTransformerType
      val transformers = Option(all.getTransformers).map(_.asScala.toList).getOrElse(Nil)
      def ofType(types: ColumnsTransformerType*) =
        transformers.filter(t => types.contains(t.getType))
      val excepts = ofType(ColumnsTransformerType.EXCEPT, ColumnsTransformerType.EXCLUDE)
      val except = excepts.flatMap { t =>
        Option(t.getExceptColumns).map(_.asScala.toList).getOrElse(Nil).map { c =>
          lower(c.getColumnName)
        }
      }
      def aliased(
        t: ColumnsTransformer,
        items: ColumnsTransformer => java.util.List[SelectItem[_]]
      ) =
        Option(items(t)).map(_.asScala.toList).getOrElse(Nil)
      val replaced = ofType(ColumnsTransformerType.REPLACE).flatMap { t =>
        aliased(t, _.getReplaceItems).flatMap(item =>
          Option(item.getAlias).map(_.getName).map(lower)
        )
      }
      val renamed: Map[String, String] = ofType(ColumnsTransformerType.RENAME).flatMap { t =>
        aliased(t, _.getRenameItems).flatMap { item =>
          (item.getExpression, Option(item.getAlias)) match {
            case (column: Column, Some(alias)) =>
              Some(lower(column.getColumnName) -> lower(alias.getName))
            case _ => None
          }
        }
      }.toMap
      val applied = ofType(ColumnsTransformerType.APPLY).nonEmpty
      val columns = sources.flatMap(_.columns.getOrElse(Nil)).filterNot { case (name, _) =>
        except.contains(name)
      } map { case (name, origin) =>
        renamed.getOrElse(name, name) ->
        (if (applied || replaced.contains(name)) Computed else origin)
      }
      val knownColumns = excepts.forall(_.getExceptPattern == null)
      (columns, knownColumns && sources.nonEmpty && sources.forall(_.complete))
    }

    private def withoutParentheses(expression: Expression): Expression =
      expression match {
        case parenthesed: ParenthesedExpressionList[_] if parenthesed.size() == 1 =>
          withoutParentheses(parenthesed.get(0))
        case other => other
      }

    /** The expression that a cast or parentheses leave as it is */
    private def unwrap(expression: Expression): Expression =
      expression match {
        case cast: CastExpression if cast.getLeftExpression != null =>
          unwrap(cast.getLeftExpression)
        case parenthesed: ParenthesedExpressionList[_] if parenthesed.size() == 1 =>
          unwrap(parenthesed.get(0))
        case other => other
      }

    private def columnOrigin(column: Column, sources: List[Source]): Origin = {
      val name = lower(column.getColumnName)
      val qualifier = Option(column.getTable).map(nameParts).getOrElse(Nil).map(lower)
      val candidates =
        if (qualifier.nonEmpty) sources.filter(_.isNamed(qualifier))
        else {
          val knowing = sources.filter(_.knows(name))
          if (knowing.nonEmpty) knowing
          else if (sources.size == 1) sources
          else sources.filter(_.columns.isEmpty)
        }
      candidates match {
        case List(source) => source.originOf(name)
        // Not found, or ambiguous
        case _ => Computed
      }
    }
  }
}
