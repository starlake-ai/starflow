package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.lineage.SelectColumnOrigins.*
import ai.starlake.schema.handlers.{SchemaHandler, TaskYamlAttributes}
import ai.starlake.schema.model.{AttributeStatus, AutoTaskInfo, SchemaInfo, TableAttribute}
import ai.starlake.sql.PresqlTemporaryTables
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** The descriptions that the columns of a task inherit through their column lineage (#1948), for
  * `transform --sync-preview` and `--sync-apply` only, never at run time.
  *
  * A column inherits the description of the column that it copies, as it is, renamed or cast: from
  * the attributes of a load or external table, or from the attributes of the tasks that write the
  * table, which inherit in turn when they declare none. A computed column, a column of several
  * columns, or of a table that is not known, inherits nothing. The branch of a UNION that reads the
  * table of the task itself, as a temporary table of its presql that reads its previous state, is
  * left aside, and a cycle stops the resolution. Everything comes from the YAML and SQL files of
  * the project: the descriptions are read as loaded, Jinja rendered.
  *
  * @param refresh
  *   whether the lineage of a column wins over the description that a task declares for it, so that
  *   a description corrected upstream reaches the columns that copied it before
  */
final class InheritedDescriptions(schemaHandler: SchemaHandler, refresh: Boolean)(implicit
  settings: Settings
) extends LazyLogging {
  import InheritedDescriptions.*

  /** The load and external tables, by every name of their domain: a load table wins over an
    * external table of the same name
    */
  private lazy val tables: Map[TableRef, (TableRef, SchemaInfo)] =
    (Try(schemaHandler.externals()).getOrElse(Nil) ++ schemaHandler.domains()).flatMap { domain =>
      Option(domain.tables).getOrElse(Nil).flatMap { table =>
        val canonical = TableRef(lower(domain.finalName), lower(table.finalName))
        List(domain.finalName, domain.name).map(lower).distinct.map { domainName =>
          TableRef(domainName, lower(table.finalName)) -> (canonical -> table)
        }
      }
    }.toMap

  /** The tasks that write each table */
  private lazy val writers: Map[TableRef, List[AutoTaskInfo]] =
    schemaHandler.tasks().groupBy(tableRefOf)

  val catalog: Catalog = new Catalog {
    def resolve(nameParts: List[String]): Option[TableRef] =
      nameParts.map(lower) match {
        case Nil => None
        // A table named alone, as Starlake resolves it: the only table of the project of that name
        case List(table) =>
          (tables.values.map(_._1).filter(_.table == table) ++ writers.keys.filter(
            _.table == table
          )).toList.distinct match {
            case List(ref) => Some(ref)
            case _         => None
          }
        // The database or project of the name is not compared
        case parts =>
          val ref = TableRef(parts(parts.size - 2), parts.last)
          tables.get(ref).map(_._1).orElse(Some(ref).filter(writers.contains))
      }

    def columns(table: TableRef): Option[List[String]] =
      writers.get(table) match {
        case Some(tasks) =>
          val columns = tasks.map(taskColumns)
          if (columns.forall(_.isDefined)) Some(columns.flatten.flatMap(_._1).distinct) else None
        case None =>
          tables
            .get(table)
            .map(_._2.attributes.filterNot(_.ignore.getOrElse(false)).map(_.getFinalName()))
            .filter(_.nonEmpty)
      }

    override def isComplete(table: TableRef): Boolean =
      writers.get(table).forall(_.forall(task => taskColumns(task).exists(_._2)))
  }

  /** The columns of the table of a task: those of its SELECT, then the attributes that it declares
    * and that its SELECT does not return (such as the validity columns of SCD2), and whether they
    * are all known
    */
  private def taskColumns(task: AutoTaskInfo): Option[(List[String], Boolean)] = {
    val declared = task.attributes.map(_.getFinalName()).map(lower)
    val select =
      if (reading.contains(task.fullName())) None
      else originsOf(task).toOption.filter(_._1.nonEmpty)
    select match {
      case Some((columns, complete)) =>
        val names = columns.map(_._1)
        Some((names ++ declared.filterNot(names.contains), complete))
      case None => Some((declared, true)).filter(_._1.nonEmpty)
    }
  }

  private val origins = mutable.Map.empty[String, Try[(List[(String, Origin)], Boolean)]]
  private val reading = mutable.Set.empty[String]

  private def originsOf(task: AutoTaskInfo): Try[(List[(String, Origin)], Boolean)] =
    origins.getOrElse(
      task.fullName(), {
        val read = readOrigins(task, None)
        read.failed.foreach { exception =>
          if (task.parseSQL.getOrElse(true) && task.python.isEmpty)
            logger.warn(
              s"The column lineage of task ${task.fullName()} is not read, the tasks that read its table inherit only the descriptions that it declares: ${exception.getMessage}"
            )
        }
        origins.put(task.fullName(), read)
        read
      }
    )

  private def readOrigins(
    task: AutoTaskInfo,
    select: Option[String]
  ): Try[(List[(String, Origin)], Boolean)] =
    if (!task.parseSQL.getOrElse(true) || task.python.isDefined)
      Failure(new IllegalStateException(s"Task ${task.fullName()} has no SQL to parse"))
    else {
      reading += task.fullName()
      try
        Try {
          // Rendered, but with the names of the project: the refs are not substituted
          val sql = select.getOrElse(Utils.parseJinja(task.getSql(), schemaHandler.activeEnvVars()))
          val engine = Try(task.getRunConnection().getJdbcEngineName().toString).toOption
          val lexing = engine
            .map(PresqlTemporaryTables.Lexing.of)
            .getOrElse(PresqlTemporaryTables.Lexing.standard)
          val castKeepsName = engine.exists(Set("postgresql", "redshift", "spark").contains)
          (PresqlTemporaryTables.withTemporaryTables(sql, task.presql, lexing)._1, castKeepsName)
        }.flatMap { case (sql, castKeepsName) =>
          SelectColumnOrigins.withCompleteness(sql, catalog, castKeepsName)
        }
      finally reading -= task.fullName()
    }

  /** @return
    *   the descriptions that the columns of the SELECT of the task inherit, by lower-cased column
    *   name: the SELECT of the task, or the given one, with the temporary tables of its presql.
    *   Nothing for a task whose SQL is not parsed, or a Python task.
    */
  def inheritedFor(task: AutoTaskInfo, select: Option[String]): Try[Map[String, Inherited]] =
    if (!task.parseSQL.getOrElse(true) || task.python.isDefined) Success(Map.empty)
    else {
      val self = tableRefOf(task)
      val read = if (select.isDefined) readOrigins(task, select) else originsOf(task)
      read.map(
        _._1
          .flatMap { case (column, origin) =>
            fromOrigin(origin, self, Set(ColumnRef(self, column))).map(column -> _)
          }
          .toMap
      )
    }

  private val described = mutable.Map.empty[ColumnRef, Option[Inherited]]

  /** Whether a resolution met a column being resolved: its result is not kept */
  private var cut = false

  private def describe(column: ColumnRef, visiting: Set[ColumnRef]): Option[Inherited] =
    if (visiting.contains(column)) {
      cut = true
      None
    } else
      described.get(column) match {
        case Some(known) => known
        case None =>
          val outer = cut
          cut = false
          val result = resolve(column, visiting + column)
          if (!cut) described.put(column, result)
          cut = outer || cut
          result
      }

  private def resolve(column: ColumnRef, visiting: Set[ColumnRef]): Option[Inherited] =
    writers.get(column.table) match {
      case Some(tasks) =>
        // The tasks that write the table must agree
        agreeing(tasks.map { task =>
          lazy val own = declared(task.attributes, column)
          lazy val lineage =
            originsOf(task).toOption
              .flatMap(_._1.find(_._1 == column.column))
              .flatMap { case (_, origin) => fromOrigin(origin, column.table, visiting) }
          if (refresh) lineage.orElse(own) else own.orElse(lineage)
        })
      case None =>
        tables.get(column.table).flatMap { case (canonical, table) =>
          declared(table.attributes, ColumnRef(canonical, column.column))
        }
    }

  /** The description of the columns that a column copies, when they all have the same */
  private def fromOrigin(
    origin: Origin,
    self: TableRef,
    visiting: Set[ColumnRef]
  ): Option[Inherited] =
    sources(origin, self).filter(_.nonEmpty).flatMap { columns =>
      agreeing(columns.map(describe(_, visiting)))
    }

  /** The columns that a column copies, without those of the table of the task, nor the branches of
    * a UNION that read it and copy no other column: None when the column is computed
    */
  private def sources(origin: Origin, self: TableRef): Option[List[ColumnRef]] =
    origin match {
      case Computed        => None
      case Copied(columns) => Some(columns.filterNot(_.table == self))
      case Branches(branches) =>
        branches.filterNot { branch =>
          branch.reads.contains(self) && !sources(branch.origin, self).exists(_.nonEmpty)
        } match {
          case Nil => None
          case kept =>
            kept.foldLeft(Option(List.empty[ColumnRef])) { (columns, branch) =>
              for {
                known  <- columns
                copied <- sources(branch.origin, self)
              } yield known ++ copied
            }
        }
    }

  private def agreeing(descriptions: List[Option[Inherited]]): Option[Inherited] =
    if (
      descriptions.nonEmpty && descriptions.forall(_.isDefined) &&
      descriptions.flatten.map(_.description).distinct.size == 1
    ) descriptions.head
    else None

  private def declared(attributes: List[TableAttribute], column: ColumnRef): Option[Inherited] =
    attributes
      .find(attribute => lower(attribute.getFinalName()) == column.column)
      .flatMap(_.comment)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Inherited(_, column))
}

object InheritedDescriptions {

  /** A description, and the column whose attribute declares it */
  final case class Inherited(description: String, declaredBy: ColumnRef)

  /** The attributes of a sync with the inherited descriptions
    * @param attributes
    *   the attributes and their status, Nil when nothing changes
    * @param inherited
    *   the attributes that get an inherited description, with it
    * @param written
    *   the attributes whose description is already the inherited one
    * @param leftOut
    *   the inherited descriptions left out, since they contain Jinja or a variable, which the YAML
    *   update refuses
    * @param undescribed
    *   the attributes left without a description
    * @param error
    *   why the column lineage of the task could not be read
    */
  final case class Result(
    attributes: List[(TableAttribute, AttributeStatus)],
    inherited: List[(String, Inherited)],
    written: List[(String, Inherited)],
    leftOut: List[(String, Inherited)],
    undescribed: List[String],
    error: Option[String]
  )

  private[lineage] def tableRefOf(task: AutoTaskInfo): TableRef =
    TableRef(lower(task.domain), lower(task.table))

  /** Puts the inherited descriptions in the attributes of a sync
    * @param taskAttributes
    *   the attributes of the task, which the sync keeps when nothing else changes
    * @param synced
    *   the attributes of the sync and their status, Nil when nothing changes
    * @param inherited
    *   the inherited descriptions, by lower-cased column name
    * @param refresh
    *   whether an inherited description replaces the one that an attribute has
    */
  def merge(
    taskAttributes: List[TableAttribute],
    synced: List[(TableAttribute, AttributeStatus)],
    inherited: Map[String, Inherited],
    refresh: Boolean
  ): Result = {
    val attributes =
      if (synced.isEmpty) taskAttributes.map(_ -> AttributeStatus.UNCHANGED) else synced
    val applied = mutable.ListBuffer.empty[(String, Inherited)]
    val written = mutable.ListBuffer.empty[(String, Inherited)]
    val leftOut = mutable.ListBuffer.empty[(String, Inherited)]
    val merged = attributes.map {
      case (attribute, status) if status != AttributeStatus.REMOVED =>
        val own = attribute.comment.map(_.trim).filter(_.nonEmpty)
        inherited.get(lower(attribute.name)) match {
          case Some(description) if own.contains(description.description) =>
            written += attribute.name -> description
            attribute                 -> status
          case Some(description) if own.isEmpty || refresh =>
            if (TaskYamlAttributes.isRendered(description.description)) {
              leftOut += attribute.name -> description
              attribute                 -> status
            } else {
              applied += attribute.name -> description
              val updatedStatus =
                if (status == AttributeStatus.UNCHANGED) AttributeStatus.MODIFIED else status
              attribute.copy(comment = Some(description.description)) -> updatedStatus
            }
          case _ => attribute -> status
        }
      case removed => removed
    }
    val undescribed = merged.collect {
      case (attribute, status)
          if status != AttributeStatus.REMOVED && attribute.comment.forall(_.trim.isEmpty) =>
        attribute.name
    }
    Result(
      attributes = if (synced.isEmpty && applied.isEmpty) Nil else merged,
      inherited = applied.toList,
      written = written.toList,
      leftOut = leftOut.toList,
      undescribed = undescribed,
      error = None
    )
  }
}
