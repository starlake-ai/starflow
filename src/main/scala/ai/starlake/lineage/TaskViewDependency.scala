package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.transform.AutoTask
import ai.starlake.lineage
import ai.starlake.lineage.AutoTaskDependencies.{Item, Relation}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{DomainInfo, WriteStrategy}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer

/*
https://medium.com/hibob-engineering/from-list-to-immutable-hierarchy-tree-with-scala-c9e16a63cb89
 */

object TaskViewDependency extends LazyLogging {
  // TODO migrate to enum once scala 3 is here
  val TASK_TYPE: String = "task"
  val CTE_TYPE: String = "cte"
  val TASKVIEW_TYPE: String = "taskview"
  val TABLE_TYPE: String = "table"
  val VIEW_TYPE: String = "view"
  val UNKNOWN_TYPE: String = "unknown"

  case class SimpleEntry(name: String, typ: String, parentRefs: List[String])
  def taskDependencies(taskName: String, tasks: List[AutoTask])(implicit
    settings: Settings,
    schemaHandler: SchemaHandler
  ): List[TaskViewDependency] = {
    val deps = dependencies(tasks)
    var result = scala.collection.mutable.ListBuffer[TaskViewDependency]()
    val roots = deps.filter(t => t.name == taskName && t.typ == TASK_TYPE)
    result ++= roots
    getHierarchy(roots, deps, result)
    result.toList.distinct
  }

  private def getHierarchy(
    roots: List[TaskViewDependency],
    allDeps: List[TaskViewDependency],
    result: ListBuffer[TaskViewDependency]
  ): Unit = {
    roots.foreach { root =>
      val subRoots = allDeps.filter(t =>
        t.typ == root.parentTyp && t.name.toLowerCase() == root.parent.toLowerCase()
      )
      val nocyclicRoots = subRoots.filter { subRoot =>
        val cycle = roots.exists(_.name.toLowerCase() == subRoot.name.toLowerCase())
        !cycle
      }
      result ++= nocyclicRoots
      getHierarchy(nocyclicRoots, allDeps, result)
    }
  }

  def dependencies(
    tasks: List[AutoTask]
  )(implicit settings: Settings, schemaHandler: SchemaHandler): List[TaskViewDependency] = {
    val jobs: Map[String, AutoTask] =
      tasks.groupBy(_.taskDesc.fullName()).map { case (name, tasks) =>
        (name, tasks.head)
      }
    val streamsMap = schemaHandler.streams()

    val jobDependencies: List[SimpleEntry] =
      jobs.view.mapValues(_.dependencies(streamsMap)).toList.map { case (jobName, dependencies) =>
        SimpleEntry(jobName, TASK_TYPE, dependencies)
      }

    val allTasks = schemaHandler.tasks()

    def tableSchedule(tableName: String): Option[String] = {
      val domains = schemaHandler.domains()
      val parts = tableName.split('.')
      parts.length match {
        case 1 =>
          val tablePart = parts.last // == 0
          val theDomain =
            domains
              .find(domain =>
                domain.tables.exists(_.finalName.toLowerCase() == tablePart.toLowerCase())
              )
          val theTable = theDomain.flatMap(
            _.tables.find(table => table.finalName.toLowerCase() == tablePart.toLowerCase())
          )
          theTable
            .flatMap(_.metadata)
            .flatMap(_.schedule)
            .orElse(theDomain.flatMap(_.metadata).flatMap(_.schedule))
        case 2 | 3 =>
          val domainPart = parts.dropRight(1).last
          val tablePart = parts.last
          val matchingDomains = domains
            .filter(_.finalName.toLowerCase() == domainPart.toLowerCase())
          val theDomainAndTable = matchingDomains.iterator
            .flatMap(d =>
              d.tables
                .find(table => table.finalName.toLowerCase() == tablePart.toLowerCase())
                .map(t => (d, t))
            )
            .nextOption()
          theDomainAndTable
            .flatMap { case (_, t) => t.metadata }
            .flatMap(_.schedule)
            .orElse(
              theDomainAndTable
                .flatMap { case (d, _) => d.metadata }
                .flatMap(_.schedule)
            )
            .orElse(matchingDomains.headOption.flatMap(_.metadata).flatMap(_.schedule))
        case _ =>
          None
      }
    }
    val jobAndViewDeps = jobDependencies.flatMap { case SimpleEntry(jobName, typ, parentRefs) =>
      logger.info(
        s"Analyzing dependency of type '$typ' for job '$jobName' with parent refs [${parentRefs.mkString(",")}]"
      )
      val task = jobs(jobName)
      if (parentRefs.isEmpty)
        List(
          TaskViewDependency(
            jobName,
            typ,
            "",
            UNKNOWN_TYPE,
            "",
            task.taskDesc.writeStrategy,
            dataset_triggering_strategy = task.taskDesc.assetTriggeringStrategy,
            freshness = task.taskDesc.freshness.map(_.acceptability()).getOrElse(0L)
          )
        )
      else {
        parentRefs.map { parentSQLRef =>
          val parts = parentSQLRef.split('.')
          // is it a job ?
          val parentJobName = parts.length match {
            case 1 =>
              val tablePart = parts.last // == 0
              val refs =
                allTasks.filter(task =>
                  task.fullName().endsWith(s".$tablePart") ||
                  task.table.toLowerCase() == tablePart.toLowerCase()
                )
              if (refs.size > 1) {
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName': Too many tasks found ${refs
                      .map(_.fullName())
                      .mkString(",")}.
                     |Make sure references in your SQL are unambiguous or fully qualified.
                     |""".stripMargin
                )

              } else
                refs.headOption.map(_.fullName())

            case 2 | 3 =>
              val domainPart = parts.dropRight(1).last
              val tablePart = parts.last
              allTasks
                .find(task =>
                  task.fullName().toLowerCase() == s"$domainPart.$tablePart".toLowerCase() ||
                  (task.table.toLowerCase() == tablePart.toLowerCase() &&
                  task.domain.toLowerCase() == domainPart.toLowerCase())
                )
                .map(_.fullName())
            case _ =>
              val errors = schemaHandler.checkJobsVars().mkString("\n")

              // Strange. This should never happen as far as I know. Let's make it clear
              if (parts.length == 0)
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName': No part found.
                     |Make sure variables defined in your job have a default value in the selected env profile.
                     |$errors""".stripMargin
                )
              else
                // Strange. This should never happen as far as I know. Let's make it clear
                throw new Exception(
                  s"""invalid parent ref '$parentSQLRef' syntax in job '$jobName'. Too many parts.
                     |Make sure variables defined in your job have a default value in the selected env profile.
                     |$errors""".stripMargin
                )

          }
          parentJobName match {
            case Some(parentJobName) =>
              val result =
                lineage.TaskViewDependency(
                  jobName,
                  typ,
                  parentJobName,
                  TASK_TYPE,
                  parentSQLRef,
                  task.taskDesc.writeStrategy,
                  dataset_triggering_strategy = task.taskDesc.assetTriggeringStrategy,
                  freshness = task.taskDesc.freshness.map(_.acceptability()).getOrElse(0L)
                )
              if (typ == TASK_TYPE) {
                // TODO We just handle one task per job which is always the case till now.
                val task = jobs(jobName)
                val sink = task.taskDesc.domain + "." + task.taskDesc.table
                result.copy(sink = Some(sink))
              } else result
            case None =>
              // is it a table ?
              val domains = schemaHandler.domains()
              val parentTable = parts.length match {
                case 1 =>
                  val tablePart = parts.last // == 0
                  val parentDomain: Option[DomainInfo] = domains
                    .find(domain =>
                      domain.tables.exists(_.finalName.toLowerCase() == tablePart.toLowerCase())
                    )
                  parentDomain.map(domain => (domain.finalName, tablePart))

                case 2 | 3 =>
                  val domainPart = parts.dropRight(1).last
                  val tablePart = parts.last
                  val parentDomainFound = domains
                    .filter(_.finalName.toLowerCase() == domainPart.toLowerCase())
                    .exists(
                      _.tables.exists(table =>
                        table.finalName.toLowerCase() == tablePart.toLowerCase()
                      )
                    )
                  if (parentDomainFound)
                    Some((domainPart, tablePart))
                  else
                    None
                case _ =>
                  // Strange. This should never happen as far as I know. Let's log it
                  throw new Exception(s"unknown $parentSQLRef syntax. Too many parts")
              }
              parentTable match {
                case Some((parentDomainName, parentTableName)) =>
                  TaskViewDependency(
                    jobName,
                    typ,
                    parentDomainName + "." + parentTableName,
                    TABLE_TYPE,
                    parentSQLRef,
                    task.taskDesc.writeStrategy,
                    dataset_triggering_strategy = task.taskDesc.assetTriggeringStrategy,
                    freshness = task.taskDesc.freshness.map(_.acceptability()).getOrElse(0L)
                  )
                case None =>
                  TaskViewDependency(
                    jobName,
                    typ,
                    "",
                    UNKNOWN_TYPE,
                    parentSQLRef,
                    task.taskDesc.writeStrategy,
                    dataset_triggering_strategy = task.taskDesc.assetTriggeringStrategy,
                    freshness = task.taskDesc.freshness.map(_.acceptability()).getOrElse(0L)
                  )
              }
          }

        }
      }
    }
    val tableNames = jobAndViewDeps.filter(_.parentTyp == TABLE_TYPE).groupBy(_.parent).keys
    val tableDeps = tableNames.map { tableName =>
      {
        val schedule = tableSchedule(tableName)
        val cron =
          Some(
            settings.appConfig.schedulePresets.getOrElse(
              schedule.getOrElse("None"),
              schedule.getOrElse("None")
            )
          )
        val tableComponents = tableName.split('.')
        val tableNameOnly = tableComponents.last
        val domainNameOnly = tableComponents.dropRight(1).last
        val matchingDomains =
          schemaHandler
            .domains()
            .filter(_.finalName.toLowerCase() == domainNameOnly.toLowerCase())
        val domainAndTable = matchingDomains.iterator
          .flatMap(d =>
            d.tables
              .find(_.finalName.toLowerCase() == tableNameOnly.toLowerCase())
              .map(t => (d, t))
          )
          .nextOption()
        val writeStrategy =
          for {
            (_, t) <- domainAndTable
            m      <- t.metadata
            w      <- m.writeStrategy
          } yield w
        val domainFreshness = domainAndTable
          .map { case (d, _) => d }
          .orElse(matchingDomains.headOption)
          .flatMap(_.metadata)
          .flatMap(_.freshness.map(_.acceptability()))
        val tableFreshness = domainAndTable
          .flatMap { case (_, t) => t.metadata }
          .flatMap(_.freshness.map(_.acceptability()))
        TaskViewDependency(
          tableName,
          TABLE_TYPE,
          "",
          UNKNOWN_TYPE,
          "",
          writeStrategy,
          None,
          cron,
          freshness = tableFreshness.orElse(domainFreshness).getOrElse(0L)
        )
      }
    }
    val jobAndViewDepsWithSink = jobAndViewDeps.map { dep =>
      if (dep.typ == TASK_TYPE) {
        // TODO We just handle one task per job which is always the case till now.
        val task = jobs(dep.name)
        val sink = task.taskDesc.domain + "." + task.taskDesc.table
        val schedule =
          allTasks.find(_.fullName().toLowerCase == dep.name.toLowerCase).flatMap(_.schedule)
        val cron =
          Some(
            settings.appConfig.schedulePresets.getOrElse(
              schedule.getOrElse("None"),
              schedule.getOrElse("None")
            )
          )
        dep.copy(sink = Some(sink), cron = cron)
      } else dep

    }
    jobAndViewDepsWithSink ++ tableDeps
  }
}

case class TaskViewDependency(
  name: String,
  typ: String,
  parent: String,
  parentTyp: String,
  parentRef: String,
  writeStrategy: Option[WriteStrategy],
  dataset_triggering_strategy: Option[String],
  sink: Option[String] = None,
  cron: Option[String] = None,
  freshness: Long = 0
) {

  @JsonIgnore
  def hasParent(): Boolean = parent.nonEmpty

  /** @return
    *   (bgColor, fontColor)
    */
  private def dotColor(): (String, String, String) = {
    import TaskViewDependency._
    typ match {
      case TASK_TYPE     => ("#00008B", "white", "Task") // darkblue
      case TASKVIEW_TYPE => ("darkcyan", "white", "TaskView")
      case VIEW_TYPE     => ("darkgrey", "white", "View")
      case TABLE_TYPE    => ("#008B00", "white", "Table")
      case CTE_TYPE      => ("darkorange", "white", "CTE")
      case UNKNOWN_TYPE  => ("black", "white", "???")
      case _             => throw new Exception(s"Unknown type $typ")
    }
  }

  def relationAsRelation(): Option[Relation] = {
    val depId = name
    val dotParent: String = if (parent.isEmpty) parentRef else parent
    if (dotParent.nonEmpty) {
      Some(Relation(dotParent, depId, typ))
    } else
      None

  }

  def relationAsDot(): Option[String] = {
    val depId = name.replaceAll("\\.", "_")
    val dotParent: String = if (parent.isEmpty) parentRef else parent
    val dotParentId = dotParent.replaceAll("\\.", "_")
    if (dotParent.nonEmpty)
      Some(s"$dotParentId -> $depId")
    else
      None

  }

  def entityAsItem(): Item = {
    val depId = name
    Item(
      id = depId,
      label = name,
      displayType = typ,
      options =
        Map("writeStrategy" -> writeStrategy.flatMap(_.`type`).map(_.toString).getOrElse(""))
    )
  }
  def entityAsDot(verbose: Boolean): String = {
    val depId = name.replaceAll("\\.", "_")
    val (bgColor, fontColor, text) = dotColor()
    if (verbose) {
      s"""
         |$depId [label=<
         |<table border="0" cellborder="1" cellspacing="0" cellpadding="10">
         |<tr>
         |<td rowspan="2" bgcolor="$bgColor" ><font color="$fontColor" face="Arial">$text</font></td>
         |<td port="0" ><font face="Arial">$name&nbsp;&nbsp;</font>
         |</td></tr>
         |<tr>
         |<td port="0" ><font face="Arial">
         |${writeStrategy.flatMap(_.`type`).getOrElse("")}
         |&nbsp;&nbsp;</font>
         |</td></tr>
         |         |</table>>];""".stripMargin

    } else {
      s"""
       |$depId [label=<
       |<table border="0" cellborder="1" cellspacing="0" cellpadding="10">
       |<tr>
       |<td bgcolor="$bgColor" ><font color="$fontColor" face="Arial">$text</font></td>
       |<td port="0" ><font face="Arial">$name&nbsp;&nbsp;</font>
       |</td></tr>
       |</table>>];""".stripMargin
    }
  }
}
