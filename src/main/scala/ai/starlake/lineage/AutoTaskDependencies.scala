package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.job.transform.AutoTask
import ai.starlake.lineage.AutoTaskDependencies.{Column, Diagram, Item}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.{JsonSerializer, Utils}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

case class DependencyContext(
  jobName: String,
  entities: List[TaskViewDependency],
  relations: List[TaskViewDependency]
)

class AutoTaskDependencies(
  settings: Settings,
  schemaHandler: SchemaHandler,
  storageHandler: StorageHandler
) extends LazyLogging {

  def run(config: AutoTaskDependenciesConfig): Try[Unit] = Try {
    val allDependencies: List[DependencyContext] = tasks(config)
    if (config.print) jobsDependencyTree(allDependencies, config)
    if (config.viz) jobAsDot(allDependencies, config)
  }

  /** @param config
    * @return
    *   List[DependencyContext(jobName, dedupEntities, relations)]
    */
  def tasks(
    config: AutoTaskDependenciesConfig
  ): List[DependencyContext] = {
    val tasks =
      AutoTask.unauthenticatedTasks(config.reload)(settings, storageHandler, schemaHandler)
    val depsMap = {
      /*
      if (config.verbose) {
        schemaHandler
          .tasks()
          .map { task =>
            (task.name, TaskViewDependency.taskDependencies(task.name, tasks)(schemaHandler))
          } :+ ("_lineage" -> TaskViewDependency.dependencies(tasks)(schemaHandler))
      } else {
      }

       */
      config.tasks.getOrElse(Nil) match {
        case Nil =>
          if (config.all)
            List("_lineage" -> TaskViewDependency.dependencies(tasks)(settings, schemaHandler))
          else
            Nil
        case taskOrDomainNames =>
          val taskNames =
            if (taskOrDomainNames.size == 1) {
              val taskOrDomainName = taskOrDomainNames.head
              if (taskOrDomainName.contains('.')) // we have a task
                taskOrDomainNames
              else {
                // we have a domain, get all tasks in the domain
                tasks.filter(_.taskDesc.domain == taskOrDomainName).map(_.taskDesc.name)
              }
            } else {
              taskOrDomainNames // we have a list of tasks
            }
          taskNames.map(taskName =>
            (
              taskName,
              TaskViewDependency.taskDependencies(taskName, tasks)(settings, schemaHandler)
            )
          )
      }
    }
    val mapper = Utils.newJsonMapper().writerWithDefaultPrettyPrinter()

    depsMap.map { case (jobName, allDeps) =>
      val deps =
        allDeps.filter(dep => config.objects.contains("all") || config.objects.contains(dep.typ))
      val dedupEntities = deps
        .groupBy(_.name)
        .view
        .mapValues(_.head)
        .values
        .toList
      val relations = deps
        .filter(dep => config.objects.contains(dep.parentTyp))

      logger.whenDebugEnabled {
        logger.debug(s"----------jobName:$jobName")
        logger.debug("----------relations------")
        logger.debug(mapper.writeValueAsString(relations))
      }
      DependencyContext(jobName, dedupEntities, relations)
    }.toList
  }

  def jobsDependencyTree(
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {
    jobsDependencyTree(tasks(config), config)
  }

  def jobsDependencyTree(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): List[TaskViewDependencyNode] = {

    val relations = allDependencies.flatMap(_.relations)
    val entities = allDependencies.flatMap(_.entities)
    val result = config.tasks match {
      case Some(taskNames) =>
        val taskEntities = taskNames.map { taskName =>
          val taskEntity = entities.find(_.name == taskName).getOrElse {
            throw new RuntimeException(s"taskName:$taskName not found")
          }
          taskEntity
        }
        taskEntities.map(entity => TaskViewDependencyNode.dependencies(entity, entities, relations))
      case None =>
        if (config.all)
          TaskViewDependencyNode.dependencies(entities, relations)
        else
          Nil

    }
    result.foreach(_.print())
    result.toList
  }

  //// DOT section
  val prefix =
    """
      |digraph {
      |graph [pad="0.5", nodesep="0.5", ranksep="2"];
      |node [shape=plain]
      |rankdir=LR;
      |
      |
      |""".stripMargin

  val aclPrefix =
    """
      |digraph {
      |graph [pad="0.5", nodesep="0.5", ranksep="2"];
      |
      |
      |""".stripMargin

  val suffix =
    """
      |}
      |""".stripMargin

  def jobAsDiagram(
    config: AutoTaskDependenciesConfig
  ): Diagram = {
    jobAsDiagram(tasks(config), config)
  }
  def jobAsDot(config: AutoTaskDependenciesConfig): Unit = {
    jobAsDot(tasks(config), config)
  }

  def jobAsDiagram(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): Diagram = {
    def distinctBy[A, B](xs: List[A])(f: A => B): List[A] =
      scala.reflect.internal.util.Collections.distinctBy(xs)(f)

    val dedupDependencies = allDependencies.foldLeft(DependencyContext("all", Nil, Nil)) {
      (acc, dep) =>
        DependencyContext(
          dep.jobName,
          distinctBy(acc.entities ++ dep.entities)(_.name),
          acc.relations ++ dep.relations
        )
    }

    val entitiesAsItems = dedupDependencies.entities.map(dep => dep.entityAsItem()).distinct
    val enrichedItems = entitiesAsItems.map(enrichItemWithColumns)
    val relationsAsRelations = dedupDependencies.relations
      .flatMap(dep => dep.relationAsRelation())
      .distinct

    val diagram = Diagram(enrichedItems, relationsAsRelations, "task")
    if (config.outputFile.isDefined) {
      val data =
        JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(diagram)
      if (config.outputFile.isDefined)
        Utils.save(config.outputFile, data)
    }
    diagram
  }

  private def enrichItemWithColumns(item: Item): Item = {
    if (item.columns.nonEmpty) return item
    val parts = item.id.split("\\.", 2)
    if (parts.length < 2) return item
    val (domainName, tableName) = (parts(0), parts(1))
    // Try load tables first, then transform tasks
    val columns = schemaHandler
      .domains()
      .filter(_.finalName.equalsIgnoreCase(domainName))
      .flatMap(_.tables.find(_.finalName.equalsIgnoreCase(tableName)))
      .headOption
      .map { schema =>
        schema.attributes.map { attr =>
          Column(
            s"${item.id}.${attr.getFinalName()}",
            attr.getFinalName(),
            attr.`type`,
            attr.comment,
            schema.isPrimaryKey(attr.getFinalName()),
            attr.deepForeignKey().isDefined
          )
        }
      }
      .orElse {
        schemaHandler
          .tasks()
          .find(t => t.domain.equalsIgnoreCase(domainName) && t.table.equalsIgnoreCase(tableName))
          .map { task =>
            task.attributes.map { attr =>
              Column(
                s"${item.id}.${attr.name}",
                attr.name,
                attr.`type`,
                attr.comment,
                primaryKey = false,
                foreignKey = false
              )
            }
          }
      }
      .getOrElse(Nil)
    item.copy(columns = columns)
  }

  /** @param allDependencies
    * @param config
    * @return
    *   (jobName, dotContent)
    */
  def jobAsDot(
    allDependencies: List[DependencyContext],
    config: AutoTaskDependenciesConfig
  ): Unit = {
    def distinctBy[A, B](xs: List[A])(f: A => B): List[A] =
      scala.reflect.internal.util.Collections.distinctBy(xs)(f)

    val dedupDependencies = allDependencies.foldLeft(DependencyContext("all", Nil, Nil)) {
      (acc, dep) =>
        DependencyContext(
          dep.jobName,
          distinctBy(acc.entities ++ dep.entities)(_.name),
          acc.relations ++ dep.relations
        )
    }

    val entitiesAsDot =
      dedupDependencies.entities.map(dep => dep.entityAsDot(config.verbose)).mkString("\n")
    val relationsAsDot = dedupDependencies.relations
      .flatMap(dep => dep.relationAsDot())
      .distinct
      .mkString("\n")
    val dotContent = List(prefix, entitiesAsDot, relationsAsDot, suffix).mkString("\n")

    if (config.svg)
      Utils.dot2Svg(config.outputFile, dotContent)
    else if (config.png)
      Utils.dot2Png(config.outputFile, dotContent)
    else
      Utils.save(config.outputFile, dotContent)
  }
}

object AutoTaskDependencies {
  case class Relation(
    source: String,
    target: String,
    relationType: String, // acl, rls
    label: Option[String] = None
  )
  case class Column(
    id: String,
    name: String,
    columnType: String,
    comment: Option[String],
    primaryKey: Boolean,
    foreignKey: Boolean = false
  )
  case class Item(
    id: String,
    label: String,
    displayType: String, // table, user, group, sa, domain, role, rls-role
    columns: List[Column] = Nil,
    options: Map[String, String] = Map.empty
  )

  case class Diagram(items: List[Item], relations: List[Relation], diagramType: String)

}
