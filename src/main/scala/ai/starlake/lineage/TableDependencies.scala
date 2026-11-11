package ai.starlake.lineage

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import AutoTaskDependencies.Diagram
import ai.starlake.utils.{JsonSerializer, Utils}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class TableDependencies(schemaHandler: SchemaHandler) extends LazyLogging {

  val prefix: String = """
                 |digraph {
                 |graph [pad="0.5", nodesep="0.5", ranksep="2"];
                 |node [shape=plain]
                 |rankdir=LR;
                 |
                 |
                 |""".stripMargin

  val suffix: String = """
                     |}
                     |""".stripMargin

  /** @param tableNames
    *   \- table names
    * @return
    *   (primary tables, fk tables)
    */
  private def relatedTableNames(tableNames: Seq[String]): (List[String], List[String]) = {
    // we extract all tables referenced by a foreign key in one of the tableNames parameter
    val foreignTableNames = schemaHandler.domains().flatMap(_.foreignTableNames(tableNames))

    val primaryTables = tableNames.flatMap { tableName =>
      schemaHandler
        .domains()
        .flatMap(d =>
          d.tables
            .filter(t => t.foreignTablesForDot(d.finalName).contains(tableName))
            .map(t => s"${d.finalName}.${t.finalName}")
        )
    }.distinct
    (primaryTables.toSet.toList, foreignTableNames.toSet.toList)
  }

  def run(args: Array[String]): Try[Unit] = {
    implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None, None)
    TableDependenciesCmd.run(args.toIndexedSeq, schemaHandler).map(_ => ())
  }

  def relationsAsDotFile(config: TableDependenciesConfig): Unit = {
    schemaHandler.domains(reload = config.reload)
    // we check if we have tables or domains
    val finalTables = config.tables match {
      case Some(tables) =>
        tables.flatMap { item =>
          if (item.contains('.')) {
            List(item) // it's already a table
          } else {
            // we have a domain, let's get all the tables
            schemaHandler.findTableNames(Some(item))
          }
        }.toList

      case None =>
        if (config.all) {
          schemaHandler.findTableNames(None)
        } else {
          Nil
        }
    }

//    val filteredTables = getTables(Some(finalTables))
    val (pkTables, sourceTables, fkTables) =
      if (config.related) {
        val (pkTables, fkTables) = relatedTableNames(finalTables)
        (pkTables.toSet, finalTables.toSet, fkTables.toSet)
      } else
        (Set.empty[String], finalTables.toSet, finalTables.toSet)

    val allTables = pkTables.union(sourceTables).union(fkTables)
    val dots =
      schemaHandler
        .domains()
        .map(_.asDot(config.includeAllAttributes, allTables.map(_.toLowerCase)))
    val dotStr = prefix + dots.mkString("\n") + suffix
    Utils.save(config.outputFile, dotStr)
  }

  private def allTables(config: TableDependenciesConfig): List[String] = {
    schemaHandler.domains(reload = config.reload)
    // we check if we have tables or domains
    val finalTableNames = config.tables match {
      case Some(tables) =>
        tables.flatMap { item =>
          if (item.contains('.')) {
            List(item) // it's already a table
          } else {
            // we have a domain, let's get all the tables
            schemaHandler.findTableNames(Some(item))
          }
        }.toList

      case None =>
        if (config.all) {
          schemaHandler.findTableNames(None)
        } else {
          Nil
        }
    }
    finalTableNames
  }

  private def asItemsAndRelations(
    allTableNames: Set[String]
  ): List[(AutoTaskDependencies.Item, List[AutoTaskDependencies.Relation])] = {
    val itemsAndRelations =
      schemaHandler
        .domains()
        .flatMap(_.asItem(allTableNames.map(_.toLowerCase)))
    itemsAndRelations
  }

  def relationsAsDiagram(config: TableDependenciesConfig): Diagram = {
    val finalTableNames = allTables(config)
    //    val filteredTables = getTables(Some(finalTables))
    val (pkTables, sourceTables, fkTables) =
      if (config.related) {
        val (pkTables, fkTables) = relatedTableNames(finalTableNames)
        (pkTables.toSet, finalTableNames.toSet, fkTables.toSet)
      } else
        (Set.empty[String], finalTableNames.toSet, finalTableNames.toSet)

    val allTableNames = pkTables.union(sourceTables).union(fkTables)
    val itemsAndRelations = asItemsAndRelations(allTableNames)
    val items = itemsAndRelations.map(_._1).distinct
    val relations = itemsAndRelations.flatMap(_._2).distinct
    val diagram = Diagram(items, relations, "table")
    val diagramAsStr =
      JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(diagram)
    Utils.save(config.outputFile, diagramAsStr)
    diagram
  }
}
