package ai.starlake.job.site

import ai.starlake.lineage.ColLineage

import scala.collection.immutable.TreeMap

case class LineageResult(
  nodes: TreeMap[String, SiteNode],
  edges: List[SiteEdge],
  columnEdges: List[SiteColumnEdge]
)

object LineageBuilder {

  private def clean(part: String): String =
    part.trim.stripPrefix("`").stripSuffix("`").stripPrefix("\"").stripSuffix("\"").toLowerCase

  private def parts(ref: String): List[String] = ref.split('.').toList.map(clean).filter(_.nonEmpty)

  /** Producers (load or transform nodes) matching a table reference. */
  private def producers(nodes: Iterable[SiteNode], ref: String, preferDomain: Option[String]) = {
    val p = parts(ref)
    val declared = nodes.filter(n => n.kind == "load" || n.kind == "transform")
    p match {
      case Nil => Nil
      case table :: Nil =>
        val byTable = declared.filter(_.table.toLowerCase == table).toList
        val sameDomain = preferDomain.toList.flatMap(d => byTable.filter(_.domain.toLowerCase == d))
        if (sameDomain.nonEmpty) sameDomain else byTable
      case _ =>
        val domain = p(p.length - 2)
        val table = p.last
        declared
          .filter(n => n.domain.toLowerCase == domain && n.table.toLowerCase == table)
          .toList
    }
  }

  private def externalNode(ref: String): SiteNode = {
    val p = parts(ref)
    val table = p.lastOption.getOrElse(ref)
    val domain = if (p.length >= 2) p(p.length - 2) else ""
    val database = if (p.length >= 3) Some(p.dropRight(2).mkString(".")) else None
    SiteNode(
      id = NodeId.external(p.mkString(".")),
      kind = "external",
      name = table,
      domain = domain,
      table = table,
      database = database
    )
  }

  def build(
    nodes: TreeMap[String, SiteNode],
    taskDeps: Map[String, List[String]],
    colLineage: Map[String, ColLineage.Lineage]
  ): LineageResult = {
    var allNodes = nodes
    val edges = List.newBuilder[SiteEdge]

    // Table lineage
    taskDeps.toList.sortBy(_._1).foreach { case (taskId, refs) =>
      refs.distinct.foreach { ref =>
        val found = producers(nodes.values, ref, None)
        val fromIds =
          if (found.nonEmpty) found.map(_.id)
          else {
            val ext = externalNode(ref)
            if (!allNodes.contains(ext.id)) allNodes = allNodes + (ext.id -> ext)
            List(ext.id)
          }
        fromIds.filter(_ != taskId).foreach(from => edges += SiteEdge(from, taskId, "lineage"))
      }
    }

    // Foreign keys: "[domain.]table.attribute"
    val withFks = allNodes.map { case (id, node) =>
      val cols = node.columns.map { col =>
        col.foreignKey match {
          case Some(fk) =>
            val tableRef = fk.split('.').dropRight(1).mkString(".")
            producers(nodes.values, tableRef, Some(node.domain.toLowerCase)).headOption match {
              case Some(target) =>
                edges += SiteEdge(id, target.id, "fk")
                col.copy(foreignKeyNode = Some(target.id))
              case None => col
            }
          case None => col
        }
      }
      id -> node.copy(columns = cols)
    }

    // Column lineage
    val columnEdges = colLineage.toList.sortBy(_._1).flatMap { case (taskId, lineage) =>
      lineage.relations.flatMap { r =>
        val fromNode = producers(withFks.values, s"${r.from.domain}.${r.from.table}", None).headOption
          .map(_.id)
          .getOrElse(NodeId.external(s"${r.from.domain}.${r.from.table}"))
        Some(SiteColumnEdge(s"$fromNode#${r.from.column}", s"$taskId#${r.to.column}", taskId))
      }
    }

    LineageResult(withFks, edges.result().distinct, columnEdges.distinct)
  }
}
