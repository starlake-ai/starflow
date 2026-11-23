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

  /** Target of a foreign key, mirroring SchemaInfo.fkComponents. A two-part reference is read as
    * `domain.table` when such a table is declared, else as `table.column`.
    */
  private def fkTarget(
    nodes: Iterable[SiteNode],
    fk: String,
    domain: String
  ): Option[SiteNode] = {
    val declared = nodes.filter(n => n.kind == "load" || n.kind == "transform")
    def exact(d: String, t: String) =
      declared.find(n => n.domain.toLowerCase == d && n.table.toLowerCase == t)
    def byTable(t: String) = producers(nodes, t, Some(domain)).headOption
    parts(fk) match {
      case Nil                    => None
      case table :: Nil           => byTable(table)
      case first :: second :: Nil => exact(first, second).orElse(byTable(first))
      case p                      => exact(p(p.length - 3), p(p.length - 2))
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
    taskDeps.toList.filter { case (taskId, _) => nodes.contains(taskId) }.sortBy(_._1).foreach {
      case (taskId, refs) =>
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

    // Foreign keys: "table", "domain.table", "table.column" or "domain.table.column"
    def resolveFks(id: String, domain: String, cols: List[SiteColumn]): List[SiteColumn] =
      cols.map { col =>
        val withChildren = col.copy(columns = resolveFks(id, domain, col.columns))
        col.foreignKey.flatMap(fk => fkTarget(nodes.values, fk, domain)) match {
          case Some(target) =>
            if (target.id != id) edges += SiteEdge(id, target.id, "fk")
            withChildren.copy(foreignKeyNode = Some(target.id))
          case None => withChildren
        }
      }
    val withFks = allNodes.map { case (id, node) =>
      id -> node.copy(columns = resolveFks(id, node.domain.toLowerCase, node.columns))
    }

    // Column lineage: keep only relations that feed the task's own output table
    var finalNodes = withFks
    val columnEdges = colLineage.toList.sortBy(_._1).flatMap { case (taskId, lineage) =>
      val taskNode = finalNodes.get(taskId)
      val outputs = taskNode.toList.flatMap(n => List(n.table.toLowerCase, n.name.toLowerCase))
      lineage.relations
        .filter(r => outputs.contains(clean(r.to.table)))
        .flatMap { r =>
          val fromDomain = Option(r.from.domain).getOrElse("")
          val ref = if (fromDomain.trim.isEmpty) r.from.table else s"$fromDomain.${r.from.table}"
          val table = clean(r.from.table)
          val domain = clean(fromDomain)
          val fromId: Option[String] =
            producers(finalNodes.values, ref, None).headOption.map(_.id).orElse {
              finalNodes.values
                .find(n =>
                  n.kind == "external" && n.table.toLowerCase == table &&
                  (domain.isEmpty || n.domain.toLowerCase == domain)
                )
                .map(_.id)
                .orElse {
                  if (domain.isEmpty) None
                  else {
                    val ext = externalNode(ref)
                    if (!finalNodes.contains(ext.id)) finalNodes = finalNodes + (ext.id -> ext)
                    Some(ext.id)
                  }
                }
            }
          fromId
            .map(id => SiteColumnEdge(s"$id#${r.from.column}", s"$taskId#${r.to.column}", taskId))
        }
    }

    LineageResult(finalNodes, edges.result().distinct, columnEdges.distinct)
  }
}
