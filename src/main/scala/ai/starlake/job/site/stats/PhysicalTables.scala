package ai.starlake.job.site.stats

import ai.starlake.job.site.SiteNode

object PhysicalTables {

  /** connectionRef -> physical tables (sorted by key), each with the sorted ids of the nodes that
    * point at it. External nodes and nodes without a connection are skipped.
    */
  def group(nodes: Iterable[SiteNode]): Map[String, List[(PhysicalTable, List[String])]] =
    nodes.toList
      .filter(_.kind != "external")
      .flatMap(n =>
        n.connectionRef.map(ref => (ref, PhysicalTable(n.database, n.domain, n.table), n.id))
      )
      .groupBy(_._1)
      .map { case (ref, entries) =>
        ref -> entries
          .groupBy(_._2.key)
          .values
          .toList
          .map(es => es.head._2 -> es.map(_._3).distinct.sorted)
          .sortBy(_._1.key)
      }
}
