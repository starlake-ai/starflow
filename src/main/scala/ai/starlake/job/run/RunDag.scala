package ai.starlake.job.run

import scala.collection.mutable

sealed trait RunNodeType

object RunNodeType {

  /** A transform task, executed via the transform path */
  case object Task extends RunNodeType

  /** A table defined in the project's load metadata, executed via the load path */
  case object LoadTable extends RunNodeType

  /** External table, view or CTE: not executable, satisfied immediately, kept only to preserve
    * transitive ordering through the graph
    */
  case object Boundary extends RunNodeType
}

/** @param id
  *   lowercase unique key
  * @param displayName
  *   original-case name used for execution and display
  */
final case class RunNode(id: String, displayName: String, typ: RunNodeType)

/** @param parents
  *   child id -> upstream ids. Every node id has an entry, possibly empty.
  */
final case class RunDag(nodes: Map[String, RunNode], parents: Map[String, Set[String]]) {

  lazy val children: Map[String, Set[String]] = {
    val acc = mutable.Map[String, Set[String]]().withDefaultValue(Set.empty)
    nodes.keys.foreach(id => acc(id) = acc(id))
    parents.foreach { case (child, ups) =>
      ups.foreach(up => acc(up) = acc(up) + child)
    }
    acc.toMap
  }

  def executableCount: Int = nodes.values.count(_.typ != RunNodeType.Boundary)

  /** Restricts the graph to `selected`, rewiring every kept node's parents to its *nearest selected
    * ancestors* rather than simply dropping edges to unselected nodes.
    *
    * Spec section 4 says unselected upstreams are not run. Deleting their edges honours that but
    * silently loses ordering: with `a -> b -> c` and b unselected, a and c would become independent
    * and could run in either order. Walking up through b instead keeps `a -> c`, so nothing
    * unselected executes and the ordering that the project's lineage asserts still holds.
    *
    * `restrictTo(nodes.keySet)` is the identity, which is what keeps a run with no selectors
    * behaving exactly as it did before selection existed.
    */
  def restrictTo(selected: Set[String]): RunDag = {
    val kept = selected.intersect(nodes.keySet)

    def nearestSelectedAncestors(id: String): Set[String] = {
      val seen = mutable.Set[String]()
      val found = mutable.Set[String]()
      val queue = mutable.Queue[String]() ++= parents.getOrElse(id, Set.empty)
      while (queue.nonEmpty) {
        val current = queue.dequeue()
        // `seen` also makes this terminate on a graph that somehow carries a cycle, rather than
        // hanging: DagBuilder rejects cycles, but this method does not depend on that.
        if (seen.add(current)) {
          if (kept.contains(current)) found += current
          else queue ++= parents.getOrElse(current, Set.empty)
        }
      }
      found.toSet
    }

    RunDag(
      nodes = nodes.view.filterKeys(kept.contains).toMap,
      parents = kept.map(id => id -> nearestSelectedAncestors(id)).toMap
    )
  }
}

object RunDag {

  /** The "domain.table" key a dotted name reduces to: its last two dot-separated segments,
    * lowercased.
    *
    * Lineage can name a table with a catalog or project prefix ("proj.sales.orders"), so every
    * comparison against a declared "domain.table" name goes through this. The lowercasing is done
    * here rather than assumed of the caller: node ids happen to be lowercase already and both
    * current callers rely on that, which makes it an invariant nothing states and one refactor away
    * from being wrong.
    *
    * DagBuilder holds an equivalent private copy. It cannot call this one today without widening
    * its own visibility contract, so the duplication is deliberate and the two must stay in step:
    * DagBuilder decides which nodes are load tables, and these callers decide what those nodes
    * execute and match against.
    */
  private[run] def lastTwoParts(name: String): String =
    name.toLowerCase.split('.').takeRight(2).mkString(".")
}
