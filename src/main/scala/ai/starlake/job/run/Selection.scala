package ai.starlake.job.run

import scala.collection.mutable

/** How many executable tasks one selector expression accounted for.
  *
  * For a `--select`, that is what the selector expanded to. For an `--exclude`, it is what the
  * selector actually removed from the selected set, not what it matched in the graph: an exclude
  * matching four nodes of which one was selected removed one, and saying "removed 4" would
  * misreport how much the plan shrank.
  */
final case class SelectorMatch(expr: String, matched: Int)

/** @param ids
  *   every selected node id, boundaries included: they cost nothing to carry and keep a run with no
  *   selectors identical to P1
  */
final case class Selection(
  ids: Set[String],
  selectMatches: List[SelectorMatch],
  excludeMatches: List[SelectorMatch]
)

object Selection {

  /** Resolves `--select` / `--exclude` against the graph.
    *
    * Selectors union; exclusion is applied afterwards and wins. No selector at all selects the
    * whole project. A malformed expression short-circuits to Left before the graph is walked, and
    * the caller maps that to exit code 2.
    */
  def resolve(
    dag: RunDag,
    tagsByNodeId: Map[String, Set[String]],
    selects: Seq[String],
    excludes: Seq[String]
  ): Either[String, Selection] =
    for {
      parsedSelects  <- parseAll(selects)
      parsedExcludes <- parseAll(excludes)
    } yield {
      val selectedPerSelector = parsedSelects.map(s => s -> expand(dag, tagsByNodeId, s))
      val base =
        if (parsedSelects.isEmpty) dag.nodes.keySet
        else selectedPerSelector.map(_._2).foldLeft(Set.empty[String])(_ ++ _)

      val excludedPerSelector = parsedExcludes.map(s => s -> expand(dag, tagsByNodeId, s))
      val excluded = excludedPerSelector.map(_._2).foldLeft(Set.empty[String])(_ ++ _)

      def executable(ids: Set[String]): Int =
        ids.count(id => dag.nodes.get(id).exists(_.typ != RunNodeType.Boundary))

      Selection(
        ids = base -- excluded,
        selectMatches = selectedPerSelector.map { case (selector, matched) =>
          SelectorMatch(selector.expr, executable(matched))
        },
        excludeMatches = excludedPerSelector.map { case (selector, matched) =>
          SelectorMatch(selector.expr, executable(matched.intersect(base)))
        }
      )
    }

  private def parseAll(exprs: Seq[String]): Either[String, List[Selector]] =
    exprs.toList.foldLeft[Either[String, List[Selector]]](Right(Nil)) { (acc, expr) =>
      for {
        already  <- acc
        selector <- Selector.parse(expr)
      } yield already :+ selector
    }

  private def expand(
    dag: RunDag,
    tagsByNodeId: Map[String, Set[String]],
    selector: Selector
  ): Set[String] = {
    val direct = dag.nodes.keySet.filter(id => matches(id, tagsByNodeId, selector.matcher))
    val up = if (selector.upstream) traverse(direct, dag.parents) else Set.empty[String]
    val down = if (selector.downstream) traverse(direct, dag.children) else Set.empty[String]
    direct ++ up ++ down
  }

  /** Every node reachable from `roots` by following `edges`, excluding the roots themselves unless
    * the graph leads back to them.
    */
  private def traverse(roots: Set[String], edges: Map[String, Set[String]]): Set[String] = {
    val seen = mutable.Set[String]()
    val queue = mutable.Queue[String]() ++= roots.flatMap(edges.getOrElse(_, Set.empty))
    while (queue.nonEmpty) {
      val id = queue.dequeue()
      if (seen.add(id)) queue ++= edges.getOrElse(id, Set.empty)
    }
    seen.toSet
  }

  /** Node ids are lowercased by DagBuilder and matcher values by Selector.parse, so comparisons
    * here are already case-insensitive. Tags come straight from the YAML and are folded here.
    */
  private def matches(
    id: String,
    tagsByNodeId: Map[String, Set[String]],
    matcher: Matcher
  ): Boolean =
    matcher match {
      // Selector.parse guarantees an Exact target is exactly two non-empty segments, so
      // `id == target` implies `lastTwoParts(id) == target` and testing it separately would be
      // dead. All three arms normalize the same way, which is what lets a node id carrying a
      // catalog or project prefix match a two-part selector.
      case Matcher.Exact(target)     => RunDag.lastTwoParts(id) == target
      case Matcher.DomainAll(domain) => RunDag.lastTwoParts(id).startsWith(s"$domain.")
      case Matcher.Tag(value)        =>
        // tagsByNodeId is keyed the way DagBuilder names nodes, which is two-part today, so the
        // full id is the key that hits. The last-two-segments fallback keeps this arm consistent
        // with the two above rather than silently matching nothing on a prefixed graph.
        val tags =
          tagsByNodeId.getOrElse(id, tagsByNodeId.getOrElse(RunDag.lastTwoParts(id), Set.empty))
        tags.exists(_.toLowerCase == value)
    }
}
