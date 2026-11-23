package ai.starlake.impact

import ai.starlake.impact.ChangeClass._

import scala.collection.mutable

object ImpactPropagator {

  private final case class State(
    changeClass: ChangeClass,
    columns: Columns,
    path: List[String],
    likelyFailure: Boolean,
    viaUnresolved: Boolean
  ) {
    def covers(other: State): Boolean =
      changeClass.rank >= other.changeClass.rank && columns.covers(other.columns) &&
      (likelyFailure || !other.likelyFailure) && (viaUnresolved || !other.viaUnresolved)

    def merge(other: State): State =
      State(
        worst(changeClass, other.changeClass),
        columns.union(other.columns),
        path, // first path wins: BFS reaches a node by its shortest path first
        likelyFailure || other.likelyFailure,
        viaUnresolved || other.viaUnresolved
      )
  }

  /** The upstream of an edge that may come from any node. */
  val AnyNode: NodeRef = NodeRef("", "(any)")

  /** One self edge per node defined more than once on either side: only one definition is analyzed.
    */
  def collisions(views: SnapshotView*): List[UnresolvedEdge] =
    views.toList
      .flatMap(_.collisions)
      .distinct
      .map { case (ref, what) =>
        UnresolvedEdge(ref, ref, s"defined more than once ($what), only one is analyzed")
      }

  private def step(ref: NodeRef, columns: Columns): String = columns match {
    case AllColumns => ref.name
    case Cols(cs)   => s"${ref.name}[${cs.toList.sorted.mkString(", ")}]"
  }

  def propagate(
    direct: List[DirectChange],
    view: SnapshotView
  ): (List[ImpactedNode], List[UnresolvedEdge]) = {
    val states = mutable.Map.empty[NodeRef, State]
    val edges = mutable.LinkedHashSet.empty[UnresolvedEdge]
    // (upstream node, upstream state, gone columns, upstream deleted)
    val queue = mutable.Queue.empty[(NodeRef, State, Set[String], Boolean)]

    direct.foreach { d =>
      val s = State(
        d.changeClass,
        d.columns,
        List(step(d.node, d.columns)),
        likelyFailure = false,
        viaUnresolved = false
      )
      queue.enqueue((d.node, s, d.goneColumns.map(_.toLowerCase), d.deleted))
    }

    while (queue.nonEmpty) {
      val (up, upState, gone, deleted) = queue.dequeue()
      if (upState.changeClass == Breaking || upState.changeClass == NonBreaking) {
        view.consumersOf(up).foreach { consumer =>
          edgeState(up, upState, gone, deleted, consumer, edges).foreach { reached =>
            val withPath = reached.copy(path = upState.path :+ step(consumer.ref, reached.columns))
            val next = states.get(consumer.ref) match {
              case Some(existing) if existing.covers(withPath) => None
              case Some(existing)                              => Some(existing.merge(withPath))
              case None                                        => Some(withPath)
            }
            next.foreach { s =>
              states(consumer.ref) = s
              queue.enqueue((consumer.ref, s, Set.empty, false))
            }
          }
        }
      }
    }

    // a python task reads tables nobody can tell offline: any breaking change may reach it
    val pythonEdges =
      if (direct.exists(_.changeClass == Breaking))
        view.nodes.values.toList
          .filter(_.shape.exists(_.unresolved.contains(TaskShape.PythonTask)))
          .map(n => UnresolvedEdge(AnyNode, n.ref, "python task, inputs unknown, not traced"))
      else Nil
    edges ++= pythonEdges

    val nodes = states.toList
      .map { case (ref, s) =>
        ImpactedNode(
          ref,
          view.nodes(ref).kind,
          s.changeClass,
          s.columns,
          s.path,
          s.likelyFailure,
          s.viaUnresolved
        )
      }
      .sortBy(_.node.name)
    (nodes, edges.toList.sortBy(e => (e.from.name, e.to.name)))
  }

  private def edgeState(
    up: NodeRef,
    upState: State,
    gone: Set[String],
    deleted: Boolean,
    consumer: NodeView,
    edges: mutable.LinkedHashSet[UnresolvedEdge]
  ): Option[State] = {
    // consumersOf only returns nodes with a shape
    consumer.shape.flatMap { shape =>
      // not read by name and domain: by a single-part name, ambiguous or whose table is gone
      val tableLevel =
        if (shape.inputs.contains(up)) shape.unresolved.orElse(shape.unresolvedInputs.get(up))
        else Some(s"ambiguous or unknown reference '${up.table}'")
      // identifiers and sources in a TaskShape are lower case, attribute names may not be
      val mentionsGone = gone.exists(shape.allIdentifiers.contains)
      def all(cls: ChangeClass, failure: Boolean = false, unresolved: Boolean = false) =
        Some(State(cls, AllColumns, Nil, failure, unresolved))

      tableLevel match {
        case Some(reason) =>
          edges += UnresolvedEdge(up, consumer.ref, reason)
          // an opaque SQL that still mentions a gone column is as likely to fail as a parsed one
          all(upState.changeClass, failure = deleted || mentionsGone, unresolved = true)
        case None if deleted =>
          all(Breaking, failure = true)
        case None if mentionsGone =>
          all(Breaking, failure = true)
        case None =>
          upState.columns match {
            case AllColumns => all(upState.changeClass)
            case Cols(names) =>
              val cs = names.map(_.toLowerCase)
              if (upState.changeClass == NonBreaking) {
                if (shape.starInputs.contains(up))
                  Some(State(NonBreaking, Cols(cs), Nil, false, false))
                else None
              } else if (cs.exists(shape.rowIdentifiers.contains)) all(Breaking)
              else {
                val reached = shape.columns
                  .filter(_.sources.exists(s => s.node == up && cs.contains(s.column)))
                  .map(_.name)
                  .toSet
                if (reached.isEmpty) None
                else Some(State(Breaking, Cols(reached), Nil, false, false))
              }
          }
      }
    }
  }
}
