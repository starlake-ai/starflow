package ai.starlake.job.run

import ai.starlake.lineage.TaskViewDependency

import scala.collection.mutable

object DagBuilder {

  /** Builds an executable DAG from the raw lineage edge list.
    *
    * @param deps
    *   raw entries from TaskViewDependency.dependencies: one per node occurrence, carrying
    *   at most one parent edge each
    * @param loadTables
    *   lowercase "domain.table" names defined in the project's load metadata
    * @return
    *   Left(cycle path, first id repeated last) or Right(dag)
    */
  def build(
    deps: List[TaskViewDependency],
    loadTables: Set[String]
  ): Either[List[String], RunDag] = {
    val loadTablesNorm = loadTables.map(_.toLowerCase)
    val nodes = mutable.Map[String, RunNode]()
    val parents = mutable.Map[String, Set[String]]().withDefaultValue(Set.empty)

    def lastTwoParts(name: String): String =
      name.toLowerCase.split('.').takeRight(2).mkString(".")

    def typeOf(name: String, typ: String): RunNodeType =
      typ match {
        case TaskViewDependency.TASK_TYPE => RunNodeType.Task
        case TaskViewDependency.TABLE_TYPE if loadTablesNorm.contains(lastTwoParts(name)) =>
          RunNodeType.LoadTable
        case _ => RunNodeType.Boundary
      }

    def rank(t: RunNodeType): Int =
      t match {
        case RunNodeType.Task      => 2
        case RunNodeType.LoadTable => 1
        case RunNodeType.Boundary  => 0
      }

    def register(name: String, typ: String): String = {
      val id = name.toLowerCase
      val candidate = RunNode(id, name, typeOf(name, typ))
      nodes.get(id) match {
        case Some(existing) if rank(existing.typ) >= rank(candidate.typ) => ()
        case _                                                          => nodes(id) = candidate
      }
      id
    }

    deps.foreach { entry =>
      val childId = register(entry.name, entry.typ)
      if (entry.hasParent()) {
        val parentId = register(entry.parent, entry.parentTyp)
        // A task reading its own output table (incremental pattern) is not a scheduling dependency
        if (parentId != childId)
          parents(childId) = parents(childId) + parentId
      }
    }
    nodes.keys.foreach(id => parents(id) = parents(id))

    findCycle(nodes.keySet.toSet, parents.toMap) match {
      case Some(cycle) => Left(cycle)
      case None        => Right(RunDag(nodes.toMap, parents.toMap))
    }
  }

  /** Kahn's algorithm; if nodes remain, every one of them sits on or downstream of a cycle,
    * and walking parent links from any of them must revisit a node.
    */
  private def findCycle(
    ids: Set[String],
    parents: Map[String, Set[String]]
  ): Option[List[String]] = {
    val pending = mutable.Map[String, mutable.Set[String]]()
    ids.foreach(id => pending(id) = mutable.Set[String]() ++= parents.getOrElse(id, Set.empty))

    val queue = mutable.Queue[String]() ++=
      pending.collect { case (id, ups) if ups.isEmpty => id }

    while (queue.nonEmpty) {
      val id = queue.dequeue()
      pending.remove(id)
      pending.foreach { case (child, ups) =>
        if (ups.remove(id) && ups.isEmpty) queue.enqueue(child)
      }
    }

    if (pending.isEmpty) None
    else {
      val path = mutable.ListBuffer[String]()
      val seen = mutable.Set[String]()
      var current = pending.keys.min
      while (!seen.contains(current)) {
        seen += current
        path += current
        current = pending(current).min // deterministic walk over remaining parents
      }
      val cycle = path.dropWhile(_ != current).toList :+ current
      Some(cycle)
    }
  }
}
