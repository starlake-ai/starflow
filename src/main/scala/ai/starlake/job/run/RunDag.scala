package ai.starlake.job.run

import scala.collection.mutable

sealed trait RunNodeType

object RunNodeType {

  /** A transform task, executed via the transform path */
  case object Task extends RunNodeType

  /** A table defined in the project's load metadata, executed via the load path */
  case object LoadTable extends RunNodeType

  /** External table, view or CTE: not executable, satisfied immediately, kept only to
    * preserve transitive ordering through the graph
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
}
