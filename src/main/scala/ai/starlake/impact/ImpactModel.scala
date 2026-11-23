package ai.starlake.impact

sealed abstract class ChangeClass(val label: String, val rank: Int)

object ChangeClass {
  case object Metadata extends ChangeClass("METADATA", 0)
  case object Added extends ChangeClass("ADDED", 1)
  case object NonBreaking extends ChangeClass("NON_BREAKING", 2)
  case object Breaking extends ChangeClass("BREAKING", 3)

  val all: List[ChangeClass] = List(Metadata, Added, NonBreaking, Breaking)

  def worst(a: ChangeClass, b: ChangeClass): ChangeClass = if (a.rank >= b.rank) a else b
}

sealed trait Columns {
  def union(other: Columns): Columns
  def covers(other: Columns): Boolean
  def label: String
}

case object AllColumns extends Columns {
  def union(other: Columns): Columns = AllColumns
  def covers(other: Columns): Boolean = true
  def label: String = "*"
}

final case class Cols(names: Set[String]) extends Columns {
  def union(other: Columns): Columns = other match {
    case AllColumns => AllColumns
    case Cols(more) => Cols(names ++ more)
  }
  def covers(other: Columns): Boolean = other match {
    case AllColumns => false
    case Cols(more) => more.subsetOf(names)
  }
  def label: String = names.toList.sorted.mkString(", ")
}

sealed abstract class NodeKind(val label: String)

object NodeKind {
  case object LoadTable extends NodeKind("load")
  case object Task extends NodeKind("task")
}

final case class NodeRef(domain: String, table: String) {
  def name: String = if (domain.isEmpty) table else s"$domain.$table"
}

object NodeRef {
  def of(domain: String, table: String): NodeRef =
    NodeRef(Option(domain).getOrElse("").toLowerCase, table.toLowerCase)
}

sealed trait ColumnChange {
  def column: String
  def describe: String
}

final case class ColumnAdded(column: String) extends ColumnChange {
  def describe: String = s"column $column added"
}
final case class ColumnRemoved(column: String) extends ColumnChange {
  def describe: String = s"column $column removed"
}
final case class ColumnRenamed(column: String, to: String) extends ColumnChange {
  def describe: String = s"column $column renamed to $to"
}
final case class ExpressionChanged(column: String) extends ColumnChange {
  def describe: String = s"column $column expression changed"
}
final case class TypeChanged(column: String, from: String, to: String) extends ColumnChange {
  def describe: String = s"column $column type changed from $from to $to"
}

/** One changed node. `columns` are the columns whose values changed (AllColumns for row-level
  * changes). `reasons` are human readable, for example "row-level: WHERE / JOIN / GROUP BY
  * changed".
  */
final case class DirectChange(
  node: NodeRef,
  kind: NodeKind,
  changeClass: ChangeClass,
  columns: Columns,
  details: List[ColumnChange],
  reasons: List[String],
  unresolved: Option[String] = None,
  deleted: Boolean = false
) {

  /** Columns that no longer exist under their old name. Consumers that still mention them are
    * likely to fail.
    */
  def goneColumns: Set[String] = details.collect {
    case ColumnRemoved(c)    => c
    case ColumnRenamed(c, _) => c
  }.toSet
}

final case class ImpactedNode(
  node: NodeRef,
  kind: NodeKind,
  changeClass: ChangeClass,
  columns: Columns,
  path: List[String],
  likelyFailure: Boolean,
  viaUnresolvedEdge: Boolean
)

final case class UnresolvedEdge(from: NodeRef, to: NodeRef, reason: String)

final case class ImpactSummary(
  direct: Map[String, Int],
  impacted: Map[String, Int],
  likelyFailures: Int,
  hasBreaking: Boolean,
  hasNonMetadata: Boolean
)

/** `otherChanges` lists changed files that are not traced downstream (DAGs, connections,
  * application.sl.yml, env files, expectation macros).
  */
final case class ImpactReport(
  oldSide: String,
  newSide: String,
  direct: List[DirectChange],
  impacted: List[ImpactedNode],
  unresolved: List[UnresolvedEdge],
  otherChanges: List[String]
) {
  def isEmpty: Boolean = direct.isEmpty && otherChanges.isEmpty

  lazy val summary: ImpactSummary = {
    def count(classes: List[ChangeClass]): Map[String, Int] =
      ChangeClass.all.map(c => c.label -> classes.count(_ == c)).toMap
    val classes = direct.map(_.changeClass) ++ impacted.map(_.changeClass)
    ImpactSummary(
      direct = count(direct.map(_.changeClass)),
      impacted = count(impacted.map(_.changeClass)),
      likelyFailures = impacted.count(_.likelyFailure),
      hasBreaking = classes.contains(ChangeClass.Breaking),
      // otherChanges (DAGs, connections...) are METADATA by definition: they never trip
      // --fail-on non-breaking
      hasNonMetadata = classes.exists(_ != ChangeClass.Metadata)
    )
  }
}
