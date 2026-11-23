package ai.starlake.impact

/** A column of a node, read by a consumer. */
final case class SourceCol(node: NodeRef, column: String)

/** One output column of a task. `fingerprint` changes whenever the definition of the column
  * changes, including inside CTEs. `sources` are the input-table columns it is computed from.
  */
final case class ColumnDef(name: String, fingerprint: String, sources: Set[SourceCol])

/** What the impact analysis knows about the SQL of a task.
  *
  * @param inputs
  *   every table the SQL reads, CTE names excluded, whether or not it exists in the project
  * @param columns
  *   output columns, empty when `unresolved` is set
  * @param starInputs
  *   inputs read through `*`: new columns upstream flow into this task
  * @param rowFingerprint
  *   the SQL with every SELECT list blanked: differs when WHERE, JOIN, GROUP BY, DISTINCT or the
  *   sources change
  * @param rowIdentifiers
  *   identifiers appearing in the row-level part (WHERE, JOIN, GROUP BY ...)
  * @param allIdentifiers
  *   identifiers appearing anywhere in the SQL
  * @param unresolvedInputs
  *   inputs whose columns are unknown, with the reason: impact through them is table level
  * @param unresolved
  *   set when the SQL could not be analyzed at all (parse failure, python task)
  */
final case class TaskShape(
  inputs: Set[NodeRef],
  columns: List[ColumnDef],
  starInputs: Set[NodeRef],
  rowFingerprint: String,
  rowIdentifiers: Set[String],
  allIdentifiers: Set[String],
  unresolvedInputs: Map[NodeRef, String],
  unresolved: Option[String]
) {
  def column(name: String): Option[ColumnDef] = columns.find(_.name == name)
}

object TaskShape {
  def unresolved(reason: String, inputs: Set[NodeRef], allIdentifiers: Set[String]): TaskShape =
    TaskShape(inputs, Nil, Set.empty, "", Set.empty, allIdentifiers, Map.empty, Some(reason))
}

/** A declared attribute. `key` is the source name (stable across a rename), `finalName` is the
  * column name consumers see.
  */
final case class AttributeView(key: String, finalName: String, tpe: String)

/** @param structural
  *   fingerprint of what changes which rows end up in the table: write strategy, merge key,
  *   partition, clustering, table filter
  * @param metadata
  *   fingerprint of what changes no data: comment, tags, ACL, RLS, expectations
  */
final case class NodeView(
  ref: NodeRef,
  kind: NodeKind,
  attributes: List[AttributeView],
  shape: Option[TaskShape],
  structural: String,
  metadata: String
) {

  /** Columns this node exposes to consumers. */
  def outputColumns: List[String] =
    shape.map(_.columns.map(_.name)).filter(_.nonEmpty).getOrElse(attributes.map(_.finalName))
}

/** @param otherFiles
  *   relative path to content hash, for files listed but not traced
  */
final case class SnapshotView(
  label: String,
  nodes: Map[NodeRef, NodeView],
  otherFiles: Map[String, String]
) {
  def consumersOf(ref: NodeRef): List[NodeView] =
    nodes.values
      .filter(n => n.ref != ref && n.shape.exists(_.inputs.contains(ref)))
      .toList
      .sortBy(_.ref.name)
}
