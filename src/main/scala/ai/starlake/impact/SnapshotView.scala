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

  /** The unresolved reason of a python task: its inputs are unknown. */
  val PythonTask = "python task"

  def unresolved(reason: String, inputs: Set[NodeRef], allIdentifiers: Set[String]): TaskShape =
    TaskShape(inputs, Nil, Set.empty, "", Set.empty, allIdentifiers, Map.empty, Some(reason))
}

/** A declared attribute. `key` is the source name (stable across a rename), `finalName` is the
  * column name consumers see. `dataFingerprint` changes when a field that changes the values of the
  * column changes (script, default, trim, required, privacy, nested attributes ...): a change of
  * the column, not of the whole table.
  */
final case class AttributeView(
  key: String,
  finalName: String,
  tpe: String,
  dataFingerprint: String = ""
)

/** @param structural
  *   fingerprint of what changes which rows end up in the table, or where: write strategy, merge
  *   key, sink, connection, pre/post SQL, pattern, table filter, parsing
  * @param metadata
  *   fingerprint of what changes no data: comment, tags, ACL, RLS, expectations, schedule, DAG
  */
final case class NodeView(
  ref: NodeRef,
  kind: NodeKind,
  attributes: List[AttributeView],
  shape: Option[TaskShape],
  structural: String,
  metadata: String
)

/** A file under `metadata/load` or `metadata/transform`, with the nodes it defines by the file name
  * convention: a table or task file defines the node of the same name, a `_config.sl.yml` every
  * node of its domain or transform.
  */
final case class DefinitionFile(hash: String, nodes: Set[NodeRef])

/** @param otherFiles
  *   relative path to content hash, for files listed but not traced: everything under `metadata/`
  *   but `load/` and `transform/`
  * @param definitionFiles
  *   relative path of every file under `metadata/load` and `metadata/transform`: a change no direct
  *   change of its nodes explains is listed as not classified
  * @param collisions
  *   nodes defined more than once (two tasks, a load table and a task ...), with what defines them:
  *   only one definition is in `nodes`
  */
final case class SnapshotView(
  label: String,
  nodes: Map[NodeRef, NodeView],
  otherFiles: Map[String, String],
  definitionFiles: Map[String, DefinitionFile] = Map.empty,
  collisions: Map[NodeRef, String] = Map.empty
) {

  /** Consumers of each input, and of each table name for the inputs that keep no domain. */
  private lazy val (byInput, byTableName): (
    Map[NodeRef, List[NodeView]],
    Map[String, List[NodeView]]
  ) = {
    val pairs = nodes.values.toList.flatMap(n => n.shape.toList.flatMap(_.inputs).map(_ -> n))
    val (unqualified, qualified) = pairs.partition(_._1.domain.isEmpty)
    (
      qualified.groupBy(_._1).map { case (ref, ps) => ref -> ps.map(_._2) },
      unqualified.groupBy(_._1.table).map { case (table, ps) => table -> ps.map(_._2) }
    )
  }

  /** The nodes reading `ref`. An input without domain (a single-part name that was ambiguous, or
    * whose table is gone) reads every node of that table name: over-reporting is safer than missing
    * an impact.
    */
  def consumersOf(ref: NodeRef): List[NodeView] =
    (byInput.getOrElse(ref, Nil) ++ byTableName.getOrElse(ref.table, Nil))
      .filter(_.ref != ref)
      .distinctBy(_.ref)
      .sortBy(_.ref.name)
}
