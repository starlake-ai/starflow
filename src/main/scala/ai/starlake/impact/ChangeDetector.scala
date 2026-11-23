package ai.starlake.impact

import ai.starlake.impact.ChangeClass._

object ChangeDetector {

  def detect(oldView: SnapshotView, newView: SnapshotView): List[DirectChange] = {
    val refs = (oldView.nodes.keySet ++ newView.nodes.keySet).toList.sortBy(_.name)
    refs.flatMap { ref =>
      (oldView.nodes.get(ref), newView.nodes.get(ref)) match {
        case (None, Some(n)) =>
          Some(DirectChange(ref, n.kind, Added, Cols(Set.empty), Nil, List("added")))
        case (Some(o), None) =>
          Some(
            DirectChange(ref, o.kind, Breaking, AllColumns, Nil, List("deleted"), deleted = true)
          )
        case (Some(o), Some(n)) => compare(o, ownShape(o, n))
        case (None, None)       => None
      }
    }
  }

  /** Changed files that are not traced (DAGs, types, connections ...), and the files under
    * `metadata/load` and `metadata/transform` whose change no direct change of the nodes they
    * define explains (a field the snapshot does not read, a file of another kind): "not
    * classified". Sorted by path.
    */
  def otherChanges(
    oldView: SnapshotView,
    newView: SnapshotView,
    direct: List[DirectChange] = Nil
  ): List[String] = {
    def status(a: Option[String], b: Option[String]): Option[String] = (a, b) match {
      case (None, Some(_))              => Some("added")
      case (Some(_), None)              => Some("deleted")
      case (Some(a), Some(b)) if a != b => Some("changed")
      case _                            => None
    }
    val other = (oldView.otherFiles.keySet ++ newView.otherFiles.keySet).toList.flatMap { p =>
      status(oldView.otherFiles.get(p), newView.otherFiles.get(p)).map(s => p -> s"$p ($s)")
    }
    val explained = direct.map(_.node).toSet
    val definitions =
      (oldView.definitionFiles.keySet ++ newView.definitionFiles.keySet).toList.flatMap { p =>
        val before = oldView.definitionFiles.get(p)
        val after = newView.definitionFiles.get(p)
        val nodes = before.toSet.flatMap((f: DefinitionFile) => f.nodes) ++
          after.toSet.flatMap((f: DefinitionFile) => f.nodes)
        status(before.map(_.hash), after.map(_.hash))
          .filterNot(_ => nodes.exists(explained))
          .map(s => p -> s"$p ($s, not classified)")
      }
    (other ++ definitions).sortBy(_._1).map(_._2)
  }

  /** A task whose own SQL did not change keeps its old shape for the comparison: a different shape
    * can then only come from its inputs (a column removed, renamed or retyped upstream), which
    * propagation reports as impact, not as a change of this task.
    */
  private def ownShape(o: NodeView, n: NodeView): NodeView =
    if (
      o.kind == NodeKind.Task && n.kind == NodeKind.Task && o.definition.nonEmpty &&
      o.definition == n.definition
    ) n.copy(shape = o.shape)
    else n

  private def compare(o: NodeView, n: NodeView): Option[DirectChange] = {
    val oldReason = o.shape.flatMap(_.unresolved)
    val newReason = n.shape.flatMap(_.unresolved)
    val unresolvedReason = newReason.orElse(oldReason)
    val sqlChanged = (o.shape, n.shape) match {
      case (Some(a), Some(b)) =>
        a.rowFingerprint != b.rowFingerprint || a.allIdentifiers != b.allIdentifiers ||
        a.columns.map(c => (c.name, c.fingerprint)) != b.columns.map(c =>
          (c.name, c.fingerprint)
        ) ||
        // the reasons themselves are not compared: a parse error message embeds raw SQL text,
        // and the whole normalized SQL in rowFingerprint already catches every real edit
        a.unresolved.isDefined != b.unresolved.isDefined
      case (None, None) => false
      case _            => true
    }

    if (o.kind != n.kind) {
      Some(
        DirectChange(
          n.ref,
          n.kind,
          Breaking,
          AllColumns,
          Nil,
          List(s"replaced: ${o.kind.label} became ${n.kind.label}")
        )
      )
    } else if (unresolvedReason.isDefined && (sqlChanged || o.attributes != n.attributes)) {
      Some(
        DirectChange(
          n.ref,
          n.kind,
          Breaking,
          AllColumns,
          Nil,
          List(newReason match {
            case Some(r) => s"SQL could not be analyzed: ${firstLine(r)}"
            case None =>
              s"SQL now analyzable (previously: ${oldReason.map(firstLine).getOrElse("")})"
          }),
          unresolved = unresolvedReason
        )
      )
    } else if (o.structural != n.structural) {
      Some(
        DirectChange(
          n.ref,
          n.kind,
          Breaking,
          AllColumns,
          Nil,
          List(
            "write strategy / merge key / sink / connection / pre-post SQL / pattern / parsing / required attributes changed"
          )
        )
      )
    } else if (rowLevelChanged(o, n)) {
      Some(
        DirectChange(
          n.ref,
          n.kind,
          Breaking,
          AllColumns,
          Nil,
          List("row-level: WHERE / JOIN / GROUP BY / sources changed")
        )
      )
    } else {
      val details = n.kind match {
        case NodeKind.LoadTable => attributeChanges(o.attributes, n.attributes)
        case NodeKind.Task      => taskColumnChanges(o, n)
      }
      if (details.nonEmpty) {
        val breaking = details.filterNot(_.isInstanceOf[ColumnAdded])
        if (breaking.nonEmpty)
          Some(
            DirectChange(n.ref, n.kind, Breaking, Cols(breaking.map(_.column).toSet), details, Nil)
          )
        else
          Some(
            DirectChange(
              n.ref,
              n.kind,
              NonBreaking,
              Cols(details.map(_.column).toSet),
              details,
              Nil
            )
          )
      } else if (o.metadata != n.metadata) {
        Some(DirectChange(n.ref, n.kind, Metadata, Cols(Set.empty), Nil, List("metadata only")))
      } else None
    }
  }

  private def firstLine(text: String): String =
    text.linesIterator.map(_.trim).find(_.nonEmpty).getOrElse("")

  private def rowLevelChanged(o: NodeView, n: NodeView): Boolean =
    (o.shape, n.shape) match {
      case (Some(a), Some(b)) => a.rowFingerprint != b.rowFingerprint || a.inputs != b.inputs
      case (None, None)       => false
      // a shape that appears or disappears: nothing to compare column by column
      case _ => true
    }

  private def attributeChanges(
    before: List[AttributeView],
    after: List[AttributeView]
  ): List[ColumnChange] = {
    val oldByKey = before.map(a => a.key.toLowerCase -> a).toMap
    val newByKey = after.map(a => a.key.toLowerCase -> a).toMap
    val removed = before
      .filterNot(a => newByKey.contains(a.key.toLowerCase))
      .map(a => ColumnRemoved(a.finalName))
    val added =
      after.filterNot(a => oldByKey.contains(a.key.toLowerCase)).map(a => ColumnAdded(a.finalName))
    val updated = before.flatMap { a =>
      newByKey.get(a.key.toLowerCase).toList.flatMap { b =>
        val rename =
          if (!a.finalName.equalsIgnoreCase(b.finalName))
            List(ColumnRenamed(a.finalName, b.finalName))
          else Nil
        val retype =
          if (!a.tpe.equalsIgnoreCase(b.tpe)) List(TypeChanged(a.finalName, a.tpe, b.tpe)) else Nil
        rename ++ retype ++ dataChange(a, b).toList
      }
    }
    removed ++ updated ++ added
  }

  /** A field that changes the values of an existing column (script, default, privacy ...) changed.
    * An added attribute is a [[ColumnAdded]] whatever its fields.
    */
  private def dataChange(before: AttributeView, after: AttributeView): Option[ColumnChange] =
    if (before.dataFingerprint != after.dataFingerprint)
      Some(ExpressionChanged(before.finalName.toLowerCase))
    else None

  private def attributeDataChanges(
    before: List[AttributeView],
    after: List[AttributeView]
  ): List[ColumnChange] = {
    val newByKey = after.map(a => a.key.toLowerCase -> a).toMap
    before.flatMap(a => newByKey.get(a.key.toLowerCase).flatMap(b => dataChange(a, b)))
  }

  private def taskColumnChanges(o: NodeView, n: NodeView): List[ColumnChange] = {
    val before = o.shape.map(_.columns).getOrElse(Nil)
    val after = n.shape.map(_.columns).getOrElse(Nil)
    val oldNames = before.map(_.name).toSet
    val newNames = after.map(_.name).toSet
    val gone = before.filterNot(c => newNames.contains(c.name))
    val fresh = after.filterNot(c => oldNames.contains(c.name))
    // one-to-one: a new column is the rename of at most one old column
    val renames = gone
      .foldLeft((List.empty[(ColumnDef, ColumnDef)], fresh)) { case ((acc, left), g) =>
        left.find(_.fingerprint == g.fingerprint) match {
          case Some(f) => (acc :+ (g -> f), left.filterNot(_ eq f))
          case None    => (acc, left)
        }
      }
      ._1
    val renamedOld = renames.map(_._1.name).toSet
    val renamedNew = renames.map(_._2.name).toSet

    val renamed = renames.map { case (g, f) => ColumnRenamed(g.name, f.name) }
    val removed = gone.filterNot(c => renamedOld.contains(c.name)).map(c => ColumnRemoved(c.name))
    val added = fresh.filterNot(c => renamedNew.contains(c.name)).map(c => ColumnAdded(c.name))
    val expr = before.flatMap { b =>
      after
        .find(_.name == b.name)
        .filter(_.fingerprint != b.fingerprint)
        .map(_ => ExpressionChanged(b.name))
    }
    val oldTypes = o.attributes.map(a => a.finalName.toLowerCase -> a.tpe).toMap
    val retyped = n.attributes.flatMap { a =>
      oldTypes
        .get(a.finalName.toLowerCase)
        .filterNot(_.equalsIgnoreCase(a.tpe))
        .map(t => TypeChanged(a.finalName.toLowerCase, t, a.tpe))
    }
    val exprColumns = expr.map(_.column).toSet
    val dataChanged =
      attributeDataChanges(o.attributes, n.attributes).filterNot(c => exprColumns(c.column))
    removed ++ renamed ++ expr ++ dataChanged ++ retyped ++ added
  }
}
