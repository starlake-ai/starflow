package ai.starlake.semantic

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

/** Makes SML `unique_name`s unique per object type. A later duplicate (compared ignoring case)
  * gets the first free `_2`, `_3`, ... suffix and every reference to it is rewritten.
  *
  * Datasets and measures (metrics and calculations share one namespace) are named through a
  * [[SMLNames.Namespace]] while they are planned, so that references to them (dataset names used
  * by every other object, hidden metrics named in calculation expressions) are built from the
  * final names. Dimensions and relationships are made unique by [[SMLNames.uniquify]] once the
  * plan is complete.
  */
private[semantic] object SMLNames extends LazyLogging {

  import SMLPlan._

  /** Names claimed so far for one object type. */
  final class Namespace(kind: String) {
    private val taken = mutable.Set[String]()

    /** `name`, or `name_<n>` with the smallest n >= 2 not claimed yet; the result is claimed. */
    def claim(name: String): String = {
      var unique = name
      var n = 1
      while (taken.contains(unique.toLowerCase)) {
        n += 1
        unique = s"${name}_$n"
      }
      if (unique != name) logger.warn(s"Duplicate $kind name '$name' renamed to '$unique'")
      taken += unique.toLowerCase
      unique
    }
  }

  /** `names` made unique in order. */
  def unique(kind: String, names: List[String]): List[String] = {
    val namespace = new Namespace(kind)
    names.map(namespace.claim)
  }

  /** Unique dimension names, model relationship names and embedded relationship names per
    * dimension. Relationship targets and degenerate dimension references follow the renames.
    */
  def uniquify(model: Model): Model = {
    val dimensionNames = unique("dimension", model.dimensions.map(_.name))
    // Relationships only target regular dimensions, whose names are unique among themselves
    // because they derive from unique dataset names.
    val renamedTargets = model.dimensions
      .zip(dimensionNames)
      .collect { case (d, name) if !d.isDegenerate => d.name -> name }
      .toMap
    def target(name: String): String = renamedTargets.getOrElse(name, name)
    def rolePlay(current: Option[String], name: String): Option[String] =
      current.map(_ => s"$name {0}")

    val dimensions = model.dimensions.zip(dimensionNames).map { case (d, name) =>
      val relationshipNames =
        unique(s"relationship of dimension '$name'", d.relationships.map(_.name))
      d.copy(
        name = name,
        relationships = d.relationships.zip(relationshipNames).map { case (r, n) =>
          r.copy(name = n, toDimension = target(r.toDimension), rolePlay = rolePlay(r.rolePlay, n))
        }
      )
    }
    val relationshipNames = unique("model relationship", model.relationships.map(_.name))
    val relationships = model.relationships.zip(relationshipNames).map { case (r, n) =>
      r.copy(name = n, toDimension = target(r.toDimension), rolePlay = rolePlay(r.rolePlay, n))
    }
    model.copy(
      dimensions = dimensions,
      relationships = relationships,
      degenerateDimensions = dimensions.filter(_.isDegenerate).map(_.name)
    )
  }
}
