package ai.starlake.semantic

/** Immutable result of planning one semantic model for the SML export. Names are SML
  * `unique_name`s; column names are dataset column names.
  */
object SMLPlan {

  case class Column(name: String, dataType: String, sql: Option[String])

  case class Connection(name: String, database: Option[String], schema: Option[String])

  case class Dataset(
    name: String,
    description: Option[String],
    connection: String,
    table: String,
    columns: List[Column]
  )

  case class LevelAttribute(
    name: String,
    label: String,
    description: Option[String],
    dataset: String,
    keyColumns: List[String],
    nameColumn: String,
    sortColumn: String,
    isUniqueKey: Boolean,
    timeUnit: Option[String],
    hidden: Boolean
  )

  case class SecondaryAttribute(
    name: String,
    label: String,
    description: Option[String],
    dataset: String,
    column: String,
    hidden: Boolean
  )

  /** `levels` holds level attribute names, coarse to fine. */
  case class Hierarchy(
    name: String,
    label: String,
    description: Option[String],
    levels: List[String]
  )

  case class EmbeddedRelationship(
    name: String,
    dataset: String,
    joinColumns: List[String],
    hierarchy: String,
    level: String,
    toDimension: String,
    toLevel: String,
    rolePlay: Option[String]
  )

  /** Secondary attributes belong to `leafLevel` in the first hierarchy. */
  case class Dimension(
    name: String,
    label: String,
    description: Option[String],
    isTime: Boolean,
    isDegenerate: Boolean,
    levelAttributes: List[LevelAttribute],
    hierarchies: List[Hierarchy],
    leafLevel: String,
    secondaryAttributes: List[SecondaryAttribute],
    relationships: List[EmbeddedRelationship]
  )

  case class Metric(
    name: String,
    label: String,
    description: Option[String],
    dataset: String,
    column: String,
    method: String,
    hidden: Boolean
  )

  case class Calculation(
    name: String,
    label: String,
    description: Option[String],
    expression: String,
    hidden: Boolean
  )

  case class ModelRelationship(
    name: String,
    dataset: String,
    joinColumns: List[String],
    toDimension: String,
    toLevel: String,
    rolePlay: Option[String]
  )

  case class Stats(
    skippedHierarchies: Int,
    skippedDimensions: Int,
    skippedRelationships: Int,
    fallbackCalculations: Int
  )

  case class Model(
    name: String,
    description: Option[String],
    connections: List[Connection],
    datasets: List[Dataset],
    dimensions: List[Dimension],
    metrics: List[Metric],
    calculations: List[Calculation],
    relationships: List[ModelRelationship],
    degenerateDimensions: List[String],
    usesTimeHierarchies: Boolean,
    stats: Stats
  )
}
