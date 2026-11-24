package ai.starlake.dbt.manifest

import java.nio.file.Path

final case class DbtColumn(
  name: String,
  description: String,
  dataType: Option[String],
  docBlocks: List[String]
)

final case class DbtTestMetadata(name: String, namespace: Option[String], kwargs: Map[String, Any])

/** A node of the manifest: model, seed, test, snapshot, analysis, as dbt resolved it */
final case class DbtNode(
  uniqueId: String,
  resourceType: String,
  packageName: String,
  name: String,
  database: Option[String],
  schema: String,
  alias: String,
  version: Option[String],
  latestVersion: Option[String],
  language: String,
  rawCode: String,
  originalFilePath: String,
  rootPath: Option[String],
  description: String,
  docBlocks: List[String],
  columns: List[DbtColumn],
  config: Map[String, Any],
  tags: List[String],
  dependsOnNodes: List[String],
  dependsOnMacros: List[String],
  testMetadata: Option[DbtTestMetadata],
  attachedNode: Option[String],
  columnName: Option[String]
) {
  def configString(key: String): Option[String] =
    config.get(key).collect { case s: String if s.nonEmpty => s }

  def materialized: String = configString("materialized").getOrElse("view")

  def enabled: Boolean = !config.get("enabled").contains(false)
}

final case class DbtPeriod(count: Int, period: String)

final case class DbtFreshness(warnAfter: Option[DbtPeriod], errorAfter: Option[DbtPeriod])

final case class DbtSource(
  uniqueId: String,
  packageName: String,
  sourceName: String,
  name: String,
  database: Option[String],
  schema: String,
  identifier: String,
  description: String,
  docBlocks: List[String],
  columns: List[DbtColumn],
  freshness: Option[DbtFreshness],
  loadedAtField: Option[String],
  tags: List[String],
  enabled: Boolean
)

final case class DbtMacro(uniqueId: String, packageName: String, name: String, macroSql: String)

final case class DbtDoc(uniqueId: String, packageName: String, name: String, blockContents: String)

final case class DbtManifestMetadata(
  schemaVersion: Int,
  dbtVersion: String,
  adapterType: String,
  projectName: String
)

/** @param reportedOnly
  *   (kind, unique id) of the objects that have no Starlake equivalent: exposures, metrics,
  *   semantic models, saved queries, unit tests
  */
final case class DbtManifest(
  metadata: DbtManifestMetadata,
  nodes: List[DbtNode],
  disabled: List[DbtNode],
  sources: List[DbtSource],
  macros: List[DbtMacro],
  docs: List[DbtDoc],
  reportedOnly: List[(String, String)],
  manifestPath: Path
) {

  /** The dbt project: manifest.json is in its target/ folder */
  def projectRoot: Path = manifestPath.toAbsolutePath.getParent.getParent

  lazy val nodesById: Map[String, DbtNode] = (nodes ++ disabled).map(n => n.uniqueId -> n).toMap
  lazy val sourcesById: Map[String, DbtSource] = sources.map(s => s.uniqueId -> s).toMap
  lazy val docsById: Map[String, DbtDoc] = docs.map(d => d.uniqueId -> d).toMap
  lazy val macrosById: Map[String, DbtMacro] = macros.map(m => m.uniqueId -> m).toMap

  def nodesOfType(resourceType: String): List[DbtNode] =
    nodes.filter(_.resourceType == resourceType)
}
