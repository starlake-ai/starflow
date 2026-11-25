package ai.starlake.dbt.manifest

/** A table as Starlake names it: domain = dbt schema, table = alias (identifier for a source).
  * `database` is set only when it is not the default database of the project.
  */
final case class DbtRelation(database: Option[String], schema: String, identifier: String) {
  def render: String = (database.toList :+ schema :+ identifier).mkString(".")
}

/** Resolves ref() and source() as dbt resolved them in the manifest */
final class DbtRelations(manifest: DbtManifest) {
  private val refTypes = Set("model", "seed", "snapshot")
  private val root = manifest.metadata.projectName

  /** The most frequent database of the models and seeds: the one of the connection */
  val defaultDatabase: Option[String] =
    manifest.nodes
      .filter(n => refTypes.contains(n.resourceType))
      .flatMap(_.database)
      .groupBy(identity)
      .maxByOption { case (db, uses) => (uses.size, db) }
      .map(_._1)

  private def database(db: Option[String]): Option[String] = db.filterNot(defaultDatabase.contains)

  def ofNode(node: DbtNode): DbtRelation =
    DbtRelation(database(node.database), node.schema, node.alias)

  def ofSource(source: DbtSource): DbtRelation =
    DbtRelation(database(source.database), source.schema, source.identifier)

  /** ref('m'), ref('pkg', 'm'), ref('m', v=2): disabled nodes included, so that a reference to a
    * model that the import skips still renders its table
    */
  def ref(
    callerPackage: String,
    packageName: Option[String],
    name: String,
    version: Option[String]
  ): Option[DbtNode] = {
    val candidates = (manifest.nodes ++ manifest.disabled).filter { n =>
      refTypes.contains(n.resourceType) && n.name == name &&
      packageName.forall(_ == n.packageName) &&
      version.forall(v => n.version.contains(v))
    }
    val versioned =
      if (version.isEmpty && candidates.exists(_.version.isDefined))
        candidates.filter(n => n.version.isEmpty || n.version == n.latestVersion)
      else candidates
    versioned.sortBy { n =>
      if (n.packageName == callerPackage) 0 else if (n.packageName == root) 1 else 2
    }.headOption
  }

  def source(sourceName: String, tableName: String): Option[DbtSource] =
    manifest.sources.find(s => s.sourceName == sourceName && s.name == tableName)
}
