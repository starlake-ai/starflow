package ai.starlake.dbt.mapping

import ai.starlake.dbt.jinja.DbtJinja
import ai.starlake.dbt.manifest.{DbtManifest, DbtRelations}
import ai.starlake.schema.model.TableAttribute

import java.nio.file.Path
import scala.util.Try

/** @param connectionRef
  *   set on every task and load domain when the import's connection is not the project's default
  * @param inferSeed
  *   the attributes of a seed CSV (path, table name): Starlake's schema inference by default
  */
final case class DbtMappingContext(
  manifest: DbtManifest,
  relations: DbtRelations,
  jinja: DbtJinja,
  connectionRef: Option[String],
  inferSeed: (Path, String) => Try[List[TableAttribute]]
) {
  def adapterType: String = manifest.metadata.adapterType
}
