package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.schema.handlers.SchemaHandler
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

/** Entry point used by the CLI and by starlake-api (SiteService). */
class SiteHandler(config: SiteConfig, schemaHandler: SchemaHandler)(implicit val settings: Settings)
    extends LazyLogging {

  def buildModel(): SiteModel = ModelBuilder(schemaHandler).build()

  def run(): Try[Unit] = Try {
    if (config.clean.getOrElse(false))
      config.outputPath.delete(swallowIOExceptions = true)
    config.outputPath.createDirectoryIfNotExists(createParents = true)

    val model = buildModel()
    SiteWriter.write(config.outputPath, model, stats = None)

    val warned = model.nodes.values.filter(_.warnings.nonEmpty)
    warned.foreach(n => n.warnings.foreach(w => logger.warn(s"${n.id}: $w")))
    val summary =
      s"Site generated in ${config.outputPath.pathAsString}: ${model.nodes.size} nodes, " +
      s"${model.edges.size} edges, ${warned.size} nodes with warnings"
    logger.info(summary)
    if (Main.cliMode) println(summary)
  }
}
