package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.job.Main
import ai.starlake.job.site.stats.StatsRunner
import ai.starlake.schema.handlers.SchemaHandler
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
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
    val stats =
      if (config.stats) Some(StatsRunner(config.statsTimeoutSeconds.seconds).run(model.nodes))
      else None
    SiteWriter.write(config.outputPath, model, stats)

    val warned = model.nodes.values.filter(_.warnings.nonEmpty)
    warned.foreach(n => n.warnings.foreach(w => logger.warn(s"${n.id}: $w")))
    stats.foreach(_.connections.foreach { case (ref, cs) =>
      cs.error.foreach(e => logger.warn(s"stats $ref (${cs.status}): $e"))
    })
    val failed = stats.map(_.connections.values.count(_.status == "error"))
    val summary =
      s"Site generated in ${config.outputPath.pathAsString}: ${model.nodes.size} nodes, " +
      s"${model.edges.size} edges, ${warned.size} nodes with warnings" +
      failed.map(n => s", $n connections failed").getOrElse("")
    logger.info(summary)
    if (Main.cliMode) println(summary)

    if (config.serve) serve()
  }

  /** Port the serve loop is bound to, or 0 when not serving. For tests. */
  @volatile private[site] var servingPort: Int = 0

  /** The shutdown hook of the last serve loop. For tests. */
  @volatile private[site] var lastHook: Option[Thread] = None

  /** Serves the output directory until the calling thread is interrupted. A JVM shutdown only stops
    * the server through the hook, it never lets this method return.
    */
  private[site] def serve(): Unit = {
    val server = new SiteServer(config.outputPath, config.port)
    val bound = server.start()
    val hook = new Thread(() => server.stop())
    lastHook = Some(hook)
    try {
      Runtime.getRuntime.addShutdownHook(hook)
      servingPort = bound
      val msg = s"Serving the site on http://127.0.0.1:$bound/ (Ctrl-C to stop)"
      logger.info(msg)
      if (Main.cliMode) println(msg)
      try new java.util.concurrent.CountDownLatch(1).await()
      catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException("Site serving interrupted", e)
      }
    } finally {
      servingPort = 0
      server.stop()
      // removing a hook throws IllegalStateException while the JVM is shutting down
      Try(Runtime.getRuntime.removeShutdownHook(hook))
    }
  }
}
