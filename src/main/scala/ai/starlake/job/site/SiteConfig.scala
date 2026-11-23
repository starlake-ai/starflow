package ai.starlake.job.site

import ai.starlake.job.ReportFormatConfig
import better.files.File

/** Site configuration
  * @param outputPath
  *   output directory
  * @param clean
  *   delete the output directory before generating
  * @param stats
  *   also query the warehouses and write sl-stats.json
  * @param statsTimeoutSeconds
  *   per-connection timeout for stats collection
  * @param serve
  *   after generating, serve the site on 127.0.0.1 until interrupted (CLI only)
  * @param port
  *   port used by serve (default 8080)
  */
case class SiteConfig(
  outputPath: File = File(".") / "site",
  clean: Option[Boolean] = Some(false),
  stats: Boolean = false,
  statsTimeoutSeconds: Int = 120,
  serve: Boolean = false,
  port: Int = 8080
) extends ReportFormatConfig {
  def reportFormat: Option[String] = None
}
