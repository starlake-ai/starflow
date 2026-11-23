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
  */
case class SiteConfig(
  outputPath: File = File(".") / "site",
  clean: Option[Boolean] = Some(false),
  stats: Boolean = false,
  statsTimeoutSeconds: Int = 120
) extends ReportFormatConfig {
  def reportFormat: Option[String] = None
}
