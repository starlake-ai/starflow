package ai.starlake.job.site

import ai.starlake.job.ReportFormatConfig
import better.files.File

/** Site configuration
  * @param outputPath
  *   output directory
  * @param clean
  *   delete the output directory before generating
  */
case class SiteConfig(
  outputPath: File = File(".") / "site",
  clean: Option[Boolean] = Some(false)
) extends ReportFormatConfig {
  def reportFormat: Option[String] = None
}
