package ai.starlake.job.site

import ai.starlake.utils.JsonSerializer
import better.files.File

import scala.io.Source

object SiteWriter {
  val ModelFile = "sl-model.json"
  val StatsFile = "sl-stats.json"
  val DataMarker = "<!--sl-data-->"
  private val BundleRoot = "/site-ui/"

  def escapeForScript(json: String): String = json.replace("</", "<\\/")

  def inline(html: String, blocks: List[(String, String)]): String = {
    val tags = blocks
      .map { case (id, json) =>
        s"""<script type="application/json" id="$id">${escapeForScript(json)}</script>"""
      }
      .mkString("\n")
    if (html.contains(DataMarker)) html.replace(DataMarker, tags)
    else {
      val idx = html.lastIndexOf("</body>")
      if (idx >= 0) html.substring(0, idx) + tags + html.substring(idx) else html + tags
    }
  }

  private def resourceBytes(name: String): Array[Byte] = {
    val stream = getClass.getResourceAsStream(BundleRoot + name)
    if (stream == null) throw new IllegalStateException(s"Missing site bundle resource: $name")
    try stream.readAllBytes()
    finally stream.close()
  }

  private def bundleFiles(): List[String] =
    Source
      .fromBytes(resourceBytes("files.txt"), "UTF-8")
      .getLines()
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  def write(outputDir: File, model: SiteModel, stats: Option[AnyRef]): Unit = {
    outputDir.createDirectoryIfNotExists(createParents = true)
    val writer = JsonSerializer.mapper.writerWithDefaultPrettyPrinter()
    val modelJson = writer.writeValueAsString(model)
    val statsJson = stats.map(writer.writeValueAsString)

    (outputDir / ModelFile).overwrite(modelJson)
    statsJson match {
      case Some(json) => (outputDir / StatsFile).overwrite(json)
      case None       => (outputDir / StatsFile).delete(swallowIOExceptions = true)
    }

    bundleFiles().filterNot(_ == "index.html").foreach { f =>
      val target = outputDir / f
      target.parent.createDirectoryIfNotExists(createParents = true)
      target.writeByteArray(resourceBytes(f))
    }

    val index = new String(resourceBytes("index.html"), "UTF-8")
    val blocks = List("sl-model" -> modelJson) ++ statsJson.map("sl-stats" -> _).toList
    (outputDir / "index.html").overwrite(inline(index, blocks))
  }
}
