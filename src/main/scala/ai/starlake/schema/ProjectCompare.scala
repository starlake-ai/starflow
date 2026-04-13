package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.schema.model.{Project, ProjectDiff}
import ai.starlake.utils.Utils
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import scala.util.Try

object ProjectCompare {
  def run(args: Array[String])(implicit settings: Settings): Try[Unit] =
    ProjectCompareCmd.run(args.toIndexedSeq, settings.schemaHandler()).map(_ => ())

  def compare(config: ProjectCompareConfig)(implicit settings: Settings): Unit = {
    val diff: ProjectDiff = Project.compare(config)
    val result = applyTemplate(diff, config.template)

    config.output match {
      case None =>
        println(result)
      case Some(output) =>
        settings.storageHandler().write(result, new Path(output))
    }
  }

  def applyTemplate(
    diff: ProjectDiff,
    templatePath: Option[String]
  )(implicit settings: Settings): String = {
    val content = templatePath match {
      case None =>
        val stream = getClass.getResourceAsStream("/templates/compare/index.html.j2")
        val source = scala.io.Source.fromInputStream(stream)
        try {
          source.mkString
        } finally {
          source.close()
          stream.close()
        }
      case Some(path) =>
        settings.storageHandler().read(new Path(path))
    }
    Utils.parseJinjaTpl(
      content,
      Map[String, Object](
        "diff"      -> diff,
        "timestamp" -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
      )
    )
  }

}
