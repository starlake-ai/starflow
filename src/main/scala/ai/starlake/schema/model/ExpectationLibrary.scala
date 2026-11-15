package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings}
import org.apache.hadoop.fs.Path

object ExpectationLibrary {

  /** The files of expectation macros. A file that only holds `sl_` macros, such as the `Engine.j2`
    * of the bootstrapped projects, holds the SQL fragments that the expectations share, not
    * expectations: it is not a category.
    */
  def categories()(implicit settings: Settings): List[String] = {
    val path = DatasetArea.expectations
    settings
      .storageHandler()
      .list(path, recursive = false)
      .map(_.path.getName)
      .filter(_.endsWith(".j2"))
      .map(_.stripSuffix(".j2"))
      .filterNot { category =>
        val all = macros(category)
        all.nonEmpty && all.forall(_.startsWith("sl_"))
      }
  }

  /** The expectation macros of a file, without the `ignore_` and `sl_` ones */
  def expectations(category: String)(implicit settings: Settings): List[String] =
    macros(category).filter(m => !m.startsWith("ignore_") && !m.startsWith("sl_"))

  private def macros(category: String)(implicit settings: Settings): List[String] = {
    val path = DatasetArea.expectations
    val catPath = new Path(path, category + ".j2")
    val data = settings.storageHandler().read(catPath)
    val regex = """\{%\s*macro\s+(.*)\s*%}""".r
    regex.findAllMatchIn(data).map(_.group(1).trim).toList
  }
}
