package ai.starlake.dbt.write

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.dbt.DbtImportException
import ai.starlake.dbt.mapping.{ConnectionEdit, CopyOutput, FileOutput, ImportPlan}
import ai.starlake.utils.YamlSerde
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.fs.Path

import scala.util.Try

/** Writes an import plan into the Starlake project. Without force, nothing is written when one of
  * the files exists, or when application.sl.yml already has the connection. With force, only the
  * files of the plan are overwritten: nothing is deleted.
  */
object ProjectWriter {

  def reportPath(implicit settings: Settings): Path =
    new Path(settings.appConfig.root, "dbt-import-report.md")

  def applicationPath(implicit settings: Settings): Path =
    new Path(DatasetArea.metadata, "application.sl.yml")

  private def normalized(path: Path): java.nio.file.Path = {
    val p = path.toUri.getPath
    java.nio.file.Paths.get(if (p.startsWith("/")) p else "/" + p).toAbsolutePath.normalize
  }

  /** The outputs of the plan that are not under the metadata folder or the datasets folder (the
    * report excepted): normalized absolute paths are compared, so that a sibling folder that shares
    * a prefix does not pass. Nothing must be written when this is not empty.
    */
  def escaping(plan: ImportPlan)(implicit settings: Settings): List[Path] = {
    val roots =
      List(normalized(DatasetArea.metadata), normalized(new Path(settings.appConfig.datasets)))
    val report = normalized(reportPath)
    plan.outputs.map(_.path).filterNot { p =>
      val n = normalized(p)
      n == report || roots.exists(n.startsWith)
    }
  }

  def requireConfined(plan: ImportPlan)(implicit settings: Settings): Unit = {
    val outside = escaping(plan)
    if (outside.nonEmpty)
      throw new DbtImportException(
        s"${outside.size} output path(s) are outside the project, nothing written: " +
        outside.mkString(", ")
      )
  }

  /** The files of the plan that exist, the report and application.sl.yml included */
  def conflicts(plan: ImportPlan)(implicit settings: Settings): List[Path] = {
    val storage = settings.storageHandler()
    val files = (plan.outputs.map(_.path) :+ reportPath).filter(storage.exists)
    val application = plan.connection.toList.flatMap { edit =>
      if (
        storage.exists(applicationPath) &&
        applicationRoot().path("application").path("connections").has(edit.name)
      )
        List(applicationPath)
      else Nil
    }
    files ++ application
  }

  /** @return the files written */
  def write(plan: ImportPlan, force: Boolean)(implicit settings: Settings): Try[List[Path]] = Try {
    requireConfined(plan)
    val existing = conflicts(plan)
    if (existing.nonEmpty && !force)
      throw new IllegalStateException(
        s"${existing.size} file(s) exist, pass --force to overwrite them"
      )
    val storage = settings.storageHandler()
    val written = plan.outputs.map {
      case FileOutput(path, content) =>
        storage.mkdirs(path.getParent)
        storage.write(content, path)
        path
      case CopyOutput(path, source) =>
        storage.mkdirs(path.getParent)
        storage.copyFromLocal(new Path(source.toUri), path)
        path
    }
    val application = plan.connection.map { edit => editApplication(edit); applicationPath }
    storage.write(plan.report.toMarkdown, reportPath)
    written ++ application.toList :+ reportPath
  }

  private def applicationRoot()(implicit settings: Settings): ObjectNode = {
    val storage = settings.storageHandler()
    if (storage.exists(applicationPath))
      YamlSerde.mapper.readTree(storage.read(applicationPath)) match {
        case o: ObjectNode => o
        case _             => YamlSerde.mapper.createObjectNode()
      }
    else YamlSerde.mapper.createObjectNode()
  }

  private def child(parent: ObjectNode, name: String): ObjectNode = parent.get(name) match {
    case o: ObjectNode => o
    case _             => parent.putObject(name)
  }

  /** Adds the DuckDB connection of the dbt target, and makes it the default when there is none. The
    * file is rewritten by Jackson: its comments are not kept.
    */
  def editApplication(edit: ConnectionEdit)(implicit settings: Settings): Unit = {
    val root = applicationRoot()
    if (!root.has("version")) root.put("version", Settings.latestSchemaVersion)
    val application = child(root, "application")
    val connection = child(application, "connections").putObject(edit.name)
    connection.put("type", "jdbc")
    connection.put("loader", "native")
    val options = connection.putObject("options")
    options.put("url", edit.url)
    options.put("driver", "org.duckdb.DuckDBDriver")
    if (application.path("connectionRef").asText("").isEmpty)
      application.put("connectionRef", edit.name)
    val auditSink = child(child(application, "audit"), "sink")
    if (auditSink.path("connectionRef").asText("").isEmpty)
      auditSink.put("connectionRef", edit.name)
    settings.storageHandler().write(YamlSerde.mapper.writeValueAsString(root), applicationPath)
  }
}
