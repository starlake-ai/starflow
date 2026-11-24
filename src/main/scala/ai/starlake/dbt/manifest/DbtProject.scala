package ai.starlake.dbt.manifest

import ai.starlake.dbt.DbtImportException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import java.nio.file.{Files, Path}
import scala.util.Try

/** What the import reads from dbt_project.yml: the manifest does not hold it */
final case class DbtProjectFile(
  name: String,
  profile: Option[String],
  vars: Map[String, Any],
  dispatch: Map[String, List[String]],
  onRunStart: List[String],
  onRunEnd: List[String]
)

object DbtProjectFile {
  private val yaml = new ObjectMapper(new YAMLFactory())

  /** @return None when the project root has no dbt_project.yml */
  def read(projectRoot: Path): Try[Option[DbtProjectFile]] = Try {
    val file = projectRoot.resolve("dbt_project.yml")
    if (!Files.isRegularFile(file)) None
    else {
      val root = DbtJson.toScala(yaml.readTree(file.toFile)) match {
        case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
        case _            => throw new DbtImportException(s"$file is not a YAML map")
      }
      def strings(key: String): List[String] = root.get(key) match {
        case Some(s: String)  => List(s)
        case Some(l: List[_]) => l.map(_.toString)
        case _                => Nil
      }
      val dispatch = root.get("dispatch") match {
        case Some(entries: List[_]) =>
          entries.collect { case m: Map[_, _] =>
            val e = m.asInstanceOf[Map[String, Any]]
            e.getOrElse("macro_namespace", "").toString ->
            (e.get("search_order") match {
              case Some(l: List[_]) => l.map(_.toString)
              case _                => Nil
            })
          }.toMap
        case _ => Map.empty[String, List[String]]
      }
      Some(
        DbtProjectFile(
          name = root.get("name").map(_.toString).getOrElse(""),
          profile = root.get("profile").collect { case s: String => s },
          vars = root.get("vars") match {
            case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, Any]]
            case _                  => Map.empty
          },
          dispatch = dispatch,
          onRunStart = strings("on-run-start"),
          onRunEnd = strings("on-run-end")
        )
      )
    }
  }
}

/** The dbt target that `target.*` renders, and the DuckDB file of a DuckDB target */
final case class DbtTarget(
  name: String,
  adapterType: String,
  schema: Option[String],
  database: Option[String],
  duckdbPath: Option[Path]
)

object DbtProfiles {
  private val yaml = new ObjectMapper(new YAMLFactory())

  /** @param projectRoot
    *   a relative DuckDB path is relative to it, the directory dbt runs from
    */
  def read(
    profilesFile: Path,
    profileName: String,
    targetName: Option[String],
    projectRoot: Path
  ): Try[DbtTarget] = Try {
    if (!Files.isRegularFile(profilesFile))
      throw new DbtImportException(s"profiles file not found: $profilesFile")
    val root = DbtJson.toScala(yaml.readTree(profilesFile.toFile)) match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
      case _            => throw new DbtImportException(s"$profilesFile is not a YAML map")
    }
    val profile = root.get(profileName) match {
      case Some(m: Map[_, _]) => m.asInstanceOf[Map[String, Any]]
      case _ => throw new DbtImportException(s"profile '$profileName' not found in $profilesFile")
    }
    val target = targetName.orElse(profile.get("target").map(_.toString)).getOrElse("dev")
    val output = profile.get("outputs").collect { case m: Map[_, _] => m } match {
      case Some(outputs) =>
        outputs.asInstanceOf[Map[String, Any]].get(target) match {
          case Some(o: Map[_, _]) => o.asInstanceOf[Map[String, Any]]
          case _ =>
            throw new DbtImportException(
              s"target '$target' not found or not a map in profile '$profileName' of $profilesFile"
            )
        }
      case None => throw new DbtImportException(s"profile '$profileName' has no outputs")
    }
    def str(key: String): Option[String] = output.get(key).collect {
      case s: String if s.nonEmpty => s
    }
    val adapterType = str("type").getOrElse("")
    val duckdbPath =
      if (adapterType != "duckdb") None
      else
        str("path").filterNot(p => p == ":memory:" || p.startsWith("md:")).map { p =>
          val path = Path.of(p)
          if (path.isAbsolute) path else projectRoot.resolve(path).normalize()
        }
    val database = str("database")
      .orElse(str("dbname"))
      .orElse(str("project"))
      .orElse(duckdbPath.map(_.getFileName.toString.replaceAll("\\.[^.]*$", "")))
    DbtTarget(target, adapterType, str("schema").orElse(str("dataset")), database, duckdbPath)
  }
}

/** dbt's var() lookup: --vars, then the vars scoped to the package, then the global ones */
final case class DbtVars(
  global: Map[String, Any],
  byPackage: Map[String, Map[String, Any]],
  cli: Map[String, Any]
) {
  def lookup(packageName: String, name: String): Option[Any] =
    cli
      .get(name)
      .orElse(byPackage.get(packageName).flatMap(_.get(name)))
      .orElse(global.get(name))
}

object DbtVars {
  private val yaml = new ObjectMapper(new YAMLFactory())

  /** @param packages
    *   the packages of the manifest: a top-level key that names one holds the vars of that package
    */
  def apply(
    projectVars: Map[String, Any],
    packages: Set[String],
    cli: Map[String, Any]
  ): DbtVars = {
    val (scoped, global) = projectVars.partition { case (k, v) =>
      packages.contains(k) && v.isInstanceOf[Map[_, _]]
    }
    DbtVars(global, scoped.map { case (k, v) => k -> v.asInstanceOf[Map[String, Any]] }, cli)
  }

  /** --vars, a YAML or JSON map as dbt reads it */
  def parseCli(text: String): Try[Map[String, Any]] = Try {
    DbtJson.toScala(yaml.readTree(text)) match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
      case _            => throw new DbtImportException(s"--vars is not a YAML or JSON map: $text")
    }
  }
}
