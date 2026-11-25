package ai.starlake.dbt.manifest

import ai.starlake.dbt.DbtImportException
import ai.starlake.dbt.jinja.DbtConfigRenderer
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

/** The dbt target that `target.*` renders, and the DuckDB file of a DuckDB target
  *
  * @param unrendered
  *   the fields of profiles.yml that the import reads but cannot render (an env var not set), with
  *   the reason: they are left out, and no connection is written
  */
final case class DbtTarget(
  name: String,
  adapterType: String,
  schema: Option[String],
  database: Option[String],
  duckdbPath: Option[Path],
  unrendered: List[(String, String)] = Nil
)

object DbtProfiles {
  private val yaml = new ObjectMapper(new YAMLFactory())

  /** @param projectRoot
    *   a relative DuckDB path is relative to it, the directory dbt runs from
    * @param renderer
    *   renders the values that hold Jinja (`{{ env_var('X') }}`)
    */
  def read(
    profilesFile: Path,
    profileName: String,
    targetName: Option[String],
    projectRoot: Path,
    renderer: DbtConfigRenderer = new DbtConfigRenderer(sys.env.get, Map.empty)
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
    val target = targetName
      .orElse(profile.get("target").map { t =>
        renderer
          .render(t.toString)
          .fold(
            e =>
              throw new DbtImportException(
                s"target of profile '$profileName' in $profilesFile: $e; pass --target"
              ),
            identity
          )
      })
      .getOrElse("dev")
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
    val unrendered = scala.collection.mutable.ListBuffer.empty[(String, String)]
    def str(key: String): Option[String] = output
      .get(key)
      .collect { case s: String if s.nonEmpty => s }
      .flatMap { s =>
        renderer.render(s) match {
          case Right(value) => Some(value).filter(_.nonEmpty)
          case Left(reason) =>
            unrendered += key -> reason
            None
        }
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
    val schema = str("schema").orElse(str("dataset"))
    DbtTarget(target, adapterType, schema, database, duckdbPath, unrendered.toList)
  }
}

/** A var of dbt_project.yml whose value holds Jinja that cannot be rendered: the models that read
  * it are skipped with the reason
  */
final case class DbtUnrenderedVar(reason: String)

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

  /** The vars of dbt_project.yml, their values that hold Jinja rendered: one that cannot be
    * rendered becomes a [[DbtUnrenderedVar]]. The vars scoped to a package are rendered one by one.
    */
  def rendered(
    projectVars: Map[String, Any],
    packages: Set[String],
    renderer: DbtConfigRenderer
  ): Map[String, Any] = {
    def one(name: String, value: Any): Any =
      renderer.renderValue(value) match {
        case Right(v) => v
        case Left(reason) =>
          DbtUnrenderedVar(s"var('$name') of dbt_project.yml cannot be rendered: $reason")
      }
    scala.collection.immutable.ListMap(projectVars.toList.map {
      case (pkg, scoped: Map[_, _]) if packages.contains(pkg) =>
        pkg -> scala.collection.immutable.ListMap(
          scoped.asInstanceOf[Map[String, Any]].toList.map { case (k, v) => k -> one(k, v) }: _*
        )
      case (k, v) => k -> one(k, v)
    }: _*)
  }

  /** --vars, a YAML or JSON map as dbt reads it */
  def parseCli(text: String): Try[Map[String, Any]] = Try {
    DbtJson.toScala(yaml.readTree(text)) match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
      case _            => throw new DbtImportException(s"--vars is not a YAML or JSON map: $text")
    }
  }
}
