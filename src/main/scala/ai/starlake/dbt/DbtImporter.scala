package ai.starlake.dbt

import ai.starlake.config.Settings
import ai.starlake.dbt.jinja.{DbtConfigRenderer, DbtJinja, DbtJinjaContext}
import ai.starlake.dbt.manifest._
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.{BakedValue, ImportReport}
import ai.starlake.dbt.warehouse.{
  BigQueryWarehouse,
  CachingWarehouse,
  DbtWarehouse,
  JdbcWarehouse,
  OfflineWarehouse
}
import ai.starlake.dbt.write.ProjectWriter
import ai.starlake.schema.model.{ConnectionType, TableAttribute}
import ai.starlake.utils.{ExitCodeJobResult, Utils}
import org.apache.hadoop.fs.Path

import java.nio.file.Paths
import java.time.ZonedDateTime
import scala.util.Try

/** @param conflicts
  *   the files that exist, when the import refused to overwrite them (exit code 1)
  */
final case class DbtImportResult(
  report: ImportReport,
  written: List[Path],
  conflicts: List[Path],
  exitCode: Int
) extends ExitCodeJobResult

/** `starlake dbt import`: manifest -> plan -> files of the current Starlake project */
object DbtImporter {

  def run(
    config: DbtImportConfig,
    inferSeed: Option[(java.nio.file.Path, String) => Try[List[TableAttribute]]] = None,
    env: String => Option[String] = sys.env.get
  )(implicit settings: Settings): Try[DbtImportResult] = Try {
    config.connection.filterNot(settings.appConfig.connections.contains).foreach { c =>
      throw new DbtImportException(
        s"--connection $c: the Starlake project has no such connection (connections: " +
        s"${settings.appConfig.connections.keys.toList.sorted.mkString(", ")})"
      )
    }
    val manifest = ManifestReader.read(Paths.get(config.manifest)).get
    val project = DbtProjectFile.read(manifest.projectRoot).get
    val cliVars = config.vars.map(DbtVars.parseCli(_).get).getOrElse(Map.empty)
    val relations = new DbtRelations(manifest)
    val profileName = project.flatMap(_.profile).getOrElse(manifest.metadata.projectName)
    val profileRenderer = new DbtConfigRenderer(env, cliVars)
    val profileTarget =
      config.profiles.map(p =>
        DbtProfiles
          .read(Paths.get(p), profileName, config.target, manifest.projectRoot, profileRenderer)
          .get
      )
    val target = profileTarget.getOrElse(
      DbtTarget(
        config.target.getOrElse("dev"),
        manifest.metadata.adapterType,
        None,
        relations.defaultDatabase,
        None
      )
    )
    val connectionEdit =
      if (config.connection.isDefined) None
      else
        profileTarget
          .filter(_.unrendered.isEmpty)
          .flatMap(t => t.duckdbPath.map(p => ConnectionEdit(s"dbt_${t.name}", s"jdbc:duckdb:$p")))
    val defaultConnection = settings.appConfig.connectionRef
    // the connection added from the profile becomes the default when there is none
    val connectionOnObjects = config.connection match {
      case Some(c) => Some(c).filter(_ != defaultConnection)
      case None =>
        connectionEdit
          .map(_.name)
          .filter(c => defaultConnection.nonEmpty && c != defaultConnection)
    }

    val packages = manifest.macros.map(_.packageName).toSet + manifest.metadata.projectName
    val varsRenderer = new DbtConfigRenderer(env, cliVars)
    val vars = DbtVars(
      DbtVars.rendered(project.map(_.vars).getOrElse(Map.empty), packages, varsRenderer),
      packages,
      cliVars
    )
    val warehouse = warehouseOf(config, connectionEdit)
    val jinja = new DbtJinja(
      manifest,
      relations,
      DbtJinjaContext(
        target,
        vars,
        project.map(_.dispatch).getOrElse(Map.empty),
        env,
        ZonedDateTime.now()
      ),
      warehouse
    )
    val ctx = DbtMappingContext(
      manifest,
      relations,
      jinja,
      connectionOnObjects,
      inferSeed.getOrElse((csv, table) => SeedInference.infer(csv, table))
    )
    val mapped = DbtMapper.plan(ctx, project)
    val notes = connectionNotes(config, profileTarget, connectionEdit, connectionOnObjects) ++
      (if (config.introspect) List(s"introspection on connection ${warehouse.name}") else Nil) ++
      nextSteps(mapped.report)
    val configEnvVars =
      profileRenderer.envVars.toList.map { case (n, v) =>
        BakedValue(n, v, List("profiles.yml"))
      } ++
      varsRenderer.envVars.toList.map { case (n, v) => BakedValue(n, v, List("dbt_project.yml")) }
    val plan = mapped.copy(
      connection = connectionEdit,
      report = mapped.report.copy(
        general = mapped.report.general ++ notes,
        envVars = BakedValue.merge(configEnvVars ++ mapped.report.envVars)
      )
    )

    ProjectWriter.requireConfined(plan)
    val conflicts = ProjectWriter.conflicts(plan)
    if (conflicts.nonEmpty && !config.force) {
      Utils.printOut(
        (s"starlake dbt import: ${conflicts.size} file(s) exist, nothing written. Pass --force to overwrite them:" ::
        conflicts.map(p => s"  $p")).mkString("\n")
      )
      DbtImportResult(plan.report, Nil, conflicts, exitCode = 1)
    } else {
      val written = ProjectWriter.write(plan, config.force).get
      Utils.printOut(plan.report.summary)
      Utils.printOut(s"Report: ${ProjectWriter.reportPath}")
      DbtImportResult(plan.report, written, Nil, exitCode = 0)
    }
  }

  /** Without --introspect, the offline warehouse. With it, the connection's warehouse behind the
    * read-only guard and the row limit; it must open before anything is rendered or written.
    */
  private def warehouseOf(config: DbtImportConfig, edit: Option[ConnectionEdit])(implicit
    settings: Settings
  ): DbtWarehouse =
    if (!config.introspect) new OfflineWarehouse
    else {
      val (name, open) = config.connection match {
        case Some(c) =>
          val info = settings.appConfig.connections(c)
          if (info.isBigQuery()) (c, () => new BigQueryWarehouse(c): DbtWarehouse)
          else if (info.`type` != ConnectionType.JDBC)
            throw new DbtImportException(
              s"--introspect: connection $c is not a JDBC or BigQuery connection"
            )
          else (c, () => new JdbcWarehouse(c, info.options): DbtWarehouse)
        case None =>
          val e = edit.getOrElse(
            throw new DbtImportException(
              "--introspect needs a connection: pass --connection, or --profiles with a DuckDB target that has a file"
            )
          )
          (
            e.name,
            () =>
              new JdbcWarehouse(
                e.name,
                Map("url" -> e.url, "driver" -> "org.duckdb.DuckDBDriver")
              ): DbtWarehouse
          )
      }
      val live = Try {
        val w = open()
        w.query("select 1").get
        w
      }.recover { case e =>
        throw new DbtImportException(
          s"--introspect: connection $name cannot be opened: ${e.getMessage}"
        )
      }.get
      new CachingWarehouse(live, config.introspectMaxRows)
    }

  private def connectionNotes(
    config: DbtImportConfig,
    profileTarget: Option[DbtTarget],
    edit: Option[ConnectionEdit],
    onObjects: Option[String]
  ): List[String] = {
    val profile = profileTarget match {
      case None =>
        List(
          s"no --profiles: target.name is ${config.target.getOrElse("dev")}, target.schema is empty; pass --profiles for the values of the dbt target"
        )
      case Some(t) if t.unrendered.nonEmpty =>
        t.unrendered.map { case (field, reason) =>
          s"profiles.yml, target ${t.name}: $field cannot be rendered ($reason), left out" +
          (if (config.connection.isEmpty) "; no connection written: pass --connection" else "")
        }
      case Some(t) if config.connection.isEmpty && t.adapterType != "duckdb" =>
        List(
          s"the dbt target is ${t.adapterType}: pass --connection with a Starlake connection to that warehouse"
        )
      case Some(t) if config.connection.isEmpty && t.duckdbPath.isEmpty =>
        List("the DuckDB target has no file (in memory or MotherDuck): pass --connection")
      case _ => Nil
    }
    profile ++
    edit
      .map(e =>
        s"connection ${e.name} added to application.sl.yml (${e.url}); the file is rewritten without its comments"
      )
      .toList ++
    onObjects
      .map(c =>
        s"the tasks and load domains name connectionRef $c, which is not the project's default"
      )
      .toList
  }

  private def nextSteps(report: ImportReport): List[String] = {
    List(
      "next: starlake stage && starlake load, to load the seeds",
      s"next: starlake transform --name ${report.runnableTasks.mkString(",")}"
    )
  }
}
