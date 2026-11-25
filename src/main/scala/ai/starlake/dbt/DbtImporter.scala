package ai.starlake.dbt

import ai.starlake.config.Settings
import ai.starlake.dbt.jinja.{DbtJinja, DbtJinjaContext}
import ai.starlake.dbt.manifest._
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.ImportReport
import ai.starlake.dbt.write.ProjectWriter
import ai.starlake.schema.model.TableAttribute
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
    val manifest = ManifestReader.read(Paths.get(config.manifest)).get
    val project = DbtProjectFile.read(manifest.projectRoot).get
    val cliVars = config.vars.map(DbtVars.parseCli(_).get).getOrElse(Map.empty)
    val relations = new DbtRelations(manifest)
    val profileName = project.flatMap(_.profile).getOrElse(manifest.metadata.projectName)
    val profileTarget =
      config.profiles.map(p =>
        DbtProfiles.read(Paths.get(p), profileName, config.target, manifest.projectRoot).get
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
        profileTarget.flatMap(t =>
          t.duckdbPath.map(p => ConnectionEdit(s"dbt_${t.name}", s"jdbc:duckdb:$p"))
        )
    val importConnection = config.connection.orElse(connectionEdit.map(_.name))
    val defaultConnection = settings.appConfig.connectionRef
    val connectionOnObjects =
      importConnection.filter(c => defaultConnection.nonEmpty && c != defaultConnection)

    val vars = DbtVars(
      project.map(_.vars).getOrElse(Map.empty),
      manifest.macros.map(_.packageName).toSet + manifest.metadata.projectName,
      cliVars
    )
    val jinja = new DbtJinja(
      manifest,
      relations,
      DbtJinjaContext(
        target,
        vars,
        project.map(_.dispatch).getOrElse(Map.empty),
        env,
        ZonedDateTime.now()
      )
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
      nextSteps(mapped.report)
    val plan = mapped.copy(
      connection = connectionEdit,
      report = mapped.report.copy(general = mapped.report.general ++ notes)
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
    val tasks =
      report.convertedTasks.filterNot(t => report.dependentsOfSkipped.exists(_._2.contains(t)))
    List(
      "next: starlake stage && starlake load, to load the seeds",
      s"next: starlake transform --name ${tasks.mkString(",")}"
    )
  }
}
