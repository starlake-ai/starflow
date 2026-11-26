package ai.starlake.impact

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import better.files.File

import scala.util.Try

/** Compares two project directories offline: direct changes, downstream impact, unresolved edges
  * and untraced files.
  */
object ImpactAnalysis {

  def analyze(oldDir: File, newDir: File, oldLabel: String, newLabel: String, env: Option[String])(
    implicit settings: Settings
  ): Try[ImpactReport] = {
    // one instant for both sides: SQL using {{sl_datetime}} must render the same on each
    val dateVars = SchemaHandler.slDateVarsNow()
    for {
      oldView <- ProjectSnapshot.load(oldDir, oldLabel, env, Some(dateVars))
      newView <- ProjectSnapshot.load(newDir, newLabel, env, Some(dateVars))
    } yield {
      val direct = ChangeDetector.detect(oldView, newView)
      val (impacted, edges) = ImpactPropagator.propagate(direct, newView)
      val unresolved = (edges ++ ImpactPropagator.collisions(oldView, newView)).distinct
        .sortBy(e => (e.from.name, e.to.name))
      val report = ImpactReport(
        oldLabel,
        newLabel,
        direct,
        impacted,
        unresolved,
        ChangeDetector.otherChanges(oldView, newView, direct)
      )
      report.copy(rebuild = RebuildPlanner.plan(report, newView))
    }
  }
}
