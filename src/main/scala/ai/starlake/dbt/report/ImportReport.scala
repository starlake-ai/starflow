package ai.starlake.dbt.report

sealed trait EntryStatus

object EntryStatus {
  case object Converted extends EntryStatus
  final case class Skipped(reason: String) extends EntryStatus
  final case class NotImported(reason: String) extends EntryStatus
  final case class ReportedOnly(reason: String) extends EntryStatus
}

/** @param dbtId
  *   unique id of the dbt object
  * @param target
  *   what it became in Starlake: `domain.table`, an expectation, a doc block
  * @param dependsOn
  *   the dbt objects it reads, for the dependents of the skipped ones
  */
final case class ReportEntry(
  kind: String,
  dbtId: String,
  target: Option[String],
  status: EntryStatus,
  notes: List[String] = Nil,
  dependsOn: List[String] = Nil
)

/** A var or env var whose value the import wrote into the SQL */
final case class BakedValue(name: String, value: String, usedBy: List[String])

final case class ImportReport(
  entries: List[ReportEntry],
  vars: List[BakedValue],
  envVars: List[BakedValue],
  general: List[String]
) {
  import EntryStatus._

  def converted: List[ReportEntry] = entries.filter(_.status == Converted)

  def withNotes: List[ReportEntry] = converted.filter(_.notes.nonEmpty)

  def skipped: List[ReportEntry] = entries.filter(_.status.isInstanceOf[Skipped])

  def notImported: List[ReportEntry] = entries.filter(_.status.isInstanceOf[NotImported])

  def reportedOnly: List[ReportEntry] = entries.filter(_.status.isInstanceOf[ReportedOnly])

  /** Each skipped or disabled object, with the converted objects that read it */
  def dependentsOfSkipped: List[(String, List[String])] = {
    val missing = (skipped ++ notImported).map(_.dbtId).toSet
    missing.toList.sorted.flatMap { id =>
      val dependents =
        converted.filter(_.dependsOn.contains(id)).map(e => e.target.getOrElse(e.dbtId))
      if (dependents.isEmpty) None else Some(id -> dependents.sorted)
    }
  }

  /** The converted transform tasks, `domain.table` */
  def convertedTasks: List[String] =
    converted.filter(_.kind == "model").flatMap(_.target)

  def summary: String = {
    val lines = List(
      s"dbt import: ${converted.size} converted (${withNotes.size} with notes), ${skipped.size} skipped, " +
      s"${notImported.size} not imported, ${reportedOnly.size} reported only"
    ) ++ skipped.map {
      case ReportEntry(_, id, _, Skipped(reason), _, _) => s"  skipped $id: $reason"
      case e                                            => s"  skipped ${e.dbtId}"
    }
    lines.mkString("\n")
  }

  def toMarkdown: String = {
    val sb = new StringBuilder
    def section(title: String, lines: List[String]): Unit =
      if (lines.nonEmpty) {
        sb.append(s"\n## $title\n\n")
        lines.foreach(l => sb.append(s"- $l\n"))
      }
    def target(e: ReportEntry): String = e.target.map(t => s" -> `$t`").getOrElse("")
    sb.append("# dbt import report\n\n")
    sb.append(s"${summary.linesIterator.next()}\n")
    section("Notes", general)
    section(
      "Converted",
      converted.filter(_.notes.isEmpty).map(e => s"${e.kind} `${e.dbtId}`${target(e)}")
    )
    section(
      "Converted with notes",
      withNotes.map(e => s"${e.kind} `${e.dbtId}`${target(e)}: ${e.notes.mkString("; ")}")
    )
    section(
      "Skipped",
      skipped.map {
        case e @ ReportEntry(_, _, _, Skipped(reason), _, _) => s"${e.kind} `${e.dbtId}`: $reason"
        case e                                               => s"${e.kind} `${e.dbtId}`"
      }
    )
    section(
      "Not imported",
      notImported.map {
        case e @ ReportEntry(_, _, _, NotImported(reason), _, _) =>
          s"${e.kind} `${e.dbtId}`: $reason"
        case e => s"${e.kind} `${e.dbtId}`"
      }
    )
    section(
      "Downstream of skipped objects",
      dependentsOfSkipped.map { case (id, deps) =>
        s"`$id` is read by ${deps.map(d => s"`$d`").mkString(", ")}"
      }
    )
    section(
      "Baked vars",
      vars.map(v => s"`${v.name}` = `${v.value}` in ${v.usedBy.map(u => s"`$u`").mkString(", ")}")
    )
    section(
      "Baked env vars",
      envVars.map(v =>
        s"`${v.name}` = `${v.value}` in ${v.usedBy.map(u => s"`$u`").mkString(", ")}"
      )
    )
    section(
      "Reported only",
      reportedOnly.map {
        case e @ ReportEntry(_, _, _, ReportedOnly(reason), _, _) =>
          s"${e.kind} `${e.dbtId}`: $reason"
        case e => s"${e.kind} `${e.dbtId}`"
      }
    )
    sb.toString
  }
}
