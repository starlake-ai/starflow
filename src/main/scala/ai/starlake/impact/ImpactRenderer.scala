package ai.starlake.impact

import ai.starlake.config.Settings
import ai.starlake.utils.Utils
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path

import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.*

object ImpactRenderer {

  private def jmap(entries: (String, Any)*): java.util.Map[String, Object] = {
    val m = new java.util.LinkedHashMap[String, Object]()
    entries.foreach { case (k, v) => m.put(k, v.asInstanceOf[Object]) }
    m
  }

  private def counts(m: Map[String, Int]): java.util.Map[String, Object] =
    jmap(ChangeClass.all.map(c => c.label -> Int.box(m.getOrElse(c.label, 0)))*)

  // plain mapper: the shared one drops nulls and empty lists, the contract keeps every key
  private val jsonMapper = new ObjectMapper()

  private val AnalyzablePrefix = "SQL now analyzable"

  /** The old unresolved reason, unless a reason already explains it ("SQL now analyzable
    * (previously: ...)").
    */
  private def shownUnresolved(d: DirectChange): Option[String] =
    d.unresolved.filterNot(_ => d.reasons.exists(_.startsWith(AnalyzablePrefix)))

  private def buildTree(
    report: ImpactReport,
    hideExplained: Boolean,
    esc: String => String
  ): java.util.Map[String, Object] = {
    def l(xs: List[String]): java.util.List[String] = xs.map(esc).asJava
    val s = report.summary
    jmap(
      "oldSide" -> esc(report.oldSide),
      "newSide" -> esc(report.newSide),
      "summary" -> jmap(
        "direct"         -> counts(s.direct),
        "impacted"       -> counts(s.impacted),
        "likelyFailures" -> Int.box(s.likelyFailures),
        "hasBreaking"    -> Boolean.box(s.hasBreaking),
        "hasNonMetadata" -> Boolean.box(s.hasNonMetadata)
      ),
      "direct" -> report.direct.map { d =>
        jmap(
          "node"       -> esc(d.node.name),
          "kind"       -> d.kind.label,
          "class"      -> d.changeClass.label,
          "columns"    -> esc(d.columns.label),
          "details"    -> l(d.details.map(_.describe)),
          "reasons"    -> l(d.reasons),
          "unresolved" -> (if (hideExplained) shownUnresolved(d) else d.unresolved).map(esc).orNull,
          "deleted"    -> Boolean.box(d.deleted)
        )
      }.asJava,
      "impacted" -> report.impacted.map { n =>
        jmap(
          "node"              -> esc(n.node.name),
          "kind"              -> n.kind.label,
          "class"             -> n.changeClass.label,
          "columns"           -> esc(n.columns.label),
          "path"              -> l(n.path),
          "likelyFailure"     -> Boolean.box(n.likelyFailure),
          "viaUnresolvedEdge" -> Boolean.box(n.viaUnresolvedEdge)
        )
      }.asJava,
      "unresolved" -> report.unresolved.map { e =>
        jmap("from" -> esc(e.from.name), "to" -> esc(e.to.name), "reason" -> esc(e.reason))
      }.asJava,
      "otherChanges" -> l(report.otherChanges)
    )
  }

  private def htmlEscape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  /** The stable JSON contract. The raw `unresolved` value is kept on purpose (it is the contract),
    * only the text and markdown renderers hide it when a reason already explains it.
    */
  def toTree(report: ImpactReport): java.util.Map[String, Object] =
    buildTree(report, false, (s: String) => s)

  def render(report: ImpactReport, format: String, template: Option[String])(implicit
    settings: Settings
  ): String = format.toLowerCase match {
    case "text"     => text(report)
    case "markdown" => markdown(report)
    case "json" =>
      jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(toTree(report))
    case "html" => html(report, template)
    case other =>
      throw new IllegalArgumentException(
        s"Unknown format '$other'. Use text, json, markdown or html."
      )
  }

  private def summaryLine(r: ImpactReport): String = {
    val s = r.summary
    val failures =
      if (s.likelyFailures == 1) "1 likely failure" else s"${s.likelyFailures} likely failures"
    s"${r.direct.size} direct, ${r.impacted.size} impacted, $failures, breaking: ${if (s.hasBreaking) "yes"
      else "no"}"
  }

  private def cols(c: Columns): String = s"[${c.label}]"

  private def directLines(d: DirectChange): List[String] =
    d.details.map(_.describe) ++ d.reasons ++ shownUnresolved(d).map(u => s"unresolved: $u")

  private def cell(s: String): String =
    s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("|", "\\|")

  private def textUnresolved(r: ImpactReport): String = {
    val sb = new StringBuilder
    sb ++= s"\nUnresolved (${r.unresolved.size})\n"
    r.unresolved.foreach(e => sb ++= s"  ${e.from.name} -> ${e.to.name}: ${e.reason}\n")
    sb.toString
  }

  private def mdUnresolved(r: ImpactReport): String = {
    val sb = new StringBuilder
    sb ++= "\n### Unresolved\n\n"
    if (r.unresolved.isEmpty) sb ++= "None\n"
    r.unresolved.foreach(e =>
      sb ++= s"- `${cell(e.from.name)}` -> `${cell(e.to.name)}`: ${cell(e.reason)}\n"
    )
    sb.toString
  }

  private def text(r: ImpactReport): String = {
    val sb = new StringBuilder
    sb ++= s"Change impact: ${r.oldSide} -> ${r.newSide}\n\n"
    if (r.isEmpty) {
      sb ++= "No changes\n"
      sb ++= textUnresolved(r)
      return sb.toString
    }
    sb ++= s"Direct changes (${r.direct.size})\n"
    r.direct.foreach { d =>
      sb ++= f"  ${d.changeClass.label}%-13s ${d.kind.label}%-5s ${d.node.name} ${cols(d.columns)}\n"
      directLines(d).foreach(line => sb ++= s"                $line\n")
    }
    sb ++= s"\nImpacted downstream (${r.impacted.size})\n"
    r.impacted.foreach { n =>
      val flag = if (n.likelyFailure) "  likely failure" else ""
      val via = if (n.viaUnresolvedEdge) "  (table level)" else ""
      sb ++= f"  ${n.changeClass.label}%-13s ${n.kind.label}%-5s ${n.node.name} ${cols(n.columns)}$flag$via\n"
      sb ++= s"                path: ${n.path.mkString(" -> ")}\n"
    }
    sb ++= textUnresolved(r)
    sb ++= s"\nOther changes, not traced (${r.otherChanges.size})\n"
    r.otherChanges.foreach(o => sb ++= s"  $o\n")
    sb ++= s"\nSummary: ${summaryLine(r)}\n"
    sb.toString
  }

  private def markdown(r: ImpactReport): String = {
    val sb = new StringBuilder
    sb ++= "## Change impact\n\n"
    sb ++= s"`${cell(r.oldSide)}` -> `${cell(r.newSide)}`\n\n"
    if (r.isEmpty) {
      sb ++= "No changes\n"
      sb ++= mdUnresolved(r)
      return sb.toString
    }
    sb ++= s"**${summaryLine(r)}**\n\n"
    sb ++= "### Direct changes\n\n| Class | Kind | Node | Columns | Details |\n|---|---|---|---|---|\n"
    r.direct.foreach { d =>
      val details = directLines(d).map(cell).mkString("<br>")
      sb ++= s"| ${d.changeClass.label} | ${d.kind.label} | `${cell(d.node.name)}` | ${cell(d.columns.label)} | $details |\n"
    }
    sb ++= "\n### Impacted downstream\n\n| Class | Kind | Node | Columns | Path | |\n|---|---|---|---|---|---|\n"
    r.impacted.foreach { n =>
      val flags = (List("likely failure").filter(_ => n.likelyFailure) ++
        List("table level").filter(_ => n.viaUnresolvedEdge)).mkString(", ")
      sb ++= s"| ${n.changeClass.label} | ${n.kind.label} | `${cell(n.node.name)}` | ${cell(
          n.columns.label
        )} | ${cell(n.path.mkString(" -> "))} | $flags |\n"
    }
    sb ++= mdUnresolved(r)
    sb ++= "\n### Other changes, not traced\n\n"
    if (r.otherChanges.isEmpty) sb ++= "None\n"
    r.otherChanges.foreach(o => sb ++= s"- ${cell(o)}\n")
    sb.toString
  }

  private def html(r: ImpactReport, template: Option[String])(implicit
    settings: Settings
  ): String = {
    val content = template match {
      case Some(path) =>
        val p = new Path(path)
        if (!settings.storageHandler().exists(p))
          throw new IllegalArgumentException(s"Template not found: $path")
        settings.storageHandler().read(p)
      case None =>
        val resource = "/templates/impact/index.html.j2"
        val stream = Option(getClass.getResourceAsStream(resource)).getOrElse(
          throw new IllegalStateException(s"Bundled template missing: $resource")
        )
        val source = scala.io.Source.fromInputStream(stream)
        try source.mkString
        finally source.close()
    }
    Utils.parseJinjaTpl(
      content,
      Map[String, Object](
        "report"    -> buildTree(r, true, htmlEscape(_)),
        "breaking"  -> (if (r.summary.hasBreaking) "yes" else "no"),
        "timestamp" -> DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now())
      )
    )
  }
}
