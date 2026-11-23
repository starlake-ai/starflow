package ai.starlake.job.site

import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path

import scala.util.matching.Regex

final case class DocBlocks(blocks: Map[String, String]) {

  /** Replaces every {{ doc("name") }} with the block's Markdown, recursively.
    * @return
    *   the resolved text and the warnings raised (unknown or cyclic references)
    */
  def resolve(text: String): (String, List[String]) = resolve(text, Set.empty)

  private def resolve(text: String, visiting: Set[String]): (String, List[String]) = {
    val warnings = List.newBuilder[String]
    val out = DocBlocks.RefRe.replaceAllIn(
      text,
      m => {
        val name = DocBlocks.refName(m)
        val literal =
          if (m.group(1) != null) s"""{{ doc("$name") }}""" else m.matched
        val replacement =
          if (visiting.contains(name)) {
            warnings += s"Cyclic doc block reference '$name'"
            literal
          } else
            blocks.get(name) match {
              case None =>
                warnings += s"Unknown doc block '$name'"
                literal
              case Some(body) =>
                val (resolved, nested) = resolve(body, visiting + name)
                if (nested.exists(_.startsWith("Cyclic"))) {
                  warnings ++= nested
                  literal
                } else {
                  warnings ++= nested
                  resolved
                }
            }
        Regex.quoteReplacement(replacement)
      }
    )
    (out, warnings.result().distinct)
  }
}

object DocBlocks {
  val Empty: DocBlocks = DocBlocks(Map.empty)

  /** True when the text holds a doc reference, in either form. */
  def hasRef(text: String): Boolean = RefRe.findFirstIn(text).isDefined

  /** Replaces the doc references held by the descriptions of a loaded model (domain, table, column,
    * row level security, job and task) with the Markdown of the blocks they name, so that the
    * descriptions written to a warehouse carry the text rather than the load-time marker. The
    * blocks are read only when a reference is met. Unknown or cyclic references are left as {{
    * doc("name") }} and reported through warn.
    */
  final class Resolver(blocks: => DocBlocks, warn: String => Unit) {
    private lazy val docs = blocks

    private def text(t: String): String =
      if (t == null || !hasRef(t)) t
      else {
        val (resolved, warnings) = docs.resolve(t)
        warnings.foreach(warn)
        resolved
      }

    private def opt(t: Option[String]): Option[String] = if (t == null) t else t.map(text)

    // Jackson may leave a list null when the YAML omits it
    private def each[A](l: List[A])(f: A => A): List[A] = if (l == null) l else l.map(f)

    private def attribute(a: TableAttribute): TableAttribute =
      a.copy(comment = opt(a.comment), attributes = each(a.attributes)(attribute))

    private def rls(r: RowLevelSecurity): RowLevelSecurity =
      r.copy(description = text(r.description))

    def table(t: SchemaInfo): SchemaInfo =
      t.copy(
        comment = opt(t.comment),
        attributes = each(t.attributes)(attribute),
        rls = each(t.rls)(rls)
      )

    def domain(d: DomainInfo): DomainInfo =
      d.copy(comment = opt(d.comment), tables = each(d.tables)(table))

    def task(t: AutoTaskInfo): AutoTaskInfo =
      t.copy(
        comment = opt(t.comment),
        attributes = each(t.attributes)(attribute),
        rls = each(t.rls)(rls)
      )

    def job(j: AutoJobInfo): AutoJobInfo =
      j.copy(
        comment = opt(j.comment),
        tasks = each(j.tasks)(task),
        default = if (j.default == null) j.default else j.default.map(task)
      )
  }

  /** Matches the load-time marker (group 1) or the {{ doc("name") }} form (group 2). */
  val RefRe: Regex =
    """<!--sl-doc:([^<>]+?)-->|\{\{\s*doc\(\s*["']([^"']+)["']\s*\)\s*\}\}""".r

  def refName(m: Regex.Match): String = Option(m.group(1)).getOrElse(m.group(2))
  private val BlockRe: Regex =
    """(?s)\{%-?\s*docs\s+([A-Za-z0-9_]+)\s*-?%\}(.*?)\{%-?\s*enddocs\s*-?%\}""".r

  def parse(contents: Seq[String]): (DocBlocks, List[String]) = {
    val warnings = List.newBuilder[String]
    val blocks =
      contents.flatMap(BlockRe.findAllMatchIn).foldLeft(Map.empty[String, String]) { (acc, m) =>
        val name = m.group(1)
        if (acc.contains(name)) {
          warnings += s"Duplicate doc block '$name' ignored"
          acc
        } else acc + (name -> m.group(2).trim)
      }
    (DocBlocks(blocks), warnings.result())
  }

  /** Reads every .md file under docsDir (recursively). Missing folder means no blocks. */
  def load(storage: StorageHandler, docsDir: Path): (DocBlocks, List[String]) =
    if (!storage.exists(docsDir)) (Empty, Nil)
    else {
      val files = storage.list(docsDir, extension = ".md", recursive = true, sortByName = true)
      parse(files.map(f => storage.read(f.path)))
    }
}
