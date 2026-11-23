package ai.starlake.job.site

import ai.starlake.schema.handlers.StorageHandler
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
        val replacement =
          if (visiting.contains(name)) {
            warnings += s"Cyclic doc block reference '$name'"
            m.matched
          } else
            blocks.get(name) match {
              case None =>
                warnings += s"Unknown doc block '$name'"
                m.matched
              case Some(body) =>
                val (resolved, nested) = resolve(body, visiting + name)
                if (nested.exists(_.startsWith("Cyclic"))) {
                  warnings ++= nested
                  m.matched
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
