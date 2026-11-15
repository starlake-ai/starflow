package ai.starlake.schema.handlers

import ai.starlake.schema.model.TableAttribute
import ai.starlake.utils.YamlSerde

import scala.util.Try
import scala.util.matching.Regex

/** Updates the attributes in the YAML file of a task as written, without rendering it (#1953): the
  * lines outside the attributes list are kept, Jinja and comments included, and so are the lines of
  * the attributes that do not change.
  */
object TaskYamlAttributes {

  private final case class Line(text: String) {
    val content: String = text.stripSuffix("\r")
    val trimmed: String = content.trim
    val indent: Int = content.takeWhile(_ == ' ').length
    def isBlank: Boolean = trimmed.isEmpty
    def isComment: Boolean = trimmed.startsWith("#")

    /** Blank lines, comments and Jinja tags do not delimit the blocks of the YAML */
    def isTransparent: Boolean =
      isBlank || isComment || trimmed.startsWith("{%") || trimmed.startsWith("{#")
    def isItem: Boolean = trimmed == "-" || trimmed.startsWith("- ")

    /** What the loading of the task renders: Jinja, and the `${var}` and `{{ var }}` variables */
    def isRendered: Boolean = Seq("{{", "{%", "{#", "${").exists(content.contains)

    /** The value written after `key:` on this line, without its comment, when the line declares
      * `key`
      */
    def valueOf(key: String): Option[String] =
      s"""^\\s*(["']?)${Regex.quote(key)}\\1\\s*:(?:\\s+(.*))?$$""".r
        .findFirstMatchIn(content)
        .map(m => Option(m.group(2)).getOrElse("").replaceAll("""(^|\s+)#.*$""", "").trim)
  }

  /** An attribute of the updated list: the lines above it, and its lines when they are kept */
  private final case class NewItem(
    attribute: TableAttribute,
    above: Vector[Line],
    kept: Option[Vector[Line]]
  )

  /** The YAML file of a task that declares only these attributes */
  def create(attributes: List[TableAttribute]): String =
    (List("version: 1", "task:", "  attributes:") ++ attributes.flatMap(serialize).map("    " + _))
      .mkString("", "\n", "\n")

  /** The lines of an attribute as an item of a YAML list, indented from column 0: every line of the
    * serializer but its document marker
    */
  private def serialize(attribute: TableAttribute): List[String] = {
    val lines = YamlSerde.mapper
      .writeValueAsString(List(attribute))
      .split("\n")
      .toList
      .map(_.stripSuffix("\r"))
    if (lines.headOption.exists(_.trim == "---")) lines.tail else lines
  }

  /** The content of a task file, with the attributes of the task replaced by `attributes`.
    *
    * The file is updated as text: the lines outside the attributes list are kept, the items of the
    * attributes that do not change keep their text and their comments, and the other attributes are
    * serialized. The result is read back, rendered as the loading of the task renders it, before it
    * is returned: it must give the same task but for its attributes, which must read back as
    * written.
    *
    * @param raw
    *   the content of the file, as written
    * @param render
    *   renders the content of the file, as the loading of the task does
    * @throws IllegalStateException
    *   when the attributes cannot be replaced without rewriting the rest of the file, Jinja in the
    *   attributes list included
    */
  def replace(
    raw: String,
    attributes: List[TableAttribute],
    render: String => String,
    path: String
  ): String = {
    def refuse(reason: String): Nothing =
      throw new IllegalStateException(
        s"Could not update the attributes in $path without rewriting the rest of the file: $reason." +
        " The file is left unchanged: update its attributes by hand"
      )
    val eol = if (raw.contains("\r\n")) "\r" else ""
    val lines = raw.split("\n", -1).toVector.map(Line.apply)

    // The lines of the task: the block of the top-level `task` key, or the whole file in the
    // layout without it, which the loading migrates
    val taskKey = lines.indexWhere(line => line.indent == 0 && line.valueOf("task").isDefined)
    if (taskKey >= 0 && lines(taskKey).valueOf("task").exists(_.nonEmpty))
      refuse("the task key has a value on its line (flow style, anchor, alias or tag)")
    val (start, end) =
      if (taskKey < 0) (0, lines.length)
      else {
        val next = lines.indexWhere(line => !line.isTransparent && line.indent == 0, taskKey + 1)
        (taskKey + 1, if (next < 0) lines.length else next)
      }
    val keyIndent = (start until end)
      .map(lines)
      .find(!_.isTransparent)
      .map(_.indent)
      .getOrElse(if (taskKey < 0) 0 else 2)
    val keys = (start until end).filter { i =>
      lines(i).indent == keyIndent && lines(i).valueOf("attributes").isDefined
    }
    if (keys.size > 1)
      refuse("it declares its attributes more than once")

    // (lines before the attributes list, its first lines to keep, the items that it had, the item
    // indentation, lines after it)
    val (head, preamble, items, itemIndent, tail) = keys.headOption match {
      case Some(key) =>
        // The attributes list: the lines indented under its key, and the items at its level
        val stop = (key + 1 until end)
          .find { i =>
            val line = lines(i)
            !line.isTransparent && line.indent <= keyIndent &&
            !(line.indent == keyIndent && line.isItem)
          }
          .getOrElse(end)
        val blockList = lines(key).valueOf("attributes").contains("")
        val firstItem = (key + 1 until stop)
          .map(lines)
          .find(!_.isTransparent)
          .filter(line => blockList && line.isItem)
        val levelIndent = firstItem.map(_.indent).getOrElse(keyIndent)
        // Up to its last line indented deeper than its items: a line that starts with `#` there may
        // be in a block scalar
        val last = (key until stop).reverse.find { i =>
          val line = lines(i)
          i == key || !(line.isBlank || (line.isTransparent && line.indent <= levelIndent))
        }.get
        // Jinja between the list and the next key may also write items
        if ((key until stop).exists(lines(_).isRendered))
          refuse("its attributes use Jinja or variables, which the sync would render")
        val body = lines.slice(key + 1, last + 1)
        val starts = firstItem.toList.flatMap { first =>
          body.indices.filter(i => body(i).isItem && body(i).indent == first.indent)
        }
        // The blank lines and the comments at the level of the items, above an item, are its own
        val bounds = starts.zip(None :: starts.map(Some(_))).map { case (start, previous) =>
          previous.fold(start) { previousStart =>
            (previousStart + 1 until start).reverse
              .takeWhile { i =>
                body(i).isBlank || (body(i).isComment && body(i).indent <= levelIndent)
              }
              .lastOption
              .getOrElse(start)
          }
        }
        // (the lines above the item, the item)
        val items = bounds.zip(bounds.drop(1) :+ body.length).map { case (from, until) =>
          val item = body.slice(from, until)
          item.splitAt(item.indexWhere(line => line.isItem && line.indent == levelIndent))
        }
        val keyLine =
          if (blockList) lines(key)
          else Line(lines(key).content.takeWhile(_ != ':') + ":" + eol)
        (
          lines.take(key) :+ keyLine,
          body.take(starts.headOption.getOrElse(0)),
          items,
          firstItem.map(_.indent).getOrElse(keyIndent + 2),
          lines.drop(last + 1)
        )
      case None =>
        // No attributes yet: a list first in the task, since lines after a block scalar that ends
        // the file would change its value, the rendering trimming the file
        val at =
          if (taskKey >= 0) taskKey + 1
          else
            (start until end)
              .find(i => !lines(i).isTransparent && lines(i).trimmed != "---")
              .getOrElse(end)
        (
          lines.take(at) :+ Line(" " * keyIndent + "attributes:" + eol),
          Vector.empty,
          Nil,
          keyIndent + 2,
          lines.drop(at)
        )
    }

    // The items of the attributes that do not change are kept as written, when the items of the
    // file are those of the task, and the lines above an item stay above its attribute
    val before = Try(YamlSerde.deserializeYamlTask(render(raw), path)).fold(
      error => refuse(s"the file cannot be read (${error.getMessage})"),
      identity
    )
    val kept = if (items.size == before.attributes.size) before.attributes.zip(items) else Nil
    val (newItems, _) = attributes.foldLeft((Vector.empty[NewItem], kept)) {
      case ((acc, available), attribute) =>
        val same = available.indexWhere { case (existing, _) => existing == attribute }
        val named = available.indexWhere { case (existing, _) =>
          existing.name.equalsIgnoreCase(attribute.name)
        }
        if (same >= 0) {
          val (_, (above, item)) = available(same)
          (acc :+ NewItem(attribute, above, Some(item)), available.patch(same, Nil, 1))
        } else if (named >= 0) {
          val (_, (above, _)) = available(named)
          (acc :+ NewItem(attribute, above, None), available.patch(named, Nil, 1))
        } else
          (acc :+ NewItem(attribute, Vector.empty, None), available)
    }
    val updated = (head ++ preamble ++ newItems.flatMap { item =>
      item.above ++ item.kept.getOrElse {
        val itemLines =
          serialize(item.attribute).map(line => Line(" " * itemIndent + line + eol))
        if (itemLines.exists(_.isRendered))
          refuse(
            s"attribute ${item.attribute.name} has a value with Jinja or variables, which the loading of the task would render"
          )
        itemLines
      }
    } ++ tail).map(_.text).mkString("\n")

    // The task read back from the updated file: the same task, with the attributes as written
    val serialized = newItems.filter(_.kept.isEmpty).map(_.attribute).toList
    val roundTripped =
      if (serialized.isEmpty) Iterator.empty
      else YamlSerde.deserializeYamlTask(create(serialized), path).attributes.iterator
    val expected = newItems.toList.map { item =>
      if (item.kept.isDefined) item.attribute else roundTripped.next()
    }
    val after = Try(YamlSerde.deserializeYamlTask(render(updated), path)).fold(
      error => refuse(s"the updated file cannot be read (${error.getMessage})"),
      identity
    )
    if (
      after.copy(attributes = Nil) != before.copy(attributes = Nil) || after.attributes != expected
    )
      refuse("the updated file does not read back as the same task with the new attributes")
    updated
  }
}
