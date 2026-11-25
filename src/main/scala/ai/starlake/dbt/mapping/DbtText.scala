package ai.starlake.dbt.mapping

import ai.starlake.dbt.manifest.DbtManifest

/** Text that the import writes into Starlake files */
object DbtText {
  private val JinjaDelimiter = """\{[{%#]""".r
  private val StringLiteral = """'(?:[^']|'')*'""".r

  /** Rendered SQL that still holds a Jinja delimiter (`{{`, `{%`, `{#`, in a string or a regex
    * literal) would be rendered again by Starlake, which renders a task's SQL when the project
    * loads and when the task runs. In a string literal, each delimiter is split in two with CONCAT,
    * which no render touches: `'a{{b'` becomes `CONCAT('a{', '{b')`. A delimiter outside a string
    * literal is left as it is.
    *
    * @return
    *   the SQL, whether a literal was split, and whether a delimiter is left outside literals
    */
  def protect(sql: String): (String, Boolean, Boolean) = {
    var split = false
    val out = StringLiteral.replaceAllIn(
      sql,
      { m =>
        val body = m.matched.substring(1, m.matched.length - 1)
        val cuts = JinjaDelimiter.findAllMatchIn(body).map(_.start + 1).toList
        if (cuts.isEmpty) java.util.regex.Matcher.quoteReplacement(m.matched)
        else {
          split = true
          val pieces =
            (0 :: cuts).zip(cuts :+ body.length).map { case (from, to) => body.substring(from, to) }
          java.util.regex.Matcher
            .quoteReplacement(pieces.map(p => s"'$p'").mkString("CONCAT(", ", ", ")"))
        }
      }
    )
    val outside = JinjaDelimiter.findFirstIn(StringLiteral.replaceAllIn(out, "''")).isDefined
    (out, split, outside)
  }

  /** dbt writes descriptions with their doc blocks expanded: put back `{{ doc('name') }}` where the
    * text of a block the description uses appears, for Starlake to resolve from metadata/docs
    */
  def withDocRefs(description: String, docBlocks: List[String], manifest: DbtManifest): String =
    docBlocks.flatMap(manifest.docsById.get).foldLeft(description) { (text, doc) =>
      val contents = doc.blockContents.trim
      val at = if (contents.isEmpty) -1 else text.indexOf(contents)
      if (at < 0) text
      else
        text.substring(0, at) + s"{{ doc('${doc.name}') }}" + text.substring(at + contents.length)
    }

  /** A value as a Jinja literal, for an expectation call */
  def jinjaLiteral(value: Any): String = value match {
    case s: String   => "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case b: Boolean  => b.toString
    case n: Number   => n.toString
    case l: List[_]  => l.map(jinjaLiteral).mkString("[", ", ", "]")
    case None | null => "none"
    case Some(v)     => jinjaLiteral(v)
    case other       => jinjaLiteral(other.toString)
  }

  /** A string as a double-quoted Jinja literal */
  def doubleQuoted(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  /** A Jinja identifier made of the name of a dbt object */
  def identifier(name: String): String = name.replaceAll("[^A-Za-z0-9_]", "_")
}
