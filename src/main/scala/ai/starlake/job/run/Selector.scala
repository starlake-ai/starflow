package ai.starlake.job.run

/** What a selector expression matches against a node, independent of the graph. */
sealed trait Matcher

object Matcher {

  /** `domain.table`: matched against a node's full id or its last two dot-separated segments */
  final case class Exact(id: String) extends Matcher

  /** `domain.*`: every node whose last two segments start with this domain */
  final case class DomainAll(domain: String) extends Matcher

  /** `tag:value` */
  final case class Tag(value: String) extends Matcher
}

/** A parsed `--select` / `--exclude` expression.
  *
  * @param upstream
  *   `+expr`: also select every transitive ancestor of the direct matches
  * @param downstream
  *   `expr+`: also select every transitive descendant of the direct matches
  * @param expr
  *   the trimmed original text, reported back in plan and error messages
  */
final case class Selector(
  matcher: Matcher,
  upstream: Boolean,
  downstream: Boolean,
  expr: String
)

/** Parsing is pure: it never sees the graph, so a malformed expression is rejected before any
  * project metadata is read. Rejection is exit code 2.
  */
object Selector {

  private val Grammar: String =
    "expected 'domain.table', 'domain.*' or 'tag:<value>', optionally prefixed with '+' to add" +
    " upstreams and/or suffixed with '+' to add downstreams"

  def parse(expr: String): Either[String, Selector] = {
    val trimmed = expr.trim
    if (trimmed.isEmpty) Left(s"Empty selector: $Grammar.")
    else {
      val upstream = trimmed.startsWith("+")
      val withoutPrefix = if (upstream) trimmed.drop(1) else trimmed
      val downstream = withoutPrefix.endsWith("+")
      val body = if (downstream) withoutPrefix.dropRight(1) else withoutPrefix
      matcherOf(trimmed, body).map(Selector(_, upstream, downstream, trimmed))
    }
  }

  /** @param original
    *   the trimmed expression as the user typed it, operators included: only ever used to name the
    *   offending input in an error message. `body` is what is actually matched on.
    */
  private def matcherOf(original: String, body: String): Either[String, Matcher] = {
    def reject: Either[String, Matcher] = Left(s"Invalid selector '$original': $Grammar.")
    // Only one '+' is allowed on each side, and it is stripped above: any '+' left here is a
    // doubled operator or a stray one in the middle.
    if (body.isEmpty || body.contains("+")) reject
    else if (body.toLowerCase.startsWith("tag:")) {
      val value = body.drop("tag:".length).trim
      if (value.isEmpty)
        Left(s"Invalid selector '$original': a tag selector needs a value, as in 'tag:daily'.")
      else Right(Matcher.Tag(value.toLowerCase))
    } else if (body.endsWith(".*")) {
      val domain = body.dropRight(".*".length)
      if (domain.isEmpty || domain.contains('.') || domain.contains('*')) reject
      else Right(Matcher.DomainAll(domain.toLowerCase))
    } else {
      // -1 keeps trailing empty segments, so "a.b." splits to three parts and is rejected
      val parts = body.split("\\.", -1)
      if (parts.length == 2 && parts.forall(part => part.nonEmpty && !part.contains('*')))
        Right(Matcher.Exact(body.toLowerCase))
      else reject
    }
  }
}
