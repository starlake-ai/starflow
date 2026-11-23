package ai.starlake.job.site.stats

import java.util.regex.Matcher

/** Error messages written to sl-stats.json are published with the site: they must not leak
  * credentials, local paths or multi-line dumps (Spark plans, stack traces). The full message still
  * goes to the log.
  */
object Sanitize {
  val MaxLength = 300
  val PathPlaceholder = "<path>"
  val SecretPlaceholder = "<redacted>"

  // key=value or key: value, the value quoted or running until a separator (DSNs, query strings,
  // header lines)
  private val keyedSecret =
    ("(?i)(password|pwd|passwd|secret|token|access_key|secret_key|private_key[^=]*|sig|signature|" +
      """x-amz-[a-z-]+)\s*[=:]\s*("[^"]*"|'[^']*'|[^\s&;,)]+)""").r

  // user:password@ in a URL
  private val urlUserInfo = """(?<=//)[^/@\s:]+:[^/@\s]+@""".r

  // A path runs until whitespace, a quote or a closing delimiter
  private val pathChars = """[^\s'"`)\]>,;]*"""

  private val fileUri = ("(?i)file:/" + pathChars).r
  // C:\x or C:/x, not preceded by a word character (so jdbc:duckdb:/x is left to the Unix rule)
  private val windowsPath = ("""(?<!\w)[A-Za-z]:[\\/]""" + pathChars).r
  // /x at the start or after a separator; //host (URLs) and a / b (operators) are kept
  private val unixPath = ("""(^|[\s'"`(\[=,:])/(?=[\w.~$-])""" + pathChars).r

  /** Credentials in `key=value` pairs and URL user info replaced by `<redacted>`. */
  def redact(s: String): String = {
    val noKeyed =
      keyedSecret.replaceAllIn(
        s,
        m => Matcher.quoteReplacement(m.group(1) + "=" + SecretPlaceholder)
      )
    urlUserInfo.replaceAllIn(noKeyed, Matcher.quoteReplacement(SecretPlaceholder + "@"))
  }

  /** First non blank line, whitespace collapsed, credentials replaced by `<redacted>` (see
    * [[redact]]), local paths replaced by `<path>`, at most 300 characters (cut ones end with
    * `...`). Idempotent.
    */
  def message(raw: String): String = {
    val firstLine = Option(raw).toList
      .flatMap(_.linesIterator)
      .map(_.replaceAll("\\s+", " ").trim)
      .find(_.nonEmpty)
      .getOrElse("")
    val noFile = fileUri.replaceAllIn(redact(firstLine), Matcher.quoteReplacement(PathPlaceholder))
    val noWindows = windowsPath.replaceAllIn(noFile, Matcher.quoteReplacement(PathPlaceholder))
    val noPaths = unixPath.replaceAllIn(
      noWindows,
      m => Matcher.quoteReplacement(m.group(1) + PathPlaceholder)
    )
    if (noPaths.length <= MaxLength) noPaths
    else noPaths.take(MaxLength - 3) + "..."
  }
}
