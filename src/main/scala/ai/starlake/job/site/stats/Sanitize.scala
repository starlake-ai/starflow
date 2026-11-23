package ai.starlake.job.site.stats

import java.util.regex.Matcher
import scala.util.matching.Regex

/** Error messages written to sl-stats.json are published with the site: they must not leak
  * credentials, local paths or multi-line dumps (Spark plans, stack traces). The full message still
  * goes to the log.
  */
object Sanitize {
  val MaxLength = 300
  val PathPlaceholder = "<path>"
  val SecretPlaceholder = "<redacted>"

  /** A key is secret when it contains one of these words, case-insensitive. */
  private val secretWords = List(
    "password",
    "passwd",
    "secret",
    "token",
    "private_key",
    "privatekey",
    "api_key",
    "apikey",
    "access_key",
    "accesskey",
    "account_key",
    "accountkey",
    "signature",
    "credential"
  )

  /** Too short to match inside a word (`design`, `signal`, `assignment`): they count only as the
    * whole key or a whole segment of it (`sig`, `url_sig`, `x-amz-sig`, `db.pwd`).
    */
  private val shortSecretWords = List("sig", "pwd")

  private val secretWordPattern =
    secretWords.mkString("|") + "|(?<![A-Za-z0-9])(?:" + shortSecretWords.mkString("|") +
    ")(?![A-Za-z0-9])"

  // A key token: word characters, dots and dashes (sfPassword, PGPASSWORD, private_key_file_pwd,
  // x-amz-signature, spark.hadoop.fs.s3a.secret.key), starting at a token boundary. The bounded
  // repetitions keep a long token cheap: no key is longer.
  private val keyToken =
    """(?<![\w.-])([\w.-]{0,64}?(?:""" + secretWordPattern + """)[\w.-]{0,64})"""

  // The separator follows the key directly, after an optional closing quote and whitespace, so
  // `Column private_key not found: x` is not a pair. `%3D` and `%3A` are URL encoded `=` and `:`.
  private val separator = """(["']?)\s*(?:=|:|%3D|%3A)\s*"""

  private val value = List(
    // an already redacted value, so that redaction is idempotent
    Regex.quote(SecretPlaceholder),
    // a PEM block, quoted or not, up to its END line or, when cut, to the end of the message
    """["']?-----BEGIN [^-]*-----(?s:.*?)-----END [^-]*-----(?:\\n)*["']?""",
    """["']?-----BEGIN (?s:.*)""",
    // quoted strings with backslash escapes
    """"[^"\\]*(?:\\.[^"\\]*)*"""",
    """'[^'\\]*(?:\\.[^'\\]*)*'""",
    // an ODBC braced value, `}}` escapes a brace
    """\{[^}]*(?:\}\}[^}]*)*\}""",
    // unquoted: `,` and `)` are legal in a password, a trailing one of the text may go with it
    """[^\s&;]+"""
  ).mkString("(?:", "|", ")")

  private val keyedSecret = ("(?i)" + keyToken + separator + value).r

  // Authorization headers; the token charset leaves `<redacted>` alone (idempotence)
  private val tokenChars = """[\w.~+/=-]+"""
  private val authorizationHeader =
    ("""(?i)\b(authorization|proxy-authorization)(["']?)\s*[:=]\s*["']?""" +
      "(?:(?:Bearer|Basic|Digest|Token)\\s+)?" + tokenChars + "[\"']?").r
  private val bearer = ("""\bBearer\s+""" + tokenChars).r

  // user:password@ in a URL; the password runs to the last @ before the next /, whitespace or end
  private val urlUserInfo = """(?<=//)[^/@\s:]+:[^\s/]*@""".r

  // A path runs until whitespace, a quote or a closing delimiter
  private val pathChars = """[^\s'"`)\]>,;]*"""

  private val fileUri = ("(?i)file:/" + pathChars).r
  // C:\x or C:/x, not preceded by a word character (so jdbc:duckdb:/x is left to the Unix rule)
  private val windowsPath = ("""(?<!\w)[A-Za-z]:[\\/]""" + pathChars).r
  // /x at the start or after a separator; //host (URLs) and a / b (operators) are kept
  private val unixPath = ("""(^|[\s'"`(\[=,:])/(?=[\w.~$-])""" + pathChars).r

  /** Credentials replaced by `<redacted>`:
    *   - `Authorization: <scheme> <token>` headers and bare `Bearer <token>`;
    *   - the `user:password@` of a URL;
    *   - the value of a key that contains a secret word (password, pwd, secret, token, private_key,
    *     api_key, access_key, account_key, signature, credential, sig...) followed directly by `=`
    *     or `:`: quoted, braced, a PEM block or an unquoted run up to whitespace, `&` or `;`. A
    *     parser message such as `unexpected token: "FROM"` is redacted too, an accepted false
    *     positive.
    */
  def redact(s: String): String = {
    val noHeaders = authorizationHeader.replaceAllIn(
      s,
      m => Matcher.quoteReplacement(m.group(1) + m.group(2) + ": " + SecretPlaceholder)
    )
    val noBearer =
      bearer.replaceAllIn(noHeaders, Matcher.quoteReplacement("Bearer " + SecretPlaceholder))
    val noUserInfo =
      urlUserInfo.replaceAllIn(noBearer, Matcher.quoteReplacement(SecretPlaceholder + "@"))
    keyedSecret.replaceAllIn(
      noUserInfo,
      m => Matcher.quoteReplacement(m.group(1) + m.group(2) + "=" + SecretPlaceholder)
    )
  }

  /** Credentials replaced by `<redacted>` (see [[redact]]) in the whole message, then its first non
    * blank line, whitespace collapsed, local paths replaced by `<path>`, at most 300 characters
    * (cut ones end with `...`). Idempotent.
    */
  def message(raw: String): String = {
    val firstLine = Option(raw).toList
      .flatMap(r => redact(r).linesIterator)
      .map(_.replaceAll("\\s+", " ").trim)
      .find(_.nonEmpty)
      .getOrElse("")
    val noFile = fileUri.replaceAllIn(firstLine, Matcher.quoteReplacement(PathPlaceholder))
    val noWindows = windowsPath.replaceAllIn(noFile, Matcher.quoteReplacement(PathPlaceholder))
    val noPaths = unixPath.replaceAllIn(
      noWindows,
      m => Matcher.quoteReplacement(m.group(1) + PathPlaceholder)
    )
    if (noPaths.length <= MaxLength) noPaths
    else noPaths.take(MaxLength - 3) + "..."
  }
}
