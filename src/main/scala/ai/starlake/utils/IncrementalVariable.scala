package ai.starlake.utils

/** The Jinja variable that tells a task's SQL whether this run is incremental: the target existed
  * when the run started and the run is not --truncate
  */
object IncrementalVariable {

  val Name = "sl_incremental"

  private val Pattern = """\bsl_incremental\b""".r

  /** Whether one of these texts refers to the variable */
  def mentionedIn(texts: List[String]): Boolean =
    texts.exists(text => Pattern.findFirstIn(text).isDefined)
}
