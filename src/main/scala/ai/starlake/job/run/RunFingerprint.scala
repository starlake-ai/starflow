package ai.starlake.job.run

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** The four whole-run inputs that decide whether a recorded run may be resumed, each hashed
  * separately so a refusal can name the one that changed. Task bodies are not in here: they are
  * digested per task and compared selectively, see RunFingerprint.changedTasks.
  */
final case class FingerprintParts(
  graph: String,
  selection: String,
  options: String,
  env: String
) {

  def toMap: Map[String, String] =
    Map("graph" -> graph, "selection" -> selection, "options" -> options, "env" -> env)

  def digest: String =
    RunFingerprint.sha256(toMap.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString("\n"))
}

object RunFingerprint {

  /** @param envVars
    *   hashed by value because they are substituted into SQL through Jinja. The hash is one-way, so
    *   no secret is stored: only the digest reaches the log.
    */
  def parts(
    dag: RunDag,
    selection: Set[String],
    options: Map[String, String],
    envName: String,
    envVars: Map[String, String]
  ): FingerprintParts =
    FingerprintParts(
      graph = sha256(canonicalGraph(dag)),
      selection = sha256(selection.toList.sorted.map(sha256).mkString(",")),
      options = sha256(canonicalMap(options)),
      env = sha256(sha256(envName) + "," + canonicalMap(envVars))
    )

  /** @param taskContent
    *   node id to the text whose change should block a resume *of that task's recorded success*.
    *   Transforms contribute their SQL; load tables contribute nothing, because a load node's
    *   behaviour is driven by whatever files are in the stage area, which change between every run
    *   by design.
    */
  def taskDigests(taskContent: Map[String, String]): Map[String, String] =
    taskContent.map { case (id, content) => id -> sha256(content) }

  /** Which recorded whole-run parts the current run disagrees with, in a stable order.
    *
    * A log that carries no breakdown yields "unknown": we know the digests differ, we cannot say
    * where, and naming a cause we did not verify would send the user to the wrong file.
    */
  def changed(recorded: Map[String, String], current: FingerprintParts): List[String] = {
    val currentParts = current.toMap
    if (recorded.isEmpty) List("unknown")
    else
      currentParts.keys.toList.sorted.filter { key =>
        recorded.get(key) match {
          case Some(value) => value != currentParts(key)
          // Recorded before this part existed. Unverifiable is not the same as equal, and guessing
          // "equal" is the direction that keeps a stale result.
          case None => true
        }
      }
  }

  /** Tasks whose body changed *and* whose recorded success a resume intends to keep.
    *
    * A task the resume will execute again is not listed however much it changed: it is re-run, so
    * nothing stale survives it. A task the recorded run never knew is not listed either, since
    * there is no recorded success of it to protect.
    */
  def changedTasks(
    recorded: Map[String, String],
    current: Map[String, String],
    amongst: Set[String]
  ): List[String] =
    amongst.toList.sorted.filter { id =>
      recorded.get(id).exists(digest => !current.get(id).contains(digest))
    }

  def label(part: String): String =
    part match {
      case "graph"     => "the task graph (a task, a table or a dependency changed)"
      case "selection" => "the set of selected tasks"
      case "options"   => "the --options values"
      case "env"       => "the active env or one of its variables"
      case _           => "something in the project"
    }

  /** Separator that cannot occur in any input: ids, type names and env values are all text, and
    * text does not contain NUL. Joining on a printable character instead would let a value that
    * contains that character imitate a different input, and a fingerprint that cannot tell two
    * projects apart is one that resumes across a change it should have refused.
    */
  private val Sep = "\u0000"

  private def canonicalGraph(dag: RunDag): String =
    dag.nodes.values.toList
      .sortBy(_.id)
      .map { node =>
        val parents = dag.parents.getOrElse(node.id, Set.empty).toList.sorted.mkString(Sep)
        // Each node collapses to a fixed-length digest, so no node's content can spill into the
        // next one's field when they are joined below.
        sha256(node.id + Sep + typeName(node.typ) + Sep + parents)
      }
      .mkString(",")

  /** Named explicitly rather than through `toString`: an added `override def toString` on
    * RunNodeType would otherwise change every recorded fingerprint with no compile-time signal.
    */
  private def typeName(typ: RunNodeType): String =
    typ match {
      case RunNodeType.Task      => "Task"
      case RunNodeType.LoadTable => "LoadTable"
      case RunNodeType.Boundary  => "Boundary"
    }

  /** Each entry is digested before joining, so every element of the final join is a fixed-length
    * hex digest that cannot itself contain the "," join separator or the `Sep` used inside an
    * entry. Without this, `Map("A" -> "1\nB=2")` and `Map("A" -> "1", "B" -> "2")` would serialize
    * to the same string and hash the same, which would let a run resume across an env or options
    * change it should have refused.
    */
  private def canonicalMap(values: Map[String, String]): String =
    values.toList.sorted.map { case (k, v) => sha256(k + Sep + v) }.mkString(",")

  private[run] def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}
