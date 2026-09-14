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
    *   hashed by value because they are substituted into SQL through Jinja. The hash is one-way,
    *   so no secret is stored: only the digest reaches the log.
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
      selection = sha256(selection.toList.sorted.mkString("\n")),
      options = sha256(canonicalMap(options)),
      env = sha256(envName + "\n" + canonicalMap(envVars))
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
      currentParts.keys.toList.sorted.filter(key => recorded.get(key).exists(_ != currentParts(key)))
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
    amongst.toList.sorted.filter(id => recorded.get(id).exists(digest => !current.get(id).contains(digest)))

  def label(part: String): String =
    part match {
      case "graph"     => "the task graph (a task, a table or a dependency changed)"
      case "selection" => "the set of selected tasks"
      case "options"   => "the --options values"
      case "env"       => "the active env or one of its variables"
      case _           => "something in the project"
    }

  private def canonicalGraph(dag: RunDag): String =
    dag.nodes.values.toList
      .sortBy(_.id)
      .map { node =>
        val parents = dag.parents.getOrElse(node.id, Set.empty).toList.sorted.mkString(",")
        s"${node.id}|${node.typ}|$parents"
      }
      .mkString("\n")

  private def canonicalMap(values: Map[String, String]): String =
    values.toList.sorted.map { case (k, v) => s"$k=$v" }.mkString("\n")

  private[run] def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}
