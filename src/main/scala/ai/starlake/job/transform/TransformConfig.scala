package ai.starlake.job.transform

import ai.starlake.job.ReportFormatConfig
import ai.starlake.schema.model.Engine

case class TransformConfig(
  action: TransformAction = TransformAction.Run,
  name: String = "",
  options: Map[String, String] = Map.empty,
  compile: Boolean = false,
  syncApply: Boolean = false,
  syncPreview: Boolean = false,
  tags: Seq[String] = Seq.empty,
  format: Boolean = false,
  interactive: Option[String] = None,
  reload: Boolean = false,
  truncate: Boolean = false,
  recursive: Boolean = false,
  test: Boolean = false,
  accessToken: Option[String] = None,
  dryRun: Boolean = false,
  query: Option[String] = None,
  pageSize: Int = 1000,
  pageNumber: Int = 1,
  scheduledDate: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig {
  def optionsAsString: String = options.map { case (k, v) => s"$k=$v" }.mkString(",")

  /** The tasks listed by `--name`, which accepts several names separated by commas */
  def names: List[String] = name.split(',').map(_.trim).filter(_.nonEmpty).toList

  /** True when this config selects a set of tasks rather than a single one. `--recursive` is not
    * part of this: it starts from one task, and starlake-api passes the single task flags with it.
    */
  def isMultiTask: Boolean = names.size > 1 || tags.nonEmpty

  /** Flags set on this config that its tasks could not honor, since each task of a batch runs with
    * a config of its own. `--compile` stays allowed with `--recursive`: it reads the one named task
    * and never reaches the batch.
    */
  def incompatibleFlags: List[String] = {
    val perTaskFlags = List(
      "--dry-run"     -> dryRun,
      "--interactive" -> interactive.isDefined,
      "--query"       -> query.isDefined
    )
    val singleTaskFlags = List(
      "--compile"      -> compile,
      "--sync-apply"   -> syncApply,
      "--sync-preview" -> syncPreview,
      "--format"       -> format
    )
    val checked =
      if (isMultiTask) perTaskFlags ++ singleTaskFlags
      else if (recursive) perTaskFlags
      else Nil
    checked.collect { case (flag, true) => flag }
  }

  def requireCompatibleFlags(): Unit =
    require(
      incompatibleFlags.isEmpty,
      s"${incompatibleFlags.mkString(", ")} cannot be used with --recursive, --tags or several tasks"
    )

  /** Only BigQuery has a dry run. The other engines ignore the flag and write their tables. */
  def dryRunSupportedOn(engine: Engine): Boolean = !dryRun || engine == Engine.BQ

  override def toString: String = {
    s"""
       |name=$name
       |options=$optionsAsString
       |compile=$compile
       |syncApply=$syncApply
       |syncPreview=$syncPreview
       |tags=$tags
       |format=$format
       |interactive=$interactive
       |reload=$reload
       |truncate=$truncate
       |recursive=$recursive
       |test=$test
       |accessToken=$accessToken
       |dryRun=$dryRun
       |query=$query
       |pageSize=$pageSize
       |pageNumber=$pageNumber
       |reportFormat=$reportFormat
       |""".stripMargin
  }
}
