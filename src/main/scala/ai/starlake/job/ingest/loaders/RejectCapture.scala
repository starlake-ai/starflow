package ai.starlake.job.ingest.loaders

import ai.starlake.config.Settings

import java.nio.file.{Path => LocalPath}

/** What a native loader kept from the lines an engine refused.
  *
  * A load that reads a multi gigabyte file with the wrong delimiter rejects every line of it, so
  * the rejected lines cannot be held in memory: the capture keeps an exact count, at most
  * `audit.maxErrors` lines materialized, and every raw line spilled to a local temporary file that
  * the replay writer streams straight to its output.
  *
  * @param count
  *   the exact number of rejected input lines, whatever was materialized. This is what the reject
  *   threshold and the reported `rejectedCount` are computed from.
  * @param sample
  *   at most `audit.maxErrors` rejected lines, the only ones the audit rejected table can hold
  *   anyway
  * @param spillFiles
  *   local temporary files holding every rejected raw line, one per line terminated by `\n`, in the
  *   order the input paths were read. They are the replay file's content, and the loader deletes
  *   them once it is written.
  */
case class RejectCapture(
  count: Long,
  sample: List[RejectedLine],
  spillFiles: List[LocalPath]
) {
  def isEmpty: Boolean = count == 0L

  def nonEmpty: Boolean = count > 0L

  /** Merges the capture of another input path into this one. The sample is capped again on the way
    * in, so loading a hundred files cannot retain a hundred times `audit.maxErrors` lines, and the
    * spill files keep the path order the replay file has to reproduce.
    */
  def ++(other: RejectCapture)(implicit settings: Settings): RejectCapture =
    RejectCapture(
      count = count + other.count,
      sample = (sample ++ other.sample).take(settings.appConfig.audit.maxErrors),
      spillFiles = spillFiles ++ other.spillFiles
    )
}

object RejectCapture {
  val empty: RejectCapture = RejectCapture(0L, Nil, Nil)
}
