package ai.starlake.job.ingest.loaders

/** A single input line rejected by a native loader.
  *
  * @param file
  *   the input file the line comes from
  * @param line
  *   the 1 based line number inside that file, when the engine reports one. POSITION
  *   rejects are found by scanning a temporary table, which carries no line number, so
  *   they leave this empty.
  * @param rawLine
  *   the original line, byte faithful, as it appears in the input file
  * @param error
  *   a human readable description of every error found on that line
  */
case class RejectedLine(file: String, line: Option[Long], rawLine: String, error: String)

/** Thrown when the number of rejected lines breaches `rejectMaxRecords`, or when
  * `rejectAllOnError` is set and there is at least one reject. It carries the rejected
  * lines so the caller can still write the replay file and the audit rejected rows before
  * failing the load.
  */
class RejectThresholdExceededException(val rejected: List[RejectedLine], message: String)
    extends RuntimeException(message)
