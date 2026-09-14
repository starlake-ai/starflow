package ai.starlake.job.run

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Run log on the local filesystem, one directory per run and one file per attempt:
  *
  * {{{
  * <rootDir>/<runId>/events.jsonl      attempt 1
  *                   events.2.jsonl     attempt 2
  * }}}
  *
  * One file per attempt rather than one appended file per run, because a `kill -9` can tear the
  * final line. Per attempt, that damage is always at the end of a file, where the reader can drop
  * it; appended into one file, the next attempt would write after the torn line and every later
  * reader would have to tolerate corruption in the middle.
  *
  * Writes go through java.nio rather than StorageHandler on purpose: the point of this file is a
  * real append with a flush per event, which object-store semantics cannot give, and
  * StorageHandler.output creates rather than appends. `flush()` hands the bytes to the OS, which
  * is what makes them survive the process being killed; no fsync, since a machine that loses
  * power is not a case resume can help with anyway.
  */
class FileRunStore(rootDir: Path) extends RunStore {

  private var writer: Option[BufferedWriter] = None

  private def runDir(runId: String): Path = rootDir.resolve(runId)

  private def attemptFile(runId: String, attempt: Int): Path =
    runDir(runId).resolve(if (attempt == 1) "events.jsonl" else s"events.$attempt.jsonl")

  private def attemptNumber(fileName: String): Option[Int] =
    fileName match {
      case "events.jsonl"                   => Some(1)
      case FileRunStore.AttemptFile(digits) => digits.toIntOption
      case _                                => None
    }

  private def attemptsOf(runId: String): List[Int] = {
    val dir = runDir(runId)
    if (!Files.isDirectory(dir)) Nil
    else {
      val stream = Files.list(dir)
      try
        // Numeric sort, not lexical: "events.10.jsonl" sorts before "events.2.jsonl" as text.
        stream.iterator().asScala.toList.flatMap(p => attemptNumber(p.getFileName.toString)).sorted
      finally stream.close()
    }
  }

  private def openFor(runId: String, attempt: Int): Unit = {
    close()
    Files.createDirectories(runDir(runId))
    writer = Some(
      Files.newBufferedWriter(
        attemptFile(runId, attempt),
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
    )
  }

  def start(header: RunLogEvent): Int = {
    openFor(header.runId, 1)
    append(header)
    1
  }

  def reopen(runId: String): Int = {
    val attempts = attemptsOf(runId)
    if (attempts.isEmpty)
      throw new RunStoreException(s"Unknown run '$runId': no run log under $rootDir")
    val next = attempts.max + 1
    openFor(runId, next)
    next
  }

  def append(event: RunLogEvent): Unit = {
    val out = writer.getOrElse(
      throw new RunStoreException("append called before start() or reopen()")
    )
    try {
      out.write(RunLog.toJson(event))
      out.newLine()
      out.flush()
    } catch {
      case NonFatal(e) =>
        throw new RunStoreException(s"Cannot write the run log under $rootDir: ${e.getMessage}", e)
    }
  }

  def read(runId: String): Option[RunHistory] = {
    val attempts = attemptsOf(runId)
    if (attempts.isEmpty) None
    else RunHistory.fold(attempts.flatMap(attempt => eventsIn(attemptFile(runId, attempt))))
  }

  def latest(): Option[RunHistory] = {
    if (!Files.isDirectory(rootDir)) None
    else {
      val stream = Files.list(rootDir)
      val runIds =
        try
          stream
            .iterator()
            .asScala
            .filter(Files.isDirectory(_))
            .map(_.getFileName.toString)
            .toList
            .sorted
            .reverse
        finally stream.close()
      // Run ids start with yyyyMMdd-HHmmss, so reverse name order is most-recent-first.
      runIds.view.flatMap(read).headOption
    }
  }

  def close(): Unit = {
    writer.foreach(_.close())
    writer = None
  }

  private def eventsIn(file: Path): List[RunLogEvent] = {
    val lines =
      Files.readAllLines(file, StandardCharsets.UTF_8).asScala.toList.filter(_.trim.nonEmpty)
    lines.zipWithIndex.flatMap { case (line, index) =>
      RunLog.fromJson(line) match {
        case Right(event) => Some(event)
        // A torn last line is the signature of a killed process: drop it. Anywhere else it is
        // corruption we did not cause, and skipping it would silently drop a recorded success.
        case Left(_) if index == lines.size - 1 => None
        case Left(error) =>
          throw new RunStoreException(s"Corrupt run log $file at line ${index + 1}: $error")
      }
    }
  }
}

object FileRunStore {
  private val AttemptFile = """events\.(\d+)\.jsonl""".r
}
