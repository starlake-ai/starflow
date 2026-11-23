package ai.starlake.utils

import better.files.File

import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Failure, Success, Try}

/** A project directory taken from git. `cleanupDir` is the temporary archive root to delete, when
  * the directory was extracted from a commit.
  */
final case class ProjectCheckout(projectDir: File, cleanupDir: Option[File]) {
  def cleanup(): Unit = cleanupDir.foreach(_.delete(swallowIOExceptions = true))
}

object GitSnapshot {

  private def git(repo: File, args: String*): Try[String] =
    Utils.runCommand(Seq("git", "-C", repo.pathAsString) ++ args).flatMap { out =>
      if (out.exit == 0) Success(out.output.trim)
      else
        Failure(
          new IllegalArgumentException(
            s"git ${args.mkString(" ")} failed: ${Option(out.error).getOrElse("").trim}"
          )
        )
    }

  def topLevel(path: File): Try[File] =
    git(path, "rev-parse", "--show-toplevel").map(File(_))

  def resolveCommit(repo: File, ref: String): Try[String] =
    git(repo, "rev-parse", "--verify", "--quiet", s"$ref^{commit}")

  def mergeBase(repo: File, a: String, b: String): Try[String] =
    git(repo, "merge-base", a, b)

  def commitOfTag(repo: File, tag: String): Try[String] = {
    val tagName =
      if (tag == "latest") git(repo, "describe", "--abbrev=0") else Success(tag)
    tagName.flatMap(name => git(repo, "rev-list", "-n", "1", s"tags/$name"))
  }

  /** The files of `commit`, extracted to a new temporary directory. git writes the archive to a
    * file first: in a pipe to tar, a failure of git (an unknown commit) would be lost, since the
    * status is the one of tar, which extracts an empty input without error on macOS.
    */
  def archive(repo: File, commit: String): Try[File] = Try {
    val target = File.newTemporaryDirectory("starlake-git-")
    val tarFile = File.newTemporaryFile("starlake-archive-", ".tar")
    val errors = new StringBuilder
    val logger = ProcessLogger(_ => (), line => errors.append(line).append('\n'))
    try {
      val gitStatus = Process(
        Seq(
          "git",
          "-C",
          repo.pathAsString,
          "archive",
          "--format=tar",
          "-o",
          tarFile.pathAsString,
          commit
        )
      ).!(logger)
      val status =
        if (gitStatus != 0) gitStatus
        else
          Process(Seq("tar", "-x", "-f", tarFile.pathAsString, "-C", target.pathAsString)).!(logger)
      if (status != 0)
        throw new IllegalArgumentException(
          s"Failed to extract commit $commit: ${errors.toString.trim}"
        )
      target
    } catch {
      case e: Throwable =>
        target.delete(swallowIOExceptions = true)
        throw e
    } finally tarFile.delete(swallowIOExceptions = true)
  }

  /** The project directory as it was at `commit`. The project may sit in a subdirectory of the
    * repository (for example samples/lineage), so the same relative path is applied inside the
    * archive.
    */
  def projectAt(projectDir: File, commit: String): Try[ProjectCheckout] =
    for {
      repo     <- topLevel(projectDir)
      relative <- Try(repo.path.toRealPath().relativize(projectDir.path.toRealPath()))
      archived <- archive(repo, commit)
    } yield ProjectCheckout(File(archived.path.resolve(relative)), Some(archived))
}
