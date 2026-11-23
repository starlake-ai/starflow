package ai.starlake.utils

import better.files.File

import scala.sys.process.Process
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

  def archive(repo: File, commit: String): Try[File] = Try {
    val target = File.newTemporaryDirectory("starlake-git-")
    val status =
      (Process(Seq("git", "-C", repo.pathAsString, "archive", commit)) #| Process(
        Seq("tar", "-x", "-C", target.pathAsString)
      )).!
    if (status != 0) {
      target.delete(swallowIOExceptions = true)
      throw new IllegalArgumentException(s"Failed to extract commit $commit")
    }
    target
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
