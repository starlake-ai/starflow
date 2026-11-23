package ai.starlake.utils

import better.files.File
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.sys.process.Process

class GitSnapshotSpec extends AnyFlatSpec with Matchers {

  private def git(dir: File, args: String*): String =
    Process(Seq("git", "-C", dir.pathAsString) ++ args).!!.trim

  private def newRepo(): File = {
    val repo = File.newTemporaryDirectory("gitsnapshot-")
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "test")
    (repo / "proj" / "metadata").createDirectories()
    (repo / "proj" / "metadata" / "a.txt").writeText("v1")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "v1")
    repo
  }

  "GitSnapshot.projectAt" should "extract the project subdirectory as of a commit" in {
    val repo = newRepo()
    val c1 = git(repo, "rev-parse", "HEAD")
    (repo / "proj" / "metadata" / "a.txt").writeText("v2")
    git(repo, "commit", "-q", "-am", "v2")

    val checkout = GitSnapshot.projectAt(repo / "proj", c1).get
    try {
      (checkout.projectDir / "metadata" / "a.txt").contentAsString shouldBe "v1"
    } finally checkout.cleanup()
    checkout.cleanupDir.get.exists shouldBe false
    repo.delete()
  }

  "GitSnapshot.mergeBase" should "find the fork point of two branches" in {
    val repo = newRepo()
    val fork = git(repo, "rev-parse", "HEAD")
    git(repo, "switch", "-q", "-c", "feature")
    (repo / "proj" / "metadata" / "b.txt").writeText("x")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "feature")
    git(repo, "switch", "-q", "main")
    (repo / "proj" / "metadata" / "c.txt").writeText("y")
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "main moved")

    GitSnapshot.mergeBase(repo, "main", "feature").get shouldBe fork
    repo.delete()
  }

  "GitSnapshot.resolveCommit" should "fail on an unknown ref" in {
    val repo = newRepo()
    GitSnapshot.resolveCommit(repo, "does-not-exist").isFailure shouldBe true
    repo.delete()
  }
}
