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

  "GitSnapshot.commitOfTag" should "resolve a named tag and the latest one" in {
    val repo = newRepo()
    try {
      val c1 = git(repo, "rev-parse", "HEAD")
      git(repo, "tag", "-a", "v1", "-m", "v1")
      (repo / "proj" / "metadata" / "a.txt").writeText("v2")
      git(repo, "commit", "-q", "-am", "v2")
      val c2 = git(repo, "rev-parse", "HEAD")
      git(repo, "tag", "-a", "v2", "-m", "v2")
      (repo / "proj" / "metadata" / "a.txt").writeText("v3")
      git(repo, "commit", "-q", "-am", "v3")

      GitSnapshot.commitOfTag(repo, "v1").get shouldBe c1
      GitSnapshot.commitOfTag(repo, "latest").get shouldBe c2
      GitSnapshot.commitOfTag(repo, "no-such-tag").isFailure shouldBe true
    } finally repo.delete()
  }

  "GitSnapshot.archive" should "fail on an unknown commit and leave no directory behind" in {
    val repo = newRepo()
    try {
      def checkouts(): Set[String] =
        File(System.getProperty("java.io.tmpdir")).list
          .filter(_.name.startsWith("starlake-git-"))
          .map(_.name)
          .toSet
      val before = checkouts()
      val result = GitSnapshot.archive(repo, "0000000000000000000000000000000000000000")
      result.failed.get.getMessage should include("0000000000000000000000000000000000000000")
      checkouts() shouldBe before
    } finally repo.delete()
  }

  "GitSnapshot.projectAt" should "extract a project at the root of the repository" in {
    val repo = newRepo()
    try {
      val c1 = git(repo, "rev-parse", "HEAD")
      (repo / "proj" / "metadata" / "a.txt").writeText("v2")
      git(repo, "commit", "-q", "-am", "v2")
      val checkout = GitSnapshot.projectAt(repo, c1).get
      try {
        checkout.projectDir shouldBe checkout.cleanupDir.get
        (checkout.projectDir / "proj" / "metadata" / "a.txt").contentAsString shouldBe "v1"
      } finally checkout.cleanup()
    } finally repo.delete()
  }
}
