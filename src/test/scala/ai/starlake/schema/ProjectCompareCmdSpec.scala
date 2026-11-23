package ai.starlake.schema

import ai.starlake.TestHelper
import ai.starlake.impact.ImpactFixtures
import better.files.File

import scala.sys.process.Process

/** `compare` through its git path, which now goes through GitSnapshot. */
class ProjectCompareCmdSpec extends TestHelper {

  private def git(repo: File, args: String*): String =
    Process(Seq("git", "-C", repo.pathAsString) ++ args).!!.trim

  /** A repository whose root is a project, with two commits: the first one tagged v1. */
  private def twoCommits(): (File, String, String) = {
    val repo = File.newTemporaryDirectory("compare-git-")
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "test")
    git(repo, "config", "commit.gpgsign", "false")
    ImpactFixtures.writeProject(repo)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "v1")
    git(repo, "tag", "-a", "v1", "-m", "v1")
    val c1 = git(repo, "rev-parse", "HEAD")
    (repo / "metadata/transform/kpi/revenue.sql")
      .writeText("SELECT s.id, s.amount * 3 AS total FROM stg.orders s\n")
    git(repo, "commit", "-q", "-am", "v2")
    (repo, c1, git(repo, "rev-parse", "HEAD"))
  }

  "compare" should "compare two commits and a tag of a git repository" in {
    new WithSettings() {
      val (repo, c1, c2) = twoCommits()
      val out = File.newTemporaryDirectory("compare-out-")
      try {
        List(
          "commits"    -> ProjectCompareConfig(commit1 = c1, commit2 = c2),
          "latest tag" -> ProjectCompareConfig(tag1 = "latest", commit2 = c2)
        ).foreach { case (name, config) =>
          withClue(name) {
            val output = out / s"$name.html"
            val result = ProjectCompareCmd.run(
              config.copy(gitWorkTree = repo.pathAsString, output = Some(output.pathAsString)),
              settings.schemaHandler()
            )
            result.isSuccess shouldBe true
            output.contentAsString should include("Project Comparison Report")
          }
        }
      } finally {
        repo.delete(swallowIOExceptions = true)
        out.delete(swallowIOExceptions = true)
      }
    }
  }

  it should "fail on an unknown commit instead of comparing an empty project" in {
    new WithSettings() {
      val (repo, _, c2) = twoCommits()
      try {
        val config = ProjectCompareConfig(
          gitWorkTree = repo.pathAsString,
          commit1 = "0000000000000000000000000000000000000000",
          commit2 = c2
        )
        an[IllegalArgumentException] should be thrownBy
        ProjectCompareCmd.run(config, settings.schemaHandler())
      } finally repo.delete(swallowIOExceptions = true)
    }
  }
}
