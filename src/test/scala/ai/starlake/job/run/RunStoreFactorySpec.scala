package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer

class RunStoreFactorySpec extends AnyFlatSpec with Matchers {

  "RunStore.fileLogRoot" should "put the log under the project root by default" in {
    val resolved = RunStore.fileLogRoot("/projects/sales", Map.empty)
    resolved.path shouldBe Paths.get("/projects/sales/.starlake/runs")
    resolved.notice shouldBe None
  }

  it should "honour SL_RUN_LOG_DIR" in {
    val resolved =
      RunStore.fileLogRoot("/projects/sales", Map(RunStore.LogDirEnv -> "/var/log/starlake"))
    resolved.path shouldBe Paths.get("/var/log/starlake")
    resolved.notice shouldBe None
  }

  it should "treat a file:// root as local" in {
    val resolved = RunStore.fileLogRoot("file:///projects/sales", Map.empty)
    resolved.path shouldBe Paths.get("/projects/sales/.starlake/runs")
    resolved.notice shouldBe None
  }

  it should "treat file:/, file:// and file://localhost roots as the same local path" in {
    val expected = Paths.get("/projects/sales/.starlake/runs")
    // Paths.get(URI) rejects an authority outright, so "file://localhost/..." must not fall through
    // to the remote branch: that would misreport a local path as not local.
    RunStore.fileLogRoot("file:/projects/sales", Map.empty).path shouldBe expected
    RunStore.fileLogRoot("file:///projects/sales", Map.empty).path shouldBe expected
    RunStore.fileLogRoot("file://localhost/projects/sales", Map.empty).path shouldBe expected
  }

  it should "fall back to a temp directory when the root is an object store" in {
    val resolved = RunStore.fileLogRoot("gs://bucket/sales", Map.empty)
    resolved.path.getParent.getFileName.toString shouldBe "starlake-runs"
    // A remote root must not mean a silently missing log, and must not fail the run either.
    val notice = resolved.notice.getOrElse(fail("expected a notice"))
    notice should include("gs://bucket/sales")
    notice should include(RunStore.LogDirEnv)
  }

  it should "namespace the fallback by the remote root, so two projects don't collide" in {
    val fallbackA = RunStore.fileLogRoot("gs://bucket-a/sales", Map.empty).path
    val fallbackB = RunStore.fileLogRoot("gs://bucket-b/sales", Map.empty).path
    fallbackA should not be fallbackB

    val fallbackAAgain = RunStore.fileLogRoot("gs://bucket-a/sales", Map.empty).path
    fallbackAAgain shouldBe fallbackA
  }

  it should "prefer SL_RUN_LOG_DIR over the fallback for a remote root" in {
    val resolved =
      RunStore.fileLogRoot("s3://bucket/sales", Map(RunStore.LogDirEnv -> "/mnt/runs"))
    resolved.path shouldBe Paths.get("/mnt/runs")
    resolved.notice shouldBe None
  }

  "RunStore.forRun" should "build a file store and report the fallback once" in {
    val notices = ListBuffer[String]()
    val store = RunStore.forRun("gs://bucket/sales", Map.empty, notices += _)
    try {
      store shouldBe a[FileRunStore]
      notices.size shouldBe 1
    } finally store.close()
  }

  it should "say nothing when the local default applies" in {
    val notices = ListBuffer[String]()
    val store = RunStore.forRun("/projects/sales", Map.empty, notices += _)
    try notices shouldBe empty
    finally store.close()
  }

  it should "take the JDBC branch when the api injected a url" in {
    // No database here: reaching JdbcRunStore.open is the assertion, and its failure message is
    // the one a user would get from a bad SL_RUN_STORE_URL.
    val thrown = the[RunStoreException] thrownBy RunStore.forRun(
      "/projects/sales",
      Map(RunStore.UrlEnv -> "jdbc:nosuchdriver://localhost/db"),
      _ => ()
    )
    thrown.getMessage should include("jdbc:nosuchdriver")
  }

  it should "treat blank JDBC user and password as absent, not as an empty login" in {
    // starlake-api exports all five env vars and may leave the credentials blank. Blank must reach
    // JdbcRunStore.open as None, the same as the var being absent altogether, rather than as
    // Some(""), which would attempt a login with an empty password instead of connecting without
    // credentials.
    val url = "jdbc:nosuchdriver://localhost/db"
    val withBlankCredentials = the[RunStoreException] thrownBy RunStore.forRun(
      "/projects/sales",
      Map(RunStore.UrlEnv -> url, RunStore.UserEnv -> "", RunStore.PasswordEnv -> ""),
      _ => ()
    )
    val withNoCredentials = the[RunStoreException] thrownBy RunStore.forRun(
      "/projects/sales",
      Map(RunStore.UrlEnv -> url),
      _ => ()
    )
    withBlankCredentials.getMessage shouldBe withNoCredentials.getMessage
  }
}
