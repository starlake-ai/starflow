package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase
import better.files.File

/** Copies src/test/resources/site/fixture to a temp folder and runs the block with SL_ROOT on it.
  */
trait SiteFixture extends IntegrationTestBase {
  lazy val fixtureRoot: File = {
    val tmp = File.newTemporaryDirectory("sl-site-fixture-")
    File("src/test/resources/site/fixture").copyTo(tmp)(File.CopyOptions(overwrite = true))
    tmp
  }

  override def beforeEach(): Unit = ()
  override def afterEach(): Unit = ()

  def withFixture[T](f: Settings => T): T =
    withEnvs("SL_ROOT" -> fixtureRoot.pathAsString, "SL_ENV" -> "") {
      f(settings)
    }
}
