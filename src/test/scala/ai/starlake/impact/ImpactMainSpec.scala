package ai.starlake.impact

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.Main
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper

import java.io.ByteArrayOutputStream

/** `impact` through the CLI entry point: Main must neither create folders in the project nor print
  * anything but the report on stdout.
  */
class ImpactMainSpec extends TestHelper {

  private val breakingStg = Map(
    "metadata/transform/stg/orders.sql" ->
    "SELECT o.id, o.amount * 100 AS amount FROM raw.orders o WHERE o.status = 'ok'\n"
  )

  /** Every file and directory under `dir`, empty directories included. */
  private def listing(dir: File): Set[String] =
    dir.listRecursively.map(f => dir.relativize(f).toString).toSet

  private def runMain(env: (String, String)*)(args: String*): (Int, String) = {
    val stdout = new ByteArrayOutputStream()
    val code = withEnvs(env: _*) {
      Console.withOut(stdout)(new Main().runWithExitCode(args.toArray))
    }
    (code, stdout.toString)
  }

  "starlake impact" should "leave the project directory untouched" in {
    val oldDir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-main-old-"))
    val project =
      ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-main-new-"), breakingStg)
    try {
      val before = listing(project)
      val (code, _) = runMain("SL_ROOT" -> project.pathAsString)(
        "impact",
        "--path1",
        oldDir.pathAsString,
        "--path2",
        project.pathAsString,
        "--fail-on",
        "breaking"
      )
      code shouldBe 1
      listing(project) shouldBe before
    } finally {
      oldDir.delete(swallowIOExceptions = true)
      project.delete(swallowIOExceptions = true)
    }
  }

  it should "print only the report on stdout when validate-on-load is set" in {
    val oldDir = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-main-old-"))
    val project =
      ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-main-new-"), breakingStg)
    try {
      val before = listing(project)
      val (code, stdout) =
        runMain("SL_ROOT" -> project.pathAsString, "SL_VALIDATE_ON_LOAD" -> "true")(
          "impact",
          "--path1",
          oldDir.pathAsString,
          "--format",
          "json"
        )
      code shouldBe 0
      val json = new ObjectMapper().readTree(stdout)
      json.get("summary").get("hasBreaking").asBoolean() shouldBe true
      stdout.trim should startWith("{")
      listing(project) shouldBe before
    } finally {
      oldDir.delete(swallowIOExceptions = true)
      project.delete(swallowIOExceptions = true)
    }
  }

  it should "exit 2 on a usage error through Main" in {
    val project = ImpactFixtures.writeProject(File.newTemporaryDirectory("impact-main-"))
    try {
      val (code, stdout) = runMain("SL_ROOT" -> project.pathAsString)("impact", "--format", "pdf")
      code shouldBe 2
      stdout shouldBe empty
    } finally project.delete(swallowIOExceptions = true)
  }

  /** Sets environment variables for the duration of `fun`, as IntegrationTestBase.withEnvs does,
    * and reloads the reference configuration that Main reads them from.
    */
  private def withEnvs[T](envList: (String, String)*)(fun: => T): T = {
    val existing = envList.flatMap { case (k, _) => Option(System.getenv().get(k)).map(k -> _) }
    envList.foreach { case (k, v) => envMap.put(k, v) }
    Settings.invalidateCaches()
    try fun
    finally {
      envList.foreach { case (k, _) => envMap.remove(k) }
      existing.foreach { case (k, v) => envMap.put(k, v) }
      Settings.invalidateCaches()
    }
  }

  private def envMap: java.util.Map[String, String] = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    field.get(System.getenv()).asInstanceOf[java.util.Map[String, String]]
  }
}
