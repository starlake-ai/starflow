package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def theSampleFolder = starlakeDir / "samples" / "starbake"
  override def sampleDataDir = theSampleFolder / "sample-data"
  logger.info(theSampleFolder.pathAsString)

  "Autoload" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("autoload", "--clean")))
    }
  }

  "Infer Schema" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      copyFilesToIncomingDir(sampleDataDir)
      assert(
        new Main().run(
          Array(
            "infer-schema",
            "--input",
            s"$theSampleFolder/incoming/starbake",
            "--clean"
          )
        )
      )
    }
  }

  "Import files" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("stage")))
    }
  }

  "Load files" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("load")))
    }
  }
  "Transform revenue" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("transform", "--name", "kpi.revenue_summary")))
    }
  }

  "Transform product" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("transform", "--name", "kpi.product_summary")))
    }
  }

  "Transform order" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("transform", "--name", "kpi.order_summary")))
    }
  }

  "Transform lineage" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      val output = better.files.File.newTemporaryFile("lineage", ".json")
      try {
        assert(
          new Main().run(
            Array(
              "lineage",
              "--json",
              "--task",
              "kpi.order_summary",
              "--output",
              output.pathAsString
            )
          )
        )
        output.contentAsString should include("kpi.order_summary")
      } finally output.delete(swallowIOExceptions = true)
    }
  }

  "Transform recursive" should "succeed" in {
    withEnvs(
      "SL_ROOT"             -> theSampleFolder.pathAsString,
      "SL_ENV"              -> "DUCKDB",
      "SL_VALIDATE_ON_LOAD" -> "true"
    ) {
      assert(new Main().run(Array("transform", "--recursive", "--name", "kpi.order_summary")))
    }
  }

}
