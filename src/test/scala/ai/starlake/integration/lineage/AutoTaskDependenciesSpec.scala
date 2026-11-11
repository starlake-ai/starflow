package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File

class AutoTaskDependenciesSpec extends IntegrationTestBase {
  override def theSampleFolder: File = samplesFolder / "starbake"
  override def sampleDataDir: File = theSampleFolder / "sample-data"
  override def incomingDir: File = theSampleFolder / "datasets" / "incoming"
  logger.info(theSampleFolder.pathAsString)

  override def beforeEach(): Unit = {
    // do not clean
  }

  override def afterEach(): Unit = {
    // do not clean
  }

  "Autoload" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("autoload", "--clean")))
    }
  }

  "Recursive Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "DUCKDB") {
        val result = new Main().run(
          Array("transform", "--recursive", "--name", "kpi.order_summary")
        )
        assert(result)
      }
    }
  }

  "Dependencies" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs("SL_ROOT" -> theSampleFolder.pathAsString, "SL_ENV" -> "DUCKDB") {
        assert(
          new Main().run(
            Array("acl-dependencies", "--all")
          )
        )
      }
    }
  }

  "Table Lineage" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "DUCKDB"
      ) {
        assert(
          new Main().run(
            Array("lineage", "--viz", "--all")
          )
        )
      }
    }
  }

  "Column Lineage" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> (theSampleFolder.parent / "lineage").pathAsString
    ) {
      assert(
        new Main().run(
          Array(
            "col-lineage",
            "--output",
            "/tmp/lineage.json",
            "--task",
            "starbake_analytics.order_items_analysis"
          )
        )
      )
    }
  }

  "Lineage JSON" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "DUCKDB"
      ) {
        assert(
          new Main().run(
            Array("lineage", "--json", "--print", "--all")
          )
        )
      }
    }
  }

  "Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "DUCKDB"
      ) {
        assert(
          new Main().run(
            Array("table-dependencies")
          )
        )
      }
    }
  }

  "JSON Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString,
        "SL_ENV"  -> "DUCKDB"
      ) {
        assert(
          new Main().run(
            Array("table-dependencies", "--json")
          )
        )
      }
    }
  }

  "Job DOT generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> theSampleFolder.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        assert(
          new Main().run(
            Array(
              "lineage",
              "--print",
              "--viz",
              "--task",
              "kpi.order_summary",
              "--verbose"
            )
          )
        )
      }
    }
  }
}
