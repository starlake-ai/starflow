package ai.starlake.integration.transform

import ai.starlake.integration.{JDBCIntegrationSpecBase, RedshiftSampleTables}
import ai.starlake.job.Main

class TransformIntegrationRedshiftSpec extends JDBCIntegrationSpecBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean && sys.env.contains("REDSHIFT_USER")) {

    "Native REDSHIFT Transform" should "succeed" in {
      // The transform reads sales.orders and sales.customers: load them first, as suites run in
      // no particular order
      withEnvs(
        "SL_ENV"  -> "REDSHIFT_SPARK",
        "SL_ROOT" -> theSampleFolder.pathAsString
      ) {
        cleanup()
        dropTables(RedshiftSampleTables.loaded ++ RedshiftSampleTables.transformed)
        copyFilesToIncomingDir(sampleDataDir)
        assert(new Main().run(Array("stage")))
        assert(new Main().run(Array("load")))
      }
      withEnvs(
        "SL_ENV"  -> "REDSHIFT",
        "SL_ROOT" -> theSampleFolder.pathAsString
      ) {
        assert(
          new Main().run(
            Array("transform", "--name", "sales_kpi.byseller_kpi0")
          )
        )
      }
    }
  }
}
