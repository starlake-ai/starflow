package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import org.apache.hadoop.fs.Path

class NativeRejectedSinkSpec extends TestHelper {

  new WithSettings() {

    // The delete has to take back this attempt's rows and only those. The job id does not
    // identify an attempt on its own: JobBase.appName returns SL_JOB_ID verbatim when it is set,
    // so every job of an orchestrated run shares one application id, and the domain and the table
    // are the same for two loads of the same table. The input paths are the discriminator, and
    // they are already what the sink writes into the path column, so the delete matches on the
    // very same value rather than on a second derivation of it.
    "deleteSql" should "match every value the sink wrote, input paths included" in {
      NativeRejectedSink.deleteSql(
        applicationId = "airflow-run-42",
        domainName = "sales",
        tableName = "orders",
        paths = List(new Path("/ingesting/sales/XTBL_A"), new Path("/ingesting/sales/XTBL_B"))
      ) shouldBe
      "DELETE FROM audit.rejected WHERE jobid = 'airflow-run-42' AND domain = 'sales' " +
      "AND schema = 'orders' AND path = '/ingesting/sales/XTBL_A,/ingesting/sales/XTBL_B'"
    }

    // the same escaping the sink inlines those values with, or the delete would look for a row
    // that was never written under that spelling
    it should "escape its literals the way the sink escaped them" in {
      NativeRejectedSink.deleteSql(
        applicationId = "run-{{X}}",
        domainName = "sa'les",
        tableName = "orders",
        paths = List(new Path("/ingesting/sa'les/XTBL"))
      ) shouldBe
      "DELETE FROM audit.rejected WHERE jobid = 'run-X' AND domain = 'sa-les' " +
      "AND schema = 'orders' AND path = '/ingesting/sa-les/XTBL'"
    }
  }
}
