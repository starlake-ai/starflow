package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import com.typesafe.config.{Config, ConfigFactory}

class SparkReplayFileSpec extends TestHelper {

  lazy val sparkReplayConfiguration: Config =
    ConfigFactory
      .parseString("sinkReplayToFile: true")
      .withFallback(super.testConfiguration)

  new WithSettings(sparkReplayConfiguration) {

    // The replay file name only resolves to the second, so it carries the jobid and the input
    // file name to tell two same second loads of the same table apart, exactly like the native
    // loaders: autoload triggers two ingestion jobs back to back, and without the discriminator
    // the second load silently overwrote the first one's rejected lines.
    "Spark load with sinkReplayToFile" should
    "name the replay file with the jobid and input file discriminator" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dsvduckreject/dsvduckreject.sl.yml",
        datasetDomainName = "dsvduckreject",
        sourceDatasetPathName = "/sample/dsvduckreject/XDSVREJECTTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "dsvduckreject",
          "/sample/dsvduckreject/account_dsvduckreject.sl.yml",
          Some("account.sl.yml")
        )

        // DatasetArea.replay("dsvduckreject") is shared with the native reject specs
        // (starlakeTestRoot is shared, and cleanMetadata does not touch it), so start from a
        // clean slate to keep this test independent of what other tests left behind.
        storageHandler.delete(DatasetArea.replay("dsvduckreject"))

        loadPending.isSuccess shouldBe true

        val replayFiles = storageHandler
          .list(DatasetArea.replay("dsvduckreject"), extension = ".replay", recursive = false)
          .map(_.path)
        replayFiles.size shouldBe 1
        val name = replayFiles.head.getName
        name should fullyMatch regex """dsvduckreject\.account\.\d{14}\..+\.replay"""
        name should include("XDSVREJECTTBL")

        // a plain file holding the rejected lines, not the Spark part directory the writer
        // produces: a directory cannot be dropped back into the landing area and replayed
        val content = storageHandler.read(replayFiles.head)
        content should include("NOTANUM")
        content should include("badline")
      }
    }
  }
}
