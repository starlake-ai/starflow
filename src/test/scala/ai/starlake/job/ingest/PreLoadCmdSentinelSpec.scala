package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.utils.{EmptyJobResult, FailedJobResult, PreLoadJobResult}
import org.apache.hadoop.fs.Path

import java.nio.file.Files

class PreLoadCmdSentinelSpec extends TestHelper {
  new WithSettings() {

    private def freshTempDir(): String =
      Files.createTempDirectory("preload-sentinel-test-").toString

    // --- Parser --------------------------------------------------------------

    "PreLoad parser" should "accept the --notReadySentinel option" in {
      val parsed = PreLoadCmd.parse(
        Seq(
          "--domain",
          "sales",
          "--tables",
          "customers",
          "--strategy",
          "imported",
          "--notReadySentinel",
          "file:///tmp/preload-sentinel-parser.flag"
        )
      )
      parsed.flatMap(_.notReadySentinel) shouldBe Some(
        "file:///tmp/preload-sentinel-parser.flag"
      )
    }

    "PreLoad parser" should "leave notReadySentinel as None when the flag is absent" in {
      val parsed = PreLoadCmd.parse(
        Seq(
          "--domain",
          "sales",
          "--strategy",
          "imported"
        )
      )
      parsed.flatMap(_.notReadySentinel) shouldBe None
    }

    // --- ACK strategy: legacy mode (no sentinel flag) ------------------------

    "ACK strategy, file missing, no sentinel flag" should "return FailedJobResult (legacy behavior)" in {
      val dir = freshTempDir()
      val ackPath = s"file://$dir/missing.ack"

      val config = PreLoadConfig(
        domain = "any",
        strategy = Some(PreLoadStrategy.Ack),
        globalAckFilePath = Some(ackPath),
        notReadySentinel = None
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      result.get shouldBe FailedJobResult
    }

    // --- ACK strategy: sentinel mode -----------------------------------------

    "ACK strategy, file missing, sentinel mode" should "write the sentinel and return EmptyJobResult (not FailedJobResult)" in {
      val dir = freshTempDir()
      val sentinelPath = s"file://$dir/notready.flag"
      val ackPath = s"file://$dir/missing.ack"

      val config = PreLoadConfig(
        domain = "any",
        strategy = Some(PreLoadStrategy.Ack),
        globalAckFilePath = Some(ackPath),
        notReadySentinel = Some(sentinelPath)
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      result.get shouldBe EmptyJobResult
      settings.storageHandler().exists(new Path(sentinelPath)) shouldBe true
    }

    "ACK strategy, file present, sentinel mode" should "return EmptyJobResult and NOT write the sentinel" in {
      val dir = freshTempDir()
      val sentinelPath = s"file://$dir/notready.flag"
      val ackPath = s"file://$dir/present.ack"
      settings.storageHandler().touchz(new Path(ackPath)).get

      val config = PreLoadConfig(
        domain = "any",
        strategy = Some(PreLoadStrategy.Ack),
        globalAckFilePath = Some(ackPath),
        notReadySentinel = Some(sentinelPath)
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      result.get shouldBe EmptyJobResult
      settings.storageHandler().exists(new Path(sentinelPath)) shouldBe false
    }

    "ACK strategy, no globalAckFilePath, sentinel mode" should "write the sentinel and return EmptyJobResult" in {
      val dir = freshTempDir()
      val sentinelPath = s"file://$dir/notready.flag"

      val config = PreLoadConfig(
        domain = "any",
        strategy = Some(PreLoadStrategy.Ack),
        globalAckFilePath = None,
        notReadySentinel = Some(sentinelPath)
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      result.get shouldBe EmptyJobResult
      settings.storageHandler().exists(new Path(sentinelPath)) shouldBe true
    }

    // --- IMPORTED strategy ---------------------------------------------------
    // With no domains registered, schemaHandler.domains(...) returns empty,
    // exercising the `case None` fallback branch at PreLoadCmd.scala:88-90,
    // which produces a PreLoadJobResult with all-zero counts (empty == true).

    "IMPORTED strategy, empty result, no sentinel flag" should "return an empty PreLoadJobResult (legacy behavior) and write no sentinel" in {
      val dir = freshTempDir()

      val config = PreLoadConfig(
        domain = "nosuch",
        tables = Seq("customers"),
        strategy = Some(PreLoadStrategy.Imported),
        notReadySentinel = None
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler()).get

      result shouldBe a[PreLoadJobResult]
      result.asInstanceOf[PreLoadJobResult].empty shouldBe true

      Files
        .list(new java.io.File(dir).toPath)
        .count() shouldBe 0L
    }

    "IMPORTED strategy, empty result, sentinel mode" should "write the sentinel and return EmptyJobResult (not an empty PreLoadJobResult)" in {
      val dir = freshTempDir()
      val sentinelPath = s"file://$dir/notready.flag"

      val config = PreLoadConfig(
        domain = "nosuch",
        tables = Seq("customers"),
        strategy = Some(PreLoadStrategy.Imported),
        notReadySentinel = Some(sentinelPath)
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      result.get shouldBe EmptyJobResult
      settings.storageHandler().exists(new Path(sentinelPath)) shouldBe true
    }

    // --- Robustness ----------------------------------------------------------

    "sentinel write failure" should "be non-fatal and preserve the sentinel-mode result (EmptyJobResult)" in {
      val dir = freshTempDir()
      // Point the sentinel at a path that cannot be written: a path whose
      // parent is a regular file (not a directory), causing touchz to fail.
      val blockingFile = s"$dir/blocker"
      Files.createFile(new java.io.File(blockingFile).toPath)
      val unwritableSentinelPath = s"file://$blockingFile/notready.flag"
      val ackPath = s"file://$dir/missing.ack"

      val config = PreLoadConfig(
        domain = "any",
        strategy = Some(PreLoadStrategy.Ack),
        globalAckFilePath = Some(ackPath),
        notReadySentinel = Some(unwritableSentinelPath)
      )
      val result = PreLoadCmd.run(config, settings.schemaHandler())

      // Contract: the caller's view of sentinel-mode outcomes is preserved
      // even if the side-channel write fails. Only a warning is logged.
      result.get shouldBe EmptyJobResult
    }
  }
}
