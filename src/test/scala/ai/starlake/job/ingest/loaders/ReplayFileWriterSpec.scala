package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.handlers.StorageHandler

import java.nio.charset.Charset
import java.sql.Timestamp

class ReplayFileWriterSpec extends TestHelper {

  new WithSettings() {

    implicit val implicitStorageHandler: StorageHandler = storageHandler

    private val timestamp = Timestamp.valueOf("2026-09-04 10:11:12")

    private def rejected(rawLines: String*): List[RejectedLine] =
      rawLines.toList.zipWithIndex.map { case (raw, idx) =>
        RejectedLine("file:///incoming/XTBL", Some(idx.toLong + 2), raw, "CAST: boom")
      }

    "ReplayFileWriter" should "write the header then every raw line verbatim" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "orders",
        rejectedLines = rejected("2;bob;NOTANUM", "badline;dave"),
        header = Some("id;name;amount"),
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales-orders-XTBL-1757000000000"
      )

      path.map(_.getName) shouldBe
      Some("sales.orders.20260904101112.sales-orders-XTBL-1757000000000.replay")
      path.map(_.getParent) shouldBe Some(DatasetArea.replay("sales"))
      storageHandler.read(path.get) shouldBe
      "id;name;amount\n2;bob;NOTANUM\nbadline;dave\n"
    }

    it should "omit the header when none is given" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "positions",
        rejectedLines = rejected("BadRow    abcde"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "job-1"
      )

      storageHandler.read(path.get) shouldBe "BadRow    abcde\n"
    }

    it should "write nothing when there is no rejected line" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "empty",
        rejectedLines = Nil,
        header = Some("id;name"),
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "job-1"
      )

      path shouldBe None
    }

    // autoload can trigger two ingestion jobs for the same table back to back, and the file
    // name only resolves to the second. Without the jobid the second write silently overwrote
    // the first one's rejected lines.
    it should "keep the rejected lines of two loads landing in the same second apart" in {
      storageHandler.delete(DatasetArea.replay("sales_same_second"))

      ReplayFileWriter.write(
        domainName = "sales_same_second",
        tableName = "orders",
        rejectedLines = rejected("first load"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales_same_second-orders-XTBL-1757000000000"
      )
      ReplayFileWriter.write(
        domainName = "sales_same_second",
        tableName = "orders",
        rejectedLines = rejected("second load"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales_same_second-orders-XTBL-1757000000001"
      )

      val written = storageHandler
        .list(DatasetArea.replay("sales_same_second"), extension = ".replay", recursive = false)
        .map(_.path)
      written.size shouldBe 2
      written.map(storageHandler.read(_)).sorted shouldBe List("first load\n", "second load\n")
    }

    // the job id is built from the domain, the table and the landing file name, so it can carry
    // anything that file name carried
    it should "sanitize the job id out of the file name" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "orders",
        rejectedLines = rejected("2;bob;NOTANUM"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales-orders-a/b c:d-1757000000000"
      )

      path.map(_.getName) shouldBe
      Some("sales.orders.20260904101112.sales-orders-a-b-c-d-1757000000000.replay")
    }

    it should "honor the requested encoding" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "latin",
        rejectedLines = rejected("1;café"),
        header = None,
        encoding = "ISO-8859-1",
        timestamp = timestamp,
        jobid = "job-1"
      )

      storageHandler.read(path.get, Charset.forName("ISO-8859-1")) shouldBe "1;café\n"
    }
  }
}
