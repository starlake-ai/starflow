package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.handlers.StorageHandler

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Files
import java.sql.Timestamp

class ReplayFileWriterSpec extends TestHelper {

  new WithSettings() {

    implicit val implicitStorageHandler: StorageHandler = storageHandler

    private val timestamp = Timestamp.valueOf("2026-09-04 10:11:12")

    // A capture as the DuckDB reject capture builds one: the raw lines spilled to a local file,
    // terminated by \n, and only a sample of them materialized.
    private def rejected(rawLines: String*): RejectCapture = {
      val spillFile = Files.createTempFile("replay-file-writer-spec-", ".spill")
      Files.write(
        spillFile,
        rawLines.map(_ + "\n").mkString.getBytes(StandardCharsets.UTF_8)
      )
      val sample = rawLines.toList.zipWithIndex.map { case (raw, idx) =>
        RejectedLine("file:///incoming/XTBL", Some(idx.toLong + 2), raw, "CAST: boom")
      }
      RejectCapture(rawLines.size.toLong, sample, List(spillFile))
    }

    "ReplayFileWriter" should "write the header then every raw line verbatim" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "orders",
        rejected = rejected("2;bob;NOTANUM", "badline;dave"),
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
        rejected = rejected("BadRow    abcde"),
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
        rejected = RejectCapture.empty,
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
        rejected = rejected("first load"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales_same_second-orders-XTBL-1757000000000"
      )
      ReplayFileWriter.write(
        domainName = "sales_same_second",
        tableName = "orders",
        rejected = rejected("second load"),
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
        rejected = rejected("2;bob;NOTANUM"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "sales-orders-a/b c:d-1757000000000"
      )

      path.map(_.getName) shouldBe
      Some("sales.orders.20260904101112.sales-orders-a-b-c-d-1757000000000.replay")
    }

    // one capture per input path, merged in path order by the loader, and the replay file has to
    // reproduce that order rather than whatever order the spill files were created in
    it should "concatenate the spill files of a multi file load in path order" in {
      val first = rejected("2;bob;NOTANUM")
      val second = rejected("7;grace;NOTANUM", "badline;dave")

      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "grouped",
        rejected = first ++ second,
        header = Some("id;name;amount"),
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "job-1"
      )

      storageHandler.read(path.get) shouldBe
      "id;name;amount\n2;bob;NOTANUM\n7;grace;NOTANUM\nbadline;dave\n"
    }

    // a raw line is written to the replay file byte for byte, and a lone carriage return inside
    // one is data, not a line break: reading the spill file back as lines would turn it into a
    // line feed and split the line in two
    it should "keep a raw line carrying a lone carriage return intact" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "carriage",
        rejected = rejected("2;bo\rb;NOTANUM"),
        header = None,
        encoding = "UTF-8",
        timestamp = timestamp,
        jobid = "job-1"
      )

      storageHandler.read(path.get) shouldBe "2;bo\rb;NOTANUM\n"
    }

    it should "honor the requested encoding" in {
      val path = ReplayFileWriter.write(
        domainName = "sales",
        tableName = "latin",
        rejected = rejected("1;café"),
        header = None,
        encoding = "ISO-8859-1",
        timestamp = timestamp,
        jobid = "job-1"
      )

      storageHandler.read(path.get, Charset.forName("ISO-8859-1")) shouldBe "1;café\n"
    }
  }
}
