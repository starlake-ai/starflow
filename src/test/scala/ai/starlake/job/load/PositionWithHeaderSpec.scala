package ai.starlake.job.load

import ai.starlake.TestHelper
import ai.starlake.job.ingest.PositionIngestionUtil
import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.schema.model.{Format, Metadata, SchemaInfo}
import better.files.File
import com.google.cloud.bigquery.CsvOptions
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import java.util.regex.Pattern

/** POSITION files have no header line unless `withHeader: true` is set: the native BigQuery load
  * skipped the first line of every file by default, the Spark load never did
  */
class PositionWithHeaderSpec extends TestHelper {

  private val position = Metadata(format = Some(Format.POSITION))

  "withHeader" should "be false by default for POSITION files only" in {
    position.resolveWithHeader() shouldBe false
    position.copy(withHeader = Some(true)).resolveWithHeader() shouldBe true
    position.copy(withHeader = Some(false)).resolveWithHeader() shouldBe false
    Metadata(format = Some(Format.DSV)).resolveWithHeader() shouldBe true
    Metadata().resolveWithHeader() shouldBe true
  }

  it should "be inherited by a POSITION table from its domain" in {
    Metadata(withHeader = Some(true)).merge(position).resolveWithHeader() shouldBe true
    position.merge(Metadata()).resolveWithHeader() shouldBe false
    Metadata().merge(position).resolveWithHeader() shouldBe false
  }

  it should "warn about a POSITION table that inherits withHeader: true from its domain" in {
    def table(metadata: Option[Metadata]) = SchemaInfo("t", Pattern.compile("t"), Nil, metadata)
    val domain = Some(Metadata(withHeader = Some(true)))
    table(Some(position)).inheritsPositionHeader(domain) shouldBe true
    table(None).inheritsPositionHeader(Some(position.copy(withHeader = Some(true)))) shouldBe true
    table(Some(position.copy(withHeader = Some(true))))
      .inheritsPositionHeader(domain) shouldBe false
    table(Some(position.copy(withHeader = Some(false))))
      .inheritsPositionHeader(domain) shouldBe false
    table(Some(Metadata(format = Some(Format.DSV)))).inheritsPositionHeader(domain) shouldBe false
    table(Some(position)).inheritsPositionHeader(Some(Metadata())) shouldBe false
  }

  "The BigQuery load of a POSITION file" should "skip its first line only with withHeader: true" in {
    def skipped(metadata: Metadata): Option[Long] =
      Option(
        BigQueryNativeJob.loadFormatOptions(metadata).asInstanceOf[CsvOptions].getSkipLeadingRows
      ).map(_.longValue())
    skipped(position) shouldBe None
    skipped(position.copy(withHeader = Some(false))) shouldBe None
    skipped(position.copy(withHeader = Some(true))) shouldBe Some(1L)
  }

  "The Spark reader of POSITION files" should "skip the first line of each file only with withHeader: true" in {
    new WithSettings() {
      val dir = File.newTemporaryDirectory()
      try {
        // A record equal to the header line of another file, records with quotes, which the reader
        // keeps, files with and without a final line separator, CRLF line separators, a file with a
        // header line only, and a record starting with NUL
        val files = List(
          "first.txt"  -> "HEADER 1\nC1 0001 Étienne\n  C1 0002 padded  ",
          "second.txt" -> "HEADER 2\nC1 0003 Zoé\nHEADER 1\n\"Q\" 0004 \"quoted\n",
          "third.txt"  -> "HEADER 3\r\n\"C1 0005 quoted\"\r\nC1 0006 \"\"twice\"\"\r\n",
          "fourth.txt" -> "HEADER 4\n",
          "fifth.txt"  -> "HEADER 5\n\u0000C1 0007 low-values\n",
          // Not read: the options of the table apply
          "ignored.skip" -> "NOT A RECORD\n"
        )
        def lines(encoding: String, withHeader: Boolean): List[String] = {
          val encodingDir = dir / encoding
          files.foreach { case (name, content) =>
            (encodingDir / name)
              .createIfNotExists(createParents = true)
              .writeBytes(content.getBytes(Charset.forName(encoding)).iterator)
          }
          PositionIngestionUtil
            .loadLines(
              sparkSession,
              List(new Path(encodingDir.pathAsString)),
              encoding,
              withHeader,
              // Options of the table that would parse the lines: the reader keeps its own
              Map(
                "pathGlobFilter"           -> "*.txt",
                "sep"                      -> "0",
                "delimiter"                -> "0",
                "quote"                    -> "\"",
                "comment"                  -> "C",
                "ignoreLeadingWhiteSpace"  -> "true",
                "ignoreTrailingWhiteSpace" -> "true",
                "nullValue"                -> "C1 0003 Zoé",
                "multiLine"                -> "true",
                "enforceSchema"            -> "false",
                "header"                   -> (!withHeader).toString
              )
            )
            .collect()
            .map(_.getString(0))
            .toList
        }
        val records =
          List(
            "C1 0001 Étienne",
            "  C1 0002 padded  ",
            "C1 0003 Zoé",
            "HEADER 1",
            "\"Q\" 0004 \"quoted",
            "\"C1 0005 quoted\"",
            "C1 0006 \"\"twice\"\"",
            "\u0000C1 0007 low-values"
          )
        List("UTF-8", "ISO-8859-1").foreach { encoding =>
          withClue(encoding) {
            lines(encoding, withHeader = true) should contain theSameElementsAs records
            lines(encoding, withHeader = false) should contain theSameElementsAs
            (List("HEADER 1", "HEADER 2", "HEADER 3", "HEADER 4", "HEADER 5") ++ records)
          }
        }
      } finally {
        dir.delete(swallowIOExceptions = true)
      }
    }
  }
}
