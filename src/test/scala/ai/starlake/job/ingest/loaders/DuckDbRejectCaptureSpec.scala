package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.{Position, SchemaInfo, TableAttribute}
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.DriverManager
import java.util.regex.Pattern
import scala.util.Using

class DuckDbRejectCaptureSpec extends TestHelper {

  // Same shape as the positionduckshort fixture: a required name filling [0, 9] and an optional
  // amount at [10, 14].
  private val schema = SchemaInfo(
    name = "account",
    pattern = Pattern.compile("XPOS.*"),
    attributes = List(
      TableAttribute("name", "string", required = Some(true), position = Some(Position(0, 9))),
      TableAttribute("amount", "long", required = Some(false), position = Some(Position(10, 14)))
    ),
    metadata = None,
    comment = None
  )

  private val ddlTypes = Map("name" -> "VARCHAR", "amount" -> "BIGINT")

  // The length guard is written with coalesce because an empty input line comes back from the
  // first step as value = NULL: a bare length(value) would make the whole OR evaluate to NULL,
  // so the line would be neither rejected nor deleted, and the second step would project it
  // into an all NULL row.
  //
  // It only covers the REQUIRED positions, so 10 here and not 15: a line that stops right after
  // name is what a fixed width source emits when the optional trailing amount is empty, and its
  // absent slice loads as NULL rather than being rejected.
  "positionRejectClauses" should "guard the required line length and cast every non string slice" in {
    val clauses = DuckDbRejectCapture.positionRejectClauses(schema, ddlTypes)

    clauses.map(_._1) shouldBe List(
      "coalesce(length(value), 0) < 10",
      "(TRIM(SUBSTR(value, 11, 5)) <> '' AND TRY_CAST(SUBSTR(value, 11, 5) AS BIGINT) IS NULL)"
    )
    clauses.map(_._2) shouldBe List(
      "line is shorter than the 10 characters that cover every required attribute",
      "amount: cannot cast to BIGINT"
    )
  }

  it should "produce no clause when there is no positioned attribute" in {
    DuckDbRejectCapture.positionRejectClauses(
      schema.copy(attributes = List(TableAttribute("name", "string"))),
      Map("name" -> "VARCHAR")
    ) shouldBe Nil
  }

  // No required attribute means no length to enforce: any line at all covers every required
  // field, so the length clause has to disappear rather than degenerate into `< 1`, which would
  // reject the empty line on a table that declares nothing mandatory. The cast clauses stay.
  it should "drop the length clause when no positioned attribute is required" in {
    val clauses = DuckDbRejectCapture.positionRejectClauses(
      schema.copy(attributes = schema.attributes.map(_.copy(required = Some(false)))),
      ddlTypes
    )

    clauses.map(_._1) shouldBe List(
      "(TRIM(SUBSTR(value, 11, 5)) <> '' AND TRY_CAST(SUBSTR(value, 11, 5) AS BIGINT) IS NULL)"
    )
  }

  lazy val cappedSampleConfiguration: Config =
    ConfigFactory
      .parseString("audit.maxErrors: 2")
      .withFallback(testConfiguration)

  new WithSettings(cappedSampleConfiguration) {

    // A load that reads a huge file with the wrong delimiter rejects every line of it. Nothing the
    // capture keeps may grow with that: the count is exact because it comes from a COUNT over the
    // same query, only audit.maxErrors lines are materialized because that is all the audit
    // rejected table can hold, and every raw line goes to the spill file the replay writer streams
    // from. The capture used to return one RejectedLine per rejected line, so the lines and their
    // error texts were held in memory and then doubled by the single String the replay file was
    // built as.
    "captureCsvRejects" should
    "count every rejected line, materialize at most audit.maxErrors of them and spill them all" in {
      val csv = Files.createTempFile("duckdb-reject-capture-spec-", ".csv")
      Files.write(
        csv,
        // 1 good line and 5 whose amount cannot be read as a BIGINT
        ("id;name;amount\n" +
        "1;alice;10\n" +
        "2;bob;NOTANUM\n" +
        "3;carol;NOTANUM\n" +
        "4;dave;NOTANUM\n" +
        "5;eve;NOTANUM\n" +
        "6;frank;NOTANUM\n").getBytes(StandardCharsets.UTF_8)
      )
      try {
        settings.appConfig.audit.maxErrors shouldBe 2

        val captured =
          Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
            Using.resource(conn.createStatement()) { statement =>
              statement.execute("CREATE TABLE account (id BIGINT, name VARCHAR, amount BIGINT)")
              statement.execute(
                s"""INSERT INTO account SELECT * FROM read_csv('${csv.toString}',
                   | delim = ';', header = true, store_rejects = true,
                   | columns = {'id': 'BIGINT', 'name': 'VARCHAR', 'amount': 'BIGINT'});""".stripMargin
              )
            }
            DuckDbRejectCapture.captureCsvRejects(conn)
          }

        captured.count shouldBe 5
        captured.sample.size shouldBe 2
        captured.spillFiles.size shouldBe 1
        new String(
          Files.readAllBytes(captured.spillFiles.head),
          StandardCharsets.UTF_8
        ) shouldBe
        "2;bob;NOTANUM\n3;carol;NOTANUM\n4;dave;NOTANUM\n5;eve;NOTANUM\n6;frank;NOTANUM\n"

        captured.spillFiles.foreach(Files.deleteIfExists)
      } finally {
        Files.deleteIfExists(csv)
      }
    }
  }
}
