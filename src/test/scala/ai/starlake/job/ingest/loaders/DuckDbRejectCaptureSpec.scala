package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.{Position, SchemaInfo, TableAttribute}

import java.util.regex.Pattern

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
}
