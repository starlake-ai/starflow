package ai.starlake.job.ingest.loaders

import ai.starlake.TestHelper
import ai.starlake.schema.model.{Position, SchemaInfo, TableAttribute}

import java.util.regex.Pattern

class DuckDbRejectCaptureSpec extends TestHelper {

  private val schema = SchemaInfo(
    name = "account",
    pattern = Pattern.compile("XPOS.*"),
    attributes = List(
      TableAttribute("name", "string", position = Some(Position(0, 9))),
      TableAttribute("amount", "long", position = Some(Position(10, 14)))
    ),
    metadata = None,
    comment = None
  )

  private val ddlTypes = Map("name" -> "VARCHAR", "amount" -> "BIGINT")

  // The length guard is written with coalesce because an empty input line comes back from the
  // first step as value = NULL: a bare length(value) would make the whole OR evaluate to NULL,
  // so the line would be neither rejected nor deleted, and the second step would project it
  // into an all NULL row.
  "positionRejectClauses" should "guard the line length and cast every non string slice" in {
    val clauses = DuckDbRejectCapture.positionRejectClauses(schema, ddlTypes)

    clauses.map(_._1) shouldBe List(
      "coalesce(length(value), 0) < 15",
      "(TRIM(SUBSTR(value, 11, 5)) <> '' AND TRY_CAST(SUBSTR(value, 11, 5) AS BIGINT) IS NULL)"
    )
    clauses.map(_._2) shouldBe List(
      "line is shorter than the 15 characters required by the declared positions",
      "amount: cannot cast to BIGINT"
    )
  }

  it should "produce no clause when there is no positioned attribute" in {
    DuckDbRejectCapture.positionRejectClauses(
      schema.copy(attributes = List(TableAttribute("name", "string"))),
      Map("name" -> "VARCHAR")
    ) shouldBe Nil
  }
}
