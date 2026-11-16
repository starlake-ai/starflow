package ai.starlake.lineage

import ai.starlake.lineage.InheritedDescriptions.{merge, Inherited}
import ai.starlake.lineage.SelectColumnOrigins.{ColumnRef, TableRef}
import ai.starlake.schema.model.{AttributeStatus, TableAttribute}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The inherited descriptions put into the attributes of a sync (#1948) */
class InheritedDescriptionsMergeSpec extends AnyFlatSpec with Matchers {
  import AttributeStatus.*

  private def from(column: String, description: String) =
    Inherited(description, ColumnRef(TableRef("sales", "orders"), column))

  private val inherited = Map(
    "id"     -> from("id", "Identifier of the order"),
    "amount" -> from("amount", "Amount of the order"),
    "label"  -> from("label", "Label of {{ entity }}")
  )

  "merge" should "describe the attributes without a description, and keep the others" in {
    val result = merge(
      Nil,
      List(
        TableAttribute("id", "string")                             -> ADDED,
        TableAttribute("amount", "integer", comment = Some("own")) -> UNCHANGED,
        TableAttribute("name", "string")                           -> UNCHANGED,
        TableAttribute("label", "string")                          -> MODIFIED,
        TableAttribute("old", "string")                            -> REMOVED
      ),
      inherited,
      refresh = false
    )
    result.attributes shouldBe List(
      TableAttribute("id", "string", comment = Some("Identifier of the order")) -> ADDED,
      TableAttribute("amount", "integer", comment = Some("own"))                -> UNCHANGED,
      TableAttribute("name", "string")                                          -> UNCHANGED,
      // Jinja would make the update of the YAML fail
      TableAttribute("label", "string") -> MODIFIED,
      TableAttribute("old", "string")   -> REMOVED
    )
    result.inherited shouldBe List("id" -> inherited("id"))
    result.leftOut shouldBe List("label" -> inherited("label"))
    result.written shouldBe Nil
    result.undescribed shouldBe List("name", "label")
  }

  it should "replace the description of an attribute when asked, and mark it as modified" in {
    val result = merge(
      Nil,
      List(
        TableAttribute("ID", "string", comment = Some("own"))                      -> UNCHANGED,
        TableAttribute("amount", "integer", comment = Some("Amount of the order")) -> UNCHANGED,
        TableAttribute("name", "string", comment = Some(" "))                      -> UNCHANGED
      ),
      inherited,
      refresh = true
    )
    result.attributes shouldBe List(
      TableAttribute("ID", "string", comment = Some("Identifier of the order"))  -> MODIFIED,
      TableAttribute("amount", "integer", comment = Some("Amount of the order")) -> UNCHANGED,
      TableAttribute("name", "string", comment = Some(" "))                      -> UNCHANGED
    )
    result.inherited.map(_._1) shouldBe List("ID")
    // Already written: shown as inherited by the preview
    result.written.map(_._1) shouldBe List("amount")
    result.undescribed shouldBe List("name")
  }

  it should "start from the attributes of the task when the sync changes nothing" in {
    val attributes = List(TableAttribute("id", "string"), TableAttribute("name", "string"))
    merge(attributes, Nil, inherited, refresh = false).attributes shouldBe List(
      TableAttribute("id", "string", comment = Some("Identifier of the order")) -> MODIFIED,
      TableAttribute("name", "string")                                          -> UNCHANGED
    )
    // Nothing changes
    val described = List(TableAttribute("name", "string", comment = Some("own")))
    val unchanged = merge(described, Nil, inherited, refresh = false)
    unchanged.attributes shouldBe Nil
    unchanged.undescribed shouldBe Nil
    merge(attributes, Nil, Map.empty, refresh = true).attributes shouldBe Nil
  }
}
