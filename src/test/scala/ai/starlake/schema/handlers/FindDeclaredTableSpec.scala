package ai.starlake.schema.handlers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The declared table of a load domain, a job or an external domain that share a name, which
  * `objectNames` lists as separate entries in no guaranteed order (#2029)
  */
class FindDeclaredTableSpec extends AnyFlatSpec with Matchers {

  private val job =
    DomainWithNameOnly("sales", List(TableWithNameAndType("daily_orders", Nil)))
  private val load =
    DomainWithNameOnly("sales", List(TableWithNameAndType("orders", List(("id", "long", None)))))

  "findDeclaredTable" should "search every declared object that bears the domain name" in {
    SchemaHandler.findDeclaredTable(List(job, load), "SALES", "Orders").map(_.name) shouldBe
    Some("orders")
    SchemaHandler.findDeclaredTable(List(load, job), "sales", "daily_orders").map(_.name) shouldBe
    Some("daily_orders")
    SchemaHandler.findDeclaredTable(List(job, load), "sales", "missing") shouldBe None
    SchemaHandler.findDeclaredTable(List(job, load), "other", "orders") shouldBe None
  }

  it should "prefer, in either order, the entry that lists the columns of a table two declare" in {
    val taskWritingTheLoadTable =
      DomainWithNameOnly("sales", List(TableWithNameAndType("orders", Nil)))
    for (objects <- List(List(taskWritingTheLoadTable, load), List(load, taskWritingTheLoadTable)))
      SchemaHandler
        .findDeclaredTable(objects, "sales", "orders")
        .map(_.attrs.map(_._1)) shouldBe Some(List("id"))
  }

  "declarationForLineage" should "leave a declaration without columns to the warehouse" in {
    val objects = List(load, job)
    SchemaHandler
      .declarationForLineage(objects, "sales", "daily_orders", declaredOnly = true)
      .map(_.name) shouldBe Some("daily_orders")
    SchemaHandler.declarationForLineage(
      objects,
      "sales",
      "daily_orders",
      declaredOnly = false
    ) shouldBe None
    SchemaHandler
      .declarationForLineage(objects, "sales", "orders", declaredOnly = false)
      .map(_.name) shouldBe Some("orders")
  }
}
