package ai.starlake.job.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Covers the load-table index built by runProject, without the project fixtures RunCmdSpec needs.
  */
class RunCmdLoadTableSpec extends AnyFlatSpec with Matchers {

  "the load table index" should "map each final name to its declared domain and table" in {
    val index = RunCmd.checkNoFinalNameCollision(
      List(
        "sales.orders"    -> ("sales", "orders"),
        "sales.renamed"   -> ("sales", "raw"),
        "finance.invoice" -> ("finance", "invoice")
      )
    )
    index("sales.renamed") shouldBe ("sales" -> "raw")
    index should have size 3
  }

  it should "accept the same declared table listed twice" in {
    val index = RunCmd.checkNoFinalNameCollision(
      List("sales.orders" -> ("sales", "orders"), "sales.orders" -> ("sales", "orders"))
    )
    index should have size 1
  }

  it should "reject two declared tables resolving to the same final name" in {
    // Silently keeping the last pair would make the 'sales.orders' node ingest 'sales.legacy'.
    val thrown = intercept[IllegalStateException] {
      RunCmd.checkNoFinalNameCollision(
        List("sales.orders" -> ("sales", "orders"), "sales.orders" -> ("sales", "legacy"))
      )
    }
    thrown.getMessage should include("sales.orders")
    thrown.getMessage should include("sales.legacy")
  }
}
