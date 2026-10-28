package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.metrics.ExpectationAssertionHandler
import ai.starlake.schema.model._
import com.typesafe.config.{Config, ConfigFactory}

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer

/** Expectations of a load run on a data branch must check the table the load wrote to: the branch
  * copy for a native BigQuery load, production for a Spark load (which never branches). No BigQuery
  * access: the assertion handler only records the SQL it gets.
  */
class BigQueryBranchLoadExpectationsSpec extends TestHelper {

  private def bigQueryConfiguration(branch: Option[String], loader: String): Config = {
    val branchOption = branch.map(b => s"""sl_data_branch: "$b"""").getOrElse("")
    ConfigFactory
      .parseString(s"""
                      |connectionRef = "bq"
                      |connections.bq {
                      |  type = "bigquery"
                      |  loader = "$loader"
                      |  options {
                      |    location: "europe-west1"
                      |    authType: APPLICATION_DEFAULT
                      |    projectId: "prj"
                      |    $branchOption
                      |  }
                      |}
                      |""".stripMargin)
      .withFallback(super.testConfiguration)
  }

  private def expectationSql(branch: Option[String], loader: String): List[String] = {
    var captured: List[String] = Nil
    new WithSettings(bigQueryConfiguration(branch, loader)) {
      deliverTypesFiles() // also installs the sample expectation macros
      val schemaHandler = settings.schemaHandler()
      val domain = DomainInfo("sales")
      val schema = SchemaInfo(
        name = "orders",
        pattern = Pattern.compile("orders.*\\.csv"),
        attributes = List(TableAttribute("id")),
        metadata = Some(Metadata(format = Some(Format.DSV))),
        expectations = List(ExpectationItem("is_col_value_not_unique('id')"))
      )
      val job = new DsvIngestionJob(
        domain,
        schema,
        schemaHandler.types(),
        Nil,
        storageHandler,
        schemaHandler,
        Map.empty,
        None,
        false,
        None
      )
      val sqls = ListBuffer[String]()
      val handler = new ExpectationAssertionHandler {
        override def handle(sql: String)(implicit settings: Settings): Int = {
          sqls += sql
          0
        }
      }
      job.expectationJob(handler).runExpectations().map(_.success) shouldBe List(true)
      captured = sqls.toList
    }
    captured
  }

  "Native BigQuery load expectations on a data branch" should "query the branch copy of the table" in {
    val sqls = expectationSql(Some("sl_branch"), loader = "native")
    sqls should have size 1
    sqls.head should include("prj.sl_branch.orders")
    sqls.head should not include "sales.orders"
  }

  "Native BigQuery load expectations without a data branch" should "query the loaded table" in {
    val sqls = expectationSql(None, loader = "native")
    sqls should have size 1
    sqls.head should include("sales.orders")
    sqls.head should not include "sl_branch"
  }

  "Spark load expectations on a data branch" should "query the loaded table, which Spark never branches" in {
    val sqls = expectationSql(Some("sl_branch"), loader = "spark")
    sqls should have size 1
    sqls.head should include("sales.orders")
    sqls.head should not include "sl_branch"
  }
}
