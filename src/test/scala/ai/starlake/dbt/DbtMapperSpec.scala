package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.manifest.{DbtProjectFile, DbtRelations}
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.EntryStatus
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde

import java.nio.file.Files
import scala.util.Success

/** The whole plan: tests as expectations, doc blocks, what is only reported */
class DbtMapperSpec extends TestHelper {
  new WithSettings() {
    private val manifest = DbtFixtures.shopManifest

    /** Every column of the header, as a string: Starlake's inference itself is not under test */
    private val stubInference
      : (java.nio.file.Path, String) => scala.util.Try[List[TableAttribute]] =
      (csv, _) =>
        Success(
          Files.readAllLines(csv).get(0).split(",").toList.map(c => TableAttribute(c, "string"))
        )

    private val ctx = DbtMappingContext(
      manifest,
      new DbtRelations(manifest),
      DbtFixtures.jinja(),
      None,
      stubInference
    )
    private lazy val plan = DbtMapper.plan(ctx, DbtProjectFile.read(manifest.projectRoot).get)

    private def file(suffix: String): FileOutput =
      plan.outputs
        .collect { case f: FileOutput if f.path.toString.endsWith(suffix) => f }
        .headOption
        .getOrElse(
          fail(s"no output ending with $suffix in ${plan.outputs.map(_.path)}")
        )

    private def task(domain: String, table: String): AutoTaskInfo = {
      val f = file(s"metadata/transform/$domain/$table.sl.yml")
      YamlSerde.deserializeYamlTask(f.content, f.path.toString)
    }

    private def table(area: String, domain: String, name: String): SchemaInfo = {
      val f = file(s"metadata/$area/$domain/$name.sl.yml")
      YamlSerde.deserializeYamlTables(f.content, f.path.toString).head.table
    }

    "The four built-in tests" should "be dbt_ expectations, with severity and where" in {
      val e = task("staging", "stg_orders").expectations
      e should contain(ExpectationItem("dbt_unique('order_id')", failOnError = true))
      e should contain(ExpectationItem("dbt_not_null('order_id')", failOnError = true))
      e should contain(
        ExpectationItem(
          "dbt_accepted_values('status', ['placed', 'completed', 'returned'])",
          failOnError = true
        )
      )
      e should contain(
        ExpectationItem(
          "dbt_relationships('customer_id', 'staging.stg_customers', 'customer_id')",
          failOnError = false
        )
      )
      e should contain(
        ExpectationItem(
          "dbt_not_null('amount', table=\"(SELECT * FROM sl_this WHERE status != 'returned') AS dbt_where\")",
          failOnError = true
        )
      )
      table("load", "raw", "raw_orders").expectations.map(
        _.expect
      ) should contain allOf ("dbt_unique('id')", "dbt_not_null('id')")
    }

    it should "go to the seed of a source that is a seed's table" in {
      table("load", "raw", "raw_customers").expectations.map(_.expect) shouldBe List(
        "dbt_not_null('id')"
      )
    }

    "Other tests" should "be one macro each in dbt.j2" in {
      task("marts", "orders").expectations.map(_.expect) should contain allOf (
        "dbt_test_dbt_utils_expression_is_true_orders_amount_0()",
        "dbt_test_assert_no_future_orders()"
      )
      val macros = file("metadata/expectations/dbt.j2").content
      macros should include("{% macro dbt_not_null(column_name, table='sl_this') -%}")
      macros should include(
        "{% macro dbt_test_assert_no_future_orders() -%}\nSELECT count(*) FROM (\nselect * from marts.orders where order_date > current_date\n) AS dbt_test"
      )
    }

    private def apostrophes(s: String): Int = s.count(_ == '\'')

    private def render(call: String): String =
      ai.starlake.utils.Utils
        .parseJinja(
          ai.starlake.sql.SqlCommentStripper
            .stripComments(DbtTestMapper.BuiltinMacros) + "\n" + call,
          Map.empty[String, Any]
        )
        .trim

    "The built-in macros" should "have an even number of apostrophes, in the file too" in {
      (apostrophes(DbtTestMapper.BuiltinMacros) % 2) shouldBe 0
      DbtTestMapper.BuiltinMacros.linesIterator.filter(apostrophes(_) % 2 != 0).toList shouldBe Nil
      (apostrophes(file("metadata/expectations/dbt.j2").content) % 2) shouldBe 0
    }

    they should "not invert the quote state of SqlCommentStripper" in {
      ai.starlake.sql.SqlCommentStripper
        .stripComments(DbtTestMapper.BuiltinMacros + "\nselect '--not a comment' as x") should
      include("'--not a comment'")
    }

    they should "render to the expected SQL" in {
      render("{{ dbt_not_null('c') }}") shouldBe "SELECT count(*) FROM sl_this WHERE c IS NULL"
      render("{{ dbt_unique('c') }}") shouldBe
      "SELECT count(*) FROM (SELECT c FROM sl_this WHERE c IS NOT NULL GROUP BY c HAVING count(*) > 1) AS dbt_unique"
      render("{{ dbt_accepted_values('c', ['a', 'it\\'\\'s']) }}") shouldBe
      "SELECT count(*) FROM sl_this WHERE c NOT IN ('a', 'it''s')"
      render("{{ dbt_accepted_values('c', [1, 2], quote=false) }}") shouldBe
      "SELECT count(*) FROM sl_this WHERE c NOT IN (1, 2)"
      render("{{ dbt_relationships('c', 'marts.orders', 'id') }}") shouldBe
      "SELECT count(*) FROM sl_this WHERE c IS NOT NULL AND c NOT IN (SELECT id FROM marts.orders WHERE id IS NOT NULL)"
      render(
        "{{ dbt_not_null('c', table=\"(SELECT * FROM sl_this WHERE x > 1) AS dbt_where\") }}"
      ) shouldBe
      "SELECT count(*) FROM (SELECT * FROM sl_this WHERE x > 1) AS dbt_where WHERE c IS NULL"
    }

    "Doc blocks" should "be written to metadata/docs" in {
      file("metadata/docs/dbt/shop/order_status.md").content shouldBe
      "{% docs order_status %}\nOne of `placed`, `completed` or `returned`.\n{% enddocs %}\n"
      plan.outputs.exists(_.path.toString.contains("docs/dbt/dbt/")) shouldBe false
    }

    "Snapshots" should "be reported only" in {
      plan.report.entries.find(_.dbtId == "snapshot.shop.customers_snapshot").get.status shouldBe
      EntryStatus.ReportedOnly("snapshots are not imported yet")
    }

    "A test that reads a table built after the tested one" should "run on that table, naming the tested one" in {
      task("marts", "customer_totals").expectations shouldBe List(
        ExpectationItem(
          "dbt_relationships('customer_id', 'marts.customer_totals', 'customer_id', table='marts.customers')"
        )
      )
      task("marts", "customers").expectations shouldBe Nil
    }

    "Every object" should "be in the report" in {
      plan.report.entries.map(_.dbtId).toSet should contain allOf (
        "model.shop.orders",
        "seed.shop.raw_orders",
        "source.shop.crm.crm_accounts",
        "doc.shop.order_status"
      )
      plan.report.skipped.map(_.dbtId) shouldBe List("model.shop.needs_warehouse")
    }
  }
}
