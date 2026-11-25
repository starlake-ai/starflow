package ai.starlake.dbt

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters._

/** Each fixture model rendered with is_incremental() false equals what `dbt compile` wrote, once
  * the relation names of dbt ("db"."schema"."t" -> schema.t) and the whitespace of both are
  * normalized
  */
class DbtRenderGoldenSpec extends AnyFlatSpec with Matchers {
  private lazy val manifest = DbtFixtures.shopManifest
  private lazy val jinja = DbtFixtures.jinja()

  /** customer_totals: dbt inlines the ephemeral model as a CTE, the import reads its table (checked
    * below). needs_warehouse and the five introspection models that read the warehouse: skipped by
    * the import without --introspect (model_count only reads the graph, so it is compared).
    */
  private val notCompared = Set(
    "customer_totals",
    "needs_warehouse",
    "order_status_pivot",
    "customers_star",
    "order_count",
    "customers_filled",
    "writes_at_import"
  )

  private val Relation = """"([^"]+)"\."([^"]+)"\."([^"]+)"""".r

  private def whitespace(sql: String): String = sql.split("\\s+").filter(_.nonEmpty).mkString(" ")

  /** dbt's compiled SQL: "db"."schema"."t" becomes schema.t */
  private def dbtNormalize(sql: String): String =
    whitespace(Relation.replaceAllIn(sql, m => s"${m.group(2)}.${m.group(3)}"))

  private val compiled =
    Files
      .list(DbtFixtures.expectedDir.resolve("compiled"))
      .iterator()
      .asScala
      .toList
      .sortBy(_.toString)

  compiled.map(_.getFileName.toString.stripSuffix(".sql")).filterNot(notCompared).foreach { name =>
    "DbtJinja" should s"render $name as dbt compile does" in {
      val node = DbtFixtures.node(manifest, name)
      val expected = Files.readString(DbtFixtures.expectedDir.resolve(s"compiled/$name.sql"))
      whitespace(
        jinja.render(node, node.rawCode, incremental = false).sql.get
      ) shouldBe dbtNormalize(expected)
    }
  }

  /** dbt compiles `with __dbt__cte__int_order_totals as (<its SQL>) <select>` and reads the CTE;
    * the import reads the table that DbtRelations gives the ephemeral node,
    * staging.int_order_totals. So the expected SQL is dbt's without the injected CTE, the CTE name
    * replaced by that table.
    */
  it should "render customer_totals reading the ephemeral int_order_totals as its table" in {
    val node = DbtFixtures.node(manifest, "customer_totals")
    val expected = Files.readString(DbtFixtures.expectedDir.resolve("compiled/customer_totals.sql"))
    val cte = "__dbt__cte__int_order_totals"
    val select =
      expected.substring(expected.indexOf(") select ") + 2).replace(cte, "staging.int_order_totals")
    val rendered = jinja.render(node, node.rawCode, incremental = false).sql.get
    rendered should include("staging.int_order_totals")
    whitespace(rendered) shouldBe dbtNormalize(select)
  }

  it should "skip needs_warehouse, which calls run_query" in {
    val node = DbtFixtures.node(manifest, "needs_warehouse")
    jinja.render(node, node.rawCode, incremental = false).sql.failed.get shouldBe
    DbtSkip("needs the warehouse at import (run_query)")
  }

  it should "define every macro of the manifest" in {
    jinja.definitionErrors shouldBe Nil
  }
}
