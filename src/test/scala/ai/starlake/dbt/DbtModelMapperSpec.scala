package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.manifest.DbtRelations
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.EntryStatus
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde

import java.nio.file.Files
import scala.util.Success

/** One case per row of the model mapping table, asserted on the YAML the import writes */
class DbtModelMapperSpec extends TestHelper {
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

    private lazy val mapped: Map[String, Either[ai.starlake.dbt.report.ReportEntry, MappedModel]] =
      manifest.nodesOfType("model").map(n => n.name -> DbtModelMapper.map(n, ctx)).toMap

    private def outputs(name: String): List[FileOutput] =
      mapped(name).toOption.get.outputs.collect { case f: FileOutput => f }

    private def task(domain: String, table: String): AutoTaskInfo = {
      val f = outputs(table)
        .find(_.path.toString.endsWith(s"metadata/transform/$domain/$table.sl.yml"))
        .get
      YamlSerde.deserializeYamlTask(f.content, f.path.toString)
    }

    private def sql(domain: String, table: String): String =
      outputs(table)
        .find(_.path.toString.endsWith(s"metadata/transform/$domain/$table.sql"))
        .get
        .content

    private def strategy(t: AutoTaskInfo): (Option[WriteStrategyType], List[String]) =
      (t.writeStrategy.flatMap(_.`type`), t.writeStrategy.map(_.key).getOrElse(Nil))

    private def synthetic(config: Map[String, Any]): Either[String, AutoTaskInfo] =
      DbtModelMapper
        .map(DbtFixtures.model("synthetic", config, "select 1 as id"), ctx)
        .left
        .map(_.status.toString)
        .map(_.task)

    "A table model" should "be an OVERWRITE task" in {
      val t = task("marts", "customers")
      strategy(t) shouldBe (Some(WriteStrategyType.OVERWRITE), Nil)
      t.sink.flatMap(_.materializedView) shouldBe None
      sql("marts", "customers") should include("md5(cast(coalesce(cast(customer_id as TEXT)")
    }

    "A view model" should "be an OVERWRITE task materialized as a view" in {
      val t = task("staging", "stg_orders")
      strategy(t) shouldBe (Some(WriteStrategyType.OVERWRITE), Nil)
      t.sink.flatMap(_.materializedView) shouldBe Some(Materialization.VIEW)
    }

    "An ephemeral model" should "be a view, noted in the report" in {
      task("staging", "int_order_totals").sink.flatMap(_.materializedView) shouldBe Some(
        Materialization.VIEW
      )
      mapped("int_order_totals").toOption.get.entry.notes.head should include("ephemeral")
    }

    "An incremental model" should "map append to APPEND, with its sl_incremental block" in {
      strategy(task("marts", "events_append")) shouldBe (Some(WriteStrategyType.APPEND), Nil)
      sql("marts", "events_append").replaceAll("\n+", "\n") should include(
        "{% if sl_incremental %}\nwhere occurred_at > (select max(occurred_at) from marts.events_append)\n{% endif %}"
      )
    }

    it should "map merge with unique_key to UPSERT_BY_KEY" in {
      strategy(task("marts", "customer_last_event")) shouldBe (Some(
        WriteStrategyType.UPSERT_BY_KEY
      ), List("customer_id"))
    }

    it should "map delete+insert with a list unique_key to DELETE_THEN_INSERT" in {
      strategy(task("marts", "daily_events")) shouldBe (Some(
        WriteStrategyType.DELETE_THEN_INSERT
      ), List("event_date", "event_type"))
    }

    it should "map merge without unique_key to APPEND" in {
      synthetic(Map("materialized" -> "incremental", "incremental_strategy" -> "merge"))
        .map(strategy) shouldBe
      Right((Some(WriteStrategyType.APPEND), Nil))
    }

    it should "map insert_overwrite with partition_by to OVERWRITE_BY_PARTITION" in {
      val t = synthetic(
        Map(
          "materialized"         -> "incremental",
          "incremental_strategy" -> "insert_overwrite",
          "partition_by"         -> Map("field" -> "event_date", "data_type" -> "date")
        )
      ).toOption.get
      strategy(t)._1 shouldBe Some(WriteStrategyType.OVERWRITE_BY_PARTITION)
      t.sink.flatMap(_.partition) shouldBe Some(List("event_date"))
    }

    it should "apply the adapter's default strategy when there is none" in {
      synthetic(Map("materialized" -> "incremental", "unique_key" -> "id")).map(strategy) shouldBe
      Right((Some(WriteStrategyType.DELETE_THEN_INSERT), List("id")))
      DbtModelMapper
        .writeStrategy("incremental", Map("unique_key" -> "id"), "bigquery")
        .map(_._1.`type`) shouldBe
      Right(Some(WriteStrategyType.UPSERT_BY_KEY))
      synthetic(Map("materialized" -> "incremental")).map(strategy) shouldBe Right(
        (Some(WriteStrategyType.APPEND), Nil)
      )
    }

    it should "skip microbatch, Python models and custom materializations" in {
      synthetic(
        Map("materialized" -> "incremental", "incremental_strategy" -> "microbatch")
      ).left.toOption.get should include("microbatch")
      DbtModelMapper
        .map(DbtFixtures.model("py").copy(language = "python"), ctx)
        .left
        .toOption
        .get
        .status shouldBe
      EntryStatus.Skipped("python model")
      synthetic(Map("materialized" -> "my_mat")).left.toOption.get should include(
        "custom materialization 'my_mat'"
      )
    }

    "Hooks" should "be presql and postsql, rendered like the model" in {
      val t = task("marts", "orders")
      t.presql shouldBe List("create table if not exists marts.audit_log (model_name varchar)")
      t.postsql shouldBe List("insert into marts.audit_log values ('orders')")
    }

    "cluster_by" should "be the sink clustering" in {
      synthetic(Map("cluster_by" -> List("a", "b"))).toOption.get.sink
        .flatMap(_.clustering) shouldBe Some(List("a", "b"))
    }

    "tags and descriptions" should "be tags and comments, doc blocks as doc()" in {
      task("marts", "orders").tags shouldBe Set("finance")
      val t = task("staging", "stg_orders")
      t.comment shouldBe Some("Orders, one row per order. {{ doc('order_status') }}")
      t.attributes.find(_.name == "status").flatMap(_.comment) shouldBe Some(
        "{{ doc('order_status') }}"
      )
    }

    "A column data_type" should "be the attribute type" in {
      task("staging", "stg_orders").attributes
        .find(_.name == "order_id")
        .map(_.`type`) shouldBe Some("integer")
    }

    "Settings that have no equivalent" should "be reported" in {
      val entry = DbtModelMapper
        .map(
          DbtFixtures.model(
            "noted",
            Map(
              "on_schema_change" -> "append_new_columns",
              "full_refresh"     -> false,
              "grants"           -> Map("select" -> List("r")),
              "contract"         -> Map("enforced" -> true)
            ),
            "select 1"
          ),
          ctx
        )
        .toOption
        .get
        .entry
      entry.notes should contain allOf (
        "on_schema_change: append_new_columns has no equivalent",
        "full_refresh: false has no equivalent: transform --truncate rebuilds the table",
        "grants are not imported",
        "contract enforcement is not imported"
      )
    }

    "A string literal that holds Jinja delimiters" should "be split with CONCAT" in {
      sql("staging", "stg_literal") shouldBe "select CONCAT('{', '{ not jinja }}') as braces\n"
      DbtText.protect("select 'a{{b{%c' as x, '{#' as y") shouldBe
      ("select CONCAT('a{', '{b{', '%c') as x, CONCAT('{', '#') as y", true, false)
      DbtText.protect("select {{ x }}")._3 shouldBe true
    }

    "A model that needs the warehouse" should "be skipped with the reason" in {
      mapped("needs_warehouse").left.toOption.get.status shouldBe
      EntryStatus.Skipped("needs the warehouse at import (run_query)")
    }

  }
}
