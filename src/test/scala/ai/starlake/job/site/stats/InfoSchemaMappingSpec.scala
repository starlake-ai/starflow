package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant

class InfoSchemaMappingSpec extends AnyFlatSpec with Matchers {
  import InfoSchemaMapping._

  "formatType" should "add precision to decimals and length to strings only" in {
    formatType("numeric", None, Some(18), Some(2)) shouldBe "numeric(18,2)"
    formatType("NUMBER", None, Some(38), None) shouldBe "NUMBER(38,0)"
    formatType("character varying", Some(20), None, None) shouldBe "character varying(20)"
    formatType("integer", None, Some(32), Some(0)) shouldBe "integer"
    formatType("DECIMAL(18,3)", None, Some(18), Some(3)) shouldBe "DECIMAL(18,3)"
    formatType("text", None, None, None) shouldBe "text"
  }

  "tableType" should "normalize engine labels" in {
    tableType(Some("BASE TABLE")) shouldBe "TABLE"
    tableType(Some("VIEW")) shouldBe "VIEW"
    tableType(Some("MATERIALIZED VIEW")) shouldBe "VIEW"
    tableType(Some("EXTERNAL TABLE")) shouldBe "EXTERNAL"
    tableType(Some("FOREIGN")) shouldBe "EXTERNAL"
    tableType(None) shouldBe "TABLE"
  }

  "merge" should "combine table, stats and column rows" in {
    val ts = Timestamp.from(Instant.parse("2026-11-22T03:10:00Z"))
    val tableRows = List(
      Map[String, Any]("table_name" -> "Orders", "table_type"   -> "BASE TABLE"),
      Map[String, Any]("table_name" -> "v_orders", "table_type" -> "VIEW")
    )
    val statsRows = List(
      Map[String, Any](
        "table_name"    -> "orders",
        "row_count"     -> java.lang.Long.valueOf(120345L),
        "bytes"         -> "9123456",
        "last_modified" -> ts
      ),
      Map[String, Any]("table_name" -> "ghost", "row_count" -> 1L)
    )
    val columnRows = List(
      Map[String, Any](
        "table_name"  -> "orders",
        "column_name" -> "id",
        "data_type"   -> "bigint",
        "is_nullable" -> "NO"
      ),
      Map[String, Any](
        "table_name"        -> "orders",
        "column_name"       -> "amount",
        "data_type"         -> "numeric",
        "is_nullable"       -> "YES",
        "numeric_precision" -> 18,
        "numeric_scale"     -> 2
      )
    )
    val merged = merge(tableRows, statsRows, columnRows, estimated = true)
    merged.keySet shouldBe Set("orders", "v_orders")
    merged("orders") shouldBe RawTableStats(
      exists = true,
      tableType = Some("TABLE"),
      format = None,
      rowCount = Some(120345L),
      rowCountEstimated = true,
      bytes = Some(9123456L),
      lastModified = Some("2026-11-22T03:10:00Z"),
      columns = List(
        ColumnStat("id", "bigint", nullable = false),
        ColumnStat("amount", "numeric(18,2)", nullable = true)
      )
    )
    merged("v_orders").rowCount shouldBe None
    merged("v_orders").rowCountEstimated shouldBe false
    merged("v_orders").tableType shouldBe Some("VIEW")
  }

  it should "drop negative row counts (PostgreSQL reltuples = -1 before ANALYZE)" in {
    val merged = merge(
      List(Map[String, Any]("table_name" -> "t", "table_type" -> "BASE TABLE")),
      List(Map[String, Any]("table_name" -> "t", "row_count" -> -1L, "bytes" -> 8192L)),
      Nil,
      estimated = true
    )
    merged("t").rowCount shouldBe None
    merged("t").bytes shouldBe Some(8192L)
  }

  it should "report null bytes (partitioned parents, foreign tables) as None" in {
    val merged = merge(
      List(Map[String, Any]("table_name" -> "p", "table_type" -> "BASE TABLE")),
      List(Map[String, Any]("table_name" -> "p", "row_count" -> 10L, "bytes" -> null)),
      Nil,
      estimated = true
    )
    merged("p").bytes shouldBe None
    merged("p").rowCount shouldBe Some(10L)
  }

  "columns" should "build the full spelling from length, precision and scale rows" in {
    val rows = List(
      Map[String, Any](
        "table_name"               -> "T",
        "column_name"              -> "name",
        "data_type"                -> "character varying",
        "is_nullable"              -> "YES",
        "character_maximum_length" -> 256
      ),
      Map[String, Any](
        "table_name"        -> "T",
        "column_name"       -> "amt",
        "data_type"         -> "NUMBER",
        "is_nullable"       -> "NO",
        "numeric_precision" -> java.math.BigDecimal.valueOf(38),
        "numeric_scale"     -> java.math.BigDecimal.valueOf(0)
      )
    )
    columns(rows)("t") shouldBe List(
      ColumnStat("name", "character varying(256)", nullable = true),
      ColumnStat("amt", "NUMBER(38,0)", nullable = false)
    )
  }
}
