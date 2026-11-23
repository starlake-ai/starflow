package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BigQuerySqlSpec extends AnyFlatSpec with Matchers {
  import BigQuerySql._

  "tablesSql" should "read __TABLES__ of one dataset, never count rows" in {
    val sql = tablesSql("my-project", "Sales")
    sql shouldBe
    "SELECT table_id, type, row_count, size_bytes, last_modified_time FROM `my-project.Sales.__TABLES__`"
    sql.toLowerCase should not include "count("
  }

  "columnsSql" should "read INFORMATION_SCHEMA.COLUMNS of one dataset" in {
    columnsSql("my-project", "Sales") shouldBe
    "SELECT table_name, column_name, data_type, is_nullable FROM `my-project.Sales.INFORMATION_SCHEMA.COLUMNS` ORDER BY table_name, ordinal_position"
  }

  "identifiers" should "reject backquotes and control characters" in {
    an[IllegalArgumentException] should be thrownBy tablesSql("p`; DROP", "Sales")
    an[IllegalArgumentException] should be thrownBy columnsSql("p", "Sa`les")
    an[IllegalArgumentException] should be thrownBy tablesSql("p", "Sa\nles")
    an[IllegalArgumentException] should be thrownBy tablesSql("", "Sales")
  }

  "tableType" should "map __TABLES__ type codes" in {
    tableType(Some(1L)) shouldBe "TABLE"
    tableType(Some(2L)) shouldBe "VIEW"
    tableType(Some(3L)) shouldBe "EXTERNAL"
    tableType(None) shouldBe "TABLE"
  }

  "merge" should "map rows (string values, as returned by the BigQuery client)" in {
    val tables = List(
      Map[String, Any](
        "table_id"           -> "Orders",
        "type"               -> "1",
        "row_count"          -> "120345",
        "size_bytes"         -> "9123456",
        "last_modified_time" -> "1795403400000"
      ),
      Map[String, Any](
        "table_id"           -> "v_orders",
        "type"               -> "2",
        "row_count"          -> "0",
        "size_bytes"         -> "0",
        "last_modified_time" -> "1795403400000"
      )
    )
    val columns = List(
      Map[String, Any](
        "table_name"  -> "Orders",
        "column_name" -> "id",
        "data_type"   -> "INT64",
        "is_nullable" -> "NO"
      ),
      Map[String, Any](
        "table_name"  -> "Orders",
        "column_name" -> "amount",
        "data_type"   -> "NUMERIC(18, 2)",
        "is_nullable" -> "YES"
      )
    )
    val merged = merge(tables, columns)
    merged("orders") shouldBe RawTableStats(
      exists = true,
      tableType = Some("TABLE"),
      format = None,
      rowCount = Some(120345L),
      rowCountEstimated = false,
      bytes = Some(9123456L),
      lastModified = Some("2026-11-23T03:10:00Z"),
      columns = List(
        ColumnStat("id", "INT64", nullable = false),
        ColumnStat("amount", "NUMERIC(18, 2)", nullable = true)
      )
    )
    merged("v_orders").tableType shouldBe Some("VIEW")
    merged("v_orders").rowCount shouldBe None
    merged("v_orders").bytes shouldBe None
  }
}
