package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GenericMetadataMappingSpec extends AnyFlatSpec with Matchers {
  "toInfoSchemaColumns" should "translate DatabaseMetaData.getColumns rows" in {
    val rows = List(
      Map[String, Any](
        "table_name"     -> "t",
        "column_name"    -> "amount",
        "type_name"      -> "DECIMAL",
        "column_size"    -> 18,
        "decimal_digits" -> 2,
        "nullable"       -> 1
      ),
      Map[String, Any](
        "table_name"     -> "t",
        "column_name"    -> "id",
        "type_name"      -> "BIGINT",
        "column_size"    -> 19,
        "decimal_digits" -> 0,
        "nullable"       -> 0
      )
    )
    InfoSchemaMapping.columns(GenericMetadataMapping.toInfoSchemaColumns(rows)) shouldBe Map(
      "t" -> List(
        ColumnStat("amount", "DECIMAL(18,2)", nullable = true),
        ColumnStat("id", "BIGINT", nullable = false)
      )
    )
  }
}
