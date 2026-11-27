package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.warehouse._
import com.google.cloud.bigquery.{Field, FieldValue, StandardSQLTypeName}

/** Runs on BigQuery when SL_BQ_TEST_PROJECT and SL_BQ_TEST_DATASET name a project and a dataset
  * with a table, and the environment has Google credentials; skipped otherwise
  */
class BigQueryWarehouseSpec extends TestHelper {
  private val project = sys.env.get("SL_BQ_TEST_PROJECT")
  private val dataset = sys.env.get("SL_BQ_TEST_DATASET")
  private val table = sys.env.getOrElse("SL_BQ_TEST_TABLE", "")

  private def field(t: StandardSQLTypeName) = Field.of("c", t)
  private def prim(s: String) = FieldValue.of(FieldValue.Attribute.PRIMITIVE, s)

  "BigQueryWarehouse.value" should "type the scalars BigQuery gives as strings" in {
    import StandardSQLTypeName._
    BigQueryWarehouse.value(field(INT64), prim("42")) shouldBe java.lang.Long.valueOf(42L)
    BigQueryWarehouse.value(field(FLOAT64), prim("1.5")) shouldBe java.lang.Double.valueOf(1.5)
    BigQueryWarehouse.value(field(BOOL), prim("true")) shouldBe java.lang.Boolean.TRUE
    BigQueryWarehouse.value(field(STRING), prim("abc")) shouldBe "abc"
    BigQueryWarehouse.value(field(DATE), prim("2024-01-31")) shouldBe "2024-01-31"
    BigQueryWarehouse.value(field(TIMESTAMP), prim("1.7064E9")) shouldBe "2024-01-28 00:00:00"
    BigQueryWarehouse.value(field(TIMESTAMP), prim("1.706486400123456E9")) shouldBe
    "2024-01-29 00:00:00.123456"
    BigQueryWarehouse.value(field(DATETIME), prim("2024-01-31T10:20:30")) shouldBe
    "2024-01-31 10:20:30"
    BigQueryWarehouse.value(field(TIME), prim("10:20:30")) shouldBe "10:20:30"
    BigQueryWarehouse.value(field(NUMERIC), prim("12.50")) shouldBe new java.math.BigDecimal(
      "12.50"
    )
    BigQueryWarehouse.value(field(BIGNUMERIC), prim("1")) shouldBe new java.math.BigDecimal("1")
    BigQueryWarehouse.value(
      field(INT64),
      FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)
    ) shouldBe null
  }

  it should "convert repeated and record values" in {
    import StandardSQLTypeName._
    import scala.jdk.CollectionConverters._
    val rep = Field.newBuilder("c", INT64).setMode(Field.Mode.REPEATED).build()
    val list = FieldValue.of(FieldValue.Attribute.REPEATED, java.util.List.of(prim("1"), prim("2")))
    BigQueryWarehouse.value(rep, list) shouldBe java.util.List.of(1L, 2L)
    val rec = Field.of("c", StandardSQLTypeName.STRUCT, Field.of("a", INT64), Field.of("b", STRING))
    val recValue = FieldValue.of(
      FieldValue.Attribute.RECORD,
      com.google.cloud.bigquery.FieldValueList.of(
        java.util.List.of(prim("7"), prim("x")),
        com.google.cloud.bigquery.FieldList.of(Field.of("a", INT64), Field.of("b", STRING))
      )
    )
    BigQueryWarehouse
      .value(rec, recValue)
      .asInstanceOf[java.util.Map[String, Object]]
      .asScala shouldBe
    Map("a" -> 7L, "b" -> "x")
  }

  new WithSettings() {
    "BigQueryWarehouse" should "answer a query and read the columns of a table" in {
      assume(project.isDefined && dataset.isDefined && table.nonEmpty, "no BigQuery test project")
      val w = new BigQueryWarehouse(settings.appConfig.connectionRef)
      w.query("select 42 as answer").get.get(0).get(0) shouldBe 42L
      w.columns(WarehouseRelation(project, dataset.get, table)).get should not be empty
      w.relation(WarehouseRelation(project, dataset.get, "sl_no_such_table")).get shouldBe None
    }
  }
}
