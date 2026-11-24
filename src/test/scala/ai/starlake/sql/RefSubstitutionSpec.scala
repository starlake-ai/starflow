package ai.starlake.sql

import ai.starlake.TestHelper
import ai.starlake.config.Settings.{latestSchemaVersion, ConnectionInfo}
import ai.starlake.schema.model.ConnectionType.FS
import ai.starlake.schema.model.{InputRef, OutputRef, Ref, RefDesc}

import java.util.regex.Pattern

/** Ref substitution rewrites table references before the SQL is handed to the warehouse, so a
  * mis-rewrite silently reads or writes the wrong table.
  */
class RefSubstitutionSpec extends TestHelper {
  new WithSettings() {

    private val connection =
      ConnectionInfo(FS, None, None, Some(""), Some("."), Map.empty)

    private def refTo(domain: String, table: String, outDomain: String, outTable: String) =
      RefDesc(
        latestSchemaVersion,
        List(
          Ref(
            InputRef(Pattern.compile(table), Some(Pattern.compile(domain)), None),
            OutputRef("", outDomain, outTable)
          )
        )
      )

    private def substitute(sql: String, refs: RefDesc): String =
      SQLUtils.buildSingleSQLQueryForRegex(
        sql,
        refs,
        Nil,
        Nil,
        SQLUtils.fromsRegex,
        "FROM",
        connection
      )

    "Ref substitution" should "not treat the table name as a regular expression" in {
      // 'sales.orders' as a regex also matches 'salesXorders': the unrelated table must survive.
      val resolved = substitute(
        "select * from sales.orders, salesXorders",
        refTo("sales", "orders", "warehouse", "orders")
      )
      resolved should include("warehouse.orders")
      resolved should include("salesXorders")
    }

    it should "treat '$' in the resolved name as a literal, not a group reference" in {
      // BigQuery partition decorators put a '$' in the table name.
      val resolved = substitute(
        "select * from sales.orders",
        refTo("sales", "orders", "warehouse", "orders$20260101")
      )
      resolved should include("warehouse.orders$20260101")
    }

    it should "treat a backslash in the resolved name as a literal" in {
      val resolved = substitute(
        "select * from sales.orders",
        refTo("sales", "orders", "warehouse", """orders\raw""")
      )
      resolved should include("""warehouse.orders\raw""")
    }

    it should "still rewrite a plain reference" in {
      substitute(
        "select * from sales.orders",
        refTo("sales", "orders", "warehouse", "orders")
      ) should include("warehouse.orders")
    }

    it should "leave a query with no matching ref untouched" in {
      val sql = "select * from sellers hrs, orders sos where hrs.id = sos.seller_id"
      substitute(sql, RefDesc(latestSchemaVersion, Nil)) should equal(sql)
    }
  }
}
