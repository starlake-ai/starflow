package ai.starlake.job.site.stats

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, Statement}
import java.util.Locale
import scala.collection.mutable.ListBuffer
import scala.util.Using

class JdbcHardeningSpec extends AnyFlatSpec with Matchers {

  private def withLocale[T](tag: String)(f: => T): T = {
    val default = Locale.getDefault
    try {
      Locale.setDefault(Locale.forLanguageTag(tag))
      f
    } finally Locale.setDefault(default)
  }

  private def withDuckDb[T](f: Connection => T): T =
    Using.resource(DriverManager.getConnection("jdbc:duckdb:"))(f)

  /** A proxy of `target` whose `name` calls go to `intercept`, the others to `target`. */
  private def proxy[T](cls: Class[T], target: T)(
    intercept: PartialFunction[(String, Array[AnyRef]), AnyRef]
  ): T =
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array[Class[_]](cls),
        new InvocationHandler {
          def invoke(p: Any, m: Method, args: Array[AnyRef]): AnyRef = {
            val a = Option(args).getOrElse(Array.empty[AnyRef])
            intercept.applyOrElse(
              (m.getName, a),
              (_: (String, Array[AnyRef])) =>
                try m.invoke(target, a: _*)
                catch { case e: InvocationTargetException => throw e.getCause }
            )
          }
        }
      )
      .asInstanceOf[T]

  "Rows and JdbcRows" should "match column labels under the Turkish locale" in withDuckDb { c =>
    withLocale("tr-TR") {
      val rows = JdbcRows.query(c, "SELECT 'NO' AS IS_NULLABLE, 'orders' AS TABLE_ID", 10)
      rows.head.keySet shouldBe Set("is_nullable", "table_id")
      Rows.str(rows.head, "IS_NULLABLE") shouldBe Some("NO")
      Rows.str(rows.head, "TABLE_ID") shouldBe Some("orders")
    }
  }

  "PhysicalTable.key and InfoSchemaMapping" should "not depend on the default locale" in {
    withLocale("tr-TR") {
      PhysicalTable(Some("DIM"), "INFO", "ITEMS").key shouldBe (Some("dim"), "info", "items")
      InfoSchemaMapping.formatType("NUMERIC", None, Some(18L), Some(2L)) shouldBe "NUMERIC(18,2)"
      InfoSchemaMapping.formatType("STRING", Some(10L), None, None) shouldBe "STRING(10)"
      InfoSchemaMapping.tableType(Some("view")) shouldBe "VIEW"
      InfoSchemaMapping.tableType(Some("foreign table")) shouldBe "EXTERNAL"
      val merged = InfoSchemaMapping.merge(
        List(Map("table_name" -> "ITEMS", "table_type" -> "BASE TABLE")),
        Nil,
        List(
          Map(
            "table_name"  -> "ITEMS",
            "column_name" -> "ID",
            "data_type"   -> "BIGINT",
            "is_nullable" -> "NO"
          )
        ),
        estimated = false
      )
      merged.keySet shouldBe Set("items")
      merged("items").columns shouldBe List(ColumnStat("ID", "BIGINT", nullable = false))
    }
  }

  "JdbcRows.query" should "bound each statement with the query timeout" in withDuckDb { real =>
    val timeouts = ListBuffer.empty[Int]
    val conn = proxy(classOf[Connection], real) { case ("createStatement", Array()) =>
      val st = real.createStatement()
      proxy(classOf[Statement], st) { case ("setQueryTimeout", Array(s: Integer)) =>
        timeouts += s.intValue()
        null
      }
    }
    JdbcRows.query(conn, "SELECT 1 AS one", timeoutSeconds = 42) shouldBe List(
      Map("one" -> 1)
    )
    timeouts.toList shouldBe List(42)
  }

  it should "still run the query when the driver does not support timeouts" in withDuckDb { real =>
    val conn = proxy(classOf[Connection], real) { case ("createStatement", Array()) =>
      proxy(classOf[Statement], real.createStatement()) { case ("setQueryTimeout", _) =>
        throw new java.sql.SQLFeatureNotSupportedException("no timeout")
      }
    }
    JdbcRows.query(conn, "SELECT 1 AS one", timeoutSeconds = 5) shouldBe List(Map("one" -> 1))
  }

  "StatsRunner.collectorFor" should "give JDBC collectors the query timeout" in {
    val conn = ai.starlake.config.ConnectionInfo(
      ai.starlake.schema.model.ConnectionType.JDBC,
      options = Map("url" -> "jdbc:duckdb:", "driver" -> "d")
    )
    StatsRunner.collectorFor(conn, 17).collect { case j: JdbcStatsCollector =>
      j.queryTimeoutSeconds
    } shouldBe Some(17)
    StatsRunner.collectorFor(conn).collect { case j: JdbcStatsCollector =>
      j.queryTimeoutSeconds
    } shouldBe Some(StatsRunner.DefaultQueryTimeoutSeconds)
    StatsRunner.queryTimeoutSeconds(scala.concurrent.duration.Duration(1500, "millis")) shouldBe 2
    StatsRunner.queryTimeoutSeconds(scala.concurrent.duration.Duration(10, "millis")) shouldBe 1
  }

  /** A DatabaseMetaData answering getTables and getColumns from DuckDB queries. */
  private def metadata(
    c: Connection,
    schemasInTables: Boolean,
    calls: ListBuffer[(String, String, String)]
  ): DatabaseMetaData =
    proxy(classOf[DatabaseMetaData], c.getMetaData) {
      case ("supportsSchemasInTableDefinitions", _) => java.lang.Boolean.valueOf(schemasInTables)
      case ("getSearchStringEscape", _)             => "\\"
      case ("getTables", Array(cat, sch, _, _)) =>
        calls += (("tables", cat.asInstanceOf[String], sch.asInstanceOf[String]))
        val found = if (schemasInTables) sch == "sales" else cat == "sales"
        c.createStatement()
          .executeQuery(
            "SELECT 'orders' AS table_name, 'TABLE' AS table_type" +
            (if (found) "" else " WHERE false")
          ): ResultSet
      case ("getColumns", Array(cat, sch, _, _)) =>
        calls += (("columns", cat.asInstanceOf[String], sch.asInstanceOf[String]))
        c.createStatement()
          .executeQuery(
            "SELECT 'orders' AS table_name, 'id' AS column_name, 'BIGINT' AS type_name, " +
            "0 AS nullable, 19 AS column_size, 0 AS decimal_digits"
          ): ResultSet
    }

  private val orders = PhysicalTable(Some("ignored_db"), "sales", "orders")
  private val group = SchemaGroup(Some("ignored_db"), "sales", List(orders))

  "GenericJdbcStatsCollector" should "pass the schema as the catalog on MySQL" in
  withDuckDb { c =>
    val calls = ListBuffer.empty[(String, String, String)]
    val res =
      GenericJdbcStatsCollector.collectGroup(metadata(c, schemasInTables = false, calls), group)
    calls.toList shouldBe List(("tables", "sales", null), ("columns", "sales", null))
    res.tables(orders).exists shouldBe true
    res.tables(orders).columns shouldBe List(ColumnStat("id", "BIGINT", nullable = false))
  }

  it should "pass the database as the catalog and the schema as a pattern elsewhere" in
  withDuckDb { c =>
    val calls = ListBuffer.empty[(String, String, String)]
    val res =
      GenericJdbcStatsCollector.collectGroup(metadata(c, schemasInTables = true, calls), group)
    calls.toList shouldBe List(
      ("tables", "ignored_db", "sales"),
      ("columns", "ignored_db", "sales")
    )
    res.tables(orders).exists shouldBe true
  }
}
