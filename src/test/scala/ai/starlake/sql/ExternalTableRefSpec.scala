package ai.starlake.sql

import ai.starlake.TestHelper
import ai.starlake.config.ConnectionInfo
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.schema.model.*

import java.util.regex.Pattern

/** The SQL of a task reads a table declared in metadata/external from the database that the
  * external domain declares (#2038). The externals are consulted after the load domains and the
  * tasks, so a reference that resolved before keeps its resolution.
  */
class ExternalTableRefSpec extends TestHelper {
  new WithSettings() {

    private val bigquery = ConnectionInfo(ConnectionType.BQ)
    private val jdbc = ConnectionInfo(ConnectionType.JDBC)
    private val noRefs = RefDesc(latestSchemaVersion, Nil)

    private def domain(name: String, database: Option[String], tables: String*): DomainInfo =
      DomainInfo(
        name = name,
        tables = tables.map(t => SchemaInfo(t, Pattern.compile(s"$t.*"), Nil)).toList,
        database = database
      )

    private def resolve(
      sql: String,
      connection: ConnectionInfo,
      domains: List[DomainInfo] = Nil,
      externals: List[DomainInfo] = Nil,
      refs: RefDesc = noRefs
    ): String =
      SQLUtils.substituteRefInSQLSelect(sql, refs, domains, Nil, connection, externals)

    private val producer = domain("pack", Some("proj-cli"), "stg")

    "A table of an external domain" should "be read from the database the external declares" in {
      resolve("select * from pack.stg", bigquery, externals = List(producer)) shouldBe
      "select * from `proj-cli`.`pack`.`stg`"
    }

    it should "be read from that database when cited without its domain" in {
      resolve(
        "select * from stg s join pack.stg p on s.id = p.id",
        bigquery,
        externals = List(producer)
      ) shouldBe
      "select * from `proj-cli`.`pack`.`stg` s join `proj-cli`.`pack`.`stg` p on s.id = p.id"
    }

    it should "get its database on every engine, as a table of a load domain does" in {
      // unquoted on JDBC: a database name with a dash would not parse, for a load domain too
      resolve(
        "select * from pack.stg",
        jdbc,
        externals = List(domain("pack", Some("cli"), "stg"))
      ) shouldBe
      "select * from cli.pack.stg"
      resolve(
        "select * from sales.orders",
        jdbc,
        domains = List(domain("sales", Some("dwh"), "orders"))
      ) shouldBe "select * from dwh.sales.orders"
    }

    it should "keep the default database when its database renders blank" in {
      for (blank <- List(Some(""), Some("  "), None)) {
        resolve(
          "select * from pack.stg",
          bigquery,
          externals = List(domain("pack", blank, "stg"))
        ) shouldBe resolve("select * from pack.stg", bigquery)
      }
    }

    "A load domain" should "win over an external domain of the same name" in {
      resolve(
        "select * from pack.stg",
        bigquery,
        domains = List(domain("pack", Some("proj-a"), "stg")),
        externals = List(producer)
      ) shouldBe "select * from `proj-a`.`pack`.`stg`"
    }

    it should "keep a table cited without its domain that an external also declares" in {
      val sales = List(domain("sales", None, "orders"))
      resolve(
        "select * from orders",
        bigquery,
        domains = sales,
        externals = List(domain("shared", Some("proj-b"), "orders"))
      ) shouldBe resolve("select * from orders", bigquery, domains = sales)
    }

    "Two external domains" should "leave unresolved a table they both declare, cited alone" in {
      resolve(
        "select * from t",
        bigquery,
        externals = List(domain("a", Some("p1"), "t"), domain("b", Some("p2"), "t"))
      ) shouldBe resolve("select * from t", bigquery)
    }

    "A ref of refs.sl.yml" should "still win over an external domain" in {
      val refs = RefDesc(
        latestSchemaVersion,
        List(
          Ref(
            InputRef(Pattern.compile("stg"), Some(Pattern.compile("pack")), None),
            OutputRef("proj-ref", "pack", "stg")
          )
        )
      )
      resolve("select * from pack.stg", bigquery, externals = List(producer), refs = refs) shouldBe
      "select * from `proj-ref`.`pack`.`stg`"
    }
  }
}
