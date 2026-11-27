package ai.starlake.sql

import ai.starlake.TestHelper
import ai.starlake.config.ConnectionInfo
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.schema.model.*

import java.util.regex.Pattern

/** The SQL of a task reads a table declared in metadata/external, cited with its domain, from the
  * database that the external domain declares (#2038). The externals are consulted after the load
  * domains and the tasks, so a reference that resolved before keeps its resolution.
  *
  * The externals are given here as `SchemaHandler.externalsForSql` passes them, which keeps a
  * database only when the domain writes it with a variable (see ExternalDatabaseLineageSpec).
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
      tasks: List[AutoTaskInfo] = Nil,
      externals: List[DomainInfo] = Nil,
      refs: RefDesc = noRefs
    ): String =
      SQLUtils.substituteRefInSQLSelect(sql, refs, domains, tasks, connection, externals)

    private val shared = domain("shared_ds", Some("proj-b"), "orders")

    "A table of an external domain" should "be read from the database the external declares" in {
      resolve(
        "select * from shared_ds.orders o join shared_ds.orders p on o.id = p.id",
        bigquery,
        externals = List(shared)
      ) shouldBe
      "select * from `proj-b`.`shared_ds`.`orders` o join `proj-b`.`shared_ds`.`orders` p on o.id = p.id"
    }

    it should "be left as before when cited without its domain" in {
      // a temporary table or a CTE may share its name with an external table
      resolve("select * from orders", bigquery, externals = List(shared)) shouldBe
      resolve("select * from orders", bigquery)
    }

    it should "be left as before when its external domain does not declare it" in {
      resolve("select * from shared_ds.missing", bigquery, externals = List(shared)) shouldBe
      resolve("select * from shared_ds.missing", bigquery)
    }

    it should "get its database on every engine, as a table of a load domain does" in {
      // unquoted on JDBC: a database name with a dash would not parse, for a load domain too
      resolve(
        "select * from shared_ds.orders",
        jdbc,
        externals = List(domain("shared_ds", Some("ext_db"), "orders"))
      ) shouldBe "select * from ext_db.shared_ds.orders"
      resolve(
        "select * from sales.orders",
        jdbc,
        domains = List(domain("sales", Some("dwh"), "orders"))
      ) shouldBe "select * from dwh.sales.orders"
    }

    it should "keep the default database when its database is blank" in {
      for {
        connection <- List(bigquery, jdbc)
        blank      <- List(Some(""), Some("  "), None)
      } resolve(
        "select * from shared_ds.orders",
        connection,
        externals = List(domain("shared_ds", blank, "orders"))
      ) shouldBe resolve("select * from shared_ds.orders", connection)
    }

    "A load domain" should "win over an external domain of the same name" in {
      resolve(
        "select * from shared_ds.orders",
        bigquery,
        domains = List(domain("shared_ds", Some("proj-a"), "orders")),
        externals = List(shared)
      ) shouldBe "select * from `proj-a`.`shared_ds`.`orders`"
    }

    it should "keep a table cited without its domain that an external also declares" in {
      val sales = List(domain("sales", None, "orders"))
      resolve(
        "select * from orders",
        bigquery,
        domains = sales,
        externals = List(shared)
      ) shouldBe resolve("select * from orders", bigquery, domains = sales)
    }

    "A task" should "win over an external domain that declares its table" in {
      val task = AutoTaskInfo(
        name = "orders",
        sql = None,
        database = Some("proj-a"),
        domain = "shared_ds",
        table = "orders"
      )
      resolve(
        "select * from shared_ds.orders",
        bigquery,
        tasks = List(task),
        externals = List(shared)
      ) shouldBe "select * from `proj-a`.`shared_ds`.`orders`"
    }

    "External domains of the same name" should "leave the table as before when their databases differ" in {
      resolve(
        "select * from shared_ds.orders",
        bigquery,
        externals = List(shared, domain("shared_ds", Some("proj-c"), "orders"))
      ) shouldBe resolve("select * from shared_ds.orders", bigquery)
    }

    it should "resolve the table when they agree on the database" in {
      resolve(
        "select * from shared_ds.orders",
        bigquery,
        externals = List(shared, domain("shared_ds", Some("proj-b"), "orders", "customers"))
      ) shouldBe "select * from `proj-b`.`shared_ds`.`orders`"
    }

    "A ref of refs.sl.yml" should "still win over an external domain" in {
      val refs = RefDesc(
        latestSchemaVersion,
        List(
          Ref(
            InputRef(Pattern.compile("orders"), Some(Pattern.compile("shared_ds")), None),
            OutputRef("proj-ref", "shared_ds", "orders")
          )
        )
      )
      resolve(
        "select * from shared_ds.orders",
        bigquery,
        externals = List(shared),
        refs = refs
      ) shouldBe "select * from `proj-ref`.`shared_ds`.`orders`"
    }
  }
}
