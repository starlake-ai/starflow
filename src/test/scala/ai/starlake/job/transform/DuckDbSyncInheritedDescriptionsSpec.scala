package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.io.ByteArrayOutputStream
import scala.util.Try

/** `transform --sync-preview` and `--sync-apply` give a column that copies a described column, as
  * it is, renamed or cast, its description, through the tasks of the project (#1948)
  */
class DuckDbSyncInheritedDescriptionsSpec extends TestHelper {

  lazy val duckDbPath = s"$starlakeTestRoot/test_sync_inherited_descriptions.db"

  lazy val duckDbConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$duckDbPath"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  new WithSettings(duckDbConfiguration) {

    private def execute(statements: String*): Unit = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        statements.foreach(conn.createStatement().execute)
      }
    }

    /** The load table sales.orders, described, in the project and in DuckDB */
    private def setupOrders(): Unit = {
      storageHandler.write(
        """version: 1
          |load:
          |  name: "sales"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/load/sales/_config.sl.yml")
      )
      storageHandler.write(
        """version: 1
          |table:
          |  name: "orders"
          |  pattern: "orders.*.csv"
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      comment: "Identifier of the order"
          |    - name: "amount"
          |      type: "integer"
          |      comment: "Amount of the order"
          |    - name: "CUSTOMER"
          |      type: "string"
          |      rename: "customer_id"
          |      comment: "Customer of the order"
          |    - name: "status"
          |      type: "string"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/load/sales/orders.sl.yml")
      )
      execute(
        "CREATE SCHEMA IF NOT EXISTS sales",
        "CREATE OR REPLACE TABLE sales.orders(id VARCHAR, amount INTEGER, customer_id VARCHAR, status VARCHAR)",
        "INSERT INTO sales.orders VALUES ('1', 100, 'c1', 'open'), ('2', 200, 'c2', 'closed')"
      )
    }

    private def taskPath(domain: String, task: String) =
      new Path(s"$starlakeMetadataPath/transform/$domain/$task.sl.yml")

    /** Writes the task, then reloads the project */
    private def writeTask(
      domain: String,
      task: String,
      sql: String,
      attributes: List[TableAttribute],
      presql: List[String] = Nil,
      parseSQL: Option[Boolean] = None
    ): Unit = {
      val taskInfo = AutoTaskInfo(
        name = "",
        sql = None,
        database = None,
        domain = domain,
        table = task,
        presql = presql,
        sink = Some(JdbcSink(connectionRef = Some("test-duckdb")).toAllSinks()),
        python = None,
        writeStrategy = Some(WriteStrategy(`type` = Some(WriteStrategyType.OVERWRITE))),
        attributes = attributes,
        parseSQL = parseSQL
      )
      storageHandler.write(
        mapper.writer().withAttribute(classOf[Settings], settings).writeValueAsString(taskInfo),
        taskPath(domain, task)
      )
      storageHandler.write(sql, new Path(s"$starlakeMetadataPath/transform/$domain/$task.sql"))
      settings.schemaHandler(reload = true)
    }

    private def sync(
      name: String,
      apply: Boolean,
      refresh: Boolean = false
    ): Try[_] =
      TransformCmd.run(
        TransformConfig(
          name = name,
          syncApply = apply,
          syncPreview = !apply,
          refreshInheritedDescriptions = refresh
        ),
        settings.schemaHandler()
      )

    private def descriptionsOf(name: String): List[(String, Option[String])] =
      settings
        .schemaHandler(reload = true)
        .taskByFullName(name)
        .get
        .attributes
        .map(attribute => attribute.name -> attribute.comment)

    /** The descriptions that the attributes of the task would get, without the SELECT run */
    private def inherited(name: String): List[(String, Option[String])] =
      settings
        .schemaHandler()
        .inheritDescriptions(name, Nil, None, refresh = false)
        .attributes
        .map { case (attribute, _) => attribute.name -> attribute.comment }

    /** The descriptions that the attributes of the task would get, checking that the lineage is
      * read
      */
    private def inheritedRead(
      name: String,
      refresh: Boolean = false
    ): List[(String, Option[String])] = {
      val result = settings.schemaHandler().inheritDescriptions(name, Nil, None, refresh)
      result.error shouldBe None
      result.attributes.map { case (attribute, _) => attribute.name -> attribute.comment }
    }

    private val dimSql =
      """SELECT id, amount AS total, CAST(customer_id AS VARCHAR) AS customer, status,
        |  amount + 1 AS plus_one, id || customer_id AS two_columns
        |FROM sales.orders""".stripMargin

    "transform --sync-apply" should "write the description of the column that a column copies, renamed or cast, and keep its own" in {
      setupOrders()
      writeTask(
        "dims",
        "dim_orders",
        dimSql,
        List(TableAttribute("id", "string", comment = Some("written by hand")))
      )
      val preview = new ByteArrayOutputStream()
      Console.withOut(preview) {
        sync("dims.dim_orders", apply = false).isSuccess shouldBe true
      }
      preview.toString should include(
        "Attribute: total: integer, Status: ADD, Description: inherited from sales.orders.amount"
      )
      preview.toString should include(
        "Attributes without a description: status, plus_one, two_columns"
      )
      // The preview writes nothing
      descriptionsOf("dims.dim_orders") shouldBe List("id" -> Some("written by hand"))

      sync("dims.dim_orders", apply = true).isSuccess shouldBe true
      descriptionsOf("dims.dim_orders") shouldBe List(
        "id"    -> Some("written by hand"),
        "total" -> Some("Amount of the order"),
        // Renamed in the load table
        "customer" -> Some("Customer of the order"),
        // Not described in the load table
        "status"      -> None,
        "plus_one"    -> None,
        "two_columns" -> None
      )

      // Already written: the preview shows them
      val afterApply = new ByteArrayOutputStream()
      Console.withOut(afterApply) {
        sync("dims.dim_orders", apply = false).isSuccess shouldBe true
      }
      afterApply.toString should include(
        "Inherited descriptions, already written: total (sales.orders.amount), customer (sales.orders.customer_id)"
      )

      // The description written in the task is replaced only when asked
      sync("dims.dim_orders", apply = true).isSuccess shouldBe true
      descriptionsOf("dims.dim_orders").head shouldBe "id" -> Some("written by hand")
      sync("dims.dim_orders", apply = true, refresh = true).isSuccess shouldBe true
      descriptionsOf("dims.dim_orders").head shouldBe "id" -> Some("Identifier of the order")
    }

    it should "inherit through a task that declares no description, from a task that declares one" in {
      setupOrders()
      writeTask(
        "dims",
        "dim_orders",
        dimSql,
        List(
          TableAttribute("id", "string", comment = Some("written in the dimension")),
          TableAttribute("total", "integer"),
          TableAttribute("plus_one", "integer")
        )
      )
      writeTask(
        "facts",
        "fact_orders",
        "SELECT d.id, d.total AS amount, d.plus_one FROM dims.dim_orders d",
        List(
          TableAttribute("id", "string"),
          TableAttribute("amount", "integer"),
          TableAttribute("plus_one", "integer")
        )
      )
      inherited("facts.fact_orders") shouldBe List(
        "id"     -> Some("written in the dimension"),
        "amount" -> Some("Amount of the order"),
        // Computed in the dimension
        "plus_one" -> None
      )
    }

    it should "inherit from the source of a UNION ALL whose other branch reads the previous state of the table" in {
      setupOrders()
      writeTask(
        "dims",
        "history",
        "SELECT id, amount FROM previous UNION ALL SELECT id, amount FROM sales.orders",
        List(TableAttribute("id", "string"), TableAttribute("amount", "integer")),
        presql = List("CREATE TEMP TABLE previous AS SELECT id, amount FROM dims.history")
      )
      inherited("dims.history") shouldBe List(
        "id"     -> Some("Identifier of the order"),
        "amount" -> Some("Amount of the order")
      )
    }

    it should "stop at a cycle, and at two sources with different descriptions" in {
      setupOrders()
      writeTask(
        "cycle",
        "first",
        "SELECT code FROM cycle.second",
        List(TableAttribute("code", "string"))
      )
      writeTask(
        "cycle",
        "second",
        "SELECT code FROM cycle.first",
        List(TableAttribute("code", "string"))
      )
      inheritedRead("cycle.first") shouldBe Nil
      writeTask(
        "cycle",
        "union",
        "SELECT id FROM sales.orders UNION ALL SELECT customer_id AS id FROM sales.orders",
        List(TableAttribute("id", "string"))
      )
      inheritedRead("cycle.union") shouldBe Nil
    }

    it should "refresh through a task that keeps an earlier copy of the description" in {
      setupOrders()
      writeTask(
        "dims",
        "dim_orders",
        dimSql,
        List(
          TableAttribute("id", "string"),
          TableAttribute("total", "integer", comment = Some("Amount, as copied before"))
        )
      )
      writeTask(
        "facts",
        "fact_orders",
        "SELECT d.total AS amount FROM dims.dim_orders d",
        List(TableAttribute("amount", "integer"))
      )
      inheritedRead("facts.fact_orders") shouldBe List("amount" -> Some("Amount, as copied before"))
      inheritedRead("facts.fact_orders", refresh = true) shouldBe List(
        "amount" -> Some("Amount of the order")
      )
    }

    it should "read a table named alone in a renamed domain, and a column that the task computes in the branch that reads its previous state" in {
      setupOrders()
      storageHandler.write(
        """version: 1
          |load:
          |  name: "crm"
          |  rename: "crm_v2"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/load/crm/_config.sl.yml")
      )
      storageHandler.write(
        """version: 1
          |table:
          |  name: "clients"
          |  pattern: "clients.*.csv"
          |  attributes:
          |    - name: "client_id"
          |      type: "string"
          |      comment: "Identifier of the client"
          |""".stripMargin,
        new Path(s"$starlakeMetadataPath/load/crm/clients.sl.yml")
      )
      writeTask(
        "facts",
        "clients_copy",
        "SELECT client_id FROM clients",
        List(TableAttribute("client_id", "string"))
      )
      inheritedRead("facts.clients_copy") shouldBe List(
        "client_id" -> Some("Identifier of the client")
      )
      writeTask(
        "dims",
        "history_with_default",
        "SELECT id, COALESCE(amount, 0) AS amount FROM previous UNION ALL SELECT id, amount FROM sales.orders",
        List(TableAttribute("id", "string"), TableAttribute("amount", "integer")),
        presql =
          List("CREATE TEMP TABLE previous AS SELECT id, amount FROM dims.history_with_default")
      )
      inheritedRead("dims.history_with_default") shouldBe List(
        "id"     -> Some("Identifier of the order"),
        "amount" -> Some("Amount of the order")
      )
      // A branch that reads the previous state and copies the source
      writeTask(
        "dims",
        "history_joined",
        """SELECT id, amount FROM previous WHERE id NOT IN (SELECT id FROM sales.orders)
          |UNION ALL
          |SELECT o.id, o.amount FROM sales.orders o LEFT JOIN previous p ON o.id = p.id""".stripMargin,
        List(TableAttribute("id", "string"), TableAttribute("amount", "integer")),
        presql = List("CREATE TEMP TABLE previous AS SELECT id, amount FROM dims.history_joined")
      )
      inheritedRead("dims.history_joined") shouldBe List(
        "id"     -> Some("Identifier of the order"),
        "amount" -> Some("Amount of the order")
      )
    }

    it should "read the attributes that an upstream task declares and that its SELECT does not return" in {
      setupOrders()
      writeTask(
        "dims",
        "validity",
        "SELECT id FROM sales.orders",
        List(
          TableAttribute("id", "string"),
          TableAttribute("start_ts", "timestamp", comment = Some("Start of the validity"))
        )
      )
      writeTask(
        "facts",
        "validity_copy",
        "SELECT v.id, start_ts FROM dims.validity v JOIN sales.orders o ON v.id = o.id",
        List(TableAttribute("id", "string"), TableAttribute("start_ts", "timestamp"))
      )
      inheritedRead("facts.validity_copy") shouldBe List(
        "id"       -> Some("Identifier of the order"),
        "start_ts" -> Some("Start of the validity")
      )
    }

    it should "keep a sync that changes nothing when the YAML of the task cannot take the inherited descriptions" in {
      setupOrders()
      val yamlPath = taskPath("dims", "jinja_comment")
      val yaml =
        """version: 1
          |task:
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      comment: "{{ id_description }}"
          |""".stripMargin
      storageHandler.write(yaml, yamlPath)
      storageHandler.write(
        "SELECT id FROM sales.orders",
        new Path(s"$starlakeMetadataPath/transform/dims/jinja_comment.sql")
      )
      settings.schemaHandler(reload = true)
      sync("dims.jinja_comment", apply = true).isSuccess shouldBe true
      storageHandler.read(yamlPath) shouldBe yaml
    }

    it should "inherit nothing for a task whose SQL is not parsed, and refuse the refresh outside a sync" in {
      setupOrders()
      writeTask(
        "dims",
        "unparsed",
        "SELECT id FROM sales.orders",
        List(TableAttribute("id", "string")),
        parseSQL = Some(false)
      )
      inheritedRead("dims.unparsed") shouldBe Nil
      TransformCmd.parse(
        Seq("--name", "dims.unparsed", "--refresh-inherited-descriptions")
      ) shouldBe
      None
      TransformCmd
        .parse(Seq("--name", "dims.unparsed", "--sync-apply", "--refresh-inherited-descriptions"))
        .map(_.refreshInheritedDescriptions) shouldBe Some(true)
    }

    it should "keep the sync, without inherited descriptions, when the engine runs a SELECT that cannot be parsed" in {
      setupOrders()
      // The FROM-first syntax of DuckDB
      writeTask("dims", "from_first", "FROM sales.orders SELECT id, amount", Nil)
      settings
        .schemaHandler()
        .inheritDescriptions("dims.from_first", Nil, None, refresh = false)
        .error shouldBe defined
      sync("dims.from_first", apply = true).isSuccess shouldBe true
      descriptionsOf("dims.from_first") shouldBe List("id" -> None, "amount" -> None)
    }
  }
}
