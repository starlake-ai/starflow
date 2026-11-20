package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.schema.model.{
  AccessControlEntry,
  AllSinks,
  DomainInfo,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute
}
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.util.Try

/** A JDBC transform applies the acl of its table after its write: no GRANT on DuckDB, which has
  * none, the PostgreSQL syntax on PostgreSQL, and a grant that fails fails the task, unless access
  * policies are not applied (#1985).
  */
class JdbcAclSpec extends TestHelper {

  private val folder = "acl_it_1985"

  private def configuration(dataConnection: String, applyPolicies: Boolean): Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "$dataConnection"
           |accessPolicies.apply = $applyPolicies
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/jdbc_acl_1985.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |connections.test-databricks {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:databricks://example.cloud.databricks.com:443/default"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private class Fixture(val dataConnection: String, applyPolicies: Boolean = true)
      extends WithSettings(configuration(dataConnection, applyPolicies)) {

    def transform(task: String, role: String, grants: String*): Try[_] =
      transformExpecting(task, Nil, role, grants: _*)

    /** @param expectations
      *   the expectations of the task, macros of `acl_1985.j2`, none failing the task
      */
    def transformExpecting(
      task: String,
      expectations: List[String],
      role: String,
      grants: String*
    ): Try[_] = {
      writeTask(task, expectations, role, grants: _*)
      new IngestionWorkflow(
        storageHandler,
        settings.schemaHandler(reload = true)
      ).autoJob(TransformConfig(name = s"$folder.$task"))
    }

    /** The GRANT statements of a task, which is not run */
    def aclOf(task: String, role: String, grants: String*): Set[String] = {
      writeTask(task, Nil, role, grants: _*)
      val schemaHandler = settings.schemaHandler(reload = true)
      val taskDesc = schemaHandler.taskOnly(s"$folder.$task").get
      TransformContext(
        appId = None,
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        accessToken = None,
        resultPageSize = 1,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = None,
        syncSchema = false
      )(settings, storageHandler, schemaHandler)
        .toTask(taskDesc.getRunEngine())
        .aclSQL()
        .toSet
    }

    def writeTask(task: String, expectations: List[String], role: String, grants: String*): Unit = {
      storageHandler.write(
        "{% macro has_rows() %}SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM sl_this{% endmacro %}",
        new Path(DatasetArea.expectations, "acl_1985.j2")
      )
      val directory = s"$starlakeMetadataPath/transform/$folder"
      val grantsYaml = grants.map(grant => s"""        - "$grant"""").mkString("\n")
      val expectationsYaml =
        if (expectations.isEmpty) ""
        else
          expectations
            .map(e => s"""    - expect: "$e"\n      failOnError: false\n""")
            .mkString("  expectations:\n", "", "")
      storageHandler.write(
        s"""version: 1
           |task:
           |  writeStrategy:
           |    type: "OVERWRITE"
           |$expectationsYaml  acl:
           |    - role: "$role"
           |      grants:
           |$grantsYaml
           |""".stripMargin,
        new Path(s"$directory/$task.sl.yml")
      )
      storageHandler.write("SELECT 1 AS id", new Path(s"$directory/$task.sql"))
    }

    /** Loads `id` 1 and 2 into a native table on the connection of the data, with an acl */
    def load(domain: String, table: String, acl: AccessControlEntry): Try[_] = {
      DomainInfo(
        name = domain,
        metadata = Some(Metadata(directory = Some(s"$starlakeTestRoot/incoming"))),
        tables = List(
          SchemaInfo(
            table,
            Pattern.compile(s"$table-.*[.]csv"),
            attributes = List(TableAttribute("id", "long")),
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(";"),
                loader = Some("native"),
                sink = Some(AllSinks(connectionRef = Some(dataConnection)))
              )
            ),
            acl = List(acl)
          )
        )
      ).writeDomainAsYaml(DatasetArea.load)(storageHandler)
      storageHandler.write("id\n1\n2\n", new Path(DatasetArea.stage(domain), s"$table-1.csv"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true)).load(
        LoadConfig(
          domains = List(domain),
          tables = List(table),
          accessToken = None,
          test = false,
          scheduledDate = None
        )
      )
    }

    /** The results of the expectations of a table, in the audit on DuckDB */
    def expectationResults(domain: String, table: String): List[String] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils
          .executeQueryAsMap(
            s"""SELECT CAST(success AS VARCHAR) AS result FROM audit.expectations
               |WHERE domain = '$domain' AND "schema" = '$table'""".stripMargin,
            connection
          )
          .toList
          .map(_("result"))
      }

    def query(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(dataConnection).options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection).toList.map(_.toMap)
      }

    def execute(sql: String): Unit =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections(dataConnection).options
      ) { connection =>
        connection.setAutoCommit(true)
        val statement = connection.createStatement()
        try statement.execute(sql)
        finally statement.close()
      }

    def ids(table: String): List[Long] =
      query(s"SELECT id AS id FROM $folder.$table").map(_("id").toLong)

    /** A role of the PostgreSQL container, created once */
    def createRole(name: String): Unit =
      execute(
        s"""DO $$$$ BEGIN CREATE ROLE "$name"; EXCEPTION WHEN duplicate_object THEN NULL; END $$$$"""
      )

    def canSelect(role: String, table: String): Boolean =
      query(
        s"""SELECT CASE WHEN has_table_privilege('$role', '$folder.$table', 'SELECT')
           |THEN 1 ELSE 0 END AS granted""".stripMargin
      ).head("granted") == "1"
  }

  new Fixture("test-duckdb") {
    "A DuckDB transform with an acl" should "run no GRANT, which DuckDB does not have" in {
      // A grant that fails would fail the task
      transform(
        "duck_granted",
        "roles/bigquery.dataViewer",
        "serviceAccount:reader@my-project.iam.gserviceaccount.com",
        "user:alice"
      ).isSuccess shouldBe true
      ids("duck_granted") shouldBe List(1L)
    }

    "A DuckDB load with an acl" should "run no GRANT either" in {
      load(
        "acl_load_1985",
        "loaded",
        AccessControlEntry(
          "roles/bigquery.dataViewer",
          Set("serviceAccount:reader@my-project.iam.gserviceaccount.com")
        )
      ).isSuccess shouldBe true
      query("SELECT id AS id FROM acl_load_1985.loaded ORDER BY id").map(_("id").toLong) shouldBe
      List(1L, 2L)
    }
  }

  new Fixture("test-pg") {
    "A PostgreSQL transform with an acl" should "grant to users, groups and roles in the syntax of PostgreSQL" in {
      List("sl_reader_1985", "sl_readers_1985", "sl_auditors_1985", "reader@example.com")
        .foreach(createRole)
      transform(
        "pg_granted",
        "SELECT",
        "user:sl_reader_1985",
        "group:sl_readers_1985",
        "role:sl_auditors_1985",
        "user:reader@example.com"
      ).isSuccess shouldBe true
      List("sl_reader_1985", "sl_readers_1985", "sl_auditors_1985", "reader@example.com")
        .foreach(role => withClue(role)(canSelect(role, "pg_granted") shouldBe true))
    }

    it should "fail when a grant fails, its rows being written" in {
      val result = transform("pg_missing_role", "SELECT", "role:sl_no_such_role_1985")
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("sl_no_such_role_1985")
      ids("pg_missing_role") shouldBe List(1L)
    }

    it should "write the reports of its expectations before failing on a grant" in {
      val result = transformExpecting(
        "pg_expected",
        List("has_rows()"),
        "SELECT",
        "role:sl_no_such_role_1985"
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("sl_no_such_role_1985")
      expectationResults(folder, "pg_expected") shouldBe List("true")
    }

    it should "fail with an explicit error for a grantee that PostgreSQL cannot express" in {
      val result = transform(
        "pg_service_account",
        "SELECT",
        "serviceAccount:reader@my-project.iam.gserviceaccount.com"
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("which postgresql cannot express")
    }
  }

  new Fixture("test-databricks") {
    "A transform through the JDBC driver of Databricks" should "grant in the syntax of Databricks" in {
      // Not run: no Databricks there
      aclOf("dbx_granted", "SELECT", "analysts", "user:me@me.com") shouldBe Set(
        s"GRANT SELECT ON TABLE $folder.dbx_granted TO analysts",
        s"GRANT SELECT ON TABLE $folder.dbx_granted TO `me@me.com`"
      )
    }
  }

  new Fixture("test-pg", applyPolicies = false) {
    "A PostgreSQL transform with an acl, when access policies are not applied," should
    "run no GRANT" in {
      transform(
        "pg_not_applied",
        "SELECT",
        "role:sl_no_such_role_1985",
        "serviceAccount:reader@my-project.iam.gserviceaccount.com"
      ).isSuccess shouldBe true
      ids("pg_not_applied") shouldBe List(1L)
    }
  }
}
