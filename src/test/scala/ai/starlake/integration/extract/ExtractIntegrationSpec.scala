package ai.starlake.integration.extract

import ai.starlake.TestHelper
import ai.starlake.extract.{ExtractDataJob, ExtractSchema}
import ai.starlake.sql.StarlakeJdbcDialects
import better.files.File

import java.sql.DriverManager

class ExtractIntegrationSpec extends TestHelper {

  new WithSettings() {
    "Extract Data from MySQL" should "succeed" in {
      StarlakeJdbcDialects.registerDialects()
      val jdbcOptions = settings.appConfig.connections("test-mysql")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      logger.info(TestHelper.mysqlContainer.jdbcUrl)
      val sqls = """
                   |CREATE SCHEMA IF NOT EXISTS starlake;
                   |create table if not exists starlake.test_table(id int,name varchar(10));
                   |insert into starlake.test_table values(1, 'a');
                   |insert into starlake.test_table values(2, 'b');
                   |insert into starlake.test_table values(3, 'c');
                   |insert into starlake.test_table values(4, 'd');
                   |insert into starlake.test_table values(5, 'e');
                   |insert into starlake.test_table values(6, 'f');
                   |insert into starlake.test_table values(7, 'g');
                   |insert into starlake.test_table values(8, 'h');
                   |insert into starlake.test_table values(9, 'i');
                   |insert into starlake.test_table values(10, 'j');
                   |""".stripMargin

      sqls.split(";").filter(_.trim.nonEmpty).foreach { sql =>
        logger.info("execute update : " + sql.trim)
        val st = conn.createStatement()
        st.executeUpdate(sql.trim)
        st.close()
      }

      val config =
        """
          |version: 1
          |extract:
          |  connectionRef: "test-mysql" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - schema: "starlake" # Database schema where tables are located
          |      tables:
          |        - name: "*" # Takes all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |""".stripMargin
      val tmpYmlFile = File.newTemporaryFile("extract", ".sl.yml")
      val tmpDir = File.newTemporaryDirectory("extract")
      tmpYmlFile.write(config)
      val schemaHandler = settings.schemaHandler()
      val result = new ExtractDataJob(schemaHandler).run(
        Array(
          "--clean",
          "--config",
          tmpYmlFile.pathAsString,
          "--outputDir",
          tmpDir.pathAsString
        )
      )
      result should be a Symbol("success")
      val files = tmpDir.listRecursively.filter(_.name.endsWith(".csv")).toList
      assert(files.size == 1)
    }
    "Extract Data from postgres" should "succeed" in {
      val jdbcOptions = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      logger.info(pgContainer.jdbcUrl)
      val sqls: String =
        """
          |drop table if exists test_table1 cascade;
          |drop table if exists test_table2 cascade;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create view test_view1 AS SELECT NAME FROM test_table1;
          |insert into test_table1 values (1,'A');
          |CREATE TABLE IF NOT EXISTS audit.SL_LAST_EXPORT (
          |                              domain VARCHAR(255) not NULL,
          |                              schema VARCHAR(255) not NULL,
          |                              last_ts TIMESTAMP,
          |                              last_date DATE,
          |                              last_long INTEGER,
          |                              last_decimal DECIMAL,
          |                              start_ts TIMESTAMP not NULL,
          |                              end_ts TIMESTAMP not NULL,
          |                              duration INTEGER not NULL,
          |                              mode VARCHAR(255) not NULL,
          |                              count BIGINT not NULL,
          |                              success BOOLEAN not NULL,
          |                              message VARCHAR(255) not NULL,
          |                              step VARCHAR(255) not NULL
          |                             );""".stripMargin
      sqls.split(";").foreach { sql =>
        logger.info("execute update : " + sql.trim)
        val st = conn.createStatement()
        st.executeUpdate(sql.trim)
        st.close()
      }
      val st = conn.createStatement()
      val rs = st.executeQuery("select current_database()")
      rs.next()
      st.close()
      conn.close()
      val config =
        """
          |version: 1
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - schema: "public" # Database schema where tables are located
          |      tables:
          |        - name: "*" # Takes all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |        - "VIEW"
          |        - "SYSTEM TABLE"
          |        - "GLOBAL TEMPORARY"
          |        - "LOCAL TEMPORARY"
          |        - "ALIAS"
          |        - "SYNONYM"
          |  #  templateFile: "domain-template.yml" # Metadata to use for the generated YML file.
          |""".stripMargin
      val tmpYmlFile = File.newTemporaryFile("extract", ".sl.yml")
      val tmpDir = File.newTemporaryDirectory("extract")
      tmpYmlFile.write(config)
      val schemaHandler = settings.schemaHandler()
      val result = new ExtractDataJob(schemaHandler).run(
        Array(
          "--clean",
          "--config",
          tmpYmlFile.pathAsString,
          "--outputDir",
          tmpDir.pathAsString
        )
      )
      result should be a Symbol("success")
      val files = tmpDir.listRecursively.filter(_.name.endsWith(".csv")).toList
      assert(files.size == 2)
    }
    "Extract Schema" should "succeed" in {
      val jdbcOptions = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcOptions.options("url"),
        jdbcOptions.options("user"),
        jdbcOptions.options("password")
      )
      val sqls: String =
        """
          |drop view if exists test_view1;
          |drop table if exists test_table1 cascade;
          |create table test_table1(ID INT PRIMARY KEY,NAME VARCHAR(500));
          |create view test_view1 AS SELECT NAME FROM test_table1;
          |insert into test_table1 values (1,'A');
          |CREATE TABLE IF NOT EXISTS audit.SL_LAST_EXPORT (
          |                              domain VARCHAR(255) not NULL,
          |                              schema VARCHAR(255) not NULL,
          |                              last_ts TIMESTAMP,
          |                              last_date DATE,
          |                              last_long INTEGER,
          |                              last_decimal DECIMAL,
          |                              start_ts TIMESTAMP not NULL,
          |                              end_ts TIMESTAMP not NULL,
          |                              duration INTEGER not NULL,
          |                              mode VARCHAR(255) not NULL,
          |                              count BIGINT not NULL,
          |                              success BOOLEAN not NULL,
          |                              message VARCHAR(255) not NULL,
          |                              step VARCHAR(255) not NULL
          |                             );""".stripMargin
      val st = conn.createStatement()
      sqls.split(";").foreach { sql =>
        st.executeUpdate(sql)
      }
      st.close()

      val config =
        """
          |version: 1
          |extract:
          |  connectionRef: "test-pg" # Connection name as defined in the connections section of the application.conf file
          |  jdbcSchemas:
          |    - schema: "PUBLIC" # Database schema where tables are located
          |      tables:
          |        - name: "*" # Takes all tables
          |      tableTypes: # One or many of the types below
          |        - "TABLE"
          |        - "VIEW"
          |        - "SYSTEM TABLE"
          |        - "GLOBAL TEMPORARY"
          |        - "LOCAL TEMPORARY"
          |        - "ALIAS"
          |        - "SYNONYM"
          |""".stripMargin
      val tmpYmlFile = File.newTemporaryFile("extract", ".sl.yml")
      val tmpDir = File.newTemporaryDirectory("extract")
      println(tmpDir.pathAsString)
      tmpYmlFile.write(config)
      val schemaHandler = settings.schemaHandler()
      new ExtractSchema(schemaHandler).run(
        Array(
          "--config",
          tmpYmlFile.pathAsString,
          "--outputDir",
          tmpDir.pathAsString
        )
      )
      val files =
        tmpDir.listRecursively.filter(_.name.endsWith(".sl.yml")).map(_.name.toLowerCase()).toList
      files should contain theSameElementsAs List(
        "test_table1.sl.yml",
        "test_view1.sl.yml",
        "_config.sl.yml"
      )
    }

  }
}
