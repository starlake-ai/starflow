package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.job.transform.ImplicitCommits.Checkable

import java.util.Locale

/** A JDBC write checks its expectations before its commit only when its rollback undoes it: a
  * statement of the write or of its postsql that commits on its own prevents it, and so do the
  * presql and postsql statements on an engine whose statements may commit on their own (#1976).
  */
class ImplicitCommitsSpec extends TestHelper {

  /** The engines of the reference configuration */
  private lazy val engines: Map[String, JdbcEngine] =
    new WithSettings() {}.settings.appConfig.jdbcEngines

  private def implicitCommits(engine: String): Option[List[String]] =
    engines(engine).implicitCommits

  /** Plans a write on an engine of the reference configuration */
  private def plan(
    engine: String,
    main: List[String],
    presql: List[String] = Nil,
    alters: List[String] = Nil,
    postsql: List[String] = Nil,
    createsTable: Boolean = false,
    createdBeforeWrite: Boolean = false,
    autocommitBeforeMain: Boolean = false,
    generated: Boolean = true,
    table: String = "dom.t"
  ): Either[String, Checkable] =
    ImplicitCommits.plan(
      engine = engine,
      implicitCommits = implicitCommits(engine),
      autocommitBeforeMain = autocommitBeforeMain,
      presql = presql,
      beforeMain = alters,
      main = main,
      postsql = postsql,
      table = table,
      createsTable = createsTable,
      createdBeforeWrite = createdBeforeWrite,
      generated = generated
    )

  private def checkable(
    dropsCreatedTable: Boolean = false,
    presqlCommitted: Boolean = false,
    altersCommitted: Boolean = false
  ): Either[String, Checkable] =
    Right(Checkable(dropsCreatedTable, presqlCommitted, altersCommitted))

  private def commits(
    sql: String,
    implicitCommits: List[String] = Nil,
    engine: String = ""
  ): Boolean =
    ImplicitCommits.commitsOnItsOwn(sql, implicitCommits, engine)

  private val truncate = List("TRUNCATE")
  private val overwrite = List("TRUNCATE TABLE dom.t", "INSERT INTO dom.t(id) SELECT 1")
  private val insert = List("INSERT INTO dom.t(id) SELECT 1")

  "A statement" should "commit on its own when it starts with a declared keyword or ends the transaction" in {
    commits("truncate table dom.t", truncate) shouldBe true
    commits("-- refresh\n/* all */  TRUNCATE\n TABLE dom.t;", truncate) shouldBe true
    commits("TRUNCATE;", truncate) shouldBe true
    commits("create  table x(id int)", List("CREATE TABLE")) shouldBe true
    commits("commit") shouldBe true
    commits("START TRANSACTION") shouldBe true
    commits("PREPARE TRANSACTION 'x'") shouldBe true
    // Only the leading keyword counts
    commits("CREATED_AT_UPDATE dom.t", List("CREATE")) shouldBe false
    commits("INSERT INTO dom.t SELECT 'TRUNCATE'", truncate) shouldBe false
    commits("-- TRUNCATE\nDELETE FROM dom.t", truncate) shouldBe false
  }

  it should "commit on its own when it turns autocommit on, whatever its form" in {
    commits("SET autocommit=1") shouldBe true
    commits("SET SESSION autocommit = 1") shouldBe true
    commits("set @@session.autocommit = ON") shouldBe true
    commits("SET sql_mode = '', autocommit = 1") shouldBe true
    commits("SET search_path = dom") shouldBe false
  }

  it should "be found among the statements of an entry, outside its quotes and comments" in {
    commits("UPDATE s SET n = n + 1; COMMIT") shouldBe true
    commits("DELETE FROM s;TRUNCATE TABLE s2", truncate) shouldBe true
    commits("INSERT INTO t SELECT ';TRUNCATE t'", truncate) shouldBe false
    commits("INSERT INTO t SELECT 'a''b;TRUNCATE t'", truncate) shouldBe false
    commits("INSERT INTO \"a;truncate\" SELECT 1", truncate) shouldBe false
    commits("INSERT INTO t SELECT 1 -- ; TRUNCATE t", truncate) shouldBe false
    commits("/* ; TRUNCATE t */ INSERT INTO t SELECT 1", truncate) shouldBe false
  }

  "The statements of an entry" should "be read with the quote and comment rules of PostgreSQL" in {
    commits("SELECT E'\\'' , '\\' ; COMMIT ; SELECT '\\' , E'\\''", engine = "postgresql") shouldBe
    true
    // A string that ends with a backslash does not end without standard_conforming_strings: the
    // check runs after the write
    commits("UPDATE t SET p = replace(p, '\\', '/')", engine = "postgresql") shouldBe true
    commits("UPDATE t SET p = replace(p, 'a', '/')", engine = "postgresql") shouldBe false
    // An executable comment is a comment
    commits(
      "/*! don't */ UPDATE s SET n = 1; COMMIT; /* that's all */",
      engine = "postgresql"
    ) shouldBe
    true
    commits("SELECT $$it's$$; COMMIT", engine = "postgresql") shouldBe true
    commits("SELECT $body$ a; b $body$; COMMIT", engine = "postgresql") shouldBe true
    commits("/* a /* b */ c */ COMMIT", engine = "postgresql") shouldBe true
    commits("INSERT INTO t VALUES (1) -- note\r; COMMIT", engine = "postgresql") shouldBe true
    // A procedure body is one statement
    commits("DO $$ BEGIN INSERT INTO t VALUES (1); END $$", engine = "postgresql") shouldBe false
  }

  it should "be read with the backslash escapes of PostgreSQL without standard_conforming_strings, and of Redshift" in {
    commits("SELECT 'a\\' , ' ; COMMIT ; SELECT 'b\\' , '", engine = "postgresql") shouldBe true
    commits("SELECT 'it\\'s'; TRUNCATE t", truncate, "redshift") shouldBe true
  }

  it should "be read every way when its engine is not known" in {
    commits("# purge\nTRUNCATE TABLE t", truncate) shouldBe true
    commits("// grant access\nGRANT SELECT ON t TO ROLE r", List("GRANT")) shouldBe true
    commits("INSERT INTO [dom].[it's] SELECT 1; EXEC dom.proc; SELECT ']'", List("EXEC")) shouldBe
    true
  }

  it should "be read with the quote and comment rules of MySQL" in {
    commits("INSERT INTO log VALUES ('it\\'s'); TRUNCATE t", truncate, "mysql") shouldBe true
    commits("SELECT 1 AS $a$; TRUNCATE TABLE t; SELECT 2 AS $a$", truncate, "mysql") shouldBe true
    commits("UPDATE t SET a = a --1; TRUNCATE t2", truncate, "mysql") shouldBe true
    commits("# purge\nTRUNCATE TABLE t", truncate, "mysql") shouldBe true
    commits("UPDATE t SET a = 1 # don't\n; TRUNCATE t2", truncate, "mysql") shouldBe true
    // A carriage return does not end a MySQL comment
    commits("SELECT 1 -- x\r'\n; TRUNCATE t; SELECT 'z' -- '\n", truncate, "mysql") shouldBe true
    // MySQL and MariaDB run the content of an executable comment
    commits("/*!40101 TRUNCATE TABLE t */", truncate, "mysql") shouldBe true
    commits("/*M!100100 TRUNCATE TABLE t */", truncate, "mariadb") shouldBe true
  }

  it should "be read with the quote and comment rules of Snowflake, SQL Server and H2" in {
    commits(
      "INSERT INTO dom.log VALUES ('it\\'s');\n--don't\nCREATE VIEW dom.v AS SELECT 1",
      List("CREATE"),
      "snowflake"
    ) shouldBe true
    commits(
      "// grant access\nGRANT SELECT ON t TO ROLE r",
      List("GRANT"),
      "snowflake"
    ) shouldBe true
    commits(
      "INSERT INTO [dom].[it's] SELECT 1; EXEC dom.proc; INSERT INTO [dom].[o'clock] SELECT 1",
      List("EXEC"),
      "sqlserver"
    ) shouldBe true
    commits(
      "SELECT 1 // '\n, 'a\\' ; GRANT SELECT ON dom.t TO u ; SELECT 1 // '\n",
      List("GRANT"),
      "h2"
    ) shouldBe true
    // The JDBC escape of a procedure call
    commits("{call dom.refresh()}", List("CALL"), "sqlserver") shouldBe true
    commits("{? = call dom.refresh()}", List("CALL")) shouldBe true
  }

  it should "be taken as committing when a quote or a comment does not end" in {
    commits("INSERT INTO t VALUES ('C:\\'); INSERT INTO u VALUES (1)", truncate, "mysql") shouldBe
    true
    commits("INSERT INTO t VALUES ('unended", truncate) shouldBe true
    commits("INSERT INTO t VALUES (1) /* unended", truncate) shouldBe true
  }

  it should "not depend on the default locale" in {
    val default = Locale.getDefault
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"))
      commits("commit") shouldBe true
      commits("optimize table t", List("optimize")) shouldBe true
    } finally Locale.setDefault(default)
  }

  "An engine that does not declare its implicit commits" should "check after the write" in {
    plan("spark", overwrite).left.toOption.get should include("jdbcEngines.spark.implicitCommits")
  }

  "An engine whose statements stay in the transaction" should "check before the commit, its presql and postsql included" in {
    plan(
      "postgresql",
      overwrite,
      presql = List("DELETE FROM dom.t WHERE id = 1"),
      postsql = List("ALTER TABLE dom.t ADD COLUMN c INT")
    ) shouldBe checkable()
    plan("postgresql", List("CREATE TABLE dom.t AS SELECT 1 AS id"), createsTable = true) shouldBe
    checkable()
  }

  it should "check after the write when the postsql ends the transaction" in {
    plan("postgresql", overwrite, postsql = List("COMMIT")).left.toOption.get should include(
      "its postsql statement `COMMIT` commits on its own on postgresql"
    )
    plan("postgresql", overwrite, postsql = List("UPDATE s SET n = n + 1; COMMIT")).isLeft shouldBe
    true
  }

  it should "check after the write when a quote of its statements does not end" in {
    plan(
      "postgresql",
      overwrite,
      postsql = List("UPDATE s SET a = 'unended")
    ).left.toOption.get should
    include("does not end, as PostgreSQL reads it")
  }

  "An engine whose statements may commit on their own" should "check after the write when the task has presql or postsql" in {
    plan(
      "snowflake",
      insert,
      presql = List("DELETE FROM dom.t WHERE id = 1")
    ).left.toOption.get should
    include("its presql or postsql statements are not analysed on snowflake")
    plan("snowflake", insert, postsql = List("UPDATE dom.s SET n = n + 1")).isLeft shouldBe true
    plan("sqlserver", insert, postsql = List("IF @@TRANCOUNT > 0 COMMIT")).isLeft shouldBe true
    // A comment is not a statement
    plan("snowflake", insert, presql = List("-- nothing")) shouldBe checkable()
  }

  it should "check after the write when its SQL is not generated by its write strategy" in {
    plan("snowflake", insert, generated = false).left.toOption.get should include("parseSQL: false")
    plan(
      "sqlserver",
      List("DELETE FROM dbo.t\nEXEC dbo.refresh"),
      generated = false
    ).isLeft shouldBe
    true
    // Read on PostgreSQL for the statements that end the transaction
    plan("postgresql", insert, generated = false) shouldBe checkable()
    plan(
      "postgresql",
      List("INSERT INTO dom.t SELECT 1; COMMIT"),
      generated = false
    ).isLeft shouldBe
    true
  }

  "An OVERWRITE whose TRUNCATE commits on its own" should "check after the write" in {
    plan("mysql", overwrite).left.toOption.get should include(
      "its statement `TRUNCATE TABLE dom.t` commits on its own on mysql"
    )
    plan("redshift", overwrite).isLeft shouldBe true
    plan("h2", overwrite).isLeft shouldBe true
    // Even on a line that holds the INSERT too
    plan("mysql", List("INSERT INTO dom.t(id) SELECT 1; TRUNCATE TABLE dom.u")).isLeft shouldBe true
  }

  "A write of DML statements on Snowflake" should "check before the commit" in {
    plan("snowflake", overwrite) shouldBe checkable()
    plan("snowflake", List("MERGE INTO dom.t USING (SELECT 1 AS id) s ON t.id = s.id")) shouldBe
    checkable()
  }

  "A write that replaces its table with a DDL statement" should "check after the write" in {
    plan("snowflake", List("CREATE OR REPLACE TABLE dom.t AS SELECT 1 AS id")).isLeft shouldBe true
    plan(
      "snowflake",
      List("INSERT INTO dom.t SELECT 1", "CREATE TEMPORARY TABLE dom.x AS SELECT 1 AS id")
    ).isLeft shouldBe true
  }

  "A write that starts with the creation of a temporary table" should "check before the commit" in {
    val main = List(
      "CREATE OR REPLACE TEMPORARY TABLE dom.incoming AS SELECT id FROM dom.t WHERE FALSE",
      "INSERT INTO dom.incoming(id) SELECT 1",
      "DELETE FROM dom.t WHERE id IN (SELECT id FROM dom.incoming)",
      "INSERT INTO dom.t(id) SELECT id FROM dom.incoming"
    )
    plan("snowflake", main) shouldBe checkable()
    plan("snowflake", main, alters = List("ALTER TABLE dom.t ADD COLUMN c INT")) shouldBe
    checkable(altersCommitted = true)
  }

  "The first run of a write whose CREATE TABLE commits on its own" should "drop the table on failure" in {
    val create = List(
      "CREATE TABLE dom.t AS SELECT 1 AS id",
      "ALTER TABLE dom.t ADD COLUMN IF NOT EXISTS start_ts TIMESTAMP"
    )
    plan("snowflake", create, createsTable = true) shouldBe checkable(dropsCreatedTable = true)
    plan("mysql", List("CREATE TABLE `dom`.`t` AS SELECT 1 AS id"), createsTable = true) shouldBe
    checkable(dropsCreatedTable = true)
    // Snowflake folds the names that are not quoted to upper case, PostgreSQL to lower case
    plan(
      "snowflake",
      List("CREATE TABLE \"DOM\".\"T\" AS SELECT 1 AS id"),
      createsTable = true
    ) shouldBe checkable(dropsCreatedTable = true)
    plan("h2", List("CREATE TABLE \"DOM\".\"T\" AS SELECT 1 AS id"), createsTable = true) shouldBe
    checkable(dropsCreatedTable = true)
    // An ALTER alone does not tell that the run created the table
    plan("snowflake", List("ALTER TABLE dom.t ADD COLUMN c INT"), createsTable = true) shouldBe
    checkable()
  }

  it should "check after the write when a statement that commits on its own may touch another table" in {
    def created(main: String*): Either[String, Checkable] =
      plan("snowflake", main.toList, createsTable = true)
    // Another table, or a name that may be another table
    created("CREATE TABLE dom.t AS SELECT 1 AS id", "DROP TABLE dom.u").isLeft shouldBe true
    created("CREATE TABLE t AS SELECT 1 AS id").isLeft shouldBe true
    created("CREATE TABLE otherdb.dom.t AS SELECT 1 AS id").isLeft shouldBe true
    created("CREATE TABLE \"dom\".\"t\" AS SELECT 1 AS id").isLeft shouldBe true
    plan(
      "h2",
      List("CREATE TABLE \"dom\".\"t\" AS SELECT 1 AS id"),
      createsTable = true
    ).isLeft shouldBe
    true
    // MySQL compares the names as they are written
    plan("mysql", List("CREATE TABLE dom.T AS SELECT 1 AS id"), createsTable = true).isLeft shouldBe
    true
    // A table whose name cannot be read
    plan(
      "snowflake",
      List("CREATE TABLE dom.my AS SELECT 1 AS id"),
      createsTable = true,
      table = "dom.my-table"
    ).isLeft shouldBe true
    created("CREATE TABLE \"dom.t\" AS SELECT 1 AS id").isLeft shouldBe true
    // A creation that does not tell that the run created the table
    created("CREATE TABLE IF NOT EXISTS dom.t AS SELECT 1 AS id").isLeft shouldBe true
    created("CREATE OR REPLACE TABLE dom.t AS SELECT 1 AS id").isLeft shouldBe true
    // An ALTER that moves the content of another table
    created(
      "CREATE TABLE dom.t AS SELECT 1 AS id",
      "ALTER TABLE dom.t SWAP WITH dom.x"
    ).isLeft shouldBe
    true
    created(
      "CREATE TABLE dom.t AS SELECT 1 AS id",
      "ALTER TABLE dom.t RENAME TO dom.x"
    ).isLeft shouldBe
    true
    plan(
      "mysql",
      List(
        "CREATE TABLE dom.t AS SELECT 1 AS id",
        "ALTER TABLE dom.t ADD COLUMN c DECIMAL(10, 2), RENAME TO dom.x"
      ),
      createsTable = true
    ).isLeft shouldBe true
    // A column whose type or default holds a comma is one change
    plan(
      "mysql",
      List(
        "CREATE TABLE dom.t AS SELECT 1 AS id",
        "ALTER TABLE dom.t ADD COLUMN c DECIMAL(10, 2) DEFAULT 'a,b'"
      ),
      createsTable = true
    ) shouldBe checkable(dropsCreatedTable = true)
    // A creation that commits a write to another table
    plan(
      "mysql",
      List("INSERT INTO dom.other SELECT 1", "CREATE TABLE dom.t AS SELECT 1 AS id"),
      createsTable = true
    ).isLeft shouldBe true
  }

  "The first run of a write whose table is created for its presql" should "drop the table on failure" in {
    plan(
      "postgresql",
      overwrite,
      presql = List("DELETE FROM dom.t"),
      createdBeforeWrite = true
    ) shouldBe
    checkable(dropsCreatedTable = true)
    plan(
      "duckdb",
      overwrite,
      presql = List("DELETE FROM dom.t"),
      createdBeforeWrite = true,
      autocommitBeforeMain = true
    ) shouldBe checkable(dropsCreatedTable = true, presqlCommitted = true)
  }

  "The presql and ALTER statements" should "be committed when they run before the transaction, or a statement commits them" in {
    plan("postgresql", overwrite, presql = List("DELETE FROM dom.t WHERE id = 1")) shouldBe
    checkable()
    plan(
      "duckdb",
      overwrite,
      presql = List("DELETE FROM dom.t WHERE id = 1"),
      alters = List("ALTER TABLE dom.t ADD COLUMN c INT"),
      autocommitBeforeMain = true
    ) shouldBe checkable(presqlCommitted = true, altersCommitted = true)
    plan("postgresql", overwrite, alters = List("ALTER TABLE dom.t ADD COLUMN c INT")) shouldBe
    checkable()
    plan("mysql", insert, alters = List("ALTER TABLE dom.t ADD COLUMN c INT")) shouldBe
    checkable(altersCommitted = true)
    // Blank statements count for nothing
    plan("postgresql", overwrite, presql = List(" ", "-- nothing")) shouldBe checkable()
  }

  "The reference configuration" should "declare the implicit commits of the JDBC engines" in {
    implicitCommits("postgresql") shouldBe Some(Nil)
    implicitCommits("duckdb") shouldBe Some(Nil)
    implicitCommits("sqlserver") shouldBe Some(List("EXEC", "EXECUTE", "CALL"))
    implicitCommits("redshift") shouldBe Some(List("TRUNCATE", "CALL"))
    // TRUNCATE is a DML statement on Snowflake
    implicitCommits("snowflake").get should
    (contain allOf ("CREATE", "CALL", "EXECUTE") and not contain "TRUNCATE")
    for (engine <- List("mysql", "mariadb", "h2"))
      withClue(engine) {
        implicitCommits(engine).get should contain allOf ("TRUNCATE", "CREATE", "CALL", "EXECUTE")
      }
    implicitCommits("h2").get should contain allOf ("SET", "CHECKPOINT", "RUNSCRIPT", "SHUTDOWN")
    implicitCommits("spark") shouldBe None
  }

  it should "find the statements that commit on their own on MySQL" in {
    for (
      statement <- List(
        "SET PASSWORD = 'secret'",
        "CACHE INDEX dom.t IN hot",
        "LOAD INDEX INTO CACHE dom.t",
        "CHECK TABLE dom.t",
        "XA START 'x'",
        "START GROUP_REPLICATION",
        "STOP REPLICA",
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST = 'h'",
        "CALL dom.refresh()"
      )
    )
      withClue(statement) {
        plan("mysql", insert :+ statement).isLeft shouldBe true
      }
  }
}
