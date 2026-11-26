package ai.starlake.job.transform

import ai.starlake.TestHelper

class AuditTableDdlSpec extends TestHelper {

  "AuditTableDdl.columns" should "read the name and the type of each column, without NOT NULL" in {
    val ddl =
      """CREATE TABLE IF NOT EXISTS {{table}} (
        |  JOBID TEXT NOT NULL,
        |  PATHS varchar(max) NOT NULL,
        |  AMOUNT DECIMAL(65,30),
        |  RATIO DOUBLE PRECISION NULL,
        |  SCHEDULED_DATE TIMESTAMP NOT NULL
        |) USING {{writeFormat}}
        |""".stripMargin
    AuditTableDdl.columns(ddl) shouldBe List(
      "JOBID"          -> "TEXT",
      "PATHS"          -> "varchar(max)",
      "AMOUNT"         -> "DECIMAL(65,30)",
      "RATIO"          -> "DOUBLE PRECISION",
      "SCHEDULED_DATE" -> "TIMESTAMP"
    )
  }

  it should "return no column for a statement it cannot read" in {
    AuditTableDdl.columns("SELECT 1") shouldBe Nil
  }

  "AuditTableDdl.missing" should "compare the names without their case" in {
    val ddl = List("JOBID" -> "TEXT", "SCHEDULED_DATE" -> "TIMESTAMP")
    AuditTableDdl.missing(ddl, List("jobid")) shouldBe List("SCHEDULED_DATE" -> "TIMESTAMP")
    AuditTableDdl.missing(ddl, List("JobId", "scheduled_date")) shouldBe Nil
  }

  it should "add nothing to a table that has none of the DDL columns" in {
    // not an audit table of an older Starflow: its writes keep failing
    AuditTableDdl.missing(List("JOBID" -> "TEXT"), List("not_an_audit_column")) shouldBe Nil
  }

  it should "add nothing when the table columns are unknown" in {
    AuditTableDdl.missing(List("JOBID" -> "TEXT"), Nil) shouldBe Nil
  }

  "AuditTableDdl.addColumnSql" should "use the ALTER TABLE syntax of each engine" in {
    AuditTableDdl.addColumnSql("spark", "audit.audit", "SCHEDULED_DATE", "TIMESTAMP") shouldBe
    "ALTER TABLE audit.audit ADD COLUMNS (SCHEDULED_DATE TIMESTAMP)"
    AuditTableDdl.addColumnSql("sqlserver", "audit.audit", "SCHEDULED_DATE", "DATETIME") shouldBe
    "ALTER TABLE audit.audit ADD SCHEDULED_DATE DATETIME"
    AuditTableDdl.addColumnSql("redshift", "audit.audit", "SCHEDULED_DATE", "TIMESTAMP") shouldBe
    "ALTER TABLE audit.audit ADD COLUMN SCHEDULED_DATE TIMESTAMP"
  }

  "AuditTableDdl.evolve" should "try a table once, even when its ALTER TABLE fails" in {
    AuditTableDdl.forgetChecked()
    val ddl = "CREATE TABLE IF NOT EXISTS {{table}} (JOBID TEXT, SCHEDULED_DATE TIMESTAMP)"
    var runs = 0
    val failing: String => Unit = { _ =>
      runs += 1
      throw new IllegalStateException("permission denied")
    }
    an[IllegalStateException] should be thrownBy
    AuditTableDdl.evolve("cnx|audit.audit", "postgresql", "audit.audit", ddl, List("jobid"))(
      failing
    )
    AuditTableDdl.evolve("cnx|audit.audit", "postgresql", "audit.audit", ddl, List("jobid"))(
      failing
    ) shouldBe Nil
    runs shouldBe 1
    AuditTableDdl.forgetChecked()
  }

  it should "check each connection and table separately" in {
    AuditTableDdl.forgetChecked()
    val ddl = "CREATE TABLE IF NOT EXISTS {{table}} (JOBID TEXT, SCHEDULED_DATE TIMESTAMP)"
    val statements = List.newBuilder[String]
    def evolve(key: String) =
      AuditTableDdl.evolve(key, "duckdb", "audit.audit", ddl, List("jobid"))(statements += _)
    evolve("a|audit.audit") shouldBe List("SCHEDULED_DATE")
    evolve("a|audit.audit") shouldBe Nil
    evolve("b|audit.audit") shouldBe List("SCHEDULED_DATE")
    statements.result() shouldBe List.fill(2)(
      "ALTER TABLE audit.audit ADD COLUMN SCHEDULED_DATE TIMESTAMP"
    )
    AuditTableDdl.forgetChecked()
  }

  new WithSettings() {
    "Every audit table DDL of every engine" should "be readable" in {
      val engines = settings.appConfig.jdbcEngines
      engines should not be empty
      for {
        (engineName, engine) <- engines
        (tableName, ddl)     <- engine.tables
      } withClue(s"$engineName.$tableName: ") {
        val columns = AuditTableDdl.columns(ddl.createSql)
        columns should not be empty
        all(columns.map(_._1)) should fullyMatch regex "[A-Za-z_][A-Za-z0-9_]*"
        all(columns.map(_._2)) should not be empty
      }
    }

    it should "include SCHEDULED_DATE in the audit table of every engine" in {
      for (
        (engineName, engine) <- settings.appConfig.jdbcEngines; ddl <- engine.tables.get("audit")
      )
        withClue(s"$engineName: ") {
          AuditTableDdl.columns(ddl.createSql).map(_._1.toUpperCase) should contain(
            "SCHEDULED_DATE"
          )
        }
    }
  }
}
