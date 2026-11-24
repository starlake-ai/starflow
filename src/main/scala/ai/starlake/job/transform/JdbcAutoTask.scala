package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractSchemaCmd, ExtractSchemaConfig, JdbcDbUtils}
import ai.starlake.job.metrics.{
  ExpectationAssertionHandler,
  ExpectationReport,
  JdbcExpectationAssertionHandler,
  SQLConnectionExpectationAssertionHandler
}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{
  AccessControlEntry,
  AutoTaskInfo,
  Engine,
  ExpectationItem,
  Materialization,
  TableSync,
  WriteStrategy,
  WriteStrategyType
}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, SparkUtils, StarlakeNotFoundException, Utils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, Timestamp}
import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/** JdbcAutoTask executes SQL transformations natively on JDBC databases.
  *
  * ==Overview==
  * This task type executes SQL directly on any JDBC-compatible database, without requiring a Spark
  * cluster. Supported databases include:
  *   - PostgreSQL, MySQL, MariaDB
  *   - Oracle, SQL Server
  *   - Snowflake, Redshift
  *   - DuckDB (embedded analytics)
  *
  * ==Key Features==
  *   - Native JDBC SQL execution (no Spark overhead)
  *   - Transaction support with commit/rollback
  *   - Connection pooling via HikariCP
  *   - Schema synchronization with YAML definitions
  *   - Support for all write strategies (APPEND, OVERWRITE, MERGE, SCD2)
  *
  * ==Connection Reuse==
  * The `conn` parameter allows reusing an existing JDBC connection, which is useful for:
  *   - Transaction management across multiple tasks
  *   - Connection pooling optimization
  *   - Native loader integration (Snowflake, DuckDB)
  *
  * ==Write Strategies==
  *   - '''APPEND''': INSERT INTO ... SELECT ...
  *   - '''OVERWRITE''': TRUNCATE + INSERT or DROP + CREATE
  *   - '''MERGE''': MERGE INTO ... USING ... (database-specific syntax)
  *   - '''SCD2''': Slowly Changing Dimension Type 2 with start/end timestamps
  *
  * @param conn
  *   Optional existing JDBC connection to reuse (for transactions)
  */
class JdbcAutoTask(
  appId: Option[String],
  taskDesc: AutoTaskInfo,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int,
  resultPageNumber: Int,
  conn: Option[java.sql.Connection],
  scheduledDate: Option[String],
  syncSchema: Boolean
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
      truncate,
      resultPageSize,
      resultPageNumber,
      accessToken,
      conn,
      scheduledDate,
      syncSchema
    ) {

  def applyJdbcAcl(connection: Connection, forceApply: Boolean): Try[Unit] =
    Try(aclSQL(forceApply)).flatMap(AccessControlEntry.applyJdbcAcl(connection, _, forceApply))

  def applyJdbcAcl(jdbcConnection: Settings.ConnectionInfo, forceApply: Boolean): Try[Unit] =
    Try(aclSQL(forceApply)).flatMap(AccessControlEntry.applyJdbcAcl(jdbcConnection, _, forceApply))

  override def run(): Try[JobResult] = {
    runJDBC(None, this.conn)
  }

  override def tableExists: Boolean = {
    val exists =
      JdbcDbUtils.withJDBCConnection(
        this.schemaHandler.dataBranch(),
        sinkConnection.options
      ) { conn =>
        val url = sinkConnection.options("url")
        val exists = JdbcDbUtils.tableExists(
          conn,
          url,
          fullTableName,
          sinkConnection.getJdbcEngineName().toString
        )
        exists
      }
    if (!exists && taskDesc._auditTableName.isDefined)
      createAuditTable() // We are sinking to an audit table. We need to create it first in JDBC
    else
      exists
  }

  @throws[Exception]
  def createAuditTable(): Boolean = {
    // Table not found and it is an table in the audit schema defined in the reference-connections.conf file  Try to create it.
    JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
      conn =>
        JdbcDbUtils.withCatalogCreationLock {
          auditTableCreateSQL().foreach { sql =>
            JdbcDbUtils.executeUpdate(sql, conn) match {
              case Success(_) =>
              case Failure(e) =>
                logger.error(s"Error executing $sql", e)
                throw e
            }
          }
        }
        true
    }
  }

  def addSCD2Columns(connection: Connection, engineName: Engine): Unit = {
    val alterTableSqls = buildAddSCD2ColumnsSqls(engineName)
    // ignore errors if columns already exist
    Try(runSqls(connection, alterTableSqls, "addSCE2Columns"))
  }

  override protected lazy val sinkConnection: Settings.ConnectionInfo = {
    if (interactive.isDefined) {
      settings.appConfig.connections(sinkConnectionRef).withAccessToken(accessToken)
    } else {
      settings.appConfig.connections(sinkConnectionRef).withAccessToken(accessToken)
    }
  }

  /** Main JDBC execution method.
    *
    * ==Execution Modes==
    *   1. '''Native Mode''' (df = None): Builds and executes SQL entirely in JDBC 2. '''Spark
    *      Mode''' (df = Some): Uses Spark DataFrame, writes via JDBC connector
    *
    * ==Processing Flow==
    *   1. Create schema if it doesn't exist (optional) 2. Build SQL statements (ALTER + main SQL)
    *      3. For interactive: Execute query and return paginated results 4. For batch:
    *      a. Disable auto-commit for transaction support b. Execute pre-actions (engine-specific
    *         setup) c. Execute pre-SQL statements d. Write DataFrame or execute main SQL e. Execute
    *         post-SQL statements f. Commit transaction (or rollback on failure)
    *
    * ==Transaction Handling==
    * All batch operations run within a transaction. If any step fails, the entire transaction is
    * rolled back to maintain data consistency.
    *
    * @param df
    *   Optional Spark DataFrame (None for native JDBC execution)
    * @param sqlConnection
    *   Optional existing connection to reuse
    */
  /** The target table's actual column names, or Nil when the table cannot be described (absent
    * table, closed connection, ...). Used only to enrich error messages; never throws.
    */
  private def jdbcTargetColumns(conn: java.sql.Connection): List[String] =
    Try {
      val stmt = conn.prepareStatement(s"SELECT * FROM $fullTableName WHERE 1=0")
      try {
        val metaData = stmt.executeQuery().getMetaData
        (1 to metaData.getColumnCount).map(metaData.getColumnName).toList
      } finally stmt.close()
    }.getOrElse(Nil)

  def runJDBC(
    df: Option[DataFrame],
    sqlConnection: Option[java.sql.Connection] = None
  ): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())
    Try(captureIncrementalState())

    // Create schema if configured and not in interactive mode
    if (interactive.isEmpty && settings.appConfig.createSchemaIfNotExists) {
      // Creating a schema requires its own connection if called before a Spark save
      JdbcDbUtils.withJDBCConnection(
        this.schemaHandler.dataBranch(),
        sinkConnection.options,
        sqlConnection
      ) { conn =>
        JdbcDbUtils.createSchema(conn, fullDomainName)
      }
    }

    val res = Try {
      val createdForPresql =
        if (
          df.isEmpty && interactive.isEmpty &&
          taskDesc.presql.nonEmpty && taskDesc.parseSQL.getOrElse(true)
        ) createEmptyTargetForPresql(sqlConnection)
        else false
      // Whether the table exists before the write, which creates it otherwise: the expectations
      // checked before the commit drop a table that the run created, when its creation committed
      // (#1976)
      val targetExistsBeforeWrite: Option[Boolean] =
        if (
          df.isEmpty && interactive.isEmpty && expectationsBeforeCommit.nonEmpty &&
          taskDesc.parseSQL.getOrElse(true)
        ) Some(tableExists)
        else None
      val writesTable =
        !Set[Materialization](Materialization.VIEW, Materialization.MATERIALIZED_VIEW)
          .contains(resolveMaterializedView())
      val createsTable = writesTable && targetExistsBeforeWrite.contains(false)
      val createdBeforeWrite = writesTable && createdForPresql
      val (alters, mainSql) =
        if (df.isEmpty) {
          buildAllSQLQueries(
            None,
            tableExistsForcedValue = targetExistsBeforeWrite,
            // the target this run created for presql holds no earlier rows
            incrementalForcedValue = if (createdForPresql) Some(false) else None
          )
        } else {
          val sql = mainSqlSource
          val mainSql = schemaHandler.substituteRefTaskMainSQL(
            sql,
            taskDesc.getRunConnection(),
            sqlVars()
          )
          (None, mainSql)
        }

      interactive match {
        case Some(_) =>
          JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkOptions,
            sqlConnection
          ) { conn =>
            runInteractive(conn, mainSql)
          }
        case None =>
          val parsedPreActions =
            Utils.parseJinja(
              jdbcSinkEngine.preActions.getOrElse(""),
              Map("schema" -> taskDesc.domain)
            )
          // The expectations that the write checked before its commit, with their reports
          val checkedBeforeCommit: List[(ExpectationItem, ExpectationReport)] = df match {
            case Some(loadedDF) =>
              val tblExists = this.tableExists
              JdbcDbUtils.withJDBCConnection(
                this.schemaHandler.dataBranch(),
                sinkOptions,
                sqlConnection
              ) { conn =>
                Try {
                  conn.setAutoCommit(false)
                  runPreActions(conn, parsedPreActions.splitSql(";"))
                  runSqls(conn, preSql, "Pre")
                  logger.info(s"Writing dataframe to $fullTableName")
                  val saveMode = writeStrategy.toWriteMode().toSaveMode
                  if (saveMode == SaveMode.Overwrite || truncate) {
                    val jdbcUrl = sinkConnection.options("url")
                    // val dialect = SparkUtils.dialect(jdbcUrl)
                    // We always append to the table to keep the schema (Spark loose the schema otherwise). We truncate using the truncate query option
                    if (tblExists)
                      JdbcDbUtils.truncateTable(conn, fullTableName)
                  }
                } match {
                  case Success(_) =>
                    runSqls(conn, postSql, "Post")
                    conn.commit()
                  case Failure(e) =>
                    JdbcDbUtils.rollbackQuietly(conn)
                    throw e
                }
              }
              val timestamp = java.time.LocalDateTime
                .now()
                .format(
                  java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                )
              val tablePath =
                new Path(s"${settings.appConfig.datasets}/tmp/${fullTableName}_$timestamp")

              if (settings.storageHandler().exists(tablePath)) {
                settings.storageHandler().delete(tablePath)
              }
              if (sinkConnection.isDuckDb()) {
                val colNames = loadedDF.schema.fields.map(_.name)
                loadedDF.write
                  .format("parquet")
                  .mode(SaveMode.Overwrite) // truncate done above if requested
                  .save(tablePath.toString)
                JdbcDbUtils.withJDBCConnection(
                  this.schemaHandler.dataBranch(),
                  sinkConnection.options,
                  sqlConnection
                ) { conn =>
                  val quotedColumns =
                    SQLUtils.quoteCols(colNames.toList, "\"")
                  val sql =
                    s"INSERT INTO $fullTableName SELECT ${quotedColumns.mkString(",")}  FROM '$tablePath/*.parquet'"
                  JdbcDbUtils.executeUpdate(sql, conn)
                  markTargetWritten()
                }
                settings.storageHandler().delete(tablePath)
              } else {
                // Spark commits the rows of each partition on its own: a failed write may have
                // written some of them
                markTargetWritten()
                loadedDF.write
                  .format(sinkConnection.sparkDatasource().getOrElse("jdbc"))
                  .option("dbtable", fullTableName)
                  .mode(SaveMode.Append) // truncate done above if requested
                  .options(sinkConnection.options)
                  .save()
              }
              // The rows are written outside the transaction: the expectations run after them
              Nil

            case None =>
              val alterSqls = alters.toList.flatMap(_.splitSql())
              val finalSqls = mainSql.splitSql()
              val checkable =
                rollbackPlan(alterSqls, finalSqls, createsTable, createdBeforeWrite, sqlConnection)
              val written = JdbcDbUtils.withJDBCConnection(
                this.schemaHandler.dataBranch(),
                sinkOptions,
                sqlConnection
              ) { conn =>
                Try {
                  conn.setAutoCommit(sinkConnection.isDuckDb())
                  runPreActions(conn, parsedPreActions.splitSql(";"))
                  runSqls(conn, preSql, "Pre")
                  runSqls(conn, alterSqls, "Alters")
                  conn.setAutoCommit(false)
                  runSqls(conn, finalSqls, "Main")
                  // Committed after the postsql statements, unless one of them fails: some engines
                  // then commit them anyway, when a later statement commits implicitly (a DDL on
                  // Snowflake)
                  markTargetWritten()
                } match {
                  case Success(_) =>
                    runSqls(conn, postSql, "Post")
                    // In the transaction of the write, they read the new content of the table: a
                    // failure rolls the write back (#1976)
                    checkable match {
                      case None =>
                        conn.commit()
                        Right(Nil)
                      case Some(plan) =>
                        val checked = checkBeforeCommit(conn)
                        if (checked.forall { case (_, report) => report.success }) {
                          conn.commit()
                          Right(checked)
                        } else {
                          val (outcome, failure) = undo(conn, plan)
                          Left((checked.map { case (_, report) => report }, outcome, failure))
                        }
                    }
                  case Failure(e) =>
                    JdbcDbUtils.rollbackQuietly(conn)
                    throw enrichWithSchemaSyncHint(jdbcTargetColumns(conn), e)
                }
              }
              // Their reports are written once the connection of the write is released
              written match {
                case Right(checked) => checked
                case Left((reports, outcome, rollbackFailure)) =>
                  throw failedBeforeCommit(reports, outcome, rollbackFailure)
              }
          }

          // A grant that fails fails the task, as on BigQuery, once the SCD2 columns, the
          // expectations and the schema export are done; SL_ACCESS_POLICIES_APPLY=false turns the
          // grants off (#1985)
          val granted: Try[Unit] = JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkOptions,
            sqlConnection
          ) { conn =>
            addSCD2Columns(conn, sinkConnection.getJdbcEngineName())
            val applied = if (test) Success(()) else applyJdbcAcl(conn, forceApply = false)
            // In autocommit mode, as on DuckDB, no transaction is left to roll back
            applied.failed.foreach(_ => JdbcDbUtils.rollbackQuietly(conn))
            applied
          }

          Try {
            checkExpectations(checkedBeforeCommit)
            if (settings.appConfig.autoExportSchema) {
              val isTableInAuditDomain =
                taskDesc.domain == settings.appConfig.audit.getDomain()
              if (isTableInAuditDomain)
                logger.info(
                  s"Table ${taskDesc.domain}.${taskDesc.table} is in audit domain, skipping schema extraction"
                )
              else {
                val config = ExtractSchemaConfig(
                  external = true,
                  outputDir = Some(DatasetArea.external.toString),
                  tables = s"${taskDesc.domain}.${taskDesc.table}" :: Nil,
                  connectionRef = Some(sinkConnectionRef),
                  accessToken = accessToken
                )
                ExtractSchemaCmd.run(config, schemaHandler)
              }
            }
          } match {
            case Failure(e) =>
              granted.failed.foreach(e.addSuppressed)
              throw e
            case Success(_) =>
          }
          granted.get
          JdbcJobResult(Nil)
      }
    }
    val end = Timestamp.from(Instant.now())
    res match {
      case Success(_) =>
        if (logExecution && interactive.isEmpty)
          logAuditSuccess(start, end, -1, test)
      case Failure(e) =>
        if (interactive.isEmpty)
          logAuditFailure(start, end, e, test)
    }
    res
  }

  /** Runs a query of one value on the connection of the write
    *
    * @return
    *   its value, None when it returns no row or NULL
    */
  private def queryValue(
    sql: String,
    parameters: List[String],
    sqlConnection: Option[Connection]
  ): Try[Option[String]] =
    Try {
      JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkOptions, sqlConnection) {
        conn =>
          val statement = conn.prepareStatement(sql)
          try {
            statement.setQueryTimeout(30)
            parameters.zipWithIndex.foreach { case (parameter, index) =>
              statement.setString(index + 1, parameter)
            }
            val result = statement.executeQuery()
            if (result.next()) Option(result.getString(1)) else None
          } finally statement.close()
      }
    }

  /** The name of the engine and the statements that commit on their own there. A connection to a
    * PostgreSQL URL may reach another engine:
    *   - a Redshift cluster, whose TRUNCATE commits on its own: its URL or its version tells it,
    *     and a version that cannot be read counts as Redshift's;
    *   - an engine whose version names another one (CockroachDB, YugabyteDB, CrateDB), whose
    *     statements do not all stay in the transaction: it gets the list of `jdbcEngines` declared
    *     under its name, if any.
    */
  private def implicitCommitsOfSink(
    sqlConnection: Option[Connection]
  ): (String, Option[List[String]]) = {
    val engine = jdbcSinkEngineName.toString
    def declared(name: String): (String, Option[List[String]]) =
      (name, settings.appConfig.jdbcEngines.get(name).flatMap(_.implicitCommits))
    lazy val redshiftUrl = sinkConnection.options
      .get("url")
      .exists(_.toLowerCase(java.util.Locale.ROOT).contains("redshift"))
    if (engine != "postgresql") (engine, jdbcSinkEngine.implicitCommits)
    else if (redshiftUrl) declared("redshift")
    else
      queryValue("SELECT version()", Nil, sqlConnection) match {
        case Success(Some(version)) =>
          val lowerCase = version.toLowerCase(java.util.Locale.ROOT)
          List("redshift", "cockroachdb", "yugabytedb", "cratedb")
            .find(name => lowerCase.contains(name.stripSuffix("db")))
            .map(declared)
            .getOrElse((engine, jdbcSinkEngine.implicitCommits))
        case Success(None) =>
          logger.warn(
            s"The engine of $fullTableName gives no version: it is taken for Redshift, whose " +
            "TRUNCATE commits on its own"
          )
          declared("redshift")
        case Failure(e) =>
          logger.warn(
            s"The version of the engine of $fullTableName cannot be read: it is taken for " +
            s"Redshift, whose TRUNCATE commits on its own (${e.getMessage})"
          )
          declared("redshift")
      }
  }

  /** @return
    *   why a rollback cannot undo any write of the connection, if it cannot: an Arrow Flight SQL
    *   connection or a Quack attachment commits each statement on the server, and a MySQL table of
    *   a storage engine without transactions ignores a rollback
    */
  private def rollbackUnsupported(
    engine: String,
    createsTable: Boolean,
    sqlConnection: Option[Connection]
  ): Option[String] =
    if (sinkConnection.isFlightSql())
      Some("an Arrow Flight SQL connection commits each statement on the server")
    else if (sinkConnection.isQuackClient())
      Some("a Quack attachment commits each statement on the server")
    else if ((engine == "mysql" || engine == "mariadb") && !createsTable)
      // A table that the run creates is dropped instead
      queryValue(
        "SELECT GROUP_CONCAT(DISTINCT UPPER(ENGINE)) FROM information_schema.TABLES " +
        "WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?)",
        List(taskDesc.domain, taskDesc.table),
        sqlConnection
      ) match {
        case Success(Some(engines))
            if engines.split(',').forall(Set("INNODB", "NDBCLUSTER", "ROCKSDB").contains) =>
          None
        case Success(engines) =>
          Some(
            s"its table has the storage engine ${engines.getOrElse("unknown")}, whose writes a " +
            "rollback may not undo"
          )
        case Failure(e) =>
          Some(s"the storage engine of its table cannot be read (${e.getMessage})")
      }
    else None

  /** How the rollback undoes the write, when the write checks its expectations before its commit
    * (#1976). None when it has none to check, or when a rollback could not undo the write: its
    * connection, its engine or its table does not roll back, a statement of the write or of its
    * postsql commits on its own, the task has presql, postsql or SQL that its write strategy does
    * not generate on an engine whose statements may commit on their own, or a statement cannot be
    * read. The expectations then run after the write, with a warning.
    */
  private def rollbackPlan(
    alters: List[String],
    main: List[String],
    createsTable: Boolean,
    createdBeforeWrite: Boolean,
    sqlConnection: Option[Connection]
  ): Option[ImplicitCommits.Checkable] =
    if (expectationsBeforeCommit.isEmpty) None
    else {
      val (engine, implicitCommits) = implicitCommitsOfSink(sqlConnection)
      val planned = rollbackUnsupported(engine, createsTable, sqlConnection) match {
        case Some(reason) => Left(reason)
        case None =>
          ImplicitCommits.plan(
            engine = engine,
            implicitCommits = implicitCommits,
            // DuckDB runs them before the transaction
            autocommitBeforeMain = sinkConnection.isDuckDb(),
            presql = preSql,
            beforeMain = alters,
            main = main,
            postsql = postSql,
            table = fullTableName,
            createsTable = createsTable,
            createdBeforeWrite = createdBeforeWrite,
            generated = taskDesc.parseSQL.getOrElse(true)
          )
      }
      planned match {
        case Right(plan) => Some(plan)
        case Left(reason) =>
          logger.warn(
            s"The expectations of $fullTableName run after its write, which a rollback could not " +
            s"undo: $reason"
          )
          None
      }
    }

  /** Rolls back the write whose expectations failed before its commit, and drops the table that the
    * run created when its creation committed. The rows of the write are then gone, even when its
    * presql or ALTER statements stay. A rolled-back write whose presql statements stay, which may
    * have written the table, keeps it marked as written.
    *
    * @return
    *   what became of the write, and the error that left it in place, if any
    */
  private def undo(
    conn: Connection,
    plan: ImplicitCommits.Checkable
  ): (String, Option[Throwable]) = {
    val presql = if (plan.presqlCommitted) " Its presql statements stay committed." else ""
    Try(conn.rollback()) match {
      case Failure(e) =>
        ("The rollback failed: the write may be committed.", Some(e))
      case Success(_) if plan.dropsCreatedTable =>
        Try {
          JdbcDbUtils.executeUpdate(s"DROP TABLE IF EXISTS $fullTableName", conn).get
          conn.commit()
        } match {
          case Success(_) =>
            markTargetNotWritten()
            (s"The table that this run created is dropped.$presql", None)
          case Failure(e) =>
            (
              "The table that this run created could not be dropped: it keeps the rows that " +
              "failed the expectations.",
              Some(e)
            )
        }
      case Success(_) =>
        if (!plan.presqlCommitted) markTargetNotWritten()
        val committed = List(
          Option.when(plan.presqlCommitted)("its presql statements"),
          Option.when(plan.altersCommitted)(
            "the ALTER statements that aligned the schema of its table"
          )
        ).flatten
        val stay =
          if (committed.isEmpty) ""
          else s" ${committed.mkString(" and ").capitalize} stay committed."
        (s"The write is rolled back.$stay", None)
    }
  }

  /** Runs the expectations checked before the commit (#1976) on the connection of the write, in its
    * transaction. A query that fails aborts the transaction on some engines, DuckDB and PostgreSQL
    * among them: the next expectations do not run.
    *
    * @return
    *   the expectations with their reports
    */
  private def checkBeforeCommit(conn: Connection): List[(ExpectationItem, ExpectationReport)] =
    if (expectationsBeforeCommit.isEmpty) Nil
    else {
      markCheckedBeforeCommit(expectationsBeforeCommit)
      val handler = new SQLConnectionExpectationAssertionHandler(conn)
      val job = expectationJob(expectationsBeforeCommit, handler, interactive = false)
      var failedQuery: Option[String] = None
      expectationsBeforeCommit.map { expectation =>
        val sql = job.renderQuery(expectation)
        val result = failedQuery match {
          case Some(failed) =>
            Failure(
              new Exception(
                s"Not run: the query of the expectation $failed failed in the transaction of the write"
              )
            )
          case None =>
            sql.flatMap { query =>
              logger.info(
                s"Applying expectation before the commit: ${expectation.expect} with request $query"
              )
              val result = Try(handler.handle(query))
              if (result.isFailure) failedQuery = Some(expectation.expect)
              result
            }
        }
        expectation -> job.report(expectation, sql.toOption, result)
      }
    }

  /** On a first run the target table is created by the main SQL statement, but presql statements
    * such as "DELETE FROM <target> ..." legitimately reference it (idempotency / partition-refresh
    * patterns). Pre-create the target empty from the main SELECT so presql can run against it. On
    * any failure (e.g. a SELECT referencing its own target) fall back to the previous behavior.
    *
    * @return
    *   true when it created the table
    */
  private def createEmptyTargetForPresql(sqlConnection: Option[java.sql.Connection]): Boolean = {
    // Set once its CREATE TABLE, which fails on an existing table, has run: a failed check drops
    // the table that this run created (#1976)
    var created = false
    Try {
      if (!tableExists) {
        val select = buildSelectStatement()
        JdbcDbUtils.withJDBCConnection(
          this.schemaHandler.dataBranch(),
          sinkOptions,
          sqlConnection
        ) { conn =>
          runSqls(conn, testInitSqls, "TestInit")
          JdbcDbUtils.createSchema(conn, fullDomainName)
          runSqls(
            conn,
            List(
              s"CREATE TABLE $fullTableName AS SELECT * FROM ($select) AS SL_PRESQL_SELECT WHERE 1 = 0"
            ),
            "CreateTargetForPresql"
          )
          created = true
          addSCD2Columns(conn, sinkConnection.getJdbcEngineName())
        }
      }
    } match {
      case Success(_) =>
      case Failure(e) if created =>
        logger.warn(
          s"Pre-created target table $fullTableName before presql, but could not complete it: " +
          e.getMessage
        )
      case Failure(e) =>
        logger.warn(
          s"Could not pre-create target table $fullTableName before presql: ${e.getMessage}"
        )
    }
    created
  }

  override protected def expectationAssertionHandler: ExpectationAssertionHandler =
    new JdbcExpectationAssertionHandler(sinkOptions)

  private def runInteractive(conn: Connection, mainSql: String): JdbcJobResult = {
    val limitSQL = limitQuery(mainSql, resultPageSize, resultPageNumber)
    logger.info(s"Running interactive SQL query: \n$limitSQL\n")
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(mainSql)
      val result = new ListBuffer[List[String]]
      var i = 1
      val resultingSchema = ListBuffer[(String, String)]()

      while (i <= rs.getMetaData.getColumnCount) {
        val colName = rs.getMetaData.getColumnName(i)
        val colType = rs.getMetaData.getColumnTypeName(i)
        resultingSchema.append((colName, colType))

        i += 1
      }
      var rowCount = 0
      while (rs.next && rowCount < resultPageSize) {
        val rowAsSeq = new ListBuffer[String]
        var i = 1
        while (i <= rs.getMetaData.getColumnCount) {
          rowAsSeq.append(Option(rs.getObject(i)).map(_.toString).getOrElse("NULL"))
          i = i + 1
        }
        result.append(rowAsSeq.toList)
        rowCount = rowCount + 1
      }
      JdbcJobResult(resultingSchema.toList, result.toList)
    } catch {
      case e: Exception =>
        throw new Exception(s"SQLException - Error running interactive SQL query: \n$mainSql\n", e)
    } finally {
      stmt.close()
    }
  }

  lazy val fullTableName = s"$fullDomainName.${taskDesc.table}"

  private def runPreActions(conn: java.sql.Connection, preActions: List[String]): Unit = {
    runSqls(conn, preActions, "PreActions")
  }

  @throws[Exception]
  private def runSqls(conn: Connection, sqls: List[String], typ: String): Unit = {
    if (sqls.nonEmpty) {
      sqls.foreach { req =>
        JdbcDbUtils.executeUpdate(req, conn) match {
          case Success(_) =>
          case Failure(e) =>
            throw e
        }
      }
    }
  }

  /** @param incomingSchema
    * @param tableName
    * @return
    *   (sqls to execute, tableExists? ) if (table exists, sqls are actually alter table statements,
    *   else these are create schema / table statements
    */
  override def buildTableSchemaSQL(
    incomingSchema: StructType,
    tableName: String,
    syncStrategy: TableSync,
    createIfAbsent: Boolean
  ): (List[String], Boolean) = {
    // update target table schema if needed
    val isSCD2 = writeStrategy.getEffectiveType() == WriteStrategyType.SCD2
    val incomingSchemaWithSCD2 =
      if (isSCD2) {
        // Each validity column that the incoming schema lacks, whatever the case of its name, with
        // its type (#1885)
        val (startTs, endTs) = writeStrategy.validityColumns()
        val (startType, endType) = writeStrategy.validityColumnTypes()
        List(startTs -> startType, endTs -> endType)
          .filter(_._1.nonEmpty)
          .distinctBy(_._1.toLowerCase)
          .foldLeft(incomingSchema) { case (schema, (column, primitiveType)) =>
            if (schema.fieldNames.exists(_.equalsIgnoreCase(column))) {
              logger.warn(
                s"Incoming schema already contains SCD2 validity column '$column'. It will not be added again."
              )
              schema
            } else
              schema.add(
                StructField(column, WriteStrategy.validitySparkType(primitiveType), nullable = true)
              )
          }
      } else {
        incomingSchema
      }
    val sinkConnectionRefOptions = sinkConnection.options
    val jdbcUrl = sinkConnectionRefOptions("url")
    val targetTableExists: Boolean = tableExists
    JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnectionRefOptions) {
      conn =>
        if (targetTableExists) {
          val existingSchema =
            SparkUtils.getSchemaOption(
              conn,
              sinkConnectionRefOptions,
              tableName,
              sinkConnection.getJdbcEngineName().toString
            )
          val addedSchema =
            SparkUtils.added(
              incomingSchemaWithSCD2,
              existingSchema.getOrElse(incomingSchema)
            )
          val alterTableDropColumns =
            if (syncStrategy == TableSync.ALL) {
              val deletedSchema =
                SparkUtils.dropped(
                  incomingSchemaWithSCD2,
                  existingSchema.getOrElse(incomingSchema)
                )
              val columnsToDrop =
                SparkUtils.alterTableDropColumnsString(
                  sinkConnection.getJdbcEngineName().toString,
                  deletedSchema,
                  tableName
                )
              if (columnsToDrop.nonEmpty) {
                logger.info(
                  s"alter table $tableName with ${columnsToDrop.size} columns to drop"
                )
                logger.debug(s"alter table ${columnsToDrop.mkString("\n")}")
              }
              columnsToDrop
            } else {
              Nil
            }
          val alterTableAddColumns =
            if (syncStrategy == TableSync.ALL || syncStrategy == TableSync.ADD) {
              val columnsToAdd =
                SparkUtils.alterTableAddColumnsString(
                  sinkConnection.getJdbcEngineName().toString,
                  addedSchema,
                  tableName,
                  Map.empty
                )
              if (columnsToAdd.nonEmpty) {
                logger.info(
                  s"alter table $tableName with ${columnsToAdd.size} columns to add"
                )
                logger.debug(s"alter table ${columnsToAdd.mkString("\n")}")
              }
              columnsToAdd
            } else {
              Nil
            }

          // always drop after adding, in case all columns are dropped
          // because in case all columns are dropped, an error is raised for a table without any columns.
          val allAlter = alterTableAddColumns ++ alterTableDropColumns
          (allAlter.toList, true)
        } else if (createIfAbsent) {
          val optionsWrite =
            new JdbcOptionsInWrite(jdbcUrl, tableName, sinkConnectionRefOptions)
          logger.info(
            s"Table $tableName not found, creating it with schema $incomingSchemaWithSCD2"
          )
          val (createSchema, createTable, commentSQL) =
            SparkUtils.buildCreateTableSQL(
              sinkConnection.getJdbcEngineName().toString,
              tableName,
              incomingSchemaWithSCD2,
              caseSensitive = false,
              temporaryTable = false,
              optionsWrite,
              attDdl()
            )
          val allSqls = List(createSchema, createTable, commentSQL.getOrElse(""))
          (allSqls, false)
        } else {
          (Nil, false)
        }
    }
  }

  def updateJdbcTableSchema(
    incomingSchema: StructType,
    tableName: String,
    syncStrategy: TableSync,
    createIfAbsent: Boolean
  ): Unit = {
    buildTableSchemaSQL(incomingSchema, tableName, syncStrategy, createIfAbsent) match {
      case (sqls, exists) =>
        JdbcDbUtils.withJDBCConnection(this.schemaHandler.dataBranch(), sinkConnection.options) {
          conn =>
            sqls.filter(_.nonEmpty).foreach { sql =>
              if (exists) {
                JdbcDbUtils.executeAlterTable(sql, conn)
              } else {
                JdbcDbUtils.executeUpdate(sql, conn) match {
                  case Success(_) =>
                  case Failure(e) =>
                    logger.error(s"Error executing $sql", e)
                    throw e
                }
              }
            }
        }
    }
  }

  override def buildRLSQueries(): List[String] = ???
}

object JdbcAutoTask extends LazyLogging {
  def executeUpdate(sql: String, connectionName: String, accessToken: Option[String])(implicit
    settings: Settings
  ): Try[Boolean] = {
    val connection = settings.appConfig
      .connection(connectionName)
      .getOrElse(throw new StarlakeNotFoundException(s"Connection not found $connectionName"))
      .withAccessToken(accessToken)

    // This is an update statement. No readonly connection is needed
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), connection.options) {
      conn =>
        JdbcDbUtils.execute(sql, conn)
    }
  }
}
