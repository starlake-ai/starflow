package ai.starlake.job.transform

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractSchemaCmd, ExtractSchemaConfig, JdbcDbUtils}
import ai.starlake.job.metrics.{ExpectationAssertionHandler, JdbcExpectationAssertionHandler}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{
  AccessControlEntry,
  AutoTaskInfo,
  Engine,
  TableSync,
  WriteStrategyType
}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JdbcJobResult, JobResult, SparkUtils, StarlakeNotFoundException, Utils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
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
    AccessControlEntry.applyJdbcAcl(connection, aclSQL(), forceApply)

  def applyJdbcAcl(jdbcConnection: Settings.ConnectionInfo, forceApply: Boolean): Try[Unit] =
    AccessControlEntry.applyJdbcAcl(jdbcConnection, aclSQL(), forceApply)

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
        val exists = JdbcDbUtils.tableExists(conn, url, fullTableName)
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
        auditTableCreateSQL().foreach { sql =>
          JdbcDbUtils.executeUpdate(sql, conn) match {
            case Success(_) =>
            case Failure(e) =>
              logger.error(s"Error executing $sql", e)
              throw e
          }
        }
        true
    }
  }

  private def buildAddSCD2ColumnsSqls(engineName: Engine): List[String] = {
    this.taskDesc.writeStrategy match {
      case Some(strategyOptions) if strategyOptions.getEffectiveType() == WriteStrategyType.SCD2 =>
        val startTsCol = strategyOptions.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val endTsCol = strategyOptions.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
        val scd2Columns = List(startTsCol, endTsCol)
        val alterTableSqls = scd2Columns.map { column =>
          if (engineName.toString.toLowerCase() == "redshift")
            s"ALTER TABLE $fullTableName ADD COLUMN $column TIMESTAMP"
          else
            s"ALTER TABLE $fullTableName ADD COLUMN IF NOT EXISTS $column TIMESTAMP NULL"
        }
        alterTableSqls
      case _ =>
        List.empty
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
  def runJDBC(
    df: Option[DataFrame],
    sqlConnection: Option[java.sql.Connection] = None
  ): Try[JdbcJobResult] = {
    val start = Timestamp.from(Instant.now())

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
      val (alters, mainSql) =
        if (df.isEmpty) {
          buildAllSQLQueries(None)
        } else {
          val sql = taskDesc.getSql()
          val mainSql = schemaHandler.substituteRefTaskMainSQL(
            sql,
            taskDesc.getRunConnection(),
            allVars
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
          df match {
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
                    conn.rollback()
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
                }
                settings.storageHandler().delete(tablePath)
              } else {
                loadedDF.write
                  .format(sinkConnection.sparkDatasource().getOrElse("jdbc"))
                  .option("dbtable", fullTableName)
                  .mode(SaveMode.Append) // truncate done above if requested
                  .options(sinkConnection.options)
                  .save()
              }

            case None =>
              JdbcDbUtils.withJDBCConnection(
                this.schemaHandler.dataBranch(),
                sinkOptions,
                sqlConnection
              ) { conn =>
                Try {
                  conn.setAutoCommit(sinkConnection.isDuckDb())
                  runPreActions(conn, parsedPreActions.splitSql(";"))
                  runSqls(conn, preSql, "Pre")
                  alters.foreach(it => runSqls(conn, it.splitSql(), "Alters"))
                  conn.setAutoCommit(false)
                  val finalSqls = mainSql.splitSql()
                  runSqls(conn, finalSqls, "Main")
                } match {
                  case Success(_) =>
                    runSqls(conn, postSql, "Post")
                    conn.commit()
                  case Failure(e) =>
                    conn.rollback()
                    throw e
                }
              }
          }

          JdbcDbUtils.withJDBCConnection(
            this.schemaHandler.dataBranch(),
            sinkOptions,
            sqlConnection
          ) { conn =>
            Try {
              if (!test) applyJdbcAcl(conn, forceApply = true)
              addSCD2Columns(conn, sinkConnection.getJdbcEngineName())
            } match {
              case Success(_) =>
              case Failure(e) =>
                conn.rollback()
                throw e
            }
          }

          if (settings.appConfig.expectations.active && !taskDesc.isAuditTable()) {
            runAndSinkExpectations()
          }
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
        val startTs = writeStrategy.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
        val incomingSchemaWithStartTs =
          if (incomingSchema.fieldNames.contains(startTs)) {
            logger.warn(
              s"Incoming schema already contains SCD2 start timestamp column '$startTs'. It will not be added again."
            )
            incomingSchema
          } else {
            incomingSchema
              .add(
                StructField(
                  startTs,
                  TimestampType,
                  nullable = true
                )
              )
          }
        val endTs = writeStrategy.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
        if (incomingSchemaWithStartTs.fieldNames.contains(endTs)) {
          logger.warn(
            s"Incoming schema already contains SCD2 end timestamp column '$endTs'. It will not be added again."
          )
          incomingSchemaWithStartTs
        } else {
          incomingSchemaWithStartTs
            .add(
              StructField(
                endTs,
                TimestampType,
                nullable = true
              )
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
            SparkUtils.getSchemaOption(conn, sinkConnectionRefOptions, tableName)
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
