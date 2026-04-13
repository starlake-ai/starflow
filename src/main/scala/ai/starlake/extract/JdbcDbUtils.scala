package ai.starlake.extract

import ai.starlake.config.Settings.{ConnectionInfo, JdbcEngine}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.StringUtils
import ai.starlake.jdbc.StarlakeDriver
import ai.starlake.job.Main
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.tests.StarlakeTestData.DomainName
import ai.starlake.utils.{SparkUtils, StarlakeNotFoundException, Utils}
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.*

import java.sql.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try, Using}

object JdbcDbUtils extends LazyLogging {
  // Properties that should not be passed to DuckDB connections
  // todo get it from settings
  val nonDuckDbProperties = Set(
    "url",
    "driver",
    "dbtable",
    "numpartitions",
    "sl_access_token",
    "account",
    "allowUnderscoresInHost",
    "database",
    "db",
    "authenticator",
    "user",
    "password",
    "preActions",
    "DATA_PATH",
    "SL_DATA_PATH",
    "storageType",
    "quoteIdentifiers"
  )
  def removeNonDuckDbProperties(
    options: Map[String, String]
  ): Map[String, String] = {
    val filtered = options.filterNot { case (k, _) =>
      nonDuckDbProperties
        .contains(k) || k.toUpperCase().startsWith("SL_") || k.toLowerCase().startsWith("fs.")
    }
    filtered
  }

  DriverManager.registerDriver(new StarlakeDriver())
  val appType = Option(System.getenv("SL_API_APP_TYPE")).getOrElse("web")

  type TableName = String
  type TableRemarks = String
  type ColumnName = String
  type Columns = List[TableAttribute]
  type PrimaryKeys = List[String]

  case class SqlColumn(
    name: String,
    dataType: String,
    precision: Option[String],
    scale: Option[String]
  ) {
    def sqlTypeAsString(): String = {
      (precision, scale) match {
        case (Some(p), Some(s)) => s"$dataType($p,$s)"
        case (Some(p), None)    => s"$dataType($p,0)"
        case _                  => dataType
      }
    }
  }

  // Re-export for backward compatibility — extracted to StarlakeConnectionPool.scala
  val StarlakeConnectionPool = ai.starlake.extract.StarlakeConnectionPool
  val lastExportTableName = "SL_LAST_EXPORT"

  /** We are in duckdb and starlake is run using the extract-data or extract-schema command without
    * SL_API
    */
  def isExtractCommandHack(url: String) = {
    Set("extract-data", "extract-schema").contains(Main.currentCommand) &&
    !sys.env.contains("SL_API") &&
    url.startsWith("jdbc:duckdb")
  }

  /** Execute a block of code in the context of a newly created connection. We better use here a
    * Connection pool, but since starlake processes are short lived, we do not really need it.
    *
    * @param connectionSettings
    * @param f
    * @param settings
    * @tparam T
    * @return
    */
  private val depth = new AtomicInteger(0)
  private val count = new AtomicInteger(0)
  def withJDBCConnection[T](
    dataBranch: Option[String],
    connectionOptions: Map[String, String],
    existingConnection: Option[Connection] = None
  )(f: Connection => T): T = {
    count.incrementAndGet()
    val tryConn =
      Try {
        existingConnection match {
          case Some(connection) =>
            // logger.info(s"count=$count / depth=$depth Reusing existing connection $connection")
            connection
          case None =>
            val connection =
              StarlakeConnectionPool.getConnection(dataBranch, connectionOptions)
            connection
        }
      }
    tryConn match {
      case Failure(exception) =>
        // logger.error(s"count=$count / depth=$depth Error creating connection", exception)
        throw exception
      case Success(connection) =>
        // logger.info(s"count=$count / depth=$depth Created connection $connection")
        depth.incrementAndGet()
        // run preActions
        val preActions = connectionOptions.get("preActions")

        runDuckLakePreActions(connection, connectionOptions, preActions)
        val result = Try {
          f(connection)
        } match {
          case Failure(exception) =>
            logger.error(s"Error running sql", exception)
            Failure(exception)
          case Success(value) =>
            val postActions = connectionOptions.get("postActions")
            postActions.foreach { actions =>
              actions.split(";").foreach { action =>
                Try {
                  Using.resource(connection.createStatement()) { statement =>
                    statement.execute(action)
                  }
                } match {
                  case Failure(exception) =>
                    logger.error(s"Error running postAction $action", exception)
                    throw exception
                  case Success(value) =>
                }
              }
            }
            Success(value)
        }

        val url = connectionOptions.get("url").orNull
        if (existingConnection.isEmpty && url != null && !isExtractCommandHack(url)) {
          Try(connection.close()) match {
            case Success(_) =>
              logger.debug(s"Closed connection $url")

            case Failure(exception) =>
              logger.warn(s"Could not close connection to $url", exception)
          }
        }
        depth.decrementAndGet()
        result match {
          case Failure(exception) =>
            throw exception
          case Success(value) => value
        }
    }
  }

  /** Escape a value for use in a SQL single-quoted string literal to prevent SQL injection. */
  private def escapeSqlStringLiteral(value: String): String =
    value.replace("'", "''")

  def runDuckLakePreActions(
    connection: java.sql.Connection,
    connectionOptions: Map[String, String],
    preActions: Option[String]
  ): Unit = {
    val isDucklake = preActions.getOrElse("").contains("ducklake:")
    connectionOptions.get("fs.s3a.endpoint").foreach { endpoint =>
      logger.info(s"Setting s3a.endpoint")
      Using.resource(connection.createStatement()) { endpointStatement =>
        val schemeIndex = endpoint.indexOf("://") + 3
        val s3Endpoint = escapeSqlStringLiteral(endpoint.substring(schemeIndex))
        endpointStatement.execute(s"SET s3_endpoint='$s3Endpoint'")

        if (endpoint.startsWith("https"))
          endpointStatement.execute(s"SET s3_use_ssl=true")
        else
          endpointStatement.execute(s"SET s3_use_ssl=false")

        if (s3Endpoint.contains("s3.amazonaws.com"))
          endpointStatement.execute("SET s3_url_style='vhost'")
        else
          endpointStatement.execute("SET s3_url_style='path'")

        connectionOptions.get("fs.s3a.endpoint.region").foreach { region =>
          logger.info(s"Setting s3a.endpoint.region")
          val escapedRegion = escapeSqlStringLiteral(region)
          endpointStatement.execute(s"SET s3_region='$escapedRegion'")
        }
        connectionOptions.get("fs.s3a.access.key").foreach { accessKey =>
          logger.info("Setting s3a.access.key")
          val escapedAccessKey = escapeSqlStringLiteral(accessKey)
          endpointStatement.execute(s"SET s3_access_key_id='$escapedAccessKey'")
        }
        connectionOptions.get("fs.s3a.secret.key").foreach { secretKey =>
          logger.info("Setting s3a.secret.key")
          val escapedSecretKey = escapeSqlStringLiteral(secretKey)
          endpointStatement.execute(s"SET s3_secret_access_key='$escapedSecretKey'")
        }
      }
    }

    Try {
      connectionOptions
        .get("SL_DUCKDB_HOME")
        .orElse(Option(System.getenv("SL_DUCKDB_HOME")))
        .foreach { duckdbHome =>
          logger.info(s"Setting duckdb_home")
          Using.resource(connection.createStatement()) { duckdbHomeStatement =>
            val escapedHome = escapeSqlStringLiteral(duckdbHome)
            duckdbHomeStatement.execute(s"SET home_directory='$escapedHome'")
          }
        }
    } match {
      case Failure(e) => logger.warn("Failed to set duckdb home_directory", e)
      case _          =>
    }

    Try {
      connectionOptions
        .get("SL_DUCKDB_SECRET_HOME")
        .orElse(Option(System.getenv("SL_DUCKDB_SECRET_HOME")))
        .orElse(connectionOptions.get("SL_DUCKDB_HOME"))
        .orElse(Option(System.getenv("SL_DUCKDB_HOME")))
        .foreach { duckdbSecretDir =>
          logger.info(s"Setting duckdb secret directory")
          Using.resource(connection.createStatement()) { statement =>
            val escapedDir = escapeSqlStringLiteral(duckdbSecretDir)
            statement.execute(s"SET secret_directory='$escapedDir'")
          }
        }
    } match {
      case Failure(e) => logger.warn("Failed to set duckdb secret_directory", e)
      case _          =>
    }

    preActions.foreach { actions =>
      actions.split(";").filter(_.trim.nonEmpty).foreach { action =>
        def runStatement(action: String): Unit = {
          val statement = connection.createStatement()
          logger.info(s"Running preAction $action")
          statement.execute(action)
          statement.close()
        }
        Try {
          try {
            runStatement(action)
          } catch {
            case e: Exception => // handle Disk I/O latency in Docker / Network/Volume Latency
              Thread.sleep(1000)
              runStatement(action)
          }
        } match {
          case Failure(exception) =>
            // Catalog Error: SET schema: No catalog + schema named "s01" found.
            val message = Utils.exceptionMessagesChainAsString(exception).toLowerCase()
            if (message.contains("already exists") && message.contains("failed to attach")) {
              logger.info(s"Ducklake already attached, skipping preAction $action")
            } else if (message.contains("Catalog Error: SET schema: No catalog + schema")) {
              logger.info(s"Schema already set, skipping preAction $action")
            } else {
              logger.error(s"Error running preAction $action", exception)
              throw exception
            }
          case Success(value) =>
        }
      }
    }
  }

  def truncateTable(conn: java.sql.Connection, tableName: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"TRUNCATE TABLE $tableName")
    } finally {
      statement.close()
    }
  }

  @throws[Exception]
  def createSchema(conn: Connection, domainName: String): Unit = {
    executeUpdate(schemaCreateSQL(domainName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $domainName", e)
        throw e
    }
  }
  @throws[Exception]
  def createDatabase(conn: Connection, dbName: String): Unit = {
    executeUpdate(databaseCreateSQL(dbName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating database $dbName", e)
        throw e
    }
  }

  @throws[Exception]
  def schemaCreateSQL(domainName: String): String = {
    s"CREATE SCHEMA IF NOT EXISTS $domainName"
  }

  @throws[Exception]
  def databaseCreateSQL(domainName: String): String = {
    s"CREATE DATABASE IF NOT EXISTS $domainName"
  }

  def buildDropTableSQL(tableName: String): String = {
    s"DROP TABLE IF EXISTS $tableName"
  }
  @throws[Exception]
  def dropTable(conn: Connection, tableName: String): Unit = {
    executeUpdate(buildDropTableSQL(tableName), conn) match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"Error creating schema $tableName", e)
        throw e
    }
  }

  def tableExists(conn: Connection, url: String, domainAndTablename: String): Boolean = {
    val dialect = SparkUtils.dialectForUrl(url)
    Try {
      val existQuery = dialect.getTableExistsQuery(domainAndTablename)
      val statement = conn.prepareStatement(existQuery)
      try {
        val rs = statement.executeQuery()
        try {
          rs
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } match {
      case Failure(e) =>
        logger.info(s"Table $domainAndTablename does not exist")
        false
      case Success(_) => true
    }
  }

  @throws[Exception]
  def executeAlterTable(script: String, connection: java.sql.Connection): Boolean = {
    val metadata = connection.getMetaData()
    val isAutoCommit = connection.getAutoCommit()
    if (!metadata.supportsTransactions) {
      throw new Exception("Database does not support alter table feature")
    } else {
      connection.setAutoCommit(false)
    }
    val statement = connection.createStatement()
    try {
      val res = statement.executeUpdate(script)
      connection.commit()
      true
    } finally {
      statement.close()
      connection.setAutoCommit(isAutoCommit)
    }
  }

  def executeQuery[T](
    stmt: PreparedStatement
  )(apply: ResultSet => T): T = {
    val rs = stmt.executeQuery()
    val result = apply(rs)
    rs.close()
    stmt.close()
    result
  }

  def executeQueryAsMap(
    query: String,
    connection: Connection
  ): List[Map[String, String]] = {
    val resultTable = ListBuffer[Map[String, String]]()
    val statement = connection.createStatement()
    logger.info(s"executing $query")
    try {
      val resultSet = statement.executeQuery(query)
      try {
        // Get column names
        val metaData = resultSet.getMetaData
        val columnCount = metaData.getColumnCount
        val columnNames = (1 to columnCount).map(metaData.getColumnName)

        // Process the result set
        while (resultSet.next()) {
          val row = columnNames
            .map(name =>
              name -> Option(resultSet.getObject(name)).map(_.toString).getOrElse("null")
            )
            .toMap
          resultTable += row
        }
      } finally {
        resultSet.close()
      }
    } finally {
      statement.close()
    }

    resultTable.toList
  }

  def execute(script: String, connection: Connection): Try[Boolean] = {
    val statement = connection.createStatement()
    try {
      val result = Try {
        logger.info(s"execute statement: $script")
        statement.execute(script)
      }
      result match {
        case Failure(exception) =>
          logger.error(s"Error running sql $script", exception)
          throw exception
        case Success(value) => value
      }
      result
    } finally {
      statement.close()
    }
  }

  def executeUpdate(script: String, connection: Connection): Try[Boolean] = {
    val sqlId = java.util.UUID.randomUUID.toString
    val trimmedSQL = script.trim
    val formattedSQL = SQLUtils
      .format(trimmedSQL, JSQLFormatter.OutputFormat.PLAIN)
    logger.info(s"Running JDBC SQL with id $sqlId: $formattedSQL")
    val statement = connection.createStatement()
    val result = Try {
      val count = statement.executeUpdate(trimmedSQL)
      logger.info(s"$count records affected")
      true
    }
    result match {
      case Failure(exception) =>
        logger.error(
          s"Error running JDBC SQL [$trimmedSQL] with id $sqlId: ${exception.getMessage}"
        )
        throw exception
      case Success(value) =>
        logger.info(s"end running JDBC SQL [$trimmedSQL] with id $sqlId with return value $value")
    }
    statement.close()
    result
  }

  /** RUn the sql statement in the context of a connection
    *
    * @param script
    * @param connectionOptions
    * @param settings
    * @return
    */
  def execute(script: String, connectionOptions: Map[String, String])(implicit
    settings: Settings
  ): Boolean = {
    withJDBCConnection(settings.schemaHandler().dataBranch(), connectionOptions) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        stmt.execute(script)
      }
    }
  }

  /** Get table comments for a data base schema
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  private def extractTableRemarks(
    jdbcSchema: JDBCSchema,
    connection: Connection,
    table: String,
    jdbcEngine: Option[JdbcEngine]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val tableRemarks = jdbcSchema.tableRemarks.orElse(jdbcEngine.flatMap(_.tableRemarks))
    tableRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting table remarks using $sql")
      Using.resource(connection.createStatement()) { statement =>
        Using.resource(statement.executeQuery(sql)) { rs =>
          if (rs.next()) {
            rs.getString(1)
          } else {
            logger.warn(s"Not table remark found for table $table")
            ""
          }
        }
      }
    }
  }

  private def extractCaseInsensitiveSchemaName(
    connectionSettings: ConnectionInfo,
    databaseMetaData: DatabaseMetaData,
    schemaName: String
  ): Try[String] = {
    connectionSettings match {
      case d if d.isMySQLOrMariaDb() =>
        Using(
          databaseMetaData.getCatalogs()
        ) { resultSet =>
          var result: Option[String] = None
          while (result.isEmpty && resultSet.next()) {
            val tableSchema = resultSet.getString("TABLE_CAT")
            if (schemaName.equalsIgnoreCase(tableSchema)) {
              result = Some(tableSchema)
            }
          }
          result.getOrElse(throw new StarlakeNotFoundException(s"Schema $schemaName not found"))
        }
      case _ =>
        Using(
          databaseMetaData.getSchemas()
        ) { resultSet =>
          var result: Option[String] = None
          while (result.isEmpty && resultSet.next()) {
            val tableSchema = resultSet.getString("TABLE_SCHEM")
            if (schemaName.equalsIgnoreCase(tableSchema)) {
              result = Some(tableSchema)
            }
          }
          result.getOrElse(throw new StarlakeNotFoundException(s"Schema $schemaName not found"))
        }
    }
  }

  def extractSchemasAndTableNames(connectionSettings: ConnectionInfo)(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = {
    extractSchemasAndTableNamesUsingInformationSchema(connectionSettings) match {
      case Success(value) =>
        Success(value)
      case Failure(exception) =>
        logger.warn(
          s"Could not extract schemas and tables using information schema: ${exception.getMessage}"
        )
        extractSchemasAndTableNamesUsingDatabaseMetadata(connectionSettings)
    }
  }

  private def extractSchemasAndTableNamesUsingInformationSchema(
    connectionSettings: ConnectionInfo
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = Try {
    val result = ListBuffer[(String, String)]()
    ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        // We ignore the branch when reading the information schema
        None, // settings.schemaHandler().dataBranch(),
        connectionSettings.options
      ) { connection =>
        val statement = connection.prepareStatement("""
              |SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
              |WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
              |GROUP BY TABLE_SCHEMA, TABLE_NAME
              |""".stripMargin)
        JdbcDbUtils.executeQuery(statement) { rs =>
          while (rs.next()) {
            val schema = rs.getString(1)
            val table = rs.getString(2)
            logger.info(s"Schema: $schema, Table: $table")
            result.append(schema -> table)
          }
        }
      }
    }(dbExtractEC.executionContext)
    result.groupBy(_._1).toList.map { case (schema, tables) => schema -> tables.map(_._2).toList }
  }

  def extractColumnsUsingInformationSchema(
    connectionSettings: ConnectionInfo,
    tableSchema: String,
    tableName: String
  )(implicit
    settings: Settings
  ): Try[List[(String, SqlColumn)]] = Try {
    val result = ListBuffer[(String, SqlColumn)]()
    withJDBCConnection(
      // We ignore the branch when reading the information schema
      None, // settings.schemaHandler().dataBranch(),
      connectionSettings.options
    ) { connection =>
      val statement = connection.prepareStatement(s"""
                                                     |SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
                                                     |WHERE LOWER(TABLE_SCHEMA) LIKE LOWER('$tableSchema') AND LOWER(TABLE_NAME) LIKE LOWER('$tableName')
                                                     |ORDER BY ORDINAL_POSITION
                                                     |""".stripMargin)
      JdbcDbUtils.executeQuery(statement) { rs =>
        while (rs.next()) {
          val columnName = rs.getString(1)
          val dataType = rs.getString(2)
          val precision = Option(rs.getObject(3)).map(_.toString)
          val scale = Option(rs.getObject(4)).map(_.toString)
          logger.info(
            s"Column: $columnName, Data Type: $dataType, Precision: $precision, Scale: $scale"
          )
          val col = SqlColumn(columnName, dataType, precision, scale)
          result.append(columnName -> col)
        }
      }
    }
    result.toList
  }

  def existsTableUsingInformationSchema(
    connectionSettings: ConnectionInfo,
    tableSchema: String,
    tableName: String
  )(implicit
    settings: Settings
  ): Try[Boolean] = Try {
    withJDBCConnection(
      // We ignore the branch when reading the information schema
      None, // settings.schemaHandler().dataBranch(),
      connectionSettings.options
    ) { connection =>
      val statement = connection.prepareStatement(s"""
                                                     |SELECT COUNT(*) as CNT FROM INFORMATION_SCHEMA.TABLES
                                                     |WHERE LOWER(TABLE_SCHEMA) LIKE LOWER('$tableSchema') AND LOWER(TABLE_NAME) LIKE LOWER('$tableName')
                                                     |""".stripMargin)
      JdbcDbUtils.executeQuery(statement) { rs =>
        rs.next()
      }
    }
  }

  private def extractSchemasAndTableNamesUsingDatabaseMetadata(connectionSettings: ConnectionInfo)(
    implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[(DomainName, List[TableName])]] = {
    val schemaNames = extractJDBCSchemas(connectionSettings)
    schemaNames.map { schemaNames =>
      val result =
        schemaNames.map { schemaName =>
          val jdbcSchema = JDBCSchema(schema = schemaName, tableTypes = List("TABLE"))
          val tables = extractJDBCTables(
            jdbcSchema,
            connectionSettings,
            skipRemarks = true,
            keepOriginalName = true,
            includeColumns = false
          )
          logger.info(s"Extracted tables for schema $schemaName: ${tables.keys.mkString(", ")}")
          schemaName -> tables.keys.toList.sorted
        }
      result.sortBy(_._1)
    }
  }

  def extractJDBCSchemas(connectionSettings: ConnectionInfo)(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Try[List[String]] = {
    ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        // We ignore the branch when extracting tables
        None, // settings.schemaHandler().dataBranch(),
        connectionSettings.options
      ) { connection =>
        val catalog = connectionSettings.getCatalog()
        val databaseMetaData = connection.getMetaData()
        Using(databaseMetaData.getSchemas(catalog, null)) { resultSet =>
          new Iterator[String] {
            override def hasNext: Boolean = resultSet.next()

            override def next(): String =
              resultSet.getString("TABLE_SCHEM")
          }.toList.filterNot(_.equalsIgnoreCase("INFORMATION_SCHEMA")).distinct.sorted
        }
      }
    }(dbExtractEC.executionContext)
  }

  /* Extract all tables from the database and return Map of tablename -> tableDescription */
  def extractTableNames(
    schemaName: String,
    jdbcSchema: JDBCSchema,
    sqlDefinedTables: List[String],
    tablePredicate: String => Boolean,
    connectionSettings: ConnectionInfo,
    databaseMetaData: DatabaseMetaData,
    skipRemarks: Boolean,
    jdbcEngine: Option[JdbcEngine],
    connection: Connection
  )(implicit settings: Settings): Map[String, Option[String]] = {
    Try {
      val tableTypes =
        if (jdbcSchema.tableTypes.nonEmpty) jdbcSchema.tableTypes.toArray else null
      connectionSettings match {
        case d if d.isMySQLOrMariaDb() =>
          databaseMetaData.getTables(
            schemaName,
            "%",
            "%",
            tableTypes
          )
        case d if d.isDuckDb() =>
          // https://duckdb.org/docs/sql/information_schema.html
          val tableTypesWithBaseTable = jdbcSchema.tableTypes.map { tt =>
            if (tt == "TABLE")
              "BASE TABLE"
            else
              tt

          }.toArray
          val tableTypes =
            if (tableTypesWithBaseTable.nonEmpty) tableTypesWithBaseTable else null
          val resultset =
            databaseMetaData.getTables(
              jdbcSchema.catalog.orNull,
              schemaName,
              "%",
              tableTypes
            )
          resultset
        case _ =>
          val resultset =
            databaseMetaData.getTables(
              jdbcSchema.catalog.orNull,
              schemaName,
              "%",
              tableTypes
            )
          resultset

      }
    }.flatMap { resultSet =>
      Using(resultSet) { resultSet =>
        val tableNamesWithRemarks = new Iterator[(TableName, Option[TableRemarks])] {
          override def hasNext: Boolean = resultSet.next()

          override def next(): (TableName, Option[TableRemarks]) =
            resultSet.getString("TABLE_NAME") -> Option(resultSet.getString("REMARKS"))
        }.toSet ++ sqlDefinedTables.map(_ -> None).toSet
        tableNamesWithRemarks
          .filter { case (tableName, _) => tablePredicate(tableName) }
          .map { case (tableName, metadataRemarks) =>
            val localRemarks =
              if (skipRemarks) None
              else
                Try {
                  extractTableRemarks(jdbcSchema, connection, tableName, jdbcEngine)
                } match {
                  case Failure(exception) =>
                    logger.warn(exception.getMessage, exception)
                    None
                  case Success(value) => value
                }
            val remarks = localRemarks.orElse(metadataRemarks)
            tableName -> remarks
          }
          .toMap
      }
    } match {
      case Success(results) => results
      case Failure(exception) =>
        logger.warn(Utils.exceptionAsString(exception))
        logger.warn(
          s"The following schema could not be found $schemaName. All tables within this schema are not ignored."
        )
        Map.empty
    }
  }

  /** Get all tables, columns and primary keys of a database schema as described in the YML file.
    * Exclude tables not selected in the YML file
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param settings
    * @return
    */
  def extractJDBCTables(
    jdbcSchema: JDBCSchema,
    connectionSettings: ConnectionInfo,
    skipRemarks: Boolean,
    keepOriginalName: Boolean,
    includeColumns: Boolean
  )(implicit
    settings: Settings,
    dbExtractEC: ExtractExecutionContext
  ): Map[TableName, ExtractTableAttributes] = {
    val url = connectionSettings.options("url")
    val jdbcServer = url.split(":")(1)
    val jdbcEngine = settings.appConfig.jdbcEngines.get(jdbcServer)
    val jdbcTableMap =
      jdbcSchema.tables
        .map(tblSchema => tblSchema.name.toUpperCase -> tblSchema)
        .toMap
    val uppercaseTableNames = jdbcTableMap.keys.toList
    val schemaAndTableNames = ParUtils.runOneInExecutionContext {
      withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        connectionSettings.options
      ) { connection =>
        val databaseMetaData = connection.getMetaData()
        extractCaseInsensitiveSchemaName(
          connectionSettings,
          databaseMetaData,
          jdbcSchema.schema
        ).map { schemaName =>
          val lowerCasedExcludeTables = jdbcSchema.exclude.map(_.toLowerCase)

          def tablesInScopePredicate(tablesToExtract: List[String] = Nil): TableName => Boolean =
            (tableName: String) => {
              !lowerCasedExcludeTables.contains(
                tableName.toLowerCase
              ) && (tablesToExtract.isEmpty || tablesToExtract.contains(tableName.toUpperCase()))
            }

          val sqlDefinedTables = jdbcSchema.tables.filter(_.sql.isDefined).map(_.name)
          val selectedTables = uppercaseTableNames match {
            case list if list.isEmpty || list.contains("*") =>
              extractTableNames(
                schemaName,
                jdbcSchema,
                sqlDefinedTables,
                tablesInScopePredicate(),
                connectionSettings,
                databaseMetaData,
                skipRemarks,
                jdbcEngine,
                connection
              )
            case list =>
              val extractedTableNames =
                extractTableNames(
                  schemaName,
                  jdbcSchema,
                  sqlDefinedTables,
                  tablesInScopePredicate(list),
                  connectionSettings,
                  databaseMetaData,
                  skipRemarks,
                  jdbcEngine,
                  connection
                )
              val notExtractedTable = list.diff(
                extractedTableNames
                  .map { case (tableName, _) => tableName }
                  .map(_.toUpperCase())
                  .toList
              )
              if (notExtractedTable.nonEmpty) {
                val tablesNotExtractedStr = notExtractedTable.mkString(", ")
                logger.warn(
                  s"The following tables where not extracted for $schemaName.${jdbcSchema.schema} : $tablesNotExtractedStr"
                )
              }
              extractedTableNames
          }
          logger.whenDebugEnabled {
            selectedTables.keys.foreach(table => logger.debug(s"Selected: $table"))
          }
          Utils.printOut(s"Found ${selectedTables.size} tables in $schemaName")
          (schemaName, selectedTables)
        }
      }
    }(dbExtractEC.executionContext)
    val res =
      if (includeColumns) {
        val res =
          schemaAndTableNames.flatMap { case (schemaName, selectedTableNames) =>
            // Extract the Starlake Schema
            Using
              .Manager { use =>
                ParUtils
                  .runInExecutionContext(selectedTableNames.toList) {
                    case (tableName, tableRemarks) =>
                      ExtractUtils.timeIt(s"Table's schema extraction of $schemaName.$tableName") {
                        Utils.printOut(
                          s"Extracting table '$schemaName.$tableName'"
                        )
                        withJDBCConnection(
                          settings.schemaHandler().dataBranch(),
                          connectionSettings.options
                        ) { tableExtractConnection =>
                          val jdbcColumnMetadata: JdbcColumnMetadata =
                            jdbcSchema.tables
                              .find(_.name.equalsIgnoreCase(tableName))
                              .flatMap(_.sql)
                              .map { sql =>
                                // extract schema from sql metadata
                                val statement = tableExtractConnection.createStatement()
                                statement.setMaxRows(1)
                                val resultSet = use(statement.executeQuery(sql))
                                new ResultSetColumnMetadata(
                                  resultSet.getMetaData,
                                  jdbcSchema,
                                  tableName,
                                  keepOriginalName,
                                  skipRemarks,
                                  jdbcEngine
                                )
                              }
                              .getOrElse(
                                new JdbcColumnDatabaseMetadata(
                                  connectionSettings,
                                  tableExtractConnection.getMetaData,
                                  jdbcSchema,
                                  schemaName,
                                  tableName,
                                  keepOriginalName,
                                  skipRemarks,
                                  jdbcEngine
                                )
                              )
                          val primaryKeys = jdbcColumnMetadata.primaryKeys
                          val foreignKeys: Map[TableName, TableName] =
                            jdbcColumnMetadata.foreignKeys
                          val columns: List[TableAttribute] = jdbcColumnMetadata.columns
                          logger.whenDebugEnabled {
                            columns
                              .foreach(column => logger.debug(s"column: $tableName.${column.name}"))
                          }
                          val jdbcCurrentTable = jdbcTableMap
                            .get(tableName.toUpperCase)
                          // Limit to the columns specified by the user if any
                          val currentTableRequestedColumns: Map[ColumnName, Option[ColumnName]] =
                            jdbcCurrentTable
                              .map(
                                _.columns.map(c =>
                                  (if (keepOriginalName) c.name.toUpperCase.trim
                                   else c.rename.getOrElse(c.name).toUpperCase.trim) -> c.rename
                                )
                              )
                              .getOrElse(Map.empty)
                              .toMap
                          val currentFilter = jdbcCurrentTable.flatMap(_.filter)
                          val selectedColumns: List[TableAttribute] =
                            columns
                              .filter(col =>
                                currentTableRequestedColumns.isEmpty || currentTableRequestedColumns
                                  .contains("*") || currentTableRequestedColumns
                                  .contains(col.name.toUpperCase())
                              )
                              .map(c =>
                                c.copy(
                                  foreignKey = foreignKeys.get(c.name.toUpperCase)
                                )
                              )
                          logger.whenDebugEnabled {
                            val schemaMessage = selectedColumns
                              .map(c =>
                                c.name -> c.rename match {
                                  case (name, Some(newName)) => name + " as " + newName
                                  case (name, _)             => name
                                }
                              )
                              .mkString("Final schema column:\n - ", "\n - ", "")
                            logger.debug(schemaMessage)
                          }
                          tableName -> ExtractTableAttributes(
                            tableRemarks,
                            selectedColumns,
                            primaryKeys,
                            currentFilter
                          )
                        }
                      }
                  }(dbExtractEC.executionContext)
                  .toMap
              }
          }
        res
      } else {
        schemaAndTableNames.map { case (schemaName, selectedTableNames) =>
          selectedTableNames.toList.map { case (tableName, tableRemarks) =>
            Utils.printOut(
              s"Extracting table '$schemaName.$tableName'"
            )
            tableName -> ExtractTableAttributes(
              tableRemarks,
              Nil,
              Nil,
              None
            )
          }.toMap

        }
      }
    res match {
      case Failure(exception) =>
        Utils.logException(logger, exception)
        Map.empty
      case Success(value) => value
    }
  }

  def formatRemarksSQL(
    jdbcSchema: JDBCSchema,
    table: String,
    remarks: String
  )(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter.*
    val parameters = Map(
      "catalog" -> jdbcSchema.catalog.getOrElse(""),
      "schema"  -> jdbcSchema.schema,
      "table"   -> table
    )
    logger.debug(s"Interpolating remark $remarks with parameters $parameters")
    val sql = remarks.richFormat(
      parameters,
      Map.empty
    )
    logger.debug(s"Remark interpolated as $sql")
    sql
  }

  /** Create Starlake Domain for JDBC Schema
    *
    * @param jdbcSchema
    * @param domainTemplate
    * @param selectedTablesAndColumns
    * @return
    */
  def extractDomain(
    jdbcSchema: JDBCSchema,
    domainTemplate: Option[DomainInfo],
    selectedTablesAndColumns: Map[String, ExtractTableAttributes]
  ): DomainInfo = {
    def isNumeric(sparkType: String): Boolean = {
      sparkType match {
        case "double" | "decimal" | "long" => true
        case _                             => false
      }
    }

    val trimTemplate =
      domainTemplate.flatMap(_.tables.headOption.flatMap(_.attributes.head.trim))

    val starlakeSchema = selectedTablesAndColumns.map { case (tableName, tableAttrs) =>
      val sanitizedTableName = StringUtils.replaceNonAlphanumericWithUnderscore(tableName)
      SchemaInfo(
        name = tableName,
        rename = if (sanitizedTableName != tableName) Some(sanitizedTableName) else None,
        pattern = Pattern.compile(s"$tableName.*"),
        attributes = tableAttrs.columNames.map(attr =>
          attr.copy(trim =
            if (isNumeric(attr.`type`)) jdbcSchema.numericTrim.orElse(trimTemplate)
            else trimTemplate
          )
        ),
        metadata = None,
        comment = tableAttrs.tableRemarks,
        presql = Nil,
        postsql = Nil,
        primaryKey = tableAttrs.primaryKeys
      )
    }

    // Generate the domain with a dummy watch directory
    val database = domainTemplate.flatMap(_.database)
    val incomingDir = domainTemplate
      .flatMap { dom =>
        dom.resolveDirectoryOpt().map { dir =>
          DatasetArea
            .substituteDomainAndSchemaInPath(
              jdbcSchema.schema,
              "",
              dir
            )
            .toString
        }
      }

    val normalizedDomainName = StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
    val rename = domainTemplate
      .flatMap(_.rename)
      .map { name =>
        DatasetArea.substituteDomainAndSchema(jdbcSchema.schema, "", name)
      }
      .orElse(
        if (normalizedDomainName != jdbcSchema.schema) Some(normalizedDomainName) else None
      )
    val ack = domainTemplate.flatMap(_.resolveAck())

    DomainInfo(
      database = database,
      name = jdbcSchema.sanitizeName match {
        case Some(true) => StringUtils.replaceNonAlphanumericWithUnderscore(jdbcSchema.schema)
        case _          => jdbcSchema.schema
      },
      rename = rename,
      metadata = domainTemplate
        .flatMap(_.metadata)
        .map(_.copy(directory = incomingDir, ack = ack)),
      tables = starlakeSchema.toList,
      comment = None
    )
  }

  def jdbcOptions(
    jdbcOptions: Map[String, String],
    sparkFormat: String,
    accessToken: Option[String]
  ): CaseInsensitiveMap[TableName] = {
    val options = if (sparkFormat == "snowflake") {
      jdbcOptions.flatMap { case (k, v) =>
        if (k.startsWith("sf")) {
          val jdbcK = k.replace("sf", "").toLowerCase().replace("database", "db")
          val finalv =
            if (jdbcK == "url")
              "jdbc:snowflake://" + v
            else
              v
          List(
            jdbcK -> finalv,
            k     -> v
          )
        } else
          List(k -> v)

      }
    } else
      jdbcOptions
    val optionsWithAccessToken = accessToken match {
      case Some(token) =>
        options + ("sl_access_token" -> token)
      case None => options
    }
    CaseInsensitiveMap[String](optionsWithAccessToken)
  }

  def getCommonJDBCType(dt: DataType): (Option[JdbcType], Boolean) = {
    val (elementType, isArray) =
      dt match {
        case ArrayType(elementType, _) => (elementType, true)
        case _                         => (dt, false)
      }
    val jdbcType =
      elementType match {
        case IntegerType    => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
        case LongType       => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
        case DoubleType     => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
        case FloatType      => Option(JdbcType("REAL", java.sql.Types.FLOAT))
        case ShortType      => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
        case ByteType       => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
        case BooleanType    => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
        case StringType     => Option(JdbcType("TEXT", java.sql.Types.CLOB))
        case BinaryType     => Option(JdbcType("BLOB", java.sql.Types.BLOB))
        case CharType(n)    => Option(JdbcType(s"CHAR($n)", java.sql.Types.CHAR))
        case VarcharType(n) => Option(JdbcType(s"VARCHAR($n)", java.sql.Types.VARCHAR))
        case TimestampType  => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
        // This is a common case of timestamp without time zone. Most of the databases either only
        // support TIMESTAMP type or use TIMESTAMP as an alias for TIMESTAMP WITHOUT TIME ZONE.
        // Note that some dialects override this setting, e.g. as SQL Server.
        case TimestampNTZType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
        case DateType         => Option(JdbcType("DATE", java.sql.Types.DATE))
        case t: DecimalType =>
          Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
        case _ => None
      }
    (jdbcType, isArray)
  }
}
