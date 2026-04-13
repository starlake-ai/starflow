package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.TableAttribute
import ai.starlake.sql.SQLUtils
import better.files.File
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_PREFER_TIMESTAMP_NTZ
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, SQLException}
import java.util.regex.Pattern
import scala.util.{Failure, Success, Try}

object SparkUtils extends LazyLogging {
  // --- Delegated to SparkDDLBuilder ---
  def added(incoming: StructType, existing: StructType): StructType =
    SparkDDLBuilder.added(incoming, existing)
  def dropped(incoming: StructType, existing: StructType): StructType =
    SparkDDLBuilder.dropped(incoming, existing)
  def alterTableDropColumnsString(
    engineName: String,
    fields: StructType,
    tableName: String
  ): Seq[String] = SparkDDLBuilder.alterTableDropColumnsString(engineName, fields, tableName)
  def alterTableAddColumnsString(
    engineName: String,
    allFields: StructType,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Seq[String] = SparkDDLBuilder.alterTableAddColumnsString(
    engineName,
    allFields,
    tableName,
    attributesWithDDLType
  )
  def alterTableAddColumnString(
    engineName: String,
    field: StructField,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Option[String] =
    SparkDDLBuilder.alterTableAddColumnString(engineName, field, tableName, attributesWithDDLType)
  def isFlat(fields: StructType): Boolean = SparkDDLBuilder.isFlat(fields)

  def updateJdbcTableSchema(
    engineName: String,
    conn: Connection,
    jdbcOptions: Map[String, String],
    domainAndTableName: String,
    incomingSparkSchema: StructType,
    attributesWithDDLType: Map[String, String]
  ): Unit = {
    buildUpdateJdbcTableSchemaSQL(
      engineName,
      conn,
      jdbcOptions,
      domainAndTableName,
      incomingSparkSchema,
      attributesWithDDLType
    )
      .foreach(JdbcDbUtils.executeAlterTable(_, conn))
  }

  def buildUpdateJdbcTableSchemaSQL(
    engineName: String,
    conn: Connection,
    jdbcOptions: Map[String, String],
    domainAndTableName: String,
    incomingSparkSchema: StructType,
    attributesWithDDLType: Map[String, String]
  ): Seq[String] = {
    val url = jdbcOptions("url")
    if (isFlat(incomingSparkSchema)) {
      val existingSchema = getSchemaOption(conn, jdbcOptions, domainAndTableName)
      val addedSColumns =
        SparkUtils.added(incomingSparkSchema, existingSchema.getOrElse(incomingSparkSchema))
      val deletedColumns =
        SparkUtils.dropped(incomingSparkSchema, existingSchema.getOrElse(incomingSparkSchema))
      val alterTableDropColumns =
        SparkUtils.alterTableDropColumnsString(engineName, deletedColumns, domainAndTableName)
      if (alterTableDropColumns.nonEmpty) {
        logger.info(
          s"alter table ${domainAndTableName} with ${alterTableDropColumns.size} columns to drop"
        )
        logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
      }
      val alterTableAddColumns =
        SparkUtils.alterTableAddColumnsString(
          engineName,
          addedSColumns,
          domainAndTableName,
          attributesWithDDLType
        )

      if (alterTableAddColumns.nonEmpty) {
        logger.info(
          s"alter table ${domainAndTableName} with ${alterTableAddColumns.size} columns to add"
        )
        logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
      }
      alterTableDropColumns ++ alterTableAddColumns
    } else
      Seq.empty
  }

  /** Creates a table with a given schema. Updated from Spark 3.0.1
    */
  def getSchemaOption(
    conn: Connection,
    options: Map[String, String],
    table: String
  ): Option[StructType] = {
    val dialect = SparkUtils.dialectForUrl(options("url"))
    val preferTimestampNTZ =
      options
        .get(JDBC_PREFER_TIMESTAMP_NTZ)
        .map(_.toBoolean)
        .getOrElse(SQLConf.get.timestampType == TimestampNTZType)

    try {
      val statement =
        conn.prepareStatement(dialect.getSchemaQuery(table))
      try {
        Some(
          JdbcUtils.getSchema(
            statement.executeQuery(),
            dialect,
            isTimestampNTZ = preferTimestampNTZ
          )
        )
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  def createSchema(session: SparkSession, domain: String): Unit = {
    SparkUtils.sql(session, s"CREATE SCHEMA IF NOT EXISTS ${domain}")
  }

  def truncateTable(session: SparkSession, tableName: String): Unit = {
    SparkUtils.sql(session, s"TRUNCATE TABLE $tableName")
  }

  /** Creates a table with a given schema. Updated from Spark 3.0.1
    */
  def createTable(
    engineName: String,
    conn: Connection,
    domainAndTableName: String,
    schema: StructType,
    caseSensitive: Boolean,
    temporaryTable: Boolean,
    options: JdbcOptionsInWrite,
    attrDdlMapping: Map[String, Map[String, String]]
  )(implicit settings: Settings): Unit = {
    val (createSchemaSql, createTableSql, commentSql) =
      buildCreateTableSQL(
        engineName,
        domainAndTableName,
        schema,
        caseSensitive,
        temporaryTable,
        options,
        attrDdlMapping
      )
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      logger.info(s"Creating schema $createSchemaSql")
      statement.executeUpdate(createSchemaSql)
      logger.info(s"Creating table $createTableSql")
      statement.executeUpdate(createTableSql)
      commentSql.foreach { comment =>
        try {
          logger.info(s"Creating table comment $comment")
          statement.executeUpdate(comment)
        } catch {
          case _: Exception =>
            logger.warn(s"Cannot create JDBC table comment on $domainAndTableName. Skipping it.")
        }
      }
    } finally {
      statement.close()
    }
  }

  /** Creates a table with a given schema. Updated from Spark 3.0.1
    */
  def buildCreateTableSQL(
    engineName: String,
    domainAndTableName: String,
    schema: StructType,
    caseSensitive: Boolean,
    temporaryTable: Boolean,
    options: JdbcOptionsInWrite,
    attrDdlMapping: Map[String, Map[String, String]]
  )(implicit settings: Settings): (String, String, Option[String]) = {
    val strSchema =
      sqlSchemaString(
        schema,
        caseSensitive,
        options.url,
        attrDdlMapping,
        0
      ) // options.createTableColumnTypes
    val createTableOptions = options.createTableOptions
    val finalStrSchema =
      if (options.parameters.getOrElse("quoteIdentifiers", "false").toBoolean)
        strSchema
      else
        strSchema.replaceAll("\"", "")

    val domainName = domainAndTableName.split('.').head
    val createSchemaSQL =
      if (domainName != "__ignore__")
        s"CREATE SCHEMA IF NOT EXISTS $domainName"
      else
        ""
    val temporary = if (temporaryTable) "TEMP" else ""
    val createTableSQL =
      s"CREATE $temporary TABLE IF NOT EXISTS $domainAndTableName ($finalStrSchema) $createTableOptions"

    val commentSQL =
      if (options.tableComment.nonEmpty)
        Some(s"COMMENT ON TABLE $domainAndTableName IS '${options.tableComment}'")
      else
        None

    (createSchemaSQL, createTableSQL, commentSQL)
  }

  def dialectForUrl(url: String): JdbcDialect = {
    val reworkedUrl =
      url
        .replace("jdbc:redshift", "jdbc:postgresql")
        .replace("jdbc:as400", "jdbc:db2")

    val jdbcDialect: JdbcDialect = JdbcDialects.get(reworkedUrl)
    if (jdbcDialect.getClass.getSimpleName == "NoopDialect$") {
      logger.warn(s"No dialect found for $url, falling back to default one")
    } else {
      logger.debug(s"JDBC dialect $jdbcDialect")
    }
    jdbcDialect
  }
  private def getDescription(field: StructField): Option[String] = {
    val comment =
      if (!field.getComment().isEmpty) {
        field.getComment()
      } else if (field.metadata.contains("description")) {
        Option(field.metadata.getString("description"))
      } else {
        None
      }
    comment
  }

  def sqlSchemaString(
    schema: StructType,
    caseSensitive: Boolean,
    url: String,
    createTableColumnTypes: Map[String, Map[String, String]] = Map.empty,
    level: Int
  )(implicit settings: Settings): String = {
    val columns = sqlSchema(
      schema,
      caseSensitive,
      url,
      createTableColumnTypes,
      level
    )
    columns.mkString(", ")
  }

  def sqlSchema(
    schema: StructType,
    caseSensitive: Boolean,
    url: String,
    sparkToSqlTypeMappings: Map[String, Map[String, String]] = Map.empty,
    level: Int
  )(implicit settings: Settings): List[String] = {
    logger.debug(s"sqlSchema of $schema")
    sparkToSqlTypeMappings.foreach { case (k, v) =>
      logger.debug(s"Column $k has DDL types $v")
    }
    val dialectPattern = Pattern
      .compile("jdbc:([a-zA-Z]+):.*")
      .matcher(
        url.replace(":starlake:", ":")
      ) // in case we are coming with a starlake wrapped jdbc url
    require(dialectPattern.find())
    val dialectName = dialectPattern.group(1)
    val jdbcEng = settings.appConfig.jdbcEngines(dialectName)

    val dialect = dialectForUrl(url)
    val typMap =
      if (caseSensitive) sparkToSqlTypeMappings
      else
        CaseInsensitiveMap(sparkToSqlTypeMappings)
    val columns =
      schema.fields.flatMap { field =>
        val nullable = if (!field.nullable && level == 0) "NOT NULL" else ""
        val description =
          if (level == 0) getDescription(field).map(d => s"COMMENT '$d'").getOrElse("")
          else ""
        val ddlTyp = typMap.get(field.name).flatMap(_.get(dialectName))
        val quotedFieldName = dialect.quoteIdentifier(field.name)

        val dataType = field.dataType
        val name = field.name
        val column =
          if (jdbcEng.supportsJson.getOrElse(false)) { // DuckDB only
            val (elementType, repeated) =
              dataType match {
                case arrayType: ArrayType =>
                  (arrayType.elementType, true)
                case _ =>
                  (dataType, false)
              }
            val element =
              elementType match {
                case struct: StructType =>
                  val fields =
                    sqlSchema(struct, caseSensitive, url, sparkToSqlTypeMappings, level + 1)
                  if (repeated) {
                    s"$name STRUCT(${fields.mkString(",")})[]"
                  } else {
                    s"$name STRUCT(${fields.mkString(",")})"
                  }
                case decimal: DecimalType =>
                  if (repeated) {
                    s"$name DECIMAL(${decimal.precision},${decimal.scale})[]"
                  } else {
                    s"$name DECIMAL(${decimal.precision},${decimal.scale}) $nullable"
                  }
                case _ =>
                  val typ =
                    ddlTyp.getOrElse(getJdbcType(field.dataType, dialect).databaseTypeDefinition)
                  if (typ.endsWith("[]")) {
                    s"$quotedFieldName $typ"
                  } else if (repeated) {
                    s"$quotedFieldName $typ[]"
                  } else {
                    s"$quotedFieldName $typ $nullable"
                  }
              }
            Some(element)
          } else {
            if (dataType.isInstanceOf[StructType] || dataType.isInstanceOf[ArrayType]) {
              throw new IllegalArgumentException(
                "Array and nested struct types are not supported in the schema for this database"
              )
            }
            val typ =
              ddlTyp.getOrElse(getJdbcType(field.dataType, dialect).databaseTypeDefinition)
            Some(s"$quotedFieldName $typ")
          }
        column
      }
    columns.toList
  }

  def sql(session: SparkSession, sql: String): DataFrame = {
    val formattedSQL = SQLUtils.format(sql.trim, JSQLFormatter.OutputFormat.PLAIN)

    val sqlId = java.util.UUID.randomUUID.toString
    val result =
      try {
        logger.info(s"Executing statement with id $sqlId:\n $formattedSQL")
        session.sql(sql)
      } catch {
        case e: Exception =>
          logger.error(s"Error when executing statement id $sqlId")
          e.printStackTrace()
          throw e
      }
    logger.info(s"Successfully executed statement id $sqlId")
    result
  }

  def runPySpark(pythonFile: Path, commandParameters: Map[String, String])(implicit
    settings: Settings
  ): Unit = {
    // We first download locally all files because PythonRunner only support local filesystem
    val pyFiles =
      pythonFile +: settings.sparkConfig
        .getString("pyFiles")
        .split(",")
        .filter(_.nonEmpty)
        .map(x => new Path(x.trim))
    val directory = new Path(File.newTemporaryDirectory().pathAsString)
    logger.info(s"Python local directory is $directory")
    pyFiles.foreach { pyFile =>
      val pyName = pyFile.getName
      settings.storageHandler().copyToLocal(pyFile, new Path(directory, pyName))
    }
    val pythonParams = commandParameters.flatMap { case (name, value) =>
      List(s"""--$name""", s"""$value""")
    }.toArray

    PythonRunner.main(
      Array(
        new Path(directory, pythonFile.getName).toString,
        pyFiles.mkString(",")
      ) ++ pythonParams
    )
  }

  // because tableExists is not supported on all engines (specifically iceberg)
  // session.catalog.tableExists(taskDesc.domain, taskDesc.table)

  def tableExists(session: SparkSession, tableName: String): Boolean = {
    Try {
      session.catalog.getTable(tableName)
    } match {
      case Success(_) =>
        true
      case Failure(e) =>
        false
    }

  }

  def sparkSchemaWithCondition(
    schemaHandler: SchemaHandler,
    attributes: List[TableAttribute],
    p: TableAttribute => Boolean,
    withFinalName: Boolean
  ): StructType = {
    def enrichStructField(attr: TableAttribute, structField: StructField) = {
      structField.copy(
        name = if (withFinalName) attr.getFinalName() else attr.name,
        nullable = if (attr.script.isDefined) true else !attr.resolveRequired(),
        metadata =
          if (attr.`type` == "variant")
            org.apache.spark.sql.types.Metadata.fromJson("""{ "sqlType" : "JSON"}""")
          else org.apache.spark.sql.types.Metadata.empty
      )
    }

    val fields = attributes filter p map { attr =>
      val structField = StructField(
        if (withFinalName) attr.getFinalName() else attr.name,
        attr.sparkType(schemaHandler, enrichStructField),
        if (attr.script.isDefined) true else !attr.resolveRequired(),
        if (attr.`type` == "variant")
          org.apache.spark.sql.types.Metadata.fromJson("""{ "sqlType" : "JSON"}""")
        else org.apache.spark.sql.types.Metadata.empty
      )
      attr.comment.map(structField.withComment).getOrElse(structField)
    }
    StructType(fields)
  }

}
