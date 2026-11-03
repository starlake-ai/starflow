package ai.starlake.job.ingest.loaders

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.IngestionJob
import ai.starlake.job.transform.TransformContext
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{IngestionCounters, SparkUtils}
import com.google.gson.Gson
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import scala.util.{Failure, Success, Try}

class SnowflakeNativeLoader(ingestionJob: IngestionJob)(implicit settings: Settings)
    extends NativeLoader(ingestionJob, None) {

  // We do not use this in snowflake. return whatever
  override def getIncomingDir(): String = domain.resolveDirectory()

  def run(): Try[List[IngestionCounters]] = {
    Try {
      val sinkConnection = mergedMetadata.getSinkConnection()
      val twoSteps = requireTwoSteps(effectiveSchema)
      JdbcDbUtils
        .withJDBCConnection(
          this.schemaHandler.dataBranch(),
          sinkConnection.withAccessToken(ingestionJob.accessToken).options
        ) { conn =>
          logger.info(s"path count = ${path.size}")
          if (twoSteps) {
            val tempTables =
              path.map { p =>
                logger.info(s"Loading $p to temporary table")
                val tempTable = SQLUtils.temporaryTableName(effectiveSchema.finalName)
                val loadResult = singleStepLoad(
                  domain.finalName,
                  tempTable,
                  schemaWithMergedMetadata,
                  List(p),
                  conn
                )
                val escapedPath = p.toString.replace("'", "''")
                val filenameSQL =
                  s"ALTER TABLE ${domain.finalName}.$tempTable ADD COLUMN ${CometColumns.cometInputFileNameColumn} STRING DEFAULT '$escapedPath';"

                JdbcDbUtils.execute(filenameSQL, conn)
                val json = new Gson().toJson(loadResult)
                logger.info(s"Load result: $json")
                tempTable
              }

            try {
              val unionTempTables =
                tempTables
                  .map(s"SELECT * FROM ${domain.finalName}." + _)
                  .mkString("(", " UNION ALL ", ")")
              val sqlWithTransformedFields =
                starlakeSchema.buildSecondStepSqlSelectOnLoad(unionTempTables)
              val targetTableFullName = s"${domain.finalName}.${starlakeSchema.finalName}"

              val taskDesc = AutoTaskInfo(
                name = starlakeSchema.finalName,
                sql = Some(sqlWithTransformedFields),
                database = schemaHandler.getDatabase(domain),
                domain = domain.finalName,
                table = starlakeSchema.finalName,
                presql = starlakeSchema.presql,
                postsql = starlakeSchema.postsql,
                sink = mergedMetadata.sink,
                rls = starlakeSchema.rls,
                expectations = starlakeSchema.expectations,
                acl = starlakeSchema.acl,
                comment = starlakeSchema.comment,
                tags = starlakeSchema.tags,
                writeStrategy = mergedMetadata.writeStrategy.map(
                  _.withDeclaredValidityTypes(starlakeSchema.attributes)
                ),
                parseSQL = Some(true),
                connectionRef = Option(mergedMetadata.getSinkConnectionRef())
              )

              val context = TransformContext(
                appId = Option(ingestionJob.applicationId()),
                taskDesc = taskDesc,
                commandParameters = Map.empty,
                interactive = None,
                truncate = false,
                test = false,
                logExecution = true,
                accessToken = ingestionJob.accessToken,
                resultPageSize = 200,
                resultPageNumber = 1,
                dryRun = false,
                scheduledDate = scheduledDate,
                syncSchema = false
              )(settings, storageHandler, schemaHandler)
              val job = TransformContext.createJdbcTask(context, Some(conn))

              job.updateJdbcTableSchema(
                starlakeSchema.sparkSchemaWithoutIgnore(
                  schemaHandler,
                  withFinalName = true
                ),
                targetTableFullName,
                TableSync.ALL,
                true
              )

              val runResult = job.runJDBC(df = None, sqlConnection = Some(conn))

              runResult match {
                case Success(_) =>
                  logger.info(s"Table $targetTableFullName created successfully")
                case Failure(exception) =>
                  logger.error(
                    s"Error creating table $targetTableFullName: ${exception.getMessage}"
                  )
                  throw exception
              }
            } finally {
              tempTables.foreach { tempTable =>
                Try {
                  JdbcDbUtils.dropTable(conn, s"${domain.finalName}.$tempTable")
                }.recover { case e =>
                  logger.warn(
                    s"Failed to drop temporary table ${domain.finalName}.$tempTable: ${e.getMessage}"
                  )
                }
              }
            }
          } else {
            singleStepLoad(
              domain = domain.finalName,
              table = starlakeSchema.finalName,
              schema = schemaWithMergedMetadata,
              path = path,
              conn = conn
            )
          }
        }
    }.map { _ =>
      List(
        IngestionCounters(
          inputCount = -1,
          acceptedCount = -1,
          rejectedCount = -1,
          paths = path.map(_.toString),
          jobid = ingestionJob.applicationId()
        )
      )
    }

  }

  private lazy val metadataOptions = CaseInsensitiveMap[String](mergedMetadata.getOptions())
  private def getOption(option: String): Option[String] = {
    metadataOptions.get(option).orElse(metadataOptions.get(s"SNOWFLAKE_$option"))
  }

  private lazy val incomingDir: String = domain.resolveDirectory()

  /** True if incomingDir is a local or remote filesystem path (starts with / or has a URI scheme).
    * False means it is already a Snowflake stage name.
    */
  private lazy val isLocalOrRemotePath: Boolean = {
    incomingDir.startsWith("/") || incomingDir.matches("^[a-zA-Z][a-zA-Z0-9+.-]*:.*")
  }

  /** The source location to use in COPY INTO ... FROM statements. */
  private lazy val copySource: String = {
    if (isLocalOrRemotePath) {
      s"@$tempStage/${domain.finalName}/"
    } else {
      val stage = if (incomingDir.startsWith("@")) incomingDir else s"@$incomingDir"
      s"$stage/"
    }
  }

  private def copyExtraOptions(commonOptions: List[String]): String = {
    var extraOptions = ""
    val options = mergedMetadata.getOptions()

    if (options.nonEmpty) {
      options.foreach { case (k, v) =>
        // ignore any key that does not start with snowflake_
        if (k.toUpperCase().startsWith("SNOWFLAKE_")) {
          val newKey = k.substring("SNOWFLAKE_".length)
          if (!commonOptions.contains(newKey)) {
            extraOptions += s"$newKey = $v\n"
          }
        }
      }
    }
    extraOptions
  }
  private val skipCount = {
    if (getOption("SKIP_HEADER").isEmpty && mergedMetadata.resolveWithHeader()) {
      Some("1")
    } else {
      getOption("SKIP_HEADER")
    }
  }

  private val compressionFormat: String = {
    val compression =
      getOption("COMPRESSION").getOrElse("true").equalsIgnoreCase("true")
    if (compression)
      "COMPRESSION = AUTO"
    else
      "COMPRESSION = NONE"
  }

  private val pattern = this.starlakeSchema.pattern.pattern()

  private val nullIf: String = mergedMetadata.getOptions().get("NULL_IF") match {
    case Some(value) if value.nonEmpty =>
      s"NULL_IF = $value"
    case _ =>
      if (mergedMetadata.resolveEmptyIsNull())
        "NULL_IF = ('')"
      else
        ""
  }

  private def purge: String =
    if (mergedMetadata.getOptions().getOrElse("PURGE", "false").equalsIgnoreCase("true"))
      "TRUE"
    else
      "FALSE"

  private def encoding: String =
    mergedMetadata
      .getOptions()
      .getOrElse("ENCODING", mergedMetadata.resolveEncoding())

  private def buildCopyJson(domainAndTableName: String): String = {
    val stripOuterArray =
      getOption("STRIP_OUTER_ARRAY").getOrElse(mergedMetadata.resolveArray().toString.toUpperCase())
    val commonOptions = List("STRIP_OUTER_ARRAY", "NULL_IF")
    val matchByColumnName =
      if (
        starlakeSchema.attributes.exists(
          _.primitiveType(schemaHandler).getOrElse(PrimitiveType.string) ==
            PrimitiveType.variant
        )
      )
        ""
      else
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"

    val extraOptions = copyExtraOptions(commonOptions)

    // $extension is unused here because snowflake auto-detects compression and user is in charge of defining the right pattern
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM $copySource
         |PATTERN = '$pattern'
         |PURGE = ${purge}
         |FILE_FORMAT = (
         |  TYPE = JSON
         |  STRIP_OUTER_ARRAY = $stripOuterArray
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |$matchByColumnName
         |""".stripMargin
    sql
  }

  private def buildCopyXML(domainAndTableName: String): String = {
    val commonOptions = List("STRIP_OUTER_ARRAY", "NULL_IF")
    val extraOptions = copyExtraOptions(commonOptions)
    val stripOuterElement =
      if (extraOptions.toLowerCase().contains("strip_outer_element"))
        ""
      else
        "STRIP_OUTER_ELEMENT=TRUE"

    // $extension is unused here because snowflake auto-detects compression and user is in charge of defining the right pattern'
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM $copySource
         |PATTERN = '$pattern'
         |PURGE = ${purge}
         |FILE_FORMAT = (
         |  TYPE = XML
         |  $extraOptions
         |  $stripOuterElement
         |  $compressionFormat
         |)
         |""".stripMargin
    sql
  }

  private def buildCopyOther(domainAndTableName: String, format: String) = {
    val commonOptions = List("NULL_IF")
    val extraOptions = copyExtraOptions(commonOptions)

    // $extension is unused here because snowflake auto-detects compression and user is in charge of defining the right pattern
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM $copySource
         |PATTERN = '$pattern'
         |PURGE = $purge
         |FILE_FORMAT = (
         |  TYPE = $format
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |""".stripMargin
    sql
  }

  private def buildCopyCsv(domainAndTableName: String): String = {
    val commonOptions = List(
      "SKIP_HEADER",
      "NULL_IF",
      "FIELD_OPTIONALLY_ENCLOSED_BY",
      "FIELD_DELIMITER",
      "ESCAPE_UNENCLOSED_FIELD",
      "ENCODING"
    )
    val extraOptions = copyExtraOptions(commonOptions)

    val fieldOptionallyEnclosedBy =
      getOption("FIELD_OPTIONALLY_ENCLOSED_BY").getOrElse(mergedMetadata.resolveQuote())
    val separator = getOption("FIELD_DELIMITER").getOrElse(mergedMetadata.resolveSeparator())

    /*
    	First argument "\\\\": represents a single backslash in regex.
	    Second argument "\\\\\\\\": each \\ is a single backslash in the result, so this inserts two backslashes.

     */
    val escapeUnEnclosedField =
      getOption("ESCAPE_UNENCLOSED_FIELD")
        .getOrElse(mergedMetadata.resolveEscape())
        .replaceAll("\\\\", "\\\\\\\\")

    // $extension is unused here because snowflake auto-detects compression and user is in charge of defining the right pattern
    val sql =
      s"""
         |COPY INTO $domainAndTableName
         |FROM $copySource
         |PATTERN = '$pattern'
         |PURGE = $purge
         |FILE_FORMAT = (
         |  TYPE = CSV
         |  ERROR_ON_COLUMN_COUNT_MISMATCH = false
         |  SKIP_HEADER = ${skipCount.getOrElse("")}
         |  FIELD_OPTIONALLY_ENCLOSED_BY = '$fieldOptionallyEnclosedBy'
         |  FIELD_DELIMITER = '$separator'
         |  ESCAPE_UNENCLOSED_FIELD = '$escapeUnEnclosedField'
         |  ENCODING = '$encoding'
         |  $nullIf
         |  $extraOptions
         |  $compressionFormat
         |)
         |""".stripMargin
    sql
  }
  def singleStepLoad(
    domain: String,
    table: String,
    schema: SchemaInfo,
    path: List[Path],
    conn: java.sql.Connection
  ): List[Map[String, String]] = {
    val temporary = table.startsWith("zztmp_")
    val sinkConnection = mergedMetadata.getSinkConnection()
    val incomingSparkSchema =
      schema.sparkSchemaWithIgnoreAndScript(schemaHandler, withFinalName = !temporary)
    val domainAndTableName = domain + "." + table
    val optionsWrite =
      new JdbcOptionsInWrite(sinkConnection.jdbcUrl, domainAndTableName, sinkConnection.options)
    val ddlMap = schemaHandler.getDdlMapping(schema.attributes)
    val attrsWithDDLTypes = schemaHandler.getAttributesWithDDLType(schema, "snowflake")

    val tableExists = JdbcDbUtils.tableExists(
      conn,
      sinkConnection.jdbcUrl,
      domainAndTableName,
      sinkConnection.getJdbcEngineName().toString
    )
    JdbcDbUtils.createSchema(conn, domain)
    strategy.getEffectiveType() match {
      case WriteStrategyType.APPEND =>
        if (tableExists) {
          SparkUtils.updateJdbcTableSchema(
            "snowflake",
            conn,
            sinkConnection.options,
            domainAndTableName,
            incomingSparkSchema,
            attrsWithDDLTypes.toMap
          )
        } else {
          SparkUtils.createTable(
            "snowflake",
            conn,
            domainAndTableName,
            incomingSparkSchema,
            caseSensitive = true,
            temporaryTable = temporary,
            optionsWrite,
            ddlMap
          )
        }
      case _ => //  WriteStrategyType.OVERWRITE or first step of other strategies
        JdbcDbUtils.dropTable(conn, domainAndTableName)
        SparkUtils.createTable(
          "snowflake",
          conn,
          domainAndTableName,
          incomingSparkSchema,
          caseSensitive = true,
          temporaryTable = temporary,
          optionsWrite,
          ddlMap
        )
    }
    var res = JdbcDbUtils.executeQueryAsMap(s"USE SCHEMA $domain", conn)
    logger.info(res.toString())
    if (isLocalOrRemotePath) {
      val pathsAsString =
        path
          .map { p =>
            val ps = StorageHandler.localFile(p).pathAsString
            "file://" + ps
          }
      res = JdbcDbUtils.executeQueryAsMap(s"CREATE OR REPLACE TEMPORARY STAGE $tempStage", conn)
      logger.info(res.toString())
      val putSqls =
        pathsAsString.map(path => s"PUT $path @$tempStage/$domain AUTO_COMPRESS = FALSE")
      putSqls.map { putSql =>
        res = JdbcDbUtils.executeQueryAsMap(putSql, conn)
        logger.info(res.toString())
        res
      }
    }
    mergedMetadata.resolveFormat() match {
      case Format.DSV =>
        val sql = buildCopyCsv(domainAndTableName)
        JdbcDbUtils.executeQueryAsMap(sql, conn)

      case Format.JSON_FLAT | Format.JSON =>
        val sql = buildCopyJson(domainAndTableName)
        JdbcDbUtils.executeQueryAsMap(sql, conn)
      case Format.XML =>
        val sql = buildCopyXML(domainAndTableName)
        JdbcDbUtils.executeQueryAsMap(sql, conn)
      case format =>
        val sql = buildCopyOther(domainAndTableName, format.toString.toUpperCase())
        JdbcDbUtils.executeQueryAsMap(sql, conn)
    }
  }
}
