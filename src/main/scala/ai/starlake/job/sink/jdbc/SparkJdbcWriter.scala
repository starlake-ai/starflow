package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.config.{Settings, SparkSessionBuilder}
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.ConnectionType
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

import scala.util.Try

/** Used exclusively by the `cnxload` CLI command to load files (Parquet/CSV/JSON) into a JDBC
  * table. Call chain: CLI `starlake cnxload` → JdbcConnectionLoadCmd.run() →
  * IngestionWorkflow.jdbcload() → SparkJdbcWriter.run()
  *
  * This is NOT part of the ingestion or transform pipelines which use DuckDbNativeLoader,
  * SparkAutoTask.sinkToJDBC() or JdbcAutoTask.runJDBC() instead.
  *
  * For DuckDB targets, data is written to a temporary Parquet file first, then loaded into DuckDB
  * via `INSERT INTO ... SELECT ... FROM '*.parquet'` to avoid Spark JDBC driver issues.
  */
class SparkJdbcWriter(
  cliConfig: JdbcConnectionLoadConfig
)(implicit val settings: Settings)
    extends SparkJob {

  override def name: String = s"cnxload-JDBC-${cliConfig.outputDomainAndTableName}"

  val conf: Configuration = {
    require(!SparkSessionBuilder.isSparkConnectActive, "JDBC is not supported with Spark Connect")
    session.sparkContext.hadoopConfiguration
  }
  logger.info(s"JDBC Config $cliConfig")

  val jdbcOptions =
    JdbcDbUtils.jdbcOptions(cliConfig.options, cliConfig.format, cliConfig.accessToken)

  private def isFile(): Boolean = cliConfig.sourceFile.isLeft

  def runJDBC(): Try[SparkJobResult] = {
    val inputPath = cliConfig.sourceFile
    logger.info(s"Input path $inputPath")

    Try {
      val sourceDF = {
        inputPath match {
          case Left(path) => session.read.format(settings.appConfig.defaultWriteFormat).load(path)
          case Right(df)  => df
        }
      }
      val outputDomain = cliConfig.outputDomainAndTableName.split("\\.")(0)
      val url = jdbcOptions("url")
      val connectionInfo = ConnectionInfo(ConnectionType.JDBC, options = jdbcOptions)
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcOptions) { conn =>
        val tableExists = JdbcDbUtils.tableExists(conn, url, cliConfig.outputDomainAndTableName)
        if (!tableExists && settings.appConfig.createSchemaIfNotExists) {
          logger.info(s"table ${cliConfig.outputDomainAndTableName} not found, trying to create it")
          JdbcDbUtils.createSchema(conn, outputDomain)
        } else {
          logger.info(s"Schema $outputDomain found")
        }
        val schema = sourceDF.schema
        if (SparkUtils.isFlat(schema) && tableExists) {
          val existingSchema =
            SparkUtils.getSchemaOption(conn, jdbcOptions, cliConfig.outputDomainAndTableName)
          val addedSchema = SparkUtils.added(schema, existingSchema.getOrElse(schema))
          val deletedSchema = SparkUtils.dropped(schema, existingSchema.getOrElse(schema))
          val alterTableDropColumns =
            SparkUtils.alterTableDropColumnsString(
              connectionInfo.getJdbcEngineName().toString,
              deletedSchema,
              cliConfig.outputDomainAndTableName
            )
          if (alterTableDropColumns.nonEmpty) {
            logger.info(
              s"alter table ${cliConfig.outputDomainAndTableName} with ${alterTableDropColumns.size} columns to drop"
            )
            logger.debug(s"alter table ${alterTableDropColumns.mkString("\n")}")
          }
          val alterTableAddColumns =
            SparkUtils.alterTableAddColumnsString(
              connectionInfo.getJdbcEngineName().toString,
              addedSchema,
              cliConfig.outputDomainAndTableName,
              Map.empty
            )

          if (alterTableAddColumns.nonEmpty) {
            logger.info(
              s"alter table ${cliConfig.outputDomainAndTableName} with ${alterTableAddColumns.size} columns to add"
            )
            logger.debug(s"alter table ${alterTableAddColumns.mkString("\n")}")
          }
          alterTableDropColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
          alterTableAddColumns.foreach(JdbcDbUtils.executeAlterTable(_, conn))
        } else {
          val optionsWrite =
            new JdbcOptionsInWrite(url, cliConfig.outputDomainAndTableName, jdbcOptions)
          logger.info(
            s"Table ${cliConfig.outputDomainAndTableName} not found, creating it with schema $schema"
          )
          SparkUtils.createTable(
            connectionInfo.getJdbcEngineName().toString,
            conn,
            cliConfig.outputDomainAndTableName,
            schema,
            caseSensitive = false,
            temporaryTable = false,
            optionsWrite,
            Map.empty // No mapping because we are loading using the CLI
          )
        }
      }
      val isDuckDb = url.contains("jdbc:duckdb")

      // We always append to the table to keep the schema (Spark loose the schema otherwise). We truncate using the truncate query option
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcOptions) { conn =>
        JdbcDbUtils.truncateTable(conn, cliConfig.outputDomainAndTableName)
      }

      if (isDuckDb) {
        val timestamp = java.time.LocalDateTime
          .now()
          .format(
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
          )
        val tablePath =
          new Path(
            s"${settings.appConfig.datasets}/tmp/${cliConfig.outputDomainAndTableName}_$timestamp"
          )
        if (settings.storageHandler().exists(tablePath)) {
          settings.storageHandler().delete(tablePath)
        }
        val colNames = sourceDF.schema.fields.map(_.name)
        sourceDF.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save(tablePath.toString)
        JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcOptions) { conn =>
          val quotedColumns = SQLUtils.quoteCols(colNames.toList, "\"")
          val sql =
            s"INSERT INTO ${cliConfig.outputDomainAndTableName} SELECT ${quotedColumns.mkString(",")} FROM '$tablePath/*.parquet'"
          JdbcDbUtils.executeUpdate(sql, conn)
        }
        settings.storageHandler().delete(tablePath)
      } else {
        val dfw = sourceDF.write
          .format("jdbc")
          .option("dbtable", cliConfig.outputDomainAndTableName)
        dfw
          .mode(SaveMode.Append)
          .options(cliConfig.options)
          .save()
      }

      logger.info(
        s"JDBC save done to table ${cliConfig.outputDomainAndTableName} at ${cliConfig.options}"
      )
      SparkJobResult(None, None)
    }
  }

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  override def run(): Try[JobResult] = {
    val res = runJDBC()
    Utils.logFailure(res, logger)
  }
}
