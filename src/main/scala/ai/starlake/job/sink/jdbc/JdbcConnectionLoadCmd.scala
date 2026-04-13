package ai.starlake.job.sink.jdbc

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{ConnectionType, WriteStrategy, WriteStrategyType}
import ai.starlake.utils.Formatter.RichFormatter
import ai.starlake.utils.{JobResult, Utils}
import org.apache.spark.sql.DataFrame
import scopt.OParser

import java.sql.{DriverManager, SQLException}
import scala.util.{Failure, Success, Try}

/** Command to load parquet file into a JDBC Table.
  *
  * Usage: starlake cnxload [options]
  */
object JdbcConnectionLoadCmd extends Cmd[JdbcConnectionLoadConfig] {

  val command = "cnxload"

  override def pageDescription: String =
    "Load a parquet source file into a JDBC table with configurable connection options and write strategies like APPEND or OVERWRITE."
  override def pageKeywords: Seq[String] =
    Seq("starlake cnxload", "JDBC load", "database import", "connection load")

  def checkTablePresent(
    jdbcOptions: Settings.ConnectionInfo,
    jdbcEngine: Settings.JdbcEngine,
    outputDomainAndTablename: String
  )(implicit settings: Settings): Unit = {
    require(
      jdbcOptions.`type` == ConnectionType.JDBC,
      s"Only JDBC connections are supported ${jdbcOptions.`type`}"
    )

    val table = jdbcEngine.tables.get(outputDomainAndTablename.split('.').last)
    table.foreach { table =>
      Utils.withResources(
        DriverManager.getConnection(
          jdbcOptions.options("url"),
          jdbcOptions.options("user"),
          jdbcOptions.options("password")
        )
      ) { conn =>
        Try {
          val stmt = conn.createStatement
          Try {
            val pingSql = table.effectivePingSql(outputDomainAndTablename)
            val rs = stmt.executeQuery(pingSql)
            rs.close() // we don't need to fetch the result, it should be empty anyway.
          } match {
            case Failure(e) if e.isInstanceOf[SQLException] =>
              stmt.executeUpdate(
                table.createSql.richFormat(
                  Map(
                    "table"       -> outputDomainAndTablename,
                    "writeFormat" -> settings.appConfig.defaultWriteFormat
                  ),
                  Map.empty
                )
              )
              conn.commit() // some databases are transactional wrt schema updates
            case Failure(e) => throw e
            case Success(_) => ;
          }
          stmt.close()
        }
      }
    }
  }

  def fromComet(
    connectionRef: String,
    comet: Settings.AppConfig,
    sourceFile: Either[String, DataFrame],
    outputTable: String,
    strategy: WriteStrategy,
    createTableIfAbsent: Boolean
  )(implicit settings: Settings): JdbcConnectionLoadConfig = {
    // TODO: wanted to just call this "apply" but we need to get rid of the defaults in the ctor above

    val starlakeConnection = comet.connections(connectionRef)
    val jdbcEngineName = starlakeConnection.getJdbcEngineName()
    if (createTableIfAbsent)
      comet.jdbcEngines.get(jdbcEngineName.toString).foreach { jdbcEngine =>
        checkTablePresent(starlakeConnection, jdbcEngine, outputTable)
      }

    // This is to make sure that the column names are uppercase on JDBC databases
    // TODO: Once spark 3.3 is not supported anymore, switch to withColumnsRenamed(colsMap: Map[String, String])
    val dfWithUppercaseColumns = sourceFile.map { df =>
      df.columns.foldLeft(df) { case (df, colName) =>
        df.withColumnRenamed(colName, colName.toUpperCase())
      }
    }

    JdbcConnectionLoadConfig(
      sourceFile = dfWithUppercaseColumns,
      outputDomainAndTableName = outputTable.toUpperCase(),
      strategy = strategy,
      starlakeConnection.sparkDatasource().getOrElse("jdbc"),
      starlakeConnection.options
    )
  }

  val parser: OParser[Unit, JdbcConnectionLoadConfig] = {
    val builder = OParser.builder[JdbcConnectionLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note("""
             |Load Parquet, CSV, or JSON files directly into a JDBC-connected database table. Supports APPEND and OVERWRITE write strategies with configurable connection options.
             |
             |Load parquet file into JDBC Table.
             |""".stripMargin),
      builder
        .opt[String]("source_file")
        .action((x, c) => c.copy(sourceFile = Left(x)))
        .text("Full Path to source file")
        .required(),
      builder
        .opt[String]("output_table")
        .action((x, c) => c.copy(outputDomainAndTableName = x))
        .text("JDBC Output Table")
        .required(),
      builder
        .opt[Map[String, String]]("options")
        .action((x, c) => c.copy(options = x))
        .text(
          "Connection options eq for jdbc : driver, user, password, url, partitions, batchSize"
        ),
      builder
        .opt[String]("write_strategy")
        .action((x, c) => c.copy(strategy = WriteStrategy(Some(WriteStrategyType(x)))))
        .text(
          "One of the write strategies: APPEND, OVERWRITE (see strategy types)"
        ),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  // comet bqload  --source_file xxx --output_table schema --source_format parquet --create_disposition  CREATE_IF_NEEDED --write_disposition WRITE_TRUNCATE
  //               --partitions 1  --batch_size 1000 --user username --password pwd -- url jdbcurl
  def parse(args: Seq[String]): Option[JdbcConnectionLoadConfig] =
    OParser.parse(parser, args, JdbcConnectionLoadConfig(), setup)

  override def run(config: JdbcConnectionLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] =
    workflow(schemaHandler).jdbcload(config).map(_ => JobResult.empty)
}
