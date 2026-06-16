package ai.starlake.extract.freshness

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.extract.TablesExtractConfig
import ai.starlake.job.sink.bigquery.BigQuerySparkWriter
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.WriteMode
import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult}
import com.typesafe.scalalogging.LazyLogging

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class FreshnessStatus(
  domain: String,
  table: String,
  lastModifiedTime: java.sql.Timestamp,
  timestamp: java.sql.Timestamp,
  duration: Long,
  warnOrError: String,
  database: String,
  tenant: String
)

object FreshnessJob extends LazyLogging {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
  val epochMillisPattern = """^\d+(\.\d+)?$""".r

  private def formatTimestamp(dateString: String): Long = {
    // check if datestring is a decimal number (epoch millis)

    if (epochMillisPattern.matches(dateString)) {
      // check if value is in  millis or seconds
      // get whole part length
      val whole = dateString.split("\\.")(0)
      if (whole.length <= 10) {
        // seconds
        dateString.toDouble.toLong * 1000
      } else
        // millis
        dateString.toDouble.toLong
    } else {
      val localDateTime = LocalDateTime.parse(dateString, formatter)
      val instant = localDateTime.toInstant(ZoneOffset.UTC)
      val millis = instant.toEpochMilli
      millis
    }
  }
  def extractLastModifiedTime(
    config: TablesExtractConfig
  )(implicit settings: Settings): Try[List[(String, List[(String, Long)])]] = {
    val auditDb = settings.appConfig.audit.getDatabase()
    val auditDomain = settings.appConfig.audit.getDomain()
    val auditTable =
      auditDb match {
        case None | Some("") =>
          s"$auditDomain.audit"
        case Some(auditDb) =>
          s"$auditDb.$auditDomain.audit"
      }
    val conn = ConnectionInfo.getConnectionOrDefault(settings.appConfig.audit.sink.connectionRef)
    val quote = conn.getJdbcEngine().map(_.quote).getOrElse("")
    val selectSql =
      s"""
      |SELECT ${quote}DOMAIN${quote}, ${quote}SCHEMA${quote}, ${quote}TIMESTAMP${quote}
      |FROM
      |(
      |   SELECT ${quote}DOMAIN${quote}, ${quote}SCHEMA${quote}, ${quote}TIMESTAMP${quote},
      |       ROW_NUMBER() OVER(PARTITION BY DOMAIN, SCHEMA ORDER BY TIMESTAMP DESC)  AS SL_SEQ
      |   FROM $auditTable
      |)
      |WHERE SL_SEQ = 1
      |
      |
      |""".stripMargin

    // Because QUALIFY is not supported in Postgres
    //  s"""
    //     |SELECT ${quote}DOMAIN${quote}, ${quote}SCHEMA${quote}, ${quote}TIMESTAMP${quote}
    //       |FROM $auditTable
    //   |QUALIFY ROW_NUMBER() OVER (PARTITION BY DOMAIN, SCHEMA ORDER BY TIMESTAMP DESC) = 1
    //   |""".stripMargin

    val rows: Try[List[List[(String, Any)]]] =
      AutoTask
        .executeSelect(
          "__ignore__",
          "__ignore__",
          selectSql,
          summarizeOnly = false,
          settings.appConfig.audit.sink.getConnectionRef(),
          None,
          test = false,
          parseSQL = false,
          pageSize = 1000000,
          pageNumber = 1,
          scheduledDate = None // No scheduled date for validate command
        )(settings, settings.storageHandler(), settings.schemaHandler())

    rows.map { rows =>
      // convert result to (domain, table, lastModifiedTime)
      val freshnesses =
        rows.map { cols =>
          val domain = cols(0)._2.toString
          val table = cols(1)._2.toString
          val timestamp = formatTimestamp(cols(2)._2.toString())
          (domain, table, timestamp)
        }

      // filter on config.tables if not empty
      val filteredFreshnesses =
        if (config.tables.isEmpty)
          freshnesses
        else {
          val configTables = config.tables.flatMap { case (domain, tables) =>
            tables.map { table =>
              s"$domain.$table"
            }
          }.toList
          freshnesses.filter { case (domain, table, _) =>
            configTables.exists(it => it.equalsIgnoreCase(s"$domain.$table"))
          }
        }

      // group by domain
      filteredFreshnesses
        .groupBy(_._1)
        .map { case (domain, tables) =>
          val tableInfos = tables.map { case (_, table, lastModifiedTime) =>
            (table, lastModifiedTime)
          }
          (domain, tableInfos)
        }
        .toList
    }
  }

  def freshness(
    config: TablesExtractConfig,
    schemaHandler: SchemaHandler
  )(implicit mySettings: Settings): List[FreshnessStatus] = {
    val tables = extractLastModifiedTime(config) match {
      case Success(tables) => tables
      case Failure(exception) =>
        throw new Exception("Could not extract tables freshness", exception)
    }
//    val conn = ConnectionInfo.getConnectionOrDefault(config.connectionRef)
//    val tables: List[(String, List[(String, Long)])] = {
//      conn.getJdbcEngineName() match {
//        case Engine.BQ =>
//          BigQueryTableInfo.extractLastModifiedTime(config)
//        case Engine.SNOWFLAKE =>
//          new JdbcTableInfo().extractLastModifiedTime(config)
//        case _ =>
//          throw new IllegalArgumentException(
//            s"Unsupported connection type: ${conn.getJdbcEngineName()}. Only 'bigquery' & Snowflake are supported."
//          )
//      }
//
//    }
    val domains = schemaHandler.domains()
    val tablesFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val matchingDomains = domains.filter(_.finalName.equalsIgnoreCase(dsInfo))
      if (matchingDomains.isEmpty) Nil
      else {
        tableInfos.flatMap { case (tableName, lastModifiedTime) =>
          val domainAndTable = matchingDomains.iterator
            .flatMap(d => d.tables.find(_.finalName.equalsIgnoreCase(tableName)).map(t => (d, t)))
            .nextOption()
          domainAndTable match {
            case None => None
            case Some((domain, table)) =>
              val freshness =
                table.metadata.flatMap(_.freshness).orElse(domain.metadata.flatMap(_.freshness))
              freshness match {
                case None =>
                  Some(
                    FreshnessStatus(
                      domain.finalName,
                      table.finalName,
                      new Timestamp(lastModifiedTime),
                      new Timestamp(System.currentTimeMillis()),
                      0L,
                      "INFO",
                      schemaHandler.getDatabase(domain).getOrElse(""),
                      mySettings.appConfig.tenant
                    )
                  )

                case Some(freshness) =>
                  val errorStatus =
                    getFreshnessStatus(
                      schemaHandler.getDatabase(domain).getOrElse(""),
                      domain.finalName,
                      table.finalName,
                      lastModifiedTime,
                      freshness.error,
                      "ERROR",
                      "TABLE"
                    )

                  errorStatus
                    .orElse {
                      getFreshnessStatus(
                        schemaHandler.getDatabase(domain).getOrElse(""),
                        domain.finalName,
                        table.finalName,
                        lastModifiedTime,
                        freshness.warn,
                        "WARN",
                        "TABLE"
                      )
                    }
                    .orElse(
                      Some(
                        FreshnessStatus(
                          domain.finalName,
                          table.finalName,
                          new Timestamp(lastModifiedTime),
                          new Timestamp(System.currentTimeMillis()),
                          0L,
                          "INFO",
                          schemaHandler.getDatabase(domain).getOrElse(""),
                          mySettings.appConfig.tenant
                        )
                      )
                    )
              }
          }
        }
      }
    }
    val tasks = schemaHandler.tasks()
    val jobsFreshnessStatuses = tables.flatMap { case (dsInfo, tableInfos) =>
      val task = tasks
        .find(_.domain.equalsIgnoreCase(dsInfo))
      task match {
        case None => Nil
        case Some(task) =>
          val tableInfo = tableInfos.find(_._1.equalsIgnoreCase(task.table))
          tableInfo match {
            case None =>
              None
            case Some((tableName, lastModifiedTime)) =>
              val freshness = task.freshness
              freshness match {
                case None =>
                  Some(
                    FreshnessStatus(
                      task.domain,
                      task.table,
                      new Timestamp(lastModifiedTime),
                      new Timestamp(System.currentTimeMillis()),
                      0L,
                      "INFO",
                      task.database.getOrElse(mySettings.appConfig.database),
                      mySettings.appConfig.tenant
                    )
                  )

                case Some(freshness) =>
                  val errorStatus =
                    getFreshnessStatus(
                      task.database.getOrElse(mySettings.appConfig.database),
                      task.domain,
                      task.table,
                      lastModifiedTime,
                      freshness.error,
                      "ERROR",
                      "TASK"
                    )
                  errorStatus
                    .orElse {
                      getFreshnessStatus(
                        task.database.getOrElse(mySettings.appConfig.database),
                        task.domain,
                        task.table,
                        lastModifiedTime,
                        freshness.warn,
                        "WARN",
                        "TASK"
                      )
                    }
                    .orElse(
                      Some(
                        FreshnessStatus(
                          task.domain,
                          task.table,
                          new Timestamp(lastModifiedTime),
                          new Timestamp(System.currentTimeMillis()),
                          0L,
                          "INFO",
                          task.database.getOrElse(mySettings.appConfig.database),
                          mySettings.appConfig.tenant
                        )
                      )
                    )
              }
          }
      }
    }
    val statuses = tablesFreshnessStatuses ++ jobsFreshnessStatuses

    if (config.persist) {
      val job = new SparkJob {
        override def name: String = "BigQueryFreshnessInfo"

        override implicit def settings: Settings = mySettings

        /** Just to force any job to implement its entry point using within the "run" method
          *
          * @return
          *   : Spark Dataframe for Spark Jobs None otherwise
          */
        override def run(): Try[JobResult] = Try {
          val dfDataset = session.createDataFrame(statuses)
          SparkJobResult(Option(dfDataset), None)
        }
      }

      val jobResult = job.run()
      jobResult match {
        case scala.util.Success(SparkJobResult(Some(dfDataset), _)) =>
          BigQuerySparkWriter.sinkInAudit(
            dfDataset,
            "freshness_info",
            Some("Information related to table freshness"),
            Some(BigQuerySchemaConverters.toBigQuerySchema(dfDataset.schema)),
            config.writeMode.getOrElse(WriteMode.APPEND),
            accessToken = config.accessToken
          )
        case scala.util.Success(_) =>
          logger.warn("Could not extract BigQuery tables info")
        case scala.util.Failure(exception) =>
          throw new Exception("Could not extract BigQuery tables info", exception)
      }

    }
    statuses
  }

  private def getFreshnessStatus(
    domainDatabaseName: String,
    domainName: String,
    tableName: String,
    lastModifiedTime: Long,
    duration: Option[String],
    level: String,
    typ: String
  )(implicit settings: Settings): Option[FreshnessStatus] = {
    duration match {
      case None => None
      case Some(duration) =>
        val warnOrErrorDuration = Duration(duration).toMillis
        val now = System.currentTimeMillis()
        if (now - warnOrErrorDuration > lastModifiedTime)
          Some(
            FreshnessStatus(
              domainName,
              tableName,
              new Timestamp(lastModifiedTime),
              new Timestamp(now),
              warnOrErrorDuration,
              level,
              domainDatabaseName,
              settings.appConfig.tenant
            )
          )
        else
          None
    }
  }

  @nowarn
  def run(args: Array[String], schemaHandler: SchemaHandler): Try[Unit] = {
    FreshnessExtractCmd.parse(args) match {
      case Some(config) =>
        implicit val settings: Settings =
          Settings(Settings.referenceConfig, None, None, None, None)
        FreshnessExtractCmd.run(config, schemaHandler).map(_ => ())
      case None =>
        Try(throw new IllegalArgumentException(FreshnessExtractCmd.usage()))
    }
  }
}
