package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.job.common.TaskSQLStatements
import ai.starlake.job.transform.TransformContext
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.RichFormatter
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.Instant
import java.util.regex.{Matcher, Pattern}
import scala.util.{Failure, Success, Try}

case class ExpectationReport(
  jobId: String,
  database: Option[String],
  domain: String,
  schema: String,
  timestamp: Timestamp,
  name: String,
  params: String,
  sql: Option[String],
  count: Option[Long],
  exception: Option[String],
  success: Boolean
) {

  def asMap(): Map[String, Any] = {
    Map(
      "jobid"     -> jobId,
      "database"  -> database.getOrElse(""),
      "domain"    -> domain,
      "schema"    -> schema,
      "timestamp" -> timestamp,
      "name"      -> name,
      "params"    -> params,
      "sql"       -> sql.getOrElse(""),
      "count"     -> count.getOrElse(0),
      "exception" -> exception.getOrElse(""),
      "success"   -> success
    )
  }

  override def toString: String = {
    s"""name: $name, params:$params, count:${count.getOrElse(
        0
      )}, success:$success, message: ${exception.getOrElse("")}, sql:$sql""".stripMargin
  }

  def asSelect(engineName: Engine)(implicit settings: Settings): String = {
    import ai.starlake.utils.Formatter.*
    timestamp.setNanos(0)
    val template = ExpectationJob.selectTemplate(engineName)
    def replaceQuote(s: String): String =
      s.replaceAll("'", "\"").replaceAll("\n", " ").replaceAll("\\{\\{", "").replaceAll("}}", "")
    val mapParam =
      Map(
        "jobid"     -> jobId,
        "database"  -> database.getOrElse(""),
        "domain"    -> domain,
        "schema"    -> schema,
        "timestamp" -> timestamp.toString(),
        "name"      -> replaceQuote(name),
        "params"    -> replaceQuote(params),
        "sql"       -> replaceQuote(sql.getOrElse("")),
        "count"     -> count.getOrElse(0L).toString,
        "exception" -> replaceQuote(exception.getOrElse("")),
        "success"   -> success.toString
      )
    val selectStatement = template.richFormat(mapParam, Map.empty)
    selectStatement
  }
}

object ExpectationReport {
  val starlakeSchema = SchemaInfo(
    name = "expectations",
    pattern = Pattern.compile("ignore"),
    attributes = List(
      TableAttribute("jobid", "string"),
      TableAttribute("database", "string"),
      TableAttribute("domain", "string"),
      TableAttribute("schema", "string"),
      TableAttribute("timestamp", "timestamp"),
      TableAttribute("query", "string"),
      TableAttribute("expect", "string"),
      TableAttribute("sql", "string"),
      TableAttribute("count", "long"),
      TableAttribute("exception", "string"),
      TableAttribute("success", "boolean")
    ),
    None,
    None
  )
}

/** Record expectation execution
  */

/** @param domain
  *   : Domain name
  * @param schema
  *   : Schema
  * @param stage
  *   : stage
  * @param storageHandler
  *   : Storage Handler
  */
class ExpectationJob(
  appId: Option[String],
  database: Option[String],
  domainName: String,
  schemaName: String,
  expectations: List[ExpectationItem],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  sqlRunner: ExpectationAssertionHandler,
  interactive: Boolean,
  transpileSql: String => String = identity,
  variables: Map[String, Any] = Map.empty
)(implicit val settings: Settings)
    extends SparkJob {

  private var _failOnError: Boolean = false

  def failOnError(): Boolean = _failOnError

  override def name: String = "Check Expectations"

  override def applicationId(): String = this.appId.getOrElse(this.applicationId())

  def lockPath(path: String): Path = {
    new Path(
      settings.appConfig.lock.path,
      "expectations" + path
        .replace("{{domain}}", domainName)
        .replace("{{schema}}", schemaName)
        .replace(":", "_")
        .replace('/', '_') + ".lock"
    )
  }

  override def run(): Try[JobResult] = {
    val expectationReports: List[ExpectationReport] = runExpectations()
    sinkAndCheck(expectationReports, this.failOnError())
  }

  /** Sinks the reports of expectations, and fails under `expectations.failOnError` when one of them
    * failed and declared `failOnError`
    *
    * @param failed
    *   true when an expectation declared `failOnError` failed
    */
  def sinkAndCheck(expectationReports: List[ExpectationReport], failed: Boolean): Try[JobResult] = {
    val result = sinkExpectationReports(expectationReports)

    val failedCount = expectationReports.count(!_.success)
    if (settings.appConfig.expectations.failOnError && failed) {
      Failure(new Exception(s"$failedCount Expectations failed"))
    } else {
      result
    }
  }

  /** The variables of the macros: the active environment, `sl_engine`, the engine that runs the
    * expectation queries, and the variables of the job, such as the counters of a load
    */
  private def macroVars(): Map[String, Any] =
    schemaHandler.activeEnvVars() ++ sqlRunner.engineName.map("sl_engine" -> _) ++ variables

  private lazy val fullTableName = database match {
    case Some(db) => s"$db.$domainName.$schemaName"
    case None     => s"$domainName.$schemaName"
  }

  /** @return
    *   the query of an expectation, on the table of the job, or the failure to render it
    */
  private lazy val macros = SQLUtils.stripComments(schemaHandler.allMacros)

  def renderQuery(expectation: ExpectationItem): Try[String] = Try {
    val expectationWithMacroDefinitions =
      List(macros, ExpectationJob.callOnTable(expectation.queryCall(), fullTableName))
        .mkString("\n")
    val sql =
      Utils.parseJinja(
        expectationWithMacroDefinitions,
        macroVars()
      )
    transpileSql(
      if (sql.contains("sl_this")) {
        sql.replaceAll("(?i)sl_this", fullTableName)
      } else
        sql
    )
  }

  /** @param sql
    *   the query of the expectation, if it could be rendered
    * @param result
    *   the value that its query returned, or the failure to render or run it
    * @return
    *   the report of an expectation, which succeeds when its query returned 0
    */
  def report(
    expectation: ExpectationItem,
    sql: Option[String],
    result: Try[Int]
  ): ExpectationReport = {
    val success = result.toOption.contains(0)
    if (!success && expectation.failOnError) _failOnError = true
    ExpectationReport(
      applicationId(),
      database,
      domainName,
      schemaName,
      Timestamp.from(Instant.now()),
      expectation.queryCall(),
      expectation.expect,
      // A query that cannot be rendered has no SQL
      result match {
        case Failure(_: IllegalArgumentException) => None
        case _                                    => sql
      },
      result.toOption.map(_.toLong),
      result.failed.toOption.map(Utils.exceptionAsString),
      success = success
    )
  }

  def runExpectations(): List[ExpectationReport] = {
    expectations.map { expectation =>
      // An expectation that cannot be rendered is reported as one whose query cannot run
      val renderedSql: Try[String] = renderQuery(expectation)
      val result = renderedSql.flatMap { sqlWithSlThis =>
        Try {
          logger.info(
            s"Applying expectation: ${expectation.expect} with request $sqlWithSlThis"
          )
          sqlRunner.handle(sqlWithSlThis)
        }
      }
      report(expectation, renderedSql.toOption, result)
    }
  }

  private def sinkExpectationReports(
    expectationReports: List[ExpectationReport]
  ): Try[JobResult] = {
    if (expectationReports.nonEmpty && !interactive) {
      expectationReports.foreach(r => logger.info(r.toString))
      val auditSink = settings.appConfig.audit.getSink()
      auditSink.getConnectionType() match {
        case ConnectionType.GCPLOG =>
          val logName = settings.appConfig.audit.getDomainExpectation()
          GcpUtils.sinkToGcpCloudLogging(
            expectationReports.map(_.asMap()),
            "expectation",
            logName
          )
          Success(new JobResult {})
        case _ =>
          val sqls = expectationReports
            .map(
              _.asSelect(
                settings.appConfig.audit.sink.getSink().getConnection().getJdbcEngineName()
              )
            )
            .mkString("", " UNION ALL ", "")
          val taskDesc = AutoTaskInfo(
            name = applicationId(),
            sql = Some(sqls),
            database = settings.appConfig.audit.getDatabase(),
            domain = settings.appConfig.audit.getDomain(),
            table = "expectations",
            presql = Nil,
            postsql = Nil,
            connectionRef = settings.appConfig.audit.sink.connectionRef,
            sink = Some(settings.appConfig.audit.sink),
            parseSQL = Some(true),
            _auditTableName = Some("expectations")
          )
          val engine =
            if (taskDesc.getSinkConnection().isJdbcUrl())
              Engine.JDBC
            else
              taskDesc.getSinkConnection().getEngine()
          val transformContext = TransformContext(
            appId = Option(applicationId()),
            taskDesc = taskDesc,
            commandParameters = Map.empty,
            interactive = None,
            truncate = false,
            test = false,
            logExecution = false,
            accessToken = None,
            resultPageSize = 200,
            resultPageNumber = 1,
            dryRun = false,
            scheduledDate = None, // No scheduled date for expectations jobs
            syncSchema = false
          )(settings, storageHandler, schemaHandler)
          val task = transformContext.toTask(engine)
          val res = task.run()
          Utils.logFailure(res, logger)
      }
    } else
      Success(SparkJobResult(None, None))
  }

  def buildStatementsList(): Try[List[ExpectationSQL]] = Try {
    val fullTableName = database match {
      case Some(db) => s"$db.$domainName.$schemaName"
      case None     => s"$domainName.$schemaName"
    }
    val bqSlThisCTE = s"WITH SL_THIS AS (SELECT * FROM $fullTableName)\n"

    val macros = schemaHandler.allMacros
    val sqls = expectations.map { expectation =>
      val expectationWithMacroDefinitions =
        List(macros, ExpectationJob.callOnTable(expectation.queryCall(), fullTableName))
          .mkString("\n")
      val sql = bqSlThisCTE +
        Utils.parseJinja(
          expectationWithMacroDefinitions,
          macroVars()
        )
      logger.info(
        s"Applying expectation: ${expectation.expect} with request $sql"
      )
      ExpectationSQL(expectation, sql)
    }
    sqls
  }
}

object ExpectationJob {

  /** A macro call where a quoted `sl_this` argument is already the table of the expectation, so
    * that the macro can read its name: the schema macros look the table up in the catalog of
    * BigQuery. `sl_this` is still replaced in the rendered query.
    */
  def callOnTable(queryCall: String, fullTableName: String): String =
    queryCall.replaceAll(
      """(?i)(['"])sl_this\1""",
      "$1" + Matcher.quoteReplacement(fullTableName) + "$1"
    )

  def apply(
    appId: Option[String],
    database: Option[String],
    domainName: String,
    schemaName: String,
    expectations: List[ExpectationItem],
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler,
    sqlRunner: ExpectationAssertionHandler,
    interactive: Boolean,
    transpileSql: String => String = identity,
    variables: Map[String, Any] = Map.empty
  )(implicit settings: Settings): ExpectationJob = {
    new ExpectationJob(
      appId,
      database,
      domainName,
      schemaName,
      expectations,
      storageHandler,
      schemaHandler,
      sqlRunner,
      interactive,
      transpileSql,
      variables
    )
  }
  def buildSQLStatements()(implicit settings: Settings): Option[TaskSQLStatements] = {
    if (settings.appConfig.expectations.active) {
      val auditSink = settings.appConfig.audit.getSink()
      val templateSelect = selectTemplate(auditSink.getConnection().getJdbcEngineName())
      val templateCreate = createTemplate(auditSink.getConnection().getJdbcEngineName())
      Some(
        TaskSQLStatements(
          name = "audit.expectations",
          domain = settings.appConfig.audit.getDomain(),
          table = "expectations",
          createSchemaSql = List(templateCreate.pyFormat()),
          preActions = Nil,
          preSqls = Nil,
          mainSqlIfExists = List(templateSelect.pyFormat()),
          mainSqlIfNotExists = null,
          postSqls = Nil,
          addSCD2ColumnsSqls = Nil,
          targetSchema = Nil, // We do not update the expectations schema
          syncStrategy = Some(TableSync.NONE),
          settings.appConfig.audit.getSink().getConnection().`type`
        )
      )
    } else
      None
  }

  def selectTemplate(engineName: Engine)(implicit settings: Settings): String = {
    val template = settings.appConfig.jdbcEngines
      .get(engineName.toString.toLowerCase())
      .flatMap(_.tables("expectations").selectSql)
      .getOrElse("""
          SELECT
            '{{jobid}}' AS JOBID,
            '{{database}}' AS DATABASE,
            '{{domain}}' AS DOMAIN,
            '{{schema}}' AS SCHEMA,
            TO_TIMESTAMP('{{timestamp}}', 'YYYY-MM-DD HH24:MI:SS') AS TIMESTAMP,
            '{{name}}' AS NAME,
            '{{params}}' AS PARAMS,
            '{{sql}}' AS SQL,
            {{count}} AS COUNT,
            '{{exception}}' AS EXCEPTION,
            {{success}} AS SUCCESS
           """)
    template
  }

  def createTemplate(engineName: Engine)(implicit settings: Settings): String = {
    val template = settings.appConfig.jdbcEngines
      .get(engineName.toString.toLowerCase())
      .map(_.tables("expectations").createSql)
      .getOrElse("""
            CREATE TABLE IF NOT EXISTS {{table}} (
                            JOBID TEXT NOT NULL,
                            DATABASE TEXT,
                            DOMAIN TEXT NOT NULL,
                            SCHEMA TEXT NOT NULL,
                            TIMESTAMP TIMESTAMP NOT NULL,
                            NAME TEXT NOT NULL,
                            PARAMS TEXT NOT NULL,
                            SQL TEXT NOT NULL,
                            COUNT BIGINT NOT NULL,
                            EXCEPTION TEXT NOT NULL,
                            SUCCESS BOOLEAN NOT NULL
                          )
""")
    template.richFormat(
      Map(
        "table" -> (settings.appConfig.audit.getDomain() + ".expectations")
      ),
      Map.empty
    )
  }
}
