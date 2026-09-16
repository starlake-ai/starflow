package ai.starlake.sql

import ai.starlake.config.Settings
import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.schema.handlers.TableWithNameAndType
import ai.starlake.schema.model._
import ai.starlake.transpiler.{JSQLColumResolver, JSQLTranspiler}
import ai.starlake.utils.Utils
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.statement.Statement

import java.util.regex.{Matcher, Pattern}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** SQL utilities. Parsing and formatting methods are delegated to SqlParser and SqlFormatter.
  */
object SQLUtils extends LazyLogging {

  // Re-export regex patterns for backward compatibility
  val fromsRegex: Regex = SqlParser.fromsRegex
  val joinRegex: Regex = SqlParser.joinRegex

  // --- Delegated to SqlParser ---
  def extractTableNamesUsingRegEx(sql: String): List[String] =
    SqlParser.extractTableNamesUsingRegEx(sql)
  def extractColumnNames(sql: String): List[String] = SqlParser.extractColumnNames(sql)
  def extractTableNames(sql: String): List[String] = SqlParser.extractTableNames(sql)
  def extractCTENames(sql: String): List[String] = SqlParser.extractCTENames(sql)
  def extractInputTableNames(sql: String): List[String] =
    SqlParser.extractInputTableNames(sql)
  def extractCTENamesUsingRegEx(sql: String): List[String] =
    SqlParser.extractCTENamesUsingRegEx(sql)

  /** Tables read by the statement, degrading to the regex extractor when the SQL does not parse
    * (unresolved Jinja, dialect specific syntax jsqlparser does not know, ...). The regex path
    * cannot resolve CTE references, so CTE names are subtracted explicitly there.
    */
  def extractInputTableNamesWithFallback(sql: String): List[String] =
    Try(SqlParser.extractInputTableNames(sql)) match {
      case Success(tables) => tables
      case Failure(e) =>
        logger.info(
          s"Could not parse SQL, falling back to regex based table extraction: ${e.getMessage}"
        )
        val cteNames = SqlParser.extractCTENamesUsingRegEx(sql)
        SqlParser
          .extractTableNamesUsingRegEx(sql)
          .filterNot(table => cteNames.exists(_.equalsIgnoreCase(table)))
    }
  def jsqlParse(sql: String): Statement = SqlParser.jsqlParse(sql)
  def getSelectStatementIndex(sql: String): Int = SqlParser.getSelectStatementIndex(sql)

  // --- Delegated to SqlFormatter ---
  def format(input: String, outputFormat: JSQLFormatter.OutputFormat): String =
    SqlFormatter.format(input, outputFormat)
  def stripComments(sql: String): String = SqlFormatter.stripComments(sql)
  def temporaryTableName(tableName: String): String = SqlFormatter.temporaryTableName(tableName)
  def quoteCols(cols: List[String], quote: String): List[String] =
    SqlFormatter.quoteCols(cols, quote)
  def unquoteCols(cols: List[String], quote: String): List[String] =
    SqlFormatter.unquoteCols(cols, quote)
  def unquoteAgressive(cols: List[String]): List[String] = SqlFormatter.unquoteAgressive(cols)
  def unquoteAgressive(col: String): String = SqlFormatter.unquoteAgressive(col)
  def targetColumnsForSelectSql(targetTableColumns: List[String], quote: String): String =
    SqlFormatter.targetColumnsForSelectSql(targetTableColumns, quote)
  def incomingColumnsForSelectSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String = SqlFormatter.incomingColumnsForSelectSql(incomingTable, targetTableColumns, quote)
  def setForUpdateSql(
    incomingTable: String,
    targetTableColumns: List[String],
    quote: String
  ): String = SqlFormatter.setForUpdateSql(incomingTable, targetTableColumns, quote)
  def mergeKeyJoinCondition(
    incomingTable: String,
    targetTable: String,
    columns: List[String],
    quote: String
  ): String = SqlFormatter.mergeKeyJoinCondition(incomingTable, targetTable, columns, quote)
  def addSelectItem(
    statement: String,
    columnName: String,
    columnExpr: Option[String] = None
  ): String = SqlFormatter.addSelectItem(statement, columnName, columnExpr)
  def deleteSelectItem(statement: String, columnName: String): String =
    SqlFormatter.deleteSelectItem(statement, columnName)
  def upsertSelectItem(
    statement: String,
    columnName: String,
    maybePreviousColumnName: Option[String],
    columnExpr: Option[String] = None
  ): String =
    SqlFormatter.upsertSelectItem(statement, columnName, maybePreviousColumnName, columnExpr)

  // --- Transformation methods (kept — depend on refs/domains/tasks/settings) ---

  def substituteRefInSQLSelect(
    sql: String,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    connection: ConnectionInfo
  )(implicit
    settings: Settings
  ): String = {
    logger.debug(s"Source SQL: $sql")
    val fromResolved =
      buildSingleSQLQueryForRegex(sql, refs, domains, tasks, fromsRegex, "FROM", connection)
    logger.debug(s"fromResolved SQL: $fromResolved")
    val joinAndFromResolved =
      buildSingleSQLQueryForRegex(fromResolved, refs, domains, tasks, joinRegex, "JOIN", connection)
    logger.debug(s"joinAndFromResolved SQL: $joinAndFromResolved")
    joinAndFromResolved
  }

  def buildSingleSQLQueryForRegex(
    sql: String,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    fromOrJoinRegex: Regex,
    keyword: String,
    connection: ConnectionInfo
  )(implicit
    settings: Settings
  ): String = {
    val ctes = SQLUtils.extractCTENames(sql)
    var resolvedSQL = ""
    var startIndex = 0
    val fromMatches = fromOrJoinRegex.findAllMatchIn(sql).toList
    if (fromMatches.isEmpty) {
      sql
    } else {
      val tablesList = this.extractTableNames(sql)
      fromMatches.foreach { regex =>
        var source = regex.source.toString.substring(regex.start, regex.end)
        val tablesFound =
          source
            .substring(source.toUpperCase().indexOf(keyword.toUpperCase()) + keyword.length)
            .split(",")
            .map(_.trim.split("\\s").head)
            .filter(tablesList.contains(_))
            .sortBy(_.length)
            .reverse

        tablesFound.foreach { tableFound =>
          val resolvedTableName =
            resolveTableNameInSql(tableFound, refs, domains, tasks, ctes, connection)
          source = replaceTableRef(source, tableFound, resolvedTableName)
        }
        resolvedSQL += sql.substring(startIndex, regex.start) + source
        startIndex = regex.end
      }
      resolvedSQL = resolvedSQL + sql.substring(startIndex)
      resolvedSQL
    }
  }

  /** Replaces a table reference by its resolved name.
    *
    * Both operands are data, not patterns: an unescaped table name would make `.` match any
    * character (so `sales.orders` would also rewrite `salesXorders`), and an unescaped replacement
    * would read `$` as a group reference (a BigQuery partition decorator such as `orders$20260101`
    * throws rather than being inserted literally). The lookarounds keep the match on a whole
    * reference so that a short name is not substituted inside a longer one that an earlier
    * iteration already resolved.
    */
  private def replaceTableRef(
    source: String,
    tableName: String,
    resolvedTableName: String
  ): String = {
    val pattern = Pattern.compile(s"(?<![\\w.])${Pattern.quote(tableName)}(?![\\w.])")
    pattern.matcher(source).replaceAll(Matcher.quoteReplacement(resolvedTableName))
  }

  private def resolveTableNameInSql(
    tableName: String,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    ctes: List[String],
    connection: ConnectionInfo
  )(implicit
    settings: Settings
  ): String = {
    def cteContains(table: String): Boolean = ctes.exists(cte => cte.equalsIgnoreCase(table))

    if (tableName.contains('/')) {
      tableName
    } else {
      val quoteFreeTName: String = quoteFreeTableName(tableName)
      val tableTuple = quoteFreeTName.split("\\.").toList
      val activeEnvRefs = refs
      val databaseDomainTableRef =
        activeEnvRefs.getOutputRef(tableTuple).map(_.toSQLString(connection))
      val resolvedTableName = databaseDomainTableRef.getOrElse {
        resolveTableRefInDomainsAndJobs(tableTuple, domains, tasks) match {
          case Success((database, domain, table)) =>
            ai.starlake.schema.model.OutputRef(database, domain, table).toSQLString(connection)
          case Failure(e) =>
            Utils.logException(logger, e)
            throw e
        }
      }
      resolvedTableName
    }
  }

  def quoteFreeTableName(tableName: String) = {
    val quoteFreeTableName = List("\"", "`", "'").foldLeft(tableName) { (tableName, quote) =>
      tableName.replaceAll(quote, "")
    }
    quoteFreeTableName
  }

  private def resolveTableRefInDomainsAndJobs(
    tableComponents: List[String],
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo]
  )(implicit
    settings: Settings
  ): Try[(String, String, String)] = Try {
    val (database, domain, table): (Option[String], Option[String], String) =
      tableComponents match {
        case table :: Nil                       => (None, None, table)
        case domain :: table :: Nil             => (None, Some(domain), table)
        case database :: domain :: table :: Nil => (Some(database), Some(domain), table)
        case _ =>
          throw new Exception(s"Invalid table reference ${tableComponents.mkString(".")}")
      }
    (database, domain, table) match {
      case (Some(db), Some(dom), table) =>
        (db, dom, table)
      case (None, domainComponent, table) =>
        val domainsByFinalName = domains.filter { dom =>
          val domainOK = domainComponent.forall(_.equalsIgnoreCase(dom.finalName))
          domainOK && dom.tables.exists(_.finalName.equalsIgnoreCase(table))
        }
        val tasksByTable = tasks.find { task =>
          val domainOK = domainComponent.forall(_.equalsIgnoreCase(task.domain))
          domainOK && task.table.equalsIgnoreCase(table)
        }.toList

        val nameCountMatch = (domainsByFinalName.map(dom =>
          s"${dom.finalName}.$table".toLowerCase
        ) ++ tasksByTable.map(task => s"${task.domain}.$table".toLowerCase)).toSet.size

        val (database, domain) = if (nameCountMatch > 1) {
          val domainNames = domainsByFinalName.map(_.finalName).mkString(",")
          logger.error(s"Table $table is present in domain(s): $domainNames.")
          val taskNamesByTable = tasksByTable.map(_.table).mkString(",")
          logger.error(s"Table $table is present as a table in tasks(s): $taskNamesByTable.")
          val taskNames = tasksByTable.map(_.name).mkString(",")
          logger.error(s"Table $table is present as a table in tasks(s): $taskNames.")
          throw new Exception("Table is present in multiple domains and/or tasks")
        } else if (nameCountMatch == 1) {
          domainsByFinalName.headOption
            .map(dom => (dom.database, dom.finalName))
            .orElse(tasksByTable.headOption.map(task => (task.database, task.domain)))
            .getOrElse((None, ""))
        } else {
          logger.info(
            s"Table $table not found in any domain or task; This is probably a CTE or a temporary table"
          )
          (None, domainComponent.getOrElse(""))
        }
        val databaseName = database.orElse(settings.appConfig.getDefaultDatabase()).getOrElse("")
        (databaseName, domain, table)
      case _ =>
        throw new Exception(s"Invalid table reference ${tableComponents.mkString(".")}")
    }
  }

  // --- Transpilation methods (kept — depend on external JSQLTranspiler) ---

  def transpilerDialect(conn: ConnectionInfo): JSQLTranspiler.Dialect =
    conn._transpileDialect match {
      case Some(dialect) =>
        Try(JSQLTranspiler.Dialect.valueOf(dialect)).getOrElse(
          throw new IllegalArgumentException(
            s"Invalid _transpileDialect '$dialect'. Valid values are ${JSQLTranspiler.Dialect.values().map(_.name()).mkString(", ")}"
          )
        )
      case None =>
        if (conn.isSpark())
          JSQLTranspiler.Dialect.DATABRICKS
        if (conn.isBigQuery())
          JSQLTranspiler.Dialect.GOOGLE_BIG_QUERY
        else if (conn.isSnowflake())
          JSQLTranspiler.Dialect.SNOWFLAKE
        else if (conn.isRedshift())
          JSQLTranspiler.Dialect.AMAZON_REDSHIFT
        else if (conn.isDuckDb())
          JSQLTranspiler.Dialect.DUCK_DB
        else if (conn.isPostgreSql())
          JSQLTranspiler.Dialect.ANY
        else if (conn.isMySQLOrMariaDb())
          JSQLTranspiler.Dialect.ANY
        else if (conn.isJdbcUrl())
          JSQLTranspiler.Dialect.ANY
        else
          JSQLTranspiler.Dialect.ANY
    }

  def extractTableNamesWithTypes(
    sql: String
  )(implicit settings: Settings): List[(String, TableWithNameAndType)] = {
    val tables = SQLUtils.extractTableNamesUsingRegEx(
      Utils.parseJinja(sql, settings.schemaHandler().activeEnvVars())
    )
    val objects = settings.schemaHandler().objectsMap()
    tables.flatMap { t =>
      if (objects.contains(t)) {
        Some((t, objects(t)))
      } else {
        None
      }
    }
  }

  def resolveStar(sql: String)(implicit settings: Settings): String = {
    val schemaDefinition = settings.schemaHandler().objectJdbcDefinitions()
    val resolved =
      Try {
        val resolver = new JSQLColumResolver(schemaDefinition)
        resolver.setCommentFlag(false)
        resolver.getResolvedStatementText(sql)
      }
    resolved.getOrElse(sql)
  }

  def transpile(sql: String, conn: ConnectionInfo, timestamps: Map[String, AnyRef])(implicit
    settings: Settings
  ): String = {
    if (timestamps.nonEmpty) {
      logger.info(s"Transpiling SQL with timestamps: $timestamps")
    }
    import scala.jdk.CollectionConverters._
    val dialect = transpilerDialect(conn)
    val unpipedQuery = Try {
      if (false && dialect != JSQLTranspiler.Dialect.GOOGLE_BIG_QUERY) {
        JSQLTranspiler.unpipe(sql.trim)
      } else {
        sql
      }
    } match {
      case Success(unpiped) => unpiped
      case Failure(e) =>
        logger.error(s"Failed to unpipe SQL, sending as is to the dataware: $sql")
        Utils.logException(logger, e)
        sql
    }
    Try(
      JSQLTranspiler.transpileQuery(unpipedQuery.trim, dialect, timestamps.asJava)
    ) match {
      case Success(transpiled) =>
        SQLUtils.format(transpiled, JSQLFormatter.OutputFormat.PLAIN)
      case Failure(e) =>
        logger.error(s"Failed to transpile SQL with dialect $dialect: $sql")
        Utils.logException(logger, e)
        sql
    }
  }

  /** Substitute any macros except the ones that must be handled by the orchestrator
    */
  def instantiateMacrosInSql(
    sql: String,
    macros: String,
    vars: Map[String, Any]
  )(implicit settings: Settings): String = {
    val replacedSql = sql
      .replaceAll("\\{\\{\\s*sl_start_date\\s*}}", "__sl_start_date__")
      .replaceAll("\\{\\{\\s*sl_end_date\\s*}}", "__sl_end_date__")

    Utils
      .parseJinja(macros + "\n" + replacedSql, vars)
      .replaceAll("__sl_start_date__", "{{ sl_start_date }}")
      .replaceAll("__sl_end_date__", "{{ sl_end_date }}")
  }
}
