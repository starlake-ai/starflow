package ai.starlake.schema.handlers

import ai.starlake.config.ConnectionInfo
import ai.starlake.config.Settings
import ai.starlake.schema.model.*
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object SqlTransformations extends LazyLogging {

  def substituteRefTaskMainSQL(
    sql: String,
    connection: ConnectionInfo,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    allVars: Map[String, String],
    externals: List[DomainInfo]
  )(implicit settings: Settings): String = {
    if (sql.trim.isEmpty)
      sql
    else {
      val selectStatement = Utils.parseJinja(sql, allVars)
      val select =
        SQLUtils.substituteRefInSQLSelect(
          selectStatement,
          refs,
          domains,
          tasks,
          connection,
          externals
        )
      select
    }
  }

  /** Transpile the given SQL statement from the connection dialect to the target engine dialect
    * when in test mode or when the connection declares a transpile dialect. Unlike
    * [[transpileAndSubstituteSelectStatement]], no ref substitution is applied, so it is safe to
    * use on any statement (DML in presql/postsql, expectation queries ...).
    */
  def transpileIfNeeded(
    sql: String,
    connection: ConnectionInfo,
    allVars: Map[String, String] = Map.empty,
    test: Boolean
  )(implicit settings: Settings): String = {
    if (test || connection._transpileDialect.isDefined) {
      val timestamps =
        if (test) {
          List(
            "SL_CURRENT_TIMESTAMP",
            "SL_CURRENT_DATE",
            "SL_CURRENT_TIME"
          ).flatMap { e =>
            val value = allVars.get(e).orElse(Option(System.getenv().get(e)))
            value.map { v => e -> v }
          }.toMap
        } else
          Map.empty[String, String]

      SQLUtils.transpile(sql, connection, timestamps)(settings)
    } else
      sql
  }

  def transpileAndSubstituteSelectStatement(
    sql: String,
    connection: ConnectionInfo,
    refs: RefDesc,
    domains: List[DomainInfo],
    tasks: List[AutoTaskInfo],
    allVars: Map[String, String],
    test: Boolean,
    externals: List[DomainInfo]
  )(implicit settings: Settings): String = {

    if (sql.startsWith("DESCRIBE ")) {
      // Do not transpile DESCRIBE statements
      sql
    } else {
      val sqlWithParameters =
        Try {
          substituteRefTaskMainSQL(
            sql,
            connection,
            refs,
            domains,
            tasks,
            allVars,
            externals
          )
        } match {
          case Success(substitutedSQL) =>
            substitutedSQL
          case Failure(e) =>
            Utils.logException(logger, e)
            sql
        }

      transpileIfNeeded(sqlWithParameters, connection, allVars, test)
    }
  }
}
