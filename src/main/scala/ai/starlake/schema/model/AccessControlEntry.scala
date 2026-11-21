package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.utils.Utils

import scala.util.Try

case class AccessControlEntry(role: String, grants: Set[String] = Set.empty, name: String = "")
    extends SecurityLevel {

  override def toString: String = {
    s"AccessControlEntry(role=$role, grants=$grants)"
  }

  def asMap(): Map[String, String] = Map(
    "aceRole"   -> role,
    "aceGrants" -> grants.mkString(",")
  )

  def this() = this("", Set.empty) // Should never be called. Here for Jackson deserialization only

  def compare(other: AccessControlEntry): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(this.role, this, other)

  /** The type (`user`, `group` or `role`) and the name of a grantee, from its prefix: the engines
    * that grant to a typed principal cannot express any other grantee (#1985)
    */
  private def typedGrantee(grant: String, fullTableName: String, engine: String): (String, String) =
    AccessControlEntry.GranteeTypes
      .collectFirst {
        case granteeType
            if grant.startsWith(s"$granteeType:") &&
            grant.substring(granteeType.length + 1).trim.nonEmpty =>
          (granteeType, grant.substring(granteeType.length + 1).trim)
      }
      .getOrElse(
        throw new IllegalArgumentException(
          s"The acl of $fullTableName grants ${this.role} to '$grant', which $engine cannot express: " +
          "a grantee there starts with user:, group: or role:"
        )
      )

  private def asHiveSql(fullTableName: String, engine: String): Set[String] = {
    this.grants.map { grant =>
      val principal = typedGrantee(grant, fullTableName, engine) match {
        case ("user", name) => s"USER $name"
        case (_, name)      => s"ROLE $name"
      }
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  /** `TO name` or `TO GROUP name`: PostgreSQL has roles only, `GROUP` is a noise word there */
  private def asPostgresSql(fullTableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal = typedGrantee(grant, fullTableName, "postgresql") match {
        case ("group", name) => s"GROUP ${AccessControlEntry.quoteIdentifier(name)}"
        case (_, name)       => AccessControlEntry.quoteIdentifier(name)
      }
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  /** `TO name`, `TO GROUP name` or `TO ROLE name`: Redshift has users, groups and roles */
  private def asRedshiftSql(fullTableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal = typedGrantee(grant, fullTableName, "redshift") match {
        case ("user", name)  => AccessControlEntry.quoteIdentifier(name)
        case ("group", name) => s"GROUP ${AccessControlEntry.quoteIdentifier(name)}"
        case (_, name)       => s"ROLE ${AccessControlEntry.quoteIdentifier(name)}"
      }
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  private def asDatabricksSql(fullTableName: String): Set[String] = {
    /*
  GRANT
    privilege_type [, privilege_type ] ...
    ON (CATALOG | DATABASE <database-name> | TABLE <table-name> | VIEW <view-name> | FUNCTION <function-name> | ANONYMOUS FUNCTION | ANY FILE)
    TO principal

  privilege_type
    : SELECT | CREATE | MODIFY | READ_METADATA | CREATE_NAMED_FUNCTION | ALL PRIVILEGES
     */
    this.grants.map { grant =>
      // A Databricks principal has no type: the one of a typed grantee is left out (#1985)
      val name = AccessControlEntry.GranteeTypes
        .collectFirst {
          case granteeType if grant.startsWith(s"$granteeType:") =>
            grant.substring(granteeType.length + 1)
        }
        .getOrElse(grant)
        .trim
      if (name.isEmpty)
        throw new IllegalArgumentException(
          s"The acl of $fullTableName grants ${this.role} to '$grant', which names no principal"
        )
      s"GRANT ${this.role} ON TABLE $fullTableName TO ${AccessControlEntry.backquoteIdentifier(name)}"
    }
  }

  private def asSnowflakeSql(fullTableName: String): Set[String] = {
    /*
      https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
      https://hevodata.com/learn/snowflake-grant-role-to-user/
     */
    this.grants.map { principal =>
      s"GRANT ${this.role} ON TABLE $fullTableName TO DATABASE ROLE $principal"
    }
  }

  def asBigQuerySql(fullTableName: String): Set[String] = {
    this.grants.map { grant =>
      val principal =
        if (!grant.startsWith("\""))
          s""""$grant""""
        else
          grant
      s"GRANT ${this.role} ON TABLE $fullTableName TO $principal"
    }
  }

  /** The GRANT statements of this entry on the table, none on an engine that has no GRANT statement
    * (#1985)
    *
    * @param databricks
    *   whether the statements of a Spark table run on Databricks, which has a syntax of its own
    */
  def asSql(
    fullTableName: String,
    engine: Engine,
    databricks: Boolean = Utils.isRunningInDatabricks()
  ): Set[String] = engine match {
    case Engine.SPARK =>
      if (databricks)
        asDatabricksSql(fullTableName)
      else
        asHiveSql(fullTableName, engine.toString)
    case Engine.BQ                                        => asBigQuerySql(fullTableName)
    case e if !AccessControlEntry.supportsGrants(e)       => Set.empty
    case e if e.toString == Engine.SNOWFLAKE.toString     => asSnowflakeSql(fullTableName)
    case e if e.toString == AccessControlEntry.PostgreSql => asPostgresSql(fullTableName)
    case e if e.toString == AccessControlEntry.Redshift   => asRedshiftSql(fullTableName)
    case e                                                => asHiveSql(fullTableName, e.toString)
  }
}

object AccessControlEntry {
  private val GranteeTypes = List("user", "group", "role")
  private val PostgreSql = "postgresql"
  private val Redshift = "redshift"
  private val PlainIdentifier = "[A-Za-z_][A-Za-z0-9_$]*".r

  /** DuckDB has no GRANT statement: the acl of its tables is not applied (#1985) */
  def supportsGrants(engine: Engine): Boolean = engine.toString != "duckdb"

  /** Whether the statements on a connection run on Databricks: from a Databricks runtime, or
    * through its JDBC driver from anywhere else (#1985)
    */
  def runsOnDatabricks(connection: Settings.ConnectionInfo): Boolean =
    Utils.isRunningInDatabricks() ||
    connection.options.get("url").exists(_.toLowerCase.startsWith("jdbc:databricks:"))

  /** A name that is not a plain identifier, such as `me@me.com`, is quoted, unless it already is */
  private def quoteIdentifier(name: String): String =
    if (PlainIdentifier.matches(name) || name.startsWith("\"")) name
    else "\"" + name.replace("\"", "\"\"") + "\""

  /** A Databricks principal that is not a plain identifier, such as `me@me.com` or
    * `data-engineers`, is backquoted, unless it already is
    */
  private def backquoteIdentifier(name: String): String =
    if (PlainIdentifier.matches(name) || name.startsWith("`")) name
    else "`" + name.replace("`", "``") + "`"
  def applyJdbcAcl(connection: Settings.ConnectionInfo, sqls: Seq[String], forceApply: Boolean)(
    implicit settings: Settings
  ): Try[Unit] =
    Try {
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), connection.options) {
        conn =>
          applyJdbcAcl(conn, sqls, forceApply).get
      }
    }

  def applyJdbcAcl(
    jdbcConnection: java.sql.Connection,
    sqls: Seq[String],
    forceApply: Boolean
  )(implicit
    settings: Settings
  ): Try[Unit] =
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply)
        sqls.foreach { sql =>
          JdbcDbUtils.executeUpdate(sql, jdbcConnection)
        }
    }

}
