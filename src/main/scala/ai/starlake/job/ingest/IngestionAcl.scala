package ai.starlake.job.ingest

import ai.starlake.config.Settings
import ai.starlake.schema.model._
import ai.starlake.utils.SparkUtils

import scala.util.{Success, Try}

/** ACL management concerns for ingestion jobs. */
trait IngestionAcl { self: IngestionJob =>

  private def extractHiveTableAcl(): List[String] = {
    if (settings.appConfig.isHiveCompatible()) {
      val fullTableName = schemaHandler.getFullTableName(domain, schema)
      schema.acl.flatMap { ace =>
        ace.asSql(fullTableName, engine = Engine.SPARK)
      }
    } else {
      Nil
    }
  }

  def applyHiveTableAcl(): Try[Unit] =
    Try {
      val sqls = extractHiveTableAcl()
      sqls.foreach { sql =>
        SparkUtils.sql(session, sql)
      }
    }

  def applyJdbcAcl(connection: Settings.ConnectionInfo, forceApply: Boolean = false): Try[Unit] = {
    val fullTableName = schemaHandler.getFullTableName(domain, schema)
    val engine = connection.getJdbcEngineName()
    if (schema.acl.isEmpty || !(forceApply || settings.appConfig.accessPolicies.apply))
      Success(())
    else if (!AccessControlEntry.supportsGrants(engine)) {
      logger.info(s"$engine has no GRANT statement: the acl of $fullTableName is not applied")
      Success(())
    } else
      Try {
        val databricks = AccessControlEntry.runsOnDatabricks(connection)
        schema.acl.flatMap(_.asSql(fullTableName, engine, databricks))
      }.flatMap { sqls =>
        AccessControlEntry.applyJdbcAcl(
          connection,
          sqls,
          forceApply
        )
      }
  }
}
