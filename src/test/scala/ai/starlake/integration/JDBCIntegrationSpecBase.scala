package ai.starlake.integration

import ai.starlake.PgContainerHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import better.files.File

class JDBCIntegrationSpecBase extends IntegrationTestBase with PgContainerHelper {

  /** JDBC only database do not support deep json file formats
    * @param dir
    */
  override protected def copyFilesToIncomingDir(dir: File): Unit = {
    super.copyFilesToIncomingDir(dir)
  }

  /** We delete the table before running the test to ensure that the test is run in a clean
    * environment. We do not delete the table after afterwards because we want to be able to inspect
    * the table after the test.
    */
  override protected def cleanup(): Unit = {
    super.cleanup()
    if (sys.env.getOrElse("SL_JDBC_TEST", "false").toBoolean)
      dropTables(List("sales.customers", "sales.orders", "hr.locations", "hr.sellers"))
  }

  /** Drops the tables on the connection of the active env (call it inside withEnvs) */
  protected def dropTables(tables: List[String]): Unit = {
    implicit val settings: Settings = Settings(Settings.referenceConfig, None, None, None, None)
    val connectionRef = settings.appConfig.connectionRef
    val connection = settings.appConfig.connections(connectionRef)
    val jdbcOptions =
      JdbcDbUtils.jdbcOptions(
        connection.options,
        connection.sparkDatasource().getOrElse("jdbc"),
        None
      )
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcOptions) { conn =>
      tables.foreach { table =>
        conn
          .createStatement()
          .executeUpdate(s"DROP TABLE IF EXISTS $table")
      }
    }
  }
}

/** The tables the any-engine sample writes on Redshift. Runs share one live database, so each run
  * drops them first: tables left by an older Starflow (an audit table without SCHEDULED_DATE) or by
  * a failed run (an SCD2 table with several current rows per key) would fail it otherwise.
  */
object RedshiftSampleTables {
  val loaded: List[String] = List(
    "sales.customers",
    "sales.orders",
    "sales.categories",
    "sales.products",
    "hr.locations",
    "hr.flat_locations",
    "hr.sellers",
    "audit.audit",
    "audit.rejected"
  )
  val transformed: List[String] = List("sales_kpi.byseller_kpi")
}
