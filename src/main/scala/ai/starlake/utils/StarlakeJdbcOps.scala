package ai.starlake.utils

import ai.starlake.config.ConnectionInfo
import ai.starlake.jdbc.StarlakeConnection
import com.typesafe.scalalogging.StrictLogging
import org.duckdb.DuckDBConnection

import java.sql.Connection
import java.time.Instant
import scala.util.Using

object StarlakeJdbcOps extends StrictLogging {

  case class BranchStatement(sql: String, ts: Instant)

  def isBranchActive(connection: Connection, branchName: Option[String]): Boolean =
    connection match {
      case slConn: StarlakeConnection if slConn.currentSession != null =>
        branchName.forall(_ == slConn.currentSession.getSessionId)
      case _ => false
    }

  def currentBranch(connection: Connection): Option[String] =
    connection match {
      case slConn: StarlakeConnection if slConn.currentSession != null =>
        Some(slConn.currentSession.getSessionId)
      case _ => None
    }

  /** The connection to run a job's statements on when a data branch is active.
    *
    * Called once the connection preActions have run, so that the branch schema lands in the catalog
    * they select (a DuckLake `ATTACH ... ; USE lake` for instance) and they are not themselves
    * rewritten. A Snowflake, BigQuery or Flight SQL (QoD) connection is already a
    * StarlakeConnection (see [[driverAndUrl]]). A DuckDB connection comes from the in-process pool
    * rather than the DriverManager, so it is wrapped here. Other engines do not branch.
    */
  def enterBranch(dataBranch: Option[String], connection: Connection): Connection =
    dataBranch.filter(_.nonEmpty) match {
      case None => connection
      case Some(branch) =>
        val branchConnection = connection match {
          case slConn: StarlakeConnection => Some(slConn)
          case duckConn: DuckDBConnection => Some(new StarlakeConnection(duckConn))
          case _                          => None
        }
        branchConnection match {
          case Some(slConn) =>
            branchStart(branch, slConn)
            slConn
          case None =>
            connection
        }
    }

  /** Start a branch if not already started
    * @param dataBranch
    *   the branch name
    * @param connection
    *   the JDBC connection
    * @return
    *   true if branch was not yet started, false if already on that branch
    */
  def branchStart(dataBranch: String, connection: Connection): Boolean = {
    // KEEP_SNAPSHOTS == KEEP survives session closure
    // KEEP_STATEMENTS ==  PERSIST means the sql statements are physically stored in the history table
    if (isBranchActive(connection, Some(dataBranch))) {
      logger.info(s"Already on branch $dataBranch")
      false
    } else {
      // We keep the sql history and the snapshot tables
      Using.resource(connection.createStatement()) { stmt =>
        stmt.execute(s"BRANCH START $dataBranch WITH PERSIST = TRUE, KEEP = TRUE ")
      }
    }
  }

  def branchTruncate(dataBranch: String, connection: Connection): Boolean = {
    // We keep the sql statements history but deletes all snapshot tables. Schema is kept but snapshots are removed
    Using.resource(connection.createStatement()) { stmt =>
      stmt.execute(s"BRANCH DROP $dataBranch WITH KEEP = TRUE")
    }
  }

  def branchDrop(dataBranch: String, connection: Connection): Boolean = {
    // Drop all the snapshot tables and all the sql statements history. Schema is removed thus all data is lost
    Using.resource(connection.createStatement()) { stmt =>
      stmt.execute(s"BRANCH DROP $dataBranch")
    }
  }

  def branchApply(dataBranch: String, connection: Connection): Boolean = {
    // run all the sql statements in history onto the production database
    Using.resource(connection.createStatement()) { stmt =>
      stmt.execute(s"BRANCH APPLY $dataBranch")
    }
  }

  def branchDescribe(dataBranch: String, connection: Connection): List[BranchStatement] = {
    // list all sql statements in the branch history
    Using.resource(connection.createStatement()) { stmt =>
      Using.resource(stmt.executeQuery(s"BRANCH DESCRIBE $dataBranch")) { rs =>
        val result = scala.collection.mutable.ListBuffer[StarlakeJdbcOps.BranchStatement]()
        while (rs.next()) {
          val ts = rs.getObject(1).asInstanceOf[Instant]
          val sql = rs.getString(2)
          val item = StarlakeJdbcOps.BranchStatement(sql, ts)
          result += item
        }
        result.toList.sortBy(_.ts).reverse
      }
    }
  }

  def driverAndUrl(dataBranch: Option[String], driver: String, url: String): (String, String) = {
    dataBranch match {
      case Some(_) =>
        val engine = url.split(':')(1).toLowerCase()
        engine match {
          // arrow-flight-sql is QoD, which serves DuckDB and DuckLake. A DuckDB connection is
          // wrapped by enterBranch: it does not go through the DriverManager
          case "snowflake" | "bigquery" | ConnectionInfo.FlightSqlScheme =>
            val finalDriver = "ai.starlake.jdbc.StarlakeDriver"
            val finalUrl = "jdbc:starlake:" + url.substring("jdbc:".length)
            logger.info(s"Using StarlakeDriver for $engine: $finalUrl")
            (finalDriver, finalUrl)
          case _ => (driver, url)
        }
      case None => (driver, url)
    }
  }
}
