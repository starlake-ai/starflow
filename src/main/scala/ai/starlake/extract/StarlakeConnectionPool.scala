package ai.starlake.extract

import ai.starlake.utils.StarlakeJdbcOps
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.duckdb.DuckDBConnection

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object StarlakeConnectionPool extends LazyLogging {
  private val hikariPools = scala.collection.concurrent.TrieMap[String, HikariDataSource]()
  private case class DuckDbPoolEntry(connection: Connection, lastAccessTime: Long)
  private val duckDbPool = scala.collection.concurrent.TrieMap[String, DuckDbPoolEntry]()
  private val duckDbPoolMaxIdleTimeMs = 15 * 1000L // 15 seconds

  // Scheduled executor for periodic cleanup of idle DuckDB connections
  def startCleanupScheduler(
    timeoutInSeconds: Int
  ): java.util.concurrent.ScheduledExecutorService = {
    val scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
      new java.util.concurrent.ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "duckdb-pool-cleanup")
          t.setDaemon(true) // Daemon thread won't prevent JVM shutdown
          t
        }
      }
    )
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          Try(cleanupIdleDuckDbConnections()) match {
            case Failure(e) =>
              logger.warn("Error during DuckDB pool cleanup", e)
            case _ =>
          }
        }
      },
      1, // initial delay
      timeoutInSeconds, // period
      java.util.concurrent.TimeUnit.SECONDS
    )
    logger.info("DuckDB connection pool cleanup scheduler started")
    scheduler
  }

  def clearDuckdbPool(): Unit = {
    duckDbPool.values.foreach { entry =>
      Try(entry.connection.close()) match {
        case Success(_) =>
        case Failure(exception) =>
          logger.warn(s"Could not close duckdb connection", exception)
      }
    }
    duckDbPool.clear()
  }

  def shutdown(): Unit = {
    clearDuckdbPool()
    hikariPools.values.foreach { pool =>
      Try(pool.close()) match {
        case Failure(e) => logger.warn("Error closing HikariCP pool", e)
        case _          =>
      }
    }
    hikariPools.clear()
  }

  Runtime.getRuntime.addShutdownHook(new Thread("starlake-pool-shutdown") {
    override def run(): Unit = shutdown()
  })

  private def cleanupIdleDuckDbConnections(): Unit = duckDbPool.synchronized {
    val now = System.currentTimeMillis()
    duckDbPool.foreach { case (key, entry) =>
      if (now - entry.lastAccessTime > duckDbPoolMaxIdleTimeMs) {
        Try(entry.connection.close()) match {
          case Success(_) =>
            logger.debug(s"Closed idle DuckDB connection for key: $key")
          case Failure(exception) =>
            logger.warn(s"Could not close idle duckdb connection", exception)
        }
        duckDbPool.remove(key)
      }
    }
  }
  private def getHikariPoolKey(url: String, options: Map[String, String]): String = {
    val poolKey = options
      .map { case (k, v) =>
        if (
          k.toLowerCase().contains("password") || k.toLowerCase().contains("token") || k
            .toLowerCase()
            .contains("sl_access_token")
        )
          s"$k=****"
        else
          s"$k=$v"
      }
      .toList
      .sorted
      .mkString("&") + "@" + url
    // get SHA-256 hash of the pool key to avoid too long names
    val md = java.security.MessageDigest.getInstance("SHA-256")
    val hash = md.digest(poolKey.getBytes).map("%02x".format(_)).mkString
    hash
  }

  def getConnection(
    dataBranch: Option[String],
    connectionOptions: Map[String, String]
  ): java.sql.Connection = {
    require(
      connectionOptions.contains("driver"),
      s"driver class not found in JDBC connection options $connectionOptions"
    )
    val isDucklake =
      connectionOptions
        .get("preActions")
        .exists(preActions => preActions.contains("ducklake:"))

    val driver = connectionOptions("driver")
    val url =
      if (isDucklake)
        "jdbc:duckdb:"
      else
        connectionOptions("url")

    if (url.contains(":duckdb:")) {
      val duckOptions = JdbcDbUtils.removeNonDuckDbProperties(connectionOptions)
      val properties = new Properties()
      duckOptions
        .foreach { case (k, v) =>
          properties.setProperty(k, v)
        }

      val dbKey =
        if (connectionOptions.get("preActions").exists(_.contains("ducklake:"))) {
          val ducklakeAttachment =
            connectionOptions("preActions")
              .split(";")
              .find(_.contains("ducklake:"))
              .getOrElse("Should never happen")
          ducklakeAttachment.replaceAll("\\s+", " ")
        } else {
          // No connection pool for duckdb. This is a single user database on write.
          // We need to release the connection asap
          url + "?" + properties.toString
        }

      logger.debug(s"DuckDB Connection Key: $dbKey")
      // Clean up idle connections before getting/creating a new one
      // Synchronized to prevent race conditions on compound get-then-put operations
      val mainConnection = duckDbPool.synchronized {
        duckDbPool.get(dbKey) match {
          case Some(entry) if !entry.connection.isClosed =>
            logger.debug(s"Reusing existing DuckDB connection for $url")
            // Update last access time
            duckDbPool.put(dbKey, entry.copy(lastAccessTime = System.currentTimeMillis()))
            entry.connection
          case Some(entry) =>
            logger.debug(s"Removing closed DuckDB connection for $url")
            duckDbPool.remove(dbKey)
            val sqlConn = DriverManager.getConnection(url, properties)
            duckDbPool.put(dbKey, DuckDbPoolEntry(sqlConn, System.currentTimeMillis()))
            sqlConn
          case _ =>
            // Clean any existing closed connection
            duckDbPool.find { case (key, _) =>
              key.startsWith(url)
            } match {
              case Some((key, entry)) =>
                Try(entry.connection.close())
                duckDbPool.remove(key)
              case None =>
            }
            val sqlConn = DriverManager.getConnection(url, properties)
            duckDbPool.put(dbKey, DuckDbPoolEntry(sqlConn, System.currentTimeMillis()))
            sqlConn
        }
      }

      mainConnection match {
        case c: DuckDBConnection => c.duplicate()
        case _ =>
          throw new RuntimeException(
            "Expecting a duck db connection in this case but got" + mainConnection.getClass.getName
          )
      }
    } else {
      val isSnowflakeWebOAuth =
        !connectionOptions.get("SL_APP_TYPE").contains("snowflake_native_app") &&
        url.contains(":snowflake:") &&
        connectionOptions.get("authenticator").map(_.toLowerCase()).contains("oauth") &&
        connectionOptions.contains("sl_access_token") &&
        connectionOptions("sl_access_token").count(_ == ':') >= 2

      val isSnowflakeNativeApp =
        connectionOptions.get("SL_APP_TYPE").contains("snowflake_native_app") &&
        connectionOptions.get("authenticator").map(_.toLowerCase()).contains("oauth")

      val (adjustedConnectionOptions, finalUrl) = {
        if (isSnowflakeNativeApp) {
          val accountUserAndToken = connectionOptions("sl_access_token").split("°")
          val accessToken =
            if (accountUserAndToken.length >= 2)
              accountUserAndToken
                .drop(2)
                .mkString(":") // in case the token contains the ':'
            else
              connectionOptions("sl_access_token")

          val nativeOptions =
            connectionOptions
              .updated("password", accessToken)
              .updated("authenticator", "oauth")
              .removed("sl_access_token")
          (nativeOptions, url)
        } else if (isSnowflakeWebOAuth) {
          // SnowflakeOAuth account:clientid
          // this is the case for Snowflake OAuth as a web app not as a native app.
          val accountUserAndToken = connectionOptions("sl_access_token").split("°")
          val account = accountUserAndToken(0)
          val user = accountUserAndToken(1)
          val accessToken =
            accountUserAndToken
              .drop(2)
              .mkString(":") // in case the token contains the ':'
          val url = s"jdbc:snowflake://$account.snowflakecomputing.com"
          val finalConnectionOptions = connectionOptions
            .updated("account", account)
            .updated("password", accessToken)
            .updated("allowUnderscoresInHost", "true")
            .updated("url", url)
            .removed("user")
            .removed("sl_access_token")
          (finalConnectionOptions, url)
          // password is the token in Snowflake 3.13+
          // properties.setProperty("user", ...)
          // properties.setProperty("role", ...)
        } else if (url.contains(":snowflake:")) {
          (connectionOptions.updated("allowUnderscoresInHost", "true"), url)
        } else {
          (connectionOptions, url)
        }
      }

      val finalConnectionOptions =
        if (adjustedConnectionOptions.get("authenticator").contains("user/password"))
          adjustedConnectionOptions.removed("authenticator")
        else
          adjustedConnectionOptions

      val javaProperties = new Properties()
      finalConnectionOptions.foreach { case (k, v) =>
        if (
          !Set(
            "driver", // don't pass driver to DriverManager. No need
            "dbtable", // Spark only
            "numpartitions", // Spark only
            "sl_access_token", // used internally only
            "url",
            "quote",
            "separator"
          ).contains(k)
        ) {

          if (
            k != "authenticator" ||
            !Set("user/password", "programmatic_access_token").contains(
              finalConnectionOptions
                .getOrElse("authenticator", "other-value")
            ) // we don't pass user/password or programmatic_access_token authenticator to DriverManager this is internal
          )
            javaProperties.setProperty(k, v)
        }
      }
      val (finalDriver, dataBranchUrl) = StarlakeJdbcOps.driverAndUrl(
        dataBranch,
        driver,
        finalUrl
      )
      val (connection, isNewConnection) =
        if (System.getenv("SL_USE_CONNECTION_POOLING") == "true") {
          logger.info("Using connection pooling")
          val poolKey = getHikariPoolKey(finalUrl, finalConnectionOptions)

          val pool = hikariPools
            .getOrElseUpdate(
              poolKey, {
                val config = new HikariConfig()
                javaProperties.forEach { case (k, v) =>
                  config.addDataSourceProperty(k.toString, v.toString)
                }
                config.setJdbcUrl(dataBranchUrl)
                config.setDriverClassName(finalDriver)
                config.setMinimumIdle(1)
                config.setMaximumPoolSize(
                  100
                ) // dummy value since we are limited by the ForJoinPool size
                logger.info(s"Creating connection pool for $finalUrl")
                new HikariDataSource(config)
              }
            )
          val connection = pool.getConnection()
          (connection, true)
        } else {
          logger.info("Not using connection pooling")
          javaProperties.asScala.toMap.foreach { case (key, value) =>
            if (
              key
                .toLowerCase()
                .contains("password") || key.toLowerCase().contains("sl_access_token")
            ) {
              logger.info(s"Key: $key, Value: ****")
            } else
              logger.info(s"Key: $key, Value: $value")
          }
          (DriverManager.getConnection(dataBranchUrl, javaProperties), true)
        }
      //
      if (dataBranchUrl.startsWith("jdbc:starlake:")) {
        dataBranch match {
          case Some(branch) if branch.nonEmpty => StarlakeJdbcOps.branchStart(branch, connection)
          case _                               =>
        }
      }
      connection
    }
  }
}
