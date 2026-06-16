package ai.starlake.config

import ai.starlake.schema.model.*
import ai.starlake.schema.model.ConnectionType.JDBC
import ai.starlake.utils.{AESEncryption, SparkUtils, Utils}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.jdbc.JdbcDialect

import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.nowarn

/** Describes a connection to a JDBC-accessible database engine
  *
  * @param sparkFormat
  *   source / sink format (jdbc by default). Cf spark.format possible values
  * @param options
  *   any option required by the format used to ingest / tranform / compute the data. Eg for JDBC
  *   uri, user and password are required uri the URI of the database engine. It must start with
  *   "jdbc:" user the username under which to connect to the database engine password the password
  *   to use in order to connect to the database engine
  */
final case class ConnectionInfo(
  `type`: ConnectionType,
  loader: Option[String] = None,
  sparkFormat: Option[String] = None,
  quote: Option[String] = None,
  separator: Option[String] = None,
  options: Map[String, String] = Map.empty,
  _transpileDialect: Option[String] = None
) {
  @JsonIgnore
  def isDucklake(): Boolean =
    this.options.getOrElse("preActions", "").contains("'ducklake:")

  @JsonIgnore
  def isQuackClient(): Boolean = {
    val pa = this.options.getOrElse("preActions", "")
    pa.contains("'quack:") && !pa.contains("'ducklake:quack:")
  }

  @JsonIgnore
  def isQuackServer(): Boolean =
    this.options.contains("quackServerToken")

  @JsonIgnore
  def quackBind(): String =
    this.options.getOrElse("quackBind", "127.0.0.1")

  @JsonIgnore
  def quackPort(): Int =
    this.options.get("quackPort").map(_.toInt).getOrElse(9494)

  @JsonIgnore
  def quackServerToken(): Option[String] =
    this.options.get("quackServerToken")

  def withEncryptedPassword(
    secretKey: String
  ): ConnectionInfo = {
    val updatedOptions = this.options.map {
      case (key, value) if Set("password", "sfPassword").contains(key) =>
        key -> AESEncryption.encrypt(value, secretKey)
      case other => other
    }
    this.copy(options = updatedOptions)
  }

  def withDecryptedPassword(
    secretKey: String
  ): ConnectionInfo = {
    val updatedOptions = this.options.map {
      case (key, value) if Set("password", "sfPassword").contains(key) =>
        key -> AESEncryption.decrypt(value, secretKey)
      case other => other
    }
    this.copy(options = updatedOptions)
  }

  def testQuery(): String = {
    val engine = this.getJdbcEngineName()
    engine match {
      case Engine.SNOWFLAKE =>
        val db = this.options
          .get("sfDatabase")
          .orElse(this.options.get("db"))
          .orElse(this.options.get("database"))
          .getOrElse(throw new RuntimeException("Missing sfDatabase or db in connection options"))
        val schema = this.options
          .get("sfSchema")
          .orElse(this.options.get("schema"))
          .getOrElse(
            throw new RuntimeException("Missing sfSchema or schema in connection options")
          )
        s"DESCRIBE SCHEMA $db.$schema"
      case _ =>
        "SELECT 1"
    }
  }
  def asMap(): Map[String, String] = this.options

  /** Set the access token in the connection options if defined
    * @param accessToken
    * @return
    */
  def withAccessToken(accessToken: Option[String]): ConnectionInfo = {
    accessToken
      .map(accessToken => this.copy(options = this.options.updated("sl_access_token", accessToken)))
      .getOrElse {
        if (
          this.options.get("account").contains("SL_STARLAKE_ACCOUNT") && this.options
            .get("user")
            .contains("SL_STARLAKE_USERNAME") && this.options
            .get("password")
            .contains("SL_STARLAKE_PASSWORD")
        )
          this
        else
          this.copy(options = this.options.removed("sl_access_token"))
      }
  }

  @JsonIgnore
  def getJdbcEngine()(implicit settings: Settings): Option[Settings.JdbcEngine] = {
    val engineName = this.getJdbcEngineName()
    settings.appConfig.jdbcEngines.get(engineName.toString)
  }

  @JsonIgnore
  def getCatalog(): String = {
    val catalog = this.getJdbcEngineName().toString match {
      case "snowflake" =>
        val dbName =
          options
            .get("db")
            .orElse(options.get("sfDatabase"))
            .orNull
        dbName
      case "spark" =>
        val dbName =
          options
            .get("dbName")
            .orNull
        dbName
      case "postgresql" =>
        val dbName =
          options
            .get("DatabaseName")
            .orNull
        dbName
      case _ =>
        null
    }
    catalog
  }
  override def toString: String = {
    val redactOptions = Utils.redact(options)
    s"""Connection(
       |    type=${`type`},
       |    sparkFormat=$sparkFormat,
       |    quote=$quote,
       |    separator=$separator,
       |    options=$redactOptions
       |)""".stripMargin
  }

  def sparkDatasource(): Option[String] = {
    this.`type` match {
      case ConnectionType.JDBC =>
        val urlKey = if (options.contains("url")) "url" else "sfUrl"
        val engineName =
          if (urlKey == "sfUrl") "snowflake" else options(urlKey).split(':')(1).toLowerCase()
        this.sparkFormat match {
          case Some(_) =>
            engineName match {
              case "snowflake"                                 => Some("snowflake")
              case "redshift" if Utils.isRunningInDatabricks() => Some("redshift")
              case "redshift" => Some("io.github.spark_redshift_community.spark.redshift")
              case _          => Some("jdbc")
            }
          case None => None
        }
      case ConnectionType.BQ => Some("bigquery")
      case _                 => None
    }

  }

  def this() = this(ConnectionType.JDBC, None, None, None, None, Map.empty)

  @JsonIgnore
  def getFSPath(): String = {
    this.`type` match {
      case ConnectionType.FS =>
        options.getOrElse(
          "path",
          throw new RuntimeException("Expecting a path in the connection options")
        )
      case _ => throw new RuntimeException(s"Currently don't support location for ${`type`}")
    }
  }

  @JsonIgnore
  def getFSEncoding(): Charset = {
    `type` match {
      case ConnectionType.FS =>
        options.get("encoding").map(Charset.forName).getOrElse(StandardCharsets.UTF_8)
      case _ => throw new RuntimeException(s"Currently don't support encoding for ${`type`}")
    }
  }

  def checkValidity(name: String)(implicit settings: Settings): List[ValidationMessage] = {
    var errors = List.empty[ValidationMessage]
    val defaultTypes = new Path(DatasetArea.types, "default.sl.yml")
    if (!settings.storageHandler().exists(defaultTypes))
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "Types",
        s"File not found: ${defaultTypes.toString}"
      )

    val globalsCometPath = DatasetArea.env()
    if (!settings.storageHandler().exists(globalsCometPath))
      errors = errors :+ ValidationMessage(
        Severity.Warning,
        "Environment",
        s"env.sl.comet not found in ${globalsCometPath.toString}"
      )
    // Make sure we do not give twice the database name
    if (options.contains("db") && options.contains("database")) {
      errors = errors :+ ValidationMessage(
        Severity.Error,
        "Connection",
        s"Connection '${name}' requires a db or database property, not both"
      )
    }
    `type` match {
      case ConnectionType.JDBC =>
        if (!options.contains("url") && !options.contains("sfUrl")) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "Connection",
            s"Connection '${name}' requires a url or sfUrl for snowflake spark"
          )
        } else {
          sparkDatasource() match {
            case Some(datasource) =>
              if (datasource.contains("redshift")) {
                if (!options.contains("aws_iam_role")) {
                  errors = errors :+ ValidationMessage(
                    Severity.Error,
                    "Connection",
                    s"Redshift Connection '${name}' requires an aws_iam_role"
                  )
                }
                if (options.get("tempdir").isEmpty) {
                  errors = errors :+ ValidationMessage(
                    Severity.Error,
                    "Connection",
                    s"Redshift Connection '${name}' requires an tempdir"
                  )
                }
              }
              if (datasource.contains("snowflake")) {
                if (options.get("warehouse").isEmpty && options.get("sfWarehouse").isEmpty) {
                  errors = errors :+ ValidationMessage(
                    Severity.Error,
                    "Connection",
                    s"Snowflake Connection '${name}' requires a sfWarehouse property"
                  )
                }
                if (options.get("db").isEmpty && options.get("sfDatabase").isEmpty) {
                  errors = errors :+ ValidationMessage(
                    Severity.Error,
                    "Connection",
                    s"Snowflake Connection '${name}' requires a sfDatabase property"
                  )
                }
              }
            case None =>
          }
        }
      case ConnectionType.BQ =>
        if (!options.contains("location")) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "Connection",
            s"Connection '${name}' requires a location"
          )
        }
        if (this.sparkFormat.isDefined) {
          val isIndirectWriteMethod = options.getOrElse("writeMethod", "indirect") == "indirect"
          if (isIndirectWriteMethod && !options.contains("gcsBucket")) {
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "Connection",
              s"Connection '${name}' requires a gcsBucket"
            )
          }
          if (isIndirectWriteMethod && !options.contains("temporaryGcsBucket")) {
            errors = errors :+ ValidationMessage(
              Severity.Warning,
              "Connection",
              s"Connection '${name}': using gcsBucket as temporaryGcsBucket"
            )
          }
        }

        options.getOrElse("authType", "") match {
          case "APPLICATION_DEFAULT" =>
            if (!options.contains("authScopes")) {
              errors = errors :+ ValidationMessage(
                Severity.Warning,
                "Connection",
                s"authScopes not defined in Connection '${name}'. Using 'https://www.googleapis.com/auth/cloud-platform'"
              )
            }
          case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
            if (!options.contains("jsonKeyfile")) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection '${name}' requires a jsonKeyfile"
              )
            }
          case "SERVICE_ACCOUNT_JSON_KEY_BASE64" =>
            if (!options.contains("jsonKeyBase64")) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection '${name}' requires a jsonKeyBase64"
              )
            }
          case "USER_CREDENTIALS" =>
            val clientId = options.get("clientId")
            val clientSecret = options.get("clientSecret")
            val refreshToken = options.get("refreshToken")
            if (clientId.isEmpty || clientSecret.isEmpty || refreshToken.isEmpty) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection '${name}' requires a clientId, clientSecret and refreshToken options"
              )
            }
          case "ACCESS_TOKEN" =>
            val accessToken = options.get("gcpAccessToken")
            if (accessToken.isEmpty) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                "Connection",
                s"Connection '${name}' requires a gcpAccessToken option"
              )
            }
          case _ =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "Connection",
              s"Connection '${name}' requires an authType"
            )
        }
      case _ =>
    }
    errors
  }

  /** The engine is Spark when sparkFormat is defined or when the connection type is not bigquery
    * @return
    *   the engine Spark or Bigquery only
    */
  @JsonIgnore
  def getEngine(): Engine = {
    if (sparkFormat.isDefined) Engine.SPARK
    else {
      `type` match {
        case ConnectionType.BQ   => Engine.BQ
        case ConnectionType.JDBC => Engine.JDBC
        case _                   => Engine.SPARK
      }
    }
  }

  @JsonIgnore
  def targetDatawareHouse(): String = {
    `type` match {
      case ConnectionType.BQ => "bigquery"
      case ConnectionType.JDBC =>
        if (options.contains("sfUrl"))
          options("sfUrl").split(':')(1).toLowerCase() // should return snowflake
        else if (options.contains("url")) {
          options("url").split(':')(1).toLowerCase()
        } else "spark"
      case _ => "spark"
    }
  }

  @nowarn
  def datawareOptions(): Map[String, String] =
    options.view.filterKeys(!ConnectionInfo.allstorageOptions.contains(_)).toMap

  @nowarn
  def authOptions(): Map[String, String] =
    options.view.filterKeys(ConnectionInfo.allstorageOptions.contains(_)).toMap

  @JsonIgnore
  def getDatabaseName(): Option[String] = {
    def extractFromUrl(url: String): Option[String] = {
      // extract last part of path before teh query string
      val path = url.split("\\?")(0)
      if (path.nonEmpty) {
        val segments = path.split("/").filter(_.nonEmpty)
        segments.lastOption
      } else
        None
    }
    this.getJdbcEngineName().toString match {
      case "duckdb" =>
        // duckdb url example: jdbc:duckdb:/path/to/database.db returns database
        val uri = extractFromUrl(options("url"))
        uri.map { u =>
          val dbName = u.split("\\.")(0)
          dbName
        }
      case "snowflake" =>
        options
          .get("sfDatabase")
          .orElse(options.get("db"))
          .orElse(options.get("database"))
      case "redshift" =>
        extractFromUrl(options("url"))
      case _ => None
    }
  }

  @JsonIgnore
  def getJdbcEngineName(): Engine = {
    val engineName = sparkFormat match {
      case None | Some("jdbc") =>
        this.`type` match {
          case JDBC =>
            val urlKey = if (options.contains("url")) "url" else "sfUrl"
            val engineName =
              if (urlKey == "sfUrl") "snowflake" else options(urlKey).split(':')(1).toLowerCase()
            if (engineName == "mariadb")
              "mysql"
            else if (engineName == "databricks")
              "spark"
            else engineName
          case ConnectionType.BQ => "bigquery"
          case _                 =>
            // if this is a jdbc url (aka snowflake, redshift ...)
            options
              .get("url")
              .map(_.split(':')(1))
              .getOrElse("spark")
        }
      case Some(_) =>
        // if this is a jdbc url (aka snowflake, redshift ...)
        options
          .get("url")
          .map(_.split(':')(1))
          .getOrElse("spark")

    }
    Engine.fromString(engineName)
  }

  @JsonIgnore
  def isBigQuery() = this.`type` == ConnectionType.BQ

  @JsonIgnore
  def isSnowflake(): Boolean = getJdbcEngineName().toString == "snowflake"

  @JsonIgnore
  def isSpark(): Boolean =
    getJdbcEngineName().toString == "spark" || this.`type` == ConnectionType.FS

  @JsonIgnore
  def isJdbcUrl() = this.options.get("url").exists(_.startsWith("jdbc"))

  @JsonIgnore
  def isRedshift(): Boolean = getJdbcEngineName().toString == "redshift"

  @JsonIgnore
  def isPostgreSql(): Boolean = getJdbcEngineName().toString == "postgresql"

  @JsonIgnore
  def isMySQLOrMariaDb(): Boolean = isMySQL() || isMariaDb()

  @JsonIgnore
  def isCLickhouse(): Boolean =
    getJdbcEngineName().toString == "clickhouse" || getJdbcEngineName().toString == "ch"

  @JsonIgnore
  def isMySQL(): Boolean = getJdbcEngineName().toString == "mysql"

  @JsonIgnore
  def isMariaDb(): Boolean = getJdbcEngineName().toString == "mariadb"

  @JsonIgnore
  def isDuckDb(): Boolean = getJdbcEngineName().toString == "duckdb"

  @JsonIgnore
  def isMotherDuckDb(): Boolean =
    isDuckDb() && options("url").toLowerCase().startsWith("jdbc:duckdb:md:")

  @JsonIgnore
  lazy val jdbcUrl: String = applyIfConnectionTypeIs(
    ConnectionType.JDBC,
    options.getOrElse(
      "url",
      throw new RuntimeException(s"Missing url in connection options.")
    )
  )

  @JsonIgnore
  lazy val dialect: JdbcDialect =
    applyIfConnectionTypeIs(ConnectionType.JDBC, SparkUtils.dialectForUrl(jdbcUrl))

  def quoteIdentifier(identifier: String): String = dialect.quoteIdentifier(identifier)

  def mergeOptionsWith(additionalConnectionOptions: Map[String, String]): ConnectionInfo = {
    this.copy(options = options ++ additionalConnectionOptions)
  }

  private def applyIfConnectionTypeIs[T](connectionType: ConnectionType, action: => T): T = {
    `type` match {
      case `connectionType` => action
      case _ =>
        throw new RuntimeException(
          s"${`type`} found but can only be used for ${connectionType} connection type."
        )
    }
  }
}

object ConnectionInfo {
  val gcsOptions = List(
    "gcsBucket",
    "temporaryGcsBucket",
    "authType",
    "jsonKeyfile",
    "clientId",
    "clientSecret",
    "refreshToken"
  )
  val azureOptions = List(
    "azureStorageContainer",
    "azureStorageAccount",
    "azureStorageKey"
  )
  val s3Options = Nil

  val allstorageOptions = gcsOptions ++ azureOptions ++ s3Options

  def getConnectionOrDefault(
    connectionRef: Option[String]
  )(implicit settings: Settings): ConnectionInfo = {
    val conn =
      connectionRef
        .map { ref =>
          settings.appConfig.connections.getOrElse(
            ref,
            throw new IllegalArgumentException(
              s"Connection reference '${connectionRef}' not found in the configuration"
            )
          )
        }
        .orElse {
          settings.appConfig.connections.get(settings.appConfig.connectionRef)
        }
        .getOrElse(
          throw new IllegalArgumentException(
            s"Connection reference '${settings.appConfig.connectionRef}' not found in the configuration"
          )
        )
    conn
  }
}
