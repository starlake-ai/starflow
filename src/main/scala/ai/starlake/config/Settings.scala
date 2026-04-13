/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.config

import ai.starlake.config.Settings.AppConfig
import ai.starlake.config.Settings.JdbcEngine.TableDdl
import ai.starlake.job.load.LoadStrategy
import ai.starlake.job.validator.GenericRowValidator
import ai.starlake.schema.handlers.*
import ai.starlake.schema.model.*
import ai.starlake.serve.CaffeineSettingsManager
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLTranspiler
import ai.starlake.transpiler.JSQLTranspiler.Dialect
import ai.starlake.utils.*
import better.files.File
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang.SystemUtils
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import pureconfig.*
import pureconfig.ConvertHelpers.*
import pureconfig.generic.ProductHint
import pureconfig.generic.auto.*

import java.io.ObjectStreamException
import java.net.URI
import java.sql.DriverManager
import java.util.{Locale, Properties, TimeZone, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object Settings extends LazyLogging {
  val latestSchemaVersion: Int = 1
  implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  private var _referenceConfig: Config = ConfigFactory.load()
  def referenceConfig: Config = _referenceConfig

  private var _applicationConfig: Config = null

  private val referenceClassLoader = Thread.currentThread().getContextClassLoader
  def invalidateCaches(): Unit = {
    ConfigFactory.invalidateCaches()
    _referenceConfig = ConfigFactory.load(referenceClassLoader)
  }

  /** datasets in the data pipeline go through several stages and are stored on disk at each of
    * these stages. This setting allow to customize the folder names of each of these stages.
    *
    * @param stage
    *   : Name of the pending area
    * @param unresolved
    *   : Named of the unresolved area
    * @param archive
    *   : Name of the archive area
    * @param ingesting
    *   : Name of the ingesting area
    */
  @JsonIgnoreProperties(
    Array("acceptedFinal", "rejectedFinal", "businessFinal", "replayFinal")
  )
  final case class Area(
    incoming: String,
    stage: String,
    unresolved: String,
    archive: String,
    ingesting: String,
    replay: String,
    hiveDatabase: String
  ) {
    val replayFinal: String = replay.toLowerCase(Locale.ROOT)
  }

  /** @param options
    *   : Map of privacy algorightms name -> PrivacyEngine
    */
  final case class Privacy(options: Map[String, String])

  final case class Elasticsearch(active: Boolean, options: Map[String, String])

  /** @param discreteMaxCardinality
    *   : Max number of unique values allowed in cardinality compute
    */
  final case class Metrics(
    path: String,
    discreteMaxCardinality: Int,
    active: Boolean
  ) // sunk to audit

  final case class ExpectationsConfig(
    path: String,
    active: Boolean,
    failOnError: Boolean
  ) // sunk to expectations

  final case class Audit(
    path: String,
    sink: AllSinks,
    maxErrors: Int,
    database: Option[String],
    domain: Option[String],
    active: Option[Boolean],
    sql: Option[String],
    domainExpectation: Option[String],
    domainRejected: Option[String],
    detailedLoadAudit: Boolean
  ) {
    def isActive(): Boolean = this.active.getOrElse(false)

    def getConnectionRef()(implicit settings: Settings): String =
      this.sink.connectionRef.getOrElse(settings.appConfig.connectionRef)

    def getConnection()(implicit settings: Settings): ConnectionInfo =
      settings.appConfig.connections(this.getConnectionRef())

    def getSink()(implicit settings: Settings) =
      this.sink.getSink()

    def getDatabase()(implicit settings: Settings): Option[String] =
      this.database.orElse(settings.appConfig.getDefaultDatabase())

    def getDomain()(implicit settings: Settings): String =
      this.domain.getOrElse("audit")

    def getDomainExpectation()(implicit settings: Settings): String =
      this.domainExpectation.getOrElse("audit")

    def getDomainRejected()(implicit settings: Settings): String =
      this.domainRejected.getOrElse("audit")
  }

  // ConnectionInfo extracted to ConnectionInfo.scala — re-exported for backward compatibility
  type ConnectionInfo = ai.starlake.config.ConnectionInfo
  val ConnectionInfo = ai.starlake.config.ConnectionInfo

  // Keep Connections here since it references ConnectionInfo
  final case class Connections(connections: Map[String, ConnectionInfo] = Map.empty)

  /** Describes how to use a specific type of JDBC-accessible database engine
    *
    * @param tables
    *   for each of the Standard Table Names used by Comet, the specific SQL DDL statements as
    *   expected in the engine's own dialect.
    */
  final case class JdbcEngine(
    tables: Map[String, TableDdl],
    quote: String,
    viewPrefix: Option[String],
    preActions: Option[String],
    strategyBuilder: String,
    partitionBy: Option[String] = None,
    clusterBy: Option[String] = None,
    columnRemarks: Option[String] = None,
    tableRemarks: Option[String] = None,
    supportsJson: Option[Boolean] = None,
    describe: Option[String] = None,
    lastModifiedQuery: Option[String] = None
  ) {
    def quoteIdentifier(anyIdentifier: String) = {
      s"$quote$anyIdentifier$quote"
    }
  }

  object JdbcEngine {

    /** A descriptor of the specific SQL DDL statements required to manage a specific Comet table in
      * a JDBC-accessible database engine
      *
      * @param createSql
      *   the SQL Create Table statement with the database-specific type, constraints etc. tacked
      *   on.
      * @param pingSql
      *   a cheap SQL query whose results are irrelevant but guaranteed to trigger an error in case
      *   the table is absent
      * @note
      *   pingSql is optional, and will default to `select * from `name` where 1=0` as Spark SQL
      *   does
      */
    final case class TableDdl(
      createSql: String,
      pingSql: Option[String],
      selectSql: Option[String] = None
    ) {

      def effectivePingSql(tableName: String): String =
        pingSql.getOrElse(s"select count(*) from $tableName where 1=0")
    }
  }

  final case class Http(
    interface: String,
    port: Int
  )

  final case class GizmoSql(
    url: String,
    apiKey: String
  )

  final case class Lock(
    path: String,
    timeout: Long,
    pollTime: Long = 5000L,
    refreshTime: Long = 5000L
  )

  final case class Internal(
    cacheStorageLevel: StorageLevel,
    intermediateBigqueryFormat: String,
    temporaryGcsBucket: Option[String],
    substituteVars: Boolean = true,
    bqAuditSaveInBatchMode: Boolean = true
  )

  final case class KafkaTopicConfig(
    topicName: String,
    maxRead: Long = -1,
    fields: List[String] = List("key as STRING", "value as STRING"),
    partitions: Int = 1,
    replicationFactor: Short = 1,
    createOptions: Map[String, String] = Map.empty,
    accessOptions: Map[String, String] = Map.empty,
    headers: Map[String, Map[String, String]] = Map.empty
  ) {
    def allAccessOptions()(implicit settings: Settings): Map[String, String] = {
      settings.appConfig.kafka.sparkServerOptions ++ accessOptions
    }
  }

  @JsonIgnoreProperties(Array("sparkServerOptions"))
  final case class KafkaConfig(
    serverOptions: Map[String, String],
    topics: Map[String, KafkaTopicConfig],
    cometOffsetsMode: Option[String] = Some("STREAM"),
    customDeserializers: Option[Map[String, String]] = None
  ) {
    lazy val sparkServerOptions: Map[String, String] = {
      val ASSIGN = "assign"
      val SUBSCRIBE_PATTERN = "subscribepattern"
      val SUBSCRIBE = "subscribe"
      val ignoreKafkaProperties = List(
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ASSIGN,
        SUBSCRIBE_PATTERN,
        SUBSCRIBE
      )
      val kafkaServerProperties = new Properties()
      serverOptions.foreach { case (k, v) =>
        // for spark we need to prefix them with "kafka."
        kafkaServerProperties.put(k, v)
        if (!ignoreKafkaProperties.contains(k) && !k.startsWith("kafka."))
          kafkaServerProperties.put(s"kafka.$k", v)
      }
      kafkaServerProperties.asScala.toMap
    }
  }

  case class SparkScheduling(
    maxJobs: Int,
    poolName: String,
    mode: String,
    file: String
  )

  case class DagRef(load: Option[String], transform: Option[String])

  case class AccessPolicies(apply: Boolean, location: String, database: String, taxonomy: String)

  /** @param datasets
    *   : Absolute path, datasets root folder beneath which each area is defined.
    * @param metadata
    *   : Absolute path, location where all types / domains and auto jobs are defined
    * @param metrics
    *   : Absolute path, location where all computed metrics are stored
    * @param audit
    *   : Absolute path, location where all log are stored
    * @param archive
    *   : Should we backup the ingested datasets ? true by default
    * @param defaultWriteFormat
    *   : Choose between parquet, orc ... Default is parquet
    * @param defaultRejectedWriteFormat
    *   : Writing format for rejected datasets, choose between parquet, orc ... Default is parquet
    * @param defaultAuditWriteFormat
    *   : Writing format for audit datasets, choose between parquet, orc ... Default is parquet
    * @param hive
    *   : Should we create a Hive Table ? true by default
    * @param area
    *   : see Area above
    */
  @JsonIgnoreProperties(Array("cacheStorageLevel"))
  final case class AppConfig(
    env: String,
    datasets: String,
    dags: String,
    types: String,
    macros: String,
    tests: String,
    writeStrategies: String,
    prunePartitionOnMerge: Boolean,
    loadStrategies: String,
    metadata: String,
    metrics: Metrics,
    validateOnLoad: Boolean,
    rejectWithValue: Boolean,
    audit: Audit,
    archive: Boolean,
    sinkReplayToFile: Boolean,
    lock: Lock,
    defaultWriteFormat: String,
    defaultRejectedWriteFormat: String,
    defaultAuditWriteFormat: String,
    csvOutput: Boolean,
    csvOutputExt: String,
    privacyOnly: Boolean,
    emptyIsNull: Boolean,
    loader: String,
    rowValidatorClass: String,
    loadStrategyClass: String,
    grouped: Boolean,
    groupedMax: Int,
    scd2StartTimestamp: String,
    scd2EndTimestamp: String,
    area: Area,
    hadoop: Map[String, String],
    connections: Map[String, ConnectionInfo],
    jdbcEngines: Map[String, JdbcEngine],
    privacy: Privacy,
    root: String,
    internal: Option[Internal],
    accessPolicies: AccessPolicies,
    sparkScheduling: SparkScheduling,
    udfs: Option[String],
    expectations: ExpectationsConfig,
    sqlParameterPattern: String,
    rejectAllOnError: Boolean,
    rejectMaxRecords: Int,
    maxParCopy: Int,
    kafka: KafkaConfig,
    dsvOptions: Map[String, String],
    forceViewPattern: String,
    forceDomainPattern: String,
    forceTablePattern: String,
    forceJobPattern: String,
    forceTaskPattern: String,
    useLocalFileSystem: Boolean,
    sessionDurationServe: Long,
    database: String,
    tenant: String,
    connectionRef: String,
    loadConnectionRef: String,
    transformConnectionRef: String,
    schedulePresets: Map[String, String],
    maxParTask: Int,
    refs: List[Ref],
    dagRef: Option[DagRef],
    forceHalt: Boolean,
    jobIdEnvName: Option[String],
    archiveTablePattern: String,
    archiveTable: Boolean,
    version: String,
    autoExportSchema: Boolean,
    longJobTimeoutMs: Long,
    shortJobTimeoutMs: Long,
    createSchemaIfNotExists: Boolean,
    http: Http,
    timezone: TimeZone,
    hiveInTest: Boolean,
    duckdbMode: Boolean,
    duckdbExtensions: String,
    testCsvNullString: String,
    maxInteractiveRecords: Int,
    duckdbPath: Option[String],
    ack: Option[String],
    duckDbEnableExternalAccess: Boolean,
    syncSqlWithYaml: Boolean,
    syncYamlWithDb: Boolean,
    onExceptionRetries: Int,
    pythonLibsDir: String,
    gizmosql: GizmoSql
    // createTableIfNotExists: Boolean
  ) extends Serializable {

    @JsonIgnore
    def getEffectiveUdfs(): Seq[String] =
      udfs
        .map { udfs =>
          udfs.split(',').toList
        }
        .getOrElse(Nil)
        .filter(_.nonEmpty)

    def rootBucketName(): String = {
      val uri = new URI(root)
      uri.getScheme match {
        case "file" => throw new Exception("Cannot get bucket name from file system")
        case scheme =>
          root.substring(scheme.length + "://".length).takeWhile(_ != '/')
      }
    }

    @JsonIgnore
    lazy val fileSystem: String = {
      val protocolSeparator = "://"
      if (root.matches("^\\w+?:\\/\\/.*")) { // check if it follows URI pattern
        val uri = new URI(root)
        uri.getScheme match {
          case "file" => uri.getScheme + protocolSeparator
          case scheme =>
            // get bucket name
            val bucketName =
              root.substring(scheme.length + protocolSeparator.length).takeWhile(_ != '/')
            s"$scheme$protocolSeparator$bucketName"
        }
      } else {
        s"file$protocolSeparator"
      }
    }

    @JsonIgnore
    def getConnection(connectionRef: String): ConnectionInfo = {
      connections.getOrElse(
        connectionRef,
        throw new Exception(
          s"Connection $connectionRef not found. Please check your connection definition."
        )
      )
    }
    @JsonIgnore
    def getDefaultConnection(): ConnectionInfo = {
      connections.getOrElse(
        this.connectionRef,
        throw new Exception(
          s"Connection $connectionRef not found. Please check your connection definition."
        )
      )
    }

    @JsonIgnore
    def getDefaultDatabase(): Option[String] = if (database.isEmpty) None else Some(database)

    val cacheStorageLevel: StorageLevel =
      internal.map(_.cacheStorageLevel).getOrElse(StorageLevel.MEMORY_AND_DISK)

    // config.getOption("hive.metastore.uris")
    @JsonIgnore
    def isHiveCompatible(): Boolean = {
      val connectionTypeIsHive = this.connections
        .get(this.connectionRef)
        .exists { conn =>
          conn.`type` == ConnectionType.FS // && session.conf.getAll.contains("hive.metastore.uris")
        }
      connectionTypeIsHive || Utils.isRunningInDatabricks()
    }

    @JsonIgnore
    def connection(name: String): Option[ConnectionInfo] = connections.get(name)

    @JsonIgnore
    def connectionOptions(name: String): Map[String, String] =
      connections(name).options

    def checkValidity(
      storageHandler: StorageHandler,
      settings: Settings
    ): List[ValidationMessage] = {
      var errors = List.empty[ValidationMessage]
      if (this.env.nonEmpty && this.env != "None") {
        val envFile = new Path(
          DatasetArea.metadata(settings),
          "env." + this.env + ".sl.yml"
        )
        if (!storageHandler.exists(envFile)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"${envFile.getName()} not found !!!"
          )
        }
      }
      Try {
        Utils
          .loadInstance[GenericRowValidator](this.rowValidatorClass)
      } match {
        case scala.util.Failure(exception) =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"rowValidatorClass ${this.rowValidatorClass} not found"
          )
        case _ =>
      }

      Try {
        Utils
          .loadInstance[LoadStrategy](this.loadStrategyClass)
      } match {
        case scala.util.Failure(exception) =>
          exception.printStackTrace()
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"loadStrategyClass ${this.loadStrategyClass} not found"
          )
        case _ =>
      }

      if (!Set("spark", "native").contains(this.loader)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"loader ${this.loader} not supported"
        )
      }
      this.connections.foreach { case (name, connection) =>
        errors = errors ++ connection.checkValidity(name)(settings)
      }
      val path = new Path(this.root)
      if (!storageHandler.exists(path)) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"root ${this.root} not found"
        )
      }
      Try {
        this.sqlParameterPattern.r
      } match {
        case scala.util.Failure(exception) =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"sqlParameterPattern ${this.sqlParameterPattern} is not a valid regex"
          )
        case _ =>
      }
      if (this.rejectMaxRecords < 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"rejectMaxRecords ${this.rejectMaxRecords} must be positive"
        )
      }
      if (this.maxParCopy <= 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"maxParCopy ${this.maxParCopy} must be positive"
        )
      }
      val patterns = List(
        (this.forceViewPattern, "forceViewPattern"),
        (this.forceDomainPattern, "forceDomainPattern"),
        (this.forceTablePattern, "forceTablePattern"),
        (this.forceJobPattern, "forceJobPattern"),
        this.forceTaskPattern -> "forceTaskPattern"
      )
      patterns.foreach { case (value, name) =>
        Try {
          value.r
        } match {
          case Failure(exception) =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"$name value is not a valid regex"
            )
          case _ =>
        }
      }
      if (this.sessionDurationServe <= 0) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"sessionDurationServe ${this.sessionDurationServe} must be positive"
        )
      }

      if (this.connections.isEmpty) {
        errors = errors :+ ValidationMessage(
          Severity.Error,
          "AppConfig",
          s"connections: at least one connection must be defined"
        )
      }

      val validConnectionNames = this.connections.keys.mkString(", ")

      if (this.connectionRef.isEmpty) {
        val msg =
          if (this.connections.isEmpty)
            s"connectionRef must be defined. Define a connection first and set it to this newly defined connection"
          else
            s"connectionRef must be defined. Valid connection names are $validConnectionNames"
        errors = errors :+ ValidationMessage(Severity.Error, "AppConfig", msg)
      } else {
        this.connections.get(this.connectionRef) match {
          case Some(_) =>
          case None =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"Connection ${this.connectionRef} not found. Valid connection names are $validConnectionNames"
            )
        }
      }

      this.schedulePresets.foreach { case (name, cron) =>
        Try {
          cron.r
        } match {
          case Failure(exception) =>
            errors = errors :+ ValidationMessage(
              Severity.Error,
              "AppConfig",
              s"schedulePresets $name value is not a valid cron (${exception.getMessage})"
            )
          case _ =>
        }
      }

      val dagRef: List[String] =
        this.dagRef
          .map(ref => List(ref.load.toList, ref.transform.toList).flatten)
          .getOrElse(Nil)

      dagRef.foreach { dagRef =>
        val dagConfigRef = if (dagRef.endsWith(".yml")) dagRef else dagRef + ".sl.yml"
        val dagConfigPath = new Path(DatasetArea.dags(settings), dagConfigRef)
        if (!storageHandler.exists(dagConfigPath)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            "AppConfig",
            s"dagConfigRef $dagConfigRef not found in ${dagConfigPath.getParent}"
          )
        }
      }
      errors
    }

  }

  object AppConfig {
    private case class JsonWrapped(jsonValue: String) {

      @throws(classOf[ObjectStreamException])
      protected def readResolve: AnyRef = {
        val unwrapped = JsonWrapped.jsonMapper.readValue(jsonValue, classOf[AppConfig])
        unwrapped
      }
    }

    private object JsonWrapped {
      private def jsonMapper: ObjectMapper = new StarlakeObjectMapper()

      def apply(comet: AppConfig): JsonWrapped = {
        val writer = jsonMapper.writerFor(classOf[AppConfig])
        val asJson = writer.writeValueAsString(comet)
        JsonWrapped(asJson)
      }
    }
  }

  implicit val connectionTypeReader: ConfigReader[ConnectionType] =
    ConfigReader.fromString[ConnectionType](catchReadError(ConnectionType.fromString))

  implicit val storageLevelReader: ConfigReader[StorageLevel] =
    ConfigReader.fromString[StorageLevel](catchReadError(StorageLevel.fromString))

  implicit val timezoneReader: ConfigReader[TimeZone] =
    ConfigReader.fromString[TimeZone](catchReadError(TimeZone.getTimeZone))

  def loadConf(conf: Option[Config] = None): AppConfig = {
    ConfigSource
      .fromConfig(conf.getOrElse(referenceConfig))
      .loadOrThrow[AppConfig]
  }

  def loadApplication(content: String, appPath: Path, root: String): AppConfig = {
    val finalNode: JsonNode =
      YamlSerde
        .deserializeYamlApplicationNode(content, appPath.toString)
        .path("application")
    finalNode.asInstanceOf[ObjectNode].put("root", root)
    val jsonString = Utils.newJsonMapper().writeValueAsString(finalNode)
    _applicationConfig = ConfigFactory.parseString(jsonString).resolve()
    val effectiveApplicationConfig = _applicationConfig
      .withFallback(Settings.referenceConfig)
    val app = ConfigSource
      .fromConfig(effectiveApplicationConfig)
      .loadOrThrow[AppConfig]
    app
  }

  /** @param config
    *   : usually the default configuration loaded from reference.conf except in tests
    * @return
    *   final configuration after merging with application.conf & application. sl.yml
    */

  def apply(
    config: Config,
    env: Option[String],
    root: Option[String],
    aesSecretKey: Option[String],
    datasets: Option[String]
  ): Settings = {
    // Normalize Windows backslash paths to forward slashes to prevent YAML parsing issues
    val normalizedRoot = root.map(_.replace('\\', '/'))
    val normalizedDatasets = datasets.map(_.replace('\\', '/'))
    val jobId = UUID.randomUUID().toString
    val effectiveConfig =
      config.withValue("job-id", ConfigValueFactory.fromAnyRef(jobId, "per JVM instance"))

    // Load reference.conf
    val loadedConfig = loadConf(Some(effectiveConfig))
    val withRootUpdatedConfig =
      normalizedRoot
        .map { root =>
          def pathFromRoot(path: String): String = {
            val loadedConfigRootLen = loadedConfig.root.length
            if (loadedConfigRootLen == 0 && !path.contains(":/")) {
              root + path
            } else if (loadedConfigRootLen > 0 && path.startsWith(loadedConfig.root))
              root + path.substring(loadedConfigRootLen)
            else
              path
          }
          loadedConfig.copy(
            root = root,
            audit = loadedConfig.audit
              .copy(path = pathFromRoot(loadedConfig.audit.path)),
            expectations = loadedConfig.expectations
              .copy(path = pathFromRoot(loadedConfig.expectations.path)),
            datasets = normalizedDatasets.getOrElse(pathFromRoot(loadedConfig.datasets)),
            metadata = pathFromRoot(loadedConfig.metadata),
            lock = loadedConfig.lock.copy(path = pathFromRoot(loadedConfig.lock.path)),
            metrics = loadedConfig.metrics
              .copy(path = pathFromRoot(loadedConfig.metrics.path)),
            dags = pathFromRoot(loadedConfig.dags),
            writeStrategies = pathFromRoot(loadedConfig.writeStrategies),
            loadStrategies = pathFromRoot(loadedConfig.loadStrategies),
            types = pathFromRoot(loadedConfig.types),
            macros = pathFromRoot(loadedConfig.macros),
            tests = pathFromRoot(loadedConfig.tests)
          )
        }
        .getOrElse(loadedConfig)

    val withEnvUpdatedEnvConfig =
      env
        .orElse(Option(System.getenv("SL_ENV")))
        .map(env => withRootUpdatedConfig.copy(env = env))
        .getOrElse(withRootUpdatedConfig)

    logger.info(
      "root=" + config.getString("root")
    )
    logger.info(
      "ENV SL_ROOT=" + Option(System.getenv("SL_ROOT")).getOrElse("")
    )

    val rootUpdatedEffectiveConfig =
      effectiveConfig
        .withValue("root", ConfigValueFactory.fromAnyRef(root.getOrElse(System.getenv("SL_ROOT"))))
        .withValue("audit.path", ConfigValueFactory.fromAnyRef(withRootUpdatedConfig.audit.path))
        .withValue(
          "expectations.path",
          ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.expectations.path)
        )
        .withValue("types", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.types))
        .withValue("macros", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.macros))
        .withValue("tests", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.tests))
        .withValue("datasets", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.datasets))
        .withValue("metadata", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.metadata))
        .withValue("lock.path", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.lock.path))
        .withValue(
          "metrics.path",
          ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.metrics.path)
        )
        .withValue("dags", ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.dags))
        .withValue(
          "writeStrategies",
          ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.writeStrategies)
        )
        .withValue(
          "loadStrategies",
          ConfigValueFactory.fromAnyRef(withEnvUpdatedEnvConfig.loadStrategies)
        )

    val withUpdatedEnvConfig =
      env
        .orElse(Option(System.getenv("SL_ENV")))
        .map(env => rootUpdatedEffectiveConfig.withValue("env", ConfigValueFactory.fromAnyRef(env)))
        .getOrElse(rootUpdatedEffectiveConfig)

    logger.debug(YamlSerde.serialize(withEnvUpdatedEnvConfig))
    val settings =
      Settings(
        withEnvUpdatedEnvConfig,
        effectiveConfig.getConfig("spark"),
        effectiveConfig.getConfig("extra")
      )
    // Load application.conf / application.sl.yml
    val loadedSettings =
      loadApplicationYaml(withUpdatedEnvConfig, settings, env, normalizedRoot)
        .orElse(loadApplicationConf(withUpdatedEnvConfig, settings, env))
        .getOrElse(settings)

    val decryptedLoadedSettings =
      aesSecretKey.orElse(Option(System.getenv("SL_AES_SECRET_KEY"))) match {
        case None =>
          loadedSettings
        case Some(key) if key.isEmpty =>
          loadedSettings
        case Some(key) =>
          val decryptedConnections =
            loadedSettings.appConfig.connections.view.mapValues { connectionInfo =>
              connectionInfo.withDecryptedPassword(key)
            }.toMap
          loadedSettings.copy(
            appConfig = loadedSettings.appConfig.copy(
              connections = decryptedConnections
            )
          )
      }
    val applicationConfSettings =
      if (settings.appConfig.duckdbMode) duckDBMode(decryptedLoadedSettings)
      else {
        adjustConnectionProperties(decryptedLoadedSettings)
      }

    // Reload Storage Handler with the authentication settings
    applicationConfSettings.storageHandler(reload = true)

    // Reload Schema Handler to ensure it has the latest environment variables
    applicationConfSettings.schemaHandler(reload = true)

    // Load fairscheduler.xml
    val jobConf = initSparkConfig(applicationConfSettings)
    val withSparkConfig = applicationConfSettings.copy(jobConf = jobConf)
    val withDefaultSchedules = addDefaultSchedules(withSparkConfig)

    val localCatalog = jobConf.get("spark.localCatalog", "none")
    val writeFormat =
      localCatalog match {
        case "none" => // do nothing
          "parquet"
        case "iceberg" | "delta" =>
          localCatalog
        case "hive" => // do nothing
          "parquet"
        case other =>
          logger.warn(s"Unknown localCatalog type: $other")
          other
      }

    val withDefaultWriteFormat =
      withDefaultSchedules.copy(appConfig =
        withDefaultSchedules.appConfig.copy(
          defaultWriteFormat = writeFormat,
          defaultRejectedWriteFormat = writeFormat,
          defaultAuditWriteFormat = writeFormat
        )
      )
    withDefaultWriteFormat
  }

  val defaultCronPresets = Map(
    "hourly"  -> "0 * * * *",
    "daily"   -> "0 0 * * *",
    "weekly"  -> "0 0 * * 1",
    "monthly" -> "0 0 1 * *",
    "yearly"  -> "0 0 1 1 *"
  )

  def addDefaultSchedules(settings: Settings): Settings = {
    val schedules = defaultCronPresets ++ settings.appConfig.schedulePresets
    settings.copy(appConfig = settings.appConfig.copy(schedulePresets = schedules))
  }

  private def loadApplicationConf(
    effectiveConfig: Config,
    settings: Settings,
    env: Option[String] = None
  ): Option[Settings] = {
    val applicationConfPath = new Path(DatasetArea.metadata(settings), "application.conf")
    if (settings.storageHandler().exists(applicationConfPath)) {
      logger.info(s"Loading $applicationConfPath")
      val applicationConfContent = settings.storageHandler().read(applicationConfPath)
      val applicationConfig = ConfigFactory.parseString(applicationConfContent).resolve()
      val effectiveApplicationConfig = applicationConfig
        .withFallback(effectiveConfig)
      logger.debug(effectiveApplicationConfig.toString)
      val mergedSettings = ConfigSource
        .fromConfig(effectiveApplicationConfig)
        .loadOrThrow[AppConfig]

      Some(
        Settings(
          mergedSettings,
          effectiveApplicationConfig.getConfig("spark"),
          effectiveApplicationConfig.getConfig("extra")
        )
      )
    } else {
      None
    }
  }

  /** Load application.sl.yml from metadata folder
    * @param effectiveConfig:
    *   config to merge with application.sl.yml
    * @param settings
    *   :
    * @return
    */
  private def loadApplicationYaml(
    effectiveConfig: Config,
    settings: Settings,
    env: Option[String],
    root: Option[String]
  ): Option[Settings] = {
    val applicationYmlPath =
      new Path(DatasetArea.metadata(settings), "application.sl.yml")
    val applicationYmlConfig =
      if (settings.storageHandler().exists(applicationYmlPath)) {
        logger.info(s"Loading $applicationYmlPath")
        val schemaHandler = settings.schemaHandler()
        val applicationYmlContent = settings.storageHandler().read(applicationYmlPath)
        val content =
          Try {
            val vars = schemaHandler.activeEnvVars(reload = true, env, root)
            val varsWithRoot = {
              root match {
                case Some(root) =>
                  vars + ("SL_ROOT" -> root)
                case None =>
                  val slRoot = Option(System.getenv("SL_ROOT"))
                    .getOrElse(throw new StarlakeConfigException("SL_ROOT not defined"))
                  vars + ("SL_ROOT" -> slRoot)
              }
            }

            Utils
              .parseJinja(applicationYmlContent, varsWithRoot)(
                settings
              )
          } match {
            case Success(value) => value
            case Failure(exception) =>
              throw new Exception(
                s"Error while parsing Jinja in ${applicationYmlPath.toString}",
                exception
              )
          }
        val finalNode: JsonNode =
          YamlSerde
            .deserializeYamlApplicationNode(content, applicationYmlPath.toString)
            .path("application")
        val jsonString = Utils.newJsonMapper().writeValueAsString(finalNode)
        val applicationConfig = ConfigFactory.parseString(jsonString).resolve()
        Some(applicationConfig)
      } else {
        None
      }

    val applicationSettings = applicationYmlConfig match {
      case Some(applicationConfig) =>
        val effectiveApplicationConfig = applicationConfig
          .withFallback(effectiveConfig)
        logger.debug(effectiveApplicationConfig.toString)

        val mergedAppConfig = loadConf(Some(effectiveApplicationConfig))

        val applicationSettings = Settings(
          mergedAppConfig,
          effectiveApplicationConfig.getConfig("spark"),
          effectiveApplicationConfig.getConfig("extra")
        )
        Some(applicationSettings)
      case None =>
        None
    }

    // Reload with the authentication settings
    applicationSettings.foreach(_.storageHandler(reload = true))

    // Reload schema handler to ensure it has the latest environment variables
    applicationSettings.foreach(_.schemaHandler(reload = true))

    applicationSettings
  }

  private def initSparkConfig(settings: Settings): SparkConf = {
    val schedulingConfig = schedulingPath(settings)

    // When using local Spark with remote BigQuery (useful for testing)
    val initialConf =
      settings.appConfig.internal.flatMap(_.temporaryGcsBucket) match {
        case Some(value) => new SparkConf().set("temporaryGcsBucket", value)
        case None        => new SparkConf()
      }

    val thisConf = settings.sparkConfig
      .entrySet()
      .asScala
      .toVector
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .foldLeft(initialConf) { case (conf, (key, value)) =>
        logger.debug(s"Setting key: ${key}")
        // flatten keys
        val flatKey = key.replaceAll(""""""", "")
        val finalKey = if (key.startsWith("_.")) {
          flatKey.replaceFirst("_\\.", "")
        } else {
          "spark." + flatKey
        }
        conf.set(finalKey, value)
      }
      .set("spark.scheduler.mode", settings.appConfig.sparkScheduling.mode)

    schedulingConfig.foreach(path => thisConf.set("spark.scheduler.allocation.file", path.toString))

    logger.whenDebugEnabled {
      logger.debug(thisConf.toDebugString)
    }
    thisConf
  }

  private def schedulingPath(settings: Settings): Option[Path] = {
    import settings.appConfig.sparkScheduling.*
    if (file.isEmpty) {
      val schedulingPath = new Path(DatasetArea.metadata(settings), "fairscheduler.xml")
      Some(schedulingPath).filter(settings.storageHandler().exists)
    } else
      Some(new Path(file))
  }

  def adjustConnectionProperties(settings: Settings): Settings = {
    val connections = settings.appConfig.connections
    val updatedConnections =
      connections.map { case (name, connection) =>
        val updatedConnection =
          if (connection.isSnowflake()) {
            val options = connection.options
            if (options.get("authenticator").contains("user/password"))
              connection.copy(options = options.removed("authenticator"))
            else
              connection
          } else if (connection.isDuckDb())
            connection.copy(sparkFormat = None) // spark mode not supported in duckdb
          else connection
        name -> updatedConnection
      }
    val updatedAppConfig = settings.appConfig.copy(connections = updatedConnections)
    settings.copy(appConfig = updatedAppConfig)
  }

  /** Secure DuckDB path to avoid security issues with DuckDB database names
    *
    * This method ensures that the DuckDB database name does not start with "duckdb." or contain
    * invalid characters like "/" or "\\" and its name is always equal to the env name
    */
  def secureDuckDbPath(settings: Settings): Settings = {
    def removeSchemeFromFilename(filename: String): String = {
      val stripped =
        if (filename.contains("file://"))
          filename.substring(7)
        else if (filename.contains("file:"))
          filename.substring(5)
        else
          filename
      // On Windows, Hadoop paths may have a leading / before drive letter (e.g. /C:/...)
      if (stripped.length > 2 && stripped.charAt(0) == '/' && stripped.charAt(2) == ':')
        stripped.substring(1)
      else
        stripped
    }
    val updatedConnections = settings.appConfig.connections
      .map { case (k, v) =>
        val isDuckLake =
          v.options.get("preActions").exists(preActions => preActions.contains("ducklake:"))
        val connectionWithTranspileInfo = SQLUtils.transpilerDialect(v) match {
          case JSQLTranspiler.Dialect.DUCK_DB if isDuckLake =>
            v.copy(options = v.options.updated("url", s"jdbc:duckdb:"))
          case JSQLTranspiler.Dialect.DUCK_DB =>
            val urlParts = v.jdbcUrl.split(":", 3)
            require(
              urlParts.length == 3,
              "DuckDB JDBC URL should be in the form jdbc:duckdb:dbname"
            )
            val dbName = urlParts(2)
            require(!dbName.contains(".."), "DuckDB database name should not contain '..'")
            val validPath = DatasetArea.duckdbPath(s"$k.db")(settings).toString
            val duckDbPath = DatasetArea.duckdbPath()(settings).toString
            require(
              isDuckLake ||
              (k == "sl_duckdb" && urlParts(2) == removeSchemeFromFilename(duckDbPath)) ||
              dbName == removeSchemeFromFilename(validPath) ||
              dbName == s"$k.db",
              s"Invalid DuckDB database URL: $k:$dbName. It should match the connection name or be 'duckdb.db' for 'sl_duckdb'."
            )
            val pathAsString =
              java.nio.file.Paths
                .get(DatasetArea.secureDuckdbPath(dbName.split('/').last)(settings).toUri)
                .toString
                .replace('\\', '/')
            v.copy(options = v.options.updated("url", s"jdbc:duckdb:$pathAsString"))
          case _ =>
            v
        }
        k -> connectionWithTranspileInfo
      }
    val updatedAppConfig = settings.appConfig.copy(connections = updatedConnections)
    settings.copy(appConfig = updatedAppConfig)
  }

  def duckDBMode(settings: Settings): Settings = {
    val duckdbPath = DatasetArea.duckdbPath()(settings)
    val pathAsString = StorageHandler.localFile(duckdbPath).pathAsString.replace('\\', '/')
    val duckDBConnection = ConnectionInfo(
      `type` = ConnectionType.JDBC,
      sparkFormat = None,
      options = Map(
        "url"    -> s"jdbc:duckdb:$pathAsString",
        "driver" -> "org.duckdb.DuckDBDriver"
      )
    )
    val duckdbFile = File(pathAsString)
    if (!duckdbFile.exists) {
      if (!duckdbFile.parent.exists)
        duckdbFile.parent.createDirectories()
      Utils.withResources(DriverManager.getConnection(duckDBConnection.jdbcUrl)) { _ => }
    }
    val updatedConnections = settings.appConfig.connections
      .map { case (k, v) =>
        val duckDBConnectionWithTranspileInfo = SQLUtils.transpilerDialect(v) match {
          case JSQLTranspiler.Dialect.DUCK_DB =>
            duckDBConnection.copy(_transpileDialect = None)
          case Dialect.SNOWFLAKE =>
            duckDBConnection.copy(
              _transpileDialect = Some(Dialect.SNOWFLAKE.name()),
              options = duckDBConnection.options ++ v.options.view
                .filterKeys(_ == "account") // carry over account for snowflake cortex
            )
          case dialect =>
            duckDBConnection.copy(_transpileDialect = Some(dialect.name()))
        }

        k -> duckDBConnectionWithTranspileInfo
      }
      .updated("duckdb", duckDBConnection)

    val audit = settings.appConfig.audit.copy(database = None)
    val updatedAppConfig = settings.appConfig.copy(connections = updatedConnections)
    val configWithDuckDB =
      if (updatedAppConfig.connectionRef.isEmpty)
        updatedAppConfig.copy(connectionRef = "duckdb", database = "", audit = audit)
      else
        updatedAppConfig

    settings.copy(appConfig = configWithDuckDB)

  }
}

object CometColumns {
  val cometInputFileNameColumn: String = "sl_input_file_name"
  val oldCometInputFileNameColumn: String = "comet_input_file_name"
  val slSuccessColumn: String = "sl_success"
  val slErrorMessageColumn: String = "sl_error_message"
}

final case class ApplicationDesc(version: Int, application: Settings.AppConfig)

/** This class holds the current Comet settings and an assembly of reference instances for core,
  * shared services
  *
  * SMELL: this may be the start of a Dependency Injection root (but at 2-3 objects, is DI
  * justified? probably not quite yet) — cchepelov
  */
final case class Settings(
  appConfig: Settings.AppConfig,
  sparkConfig: Config,
  extraConf: Config,
  jobConf: SparkConf = new SparkConf(),
  created: Long = System.currentTimeMillis()
) {

  @transient
  def schemaHandler(
    cliEnv: Map[String, String] = Map.empty,
    reload: Boolean = false
  ): SchemaHandler = {
    val root = appConfig.root
    val env = Option(appConfig.env)
    val schemaHandler = CaffeineSettingsManager.getSchemaHandler(root, env)
    schemaHandler match {
      case Some(handler) if !reload && cliEnv.isEmpty =>
        handler
      case _ =>
        implicit val self: Settings = this
        val handler = new SchemaHandler(this.storageHandler(), cliEnv)
        CaffeineSettingsManager.setSchemaHandler(root, env, handler)
        handler
    }
  }

  @transient
  def getWarehouseDir(): Option[String] = if (this.sparkConfig.hasPath("sql.warehouse.dir"))
    Some(this.sparkConfig.getString("sql.warehouse.dir"))
  else None

  @transient
  def storageHandler(reload: Boolean = false): StorageHandler = {
    val root = appConfig.root
    val env = Option(appConfig.env)
    val storageHandler = CaffeineSettingsManager.getStorageHandler(root, env)
    storageHandler match {
      case Some(handler) if !reload => handler
      case _ =>
        implicit val self: Settings = this
        val handler =
          if (SystemUtils.IS_OS_WINDOWS || appConfig.useLocalFileSystem)
            new LocalStorageHandler()
          else
            new HdfsStorageHandler(appConfig.fileSystem)
        Try(this.appConfig.getDefaultConnection().options).map { options =>
          handler.initFS(options)
        }
        CaffeineSettingsManager.setStorageHandler(root, env, handler)
        handler
    }
  }

}

object PrivacyLevels {
  private def make(schemeName: String, encryptionAlgo: String): (TransformEngine, List[String]) = {
    val (privacyObject, typedParams) = TransformEngine.parse(encryptionAlgo)
    val encryption = Utils.loadInstance[TransformEngine](privacyObject)
    (encryption, typedParams)
  }

  private var allPrivacy = Map.empty[String, ((TransformEngine, List[String]), TransformInput)]

  def resetAllPrivacy(): Unit =
    allPrivacy = Map.empty[String, ((TransformEngine, List[String]), TransformInput)]

  @transient
  def allPrivacyLevels(
    options: Map[String, String]
  ): Map[String, ((TransformEngine, List[String]), TransformInput)] = {
    if (allPrivacy.isEmpty) {
      allPrivacy = options.map { case (k, objName) =>
        val encryption = make(k, objName)
        val key = k.toUpperCase(Locale.ROOT)
        (key, (encryption, new TransformInput(key, false)))
      }
    }
    allPrivacy
  }

  def traverse(config: AppConfig): Unit = {
    val jsonNode = YamlSerde.mapper.valueToTree(config).asInstanceOf[JsonNode]
    traverse(jsonNode, jsonNode, "")
  }

  def traverse(refNode: JsonNode, incomingNode: JsonNode, keyPrefix: String): Unit = {
    val itRef = refNode.fields().asScala.toList.sortBy(_.getKey).iterator
    val itIncoming = incomingNode.fields().asScala.toList.sortBy(_.getKey).iterator
    while (itRef.hasNext) {
      val refField = itRef.next()
      val refKey = refField.getKey
      val refValue = refField.getValue
      val incomingField = itIncoming.next()
      val incomingKey = incomingField.getKey
      val incomingValue = incomingField.getValue

      if (refValue.isObject) {
        traverse(refValue, incomingValue, s"$keyPrefix$refKey.")
      } else {
        val refText = refValue.asText()
        val incomingText = incomingValue.asText()
        if (incomingText != refText)
          println(s"$keyPrefix$refKey = $incomingText")
      }
    }
  }
}
