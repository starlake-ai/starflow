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

package ai.starlake

import ai.starlake.config.{DatasetArea, Settings, SparkEnv}
import ai.starlake.job.ingest.{IngestConfig, LoadConfig, StageConfig}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model.{AutoTaskInfo, DomainInfo, TableAttribute}
import ai.starlake.utils._
import ai.starlake.workflow.IngestionWorkflow
import better.files.{File => BetterFile}
import com.dimafeng.testcontainers._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DatasetLogging, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}
import org.testcontainers.utility.DockerImageName

import java.io.{File, InputStream}
import java.nio.file.Files
import java.time.LocalDate
import java.util.UUID
import scala.annotation.nowarn
import scala.io.{Codec, Source}
import scala.reflect.io.Directory
import scala.util.Try

trait TestHelper
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with LazyLogging
    with DatasetLogging
    with PgContainerHelper {

  override protected def beforeEach(): Unit = {}

  override protected def afterEach(): Unit = {}

  override protected def afterAll(): Unit = {
    TestHelper.stopSession()
    BetterFile(starlakeTestRoot).delete(swallowIOExceptions = true)
  }

  override protected def beforeAll(): Unit = {
    Settings.invalidateCaches()
  }

  private lazy val starlakeTestPrefix: String = s"starlake-test-${TestHelper.runtimeId}"

  private def starlakeTestInstanceId: String =
    s"${this.getClass.getSimpleName}-${java.util.UUID.randomUUID()}"

  def starlakeTestId: String = s"${starlakeTestPrefix}-${starlakeTestInstanceId}"

  lazy val tempDir = Files.createTempDirectory(starlakeTestId)

  lazy val starlakeTestRoot: String =
    if (sys.env.getOrElse("SL_INTERNAL_WITH_ENVS_SET", "false").toBoolean) {
      sys.env.getOrElse("SL_ROOT", tempDir.toString)
    } else {
      Option(System.getProperty("os.name")).map(_.toLowerCase contains "windows") match {
        case Some(true) =>
          BetterFile(tempDir).pathAsString
            .replace("\\", "/")
        case _ => BetterFile(tempDir).pathAsString
      }
    }
  lazy val starlakeDatasetsPath: String = starlakeTestRoot + "/datasets"
  lazy val starlakeMetadataPath: String = starlakeTestRoot + "/metadata"
  lazy val starlakeLoadPath: String = starlakeMetadataPath + "/load"

  def baseConfigString =
    s"""
       |SL_REJECT_WITH_VALUE=true
       |SL_VALIDATE_ON_LOAD=true
       |SL_ASSERTIONS_ACTIVE=true
       |SL_DEFAULT_WRITE_FORMAT=delta
       |SL_ROOT="${starlakeTestRoot}"
       |SL_TEST_ID="${starlakeTestId}"
       |SL_LOCK_PATH="${starlakeTestRoot}/locks"
       |SL_METRICS_PATH="${starlakeTestRoot}/audit/metrics/{{schema}}"
       |SL_AUDIT_PATH="${starlakeTestRoot}/audit"
       |SL_UDFS="ai.starlake.udf.TestUdf"
       |TEMPORARY_GCS_BUCKET="${sys.env.getOrElse("TEMPORARY_GCS_BUCKET", "invalid_gcs_bucket")}"
       |SL_ACCESS_POLICIES_LOCATION="europe-west1"
       |SL_ACCESS_POLICIES_TAXONOMY="GDPR"
       |SL_ACCESS_POLICIES_PROJECT_ID="${sys.env
        .getOrElse("SL_ACCESS_POLICIES_PROJECT_ID", "invalid_project")}"
       |include required("application-test.conf")
       |connections.test-pg {
       |    type = "jdbc"
       |    options {
       |      "url": "${pgContainer.jdbcUrl}"
       |      "user": "test"
       |      "password": "test"
       |      "driver": "org.postgresql.Driver"
       |      "quoteIdentifiers": false
       |    }
       |  }
       |connections.test-mysql {
       |    type = "jdbc"
       |    options {
       |      "url": "${TestHelper.mysqlContainer.jdbcUrl}"
       |      "user": "test"
       |      "password": "test"
       |      "driver": "com.mysql.cj.jdbc.Driver"
       |      "quoteIdentifiers": false
       |    }
       |  }
       |connections.audit {
       |    type = "jdbc"
       |    options {
       |      "url": "${pgContainer.jdbcUrl}"
       |      "driver": "org.postgresql.Driver"
       |      "user": "test"
       |      "password": "test"
       |      "quoteIdentifiers": false
       |    }
       |  }
       |spark {
       |  sql.warehouse.dir: "${starlakeTestRoot}"
       |}
       |
       |""".stripMargin

  def testConfiguration: Config = {
    Settings.invalidateCaches()
    val rootConfig = ConfigFactory.parseString(
      baseConfigString,
      ConfigParseOptions.defaults().setAllowMissing(false)
    )
    val testConfig =
      ConfigFactory
        .load(rootConfig) // , ConfigResolveOptions.noSystem())
        .withValue(
          "lock.poll-time",
          ConfigValueFactory.fromAnyRef("5 ms")
        ) // in local mode we don't need to wait quite as much as we do on a real cluster
    testConfig

  }

  val allTypes: List[FileToImport] = List(
    FileToImport(
      "default.sl.yml",
      "/types/default.sl.yml"
    ),
    FileToImport(
      "types.sl.yml",
      "/sample/types.sl.yml"
    )
  )

  val allExtracts: List[FileToImport] = List(
    FileToImport(
      "create.j2",
      "/sample/ddl/bigquery/create.j2",
      Some("ddl/bigquery")
    )
  )

  val allExpectations: List[FileToImport] = List(
    FileToImport(
      "default.j2",
      "/sample/expectations/default.j2"
    )
  )

  val allDags: List[FileToImport] = List(
    FileToImport(
      "sample.sl.yml",
      "/dag/sample.sl.yml"
    ),
    FileToImport(
      "sample.py.j2",
      "/templates/dags/sample.py.j2"
    )
  )

  val applicationYmlConfig: List[FileToImport] = List(
    FileToImport(
      "application.sl.yml",
      "/config/application.sl.yml"
    )
  )

  private def readSourceContentAsString(source: Source): String = {
    source.getLines().mkString("\n")
  }

  def loadTextFile(filename: String)(implicit codec: Codec): String = {
    logger.info("loading resource " + filename)
    val stream: InputStream = getClass.getResourceAsStream(filename)
    Utils.withResources(Source.fromInputStream(stream))(readSourceContentAsString)
  }

  def loadBinaryFile(filename: String)(implicit codec: Codec): Array[Char] = {
    val stream: InputStream = getClass.getResourceAsStream(filename)
    Iterator
      .continually(stream.read())
      .takeWhile(_ != -1)
      .toArray
      .map(_.toChar)
  }

  def readFileContent(path: String): String =
    Utils.withResources(Source.fromFile(path))(readSourceContentAsString)

  def readFileContent(path: Path): String = readFileContent(path.toUri.getPath)

  /** One BigQuery dataset per suite instance, so live-BQ suites in the same sequential run cannot
    * pollute each other's datasets. Referenced in fixtures via __SL_BQ_DATASET__.
    */
  lazy val testBQDatasetName: String =
    s"bqtest_${this.getClass.getSimpleName}_${_testId.replace("-", "").take(8)}".toLowerCase
      .replaceAll("[^a-z0-9_]", "_")

  def applyTestFileSubstitutions(fileContent: String): String = {
    fileContent
      .replaceAll("__SL_TEST_ROOT__", starlakeTestRoot)
      .replaceAll("__SL_BQ_DATASET__", testBQDatasetName)
  }

  def withSettings(configuration: Config)(op: Settings => Assertion): Assertion = {
    try {
      implicit val settings = Settings(configuration, None, None, None, None)
      op(settings)
    } catch {
      case e: Throwable =>
        logger.error("Error in test", e)
        throw e
    } finally {
      // SparkSession.clearActiveSession()
      // SparkSession.clearDefaultSession()
    }
  }

  def withSettings(op: Settings => Assertion): Assertion = withSettings(testConfiguration)(op)

  def getResPath(path: String): String = getClass.getResource(path).toURI.getPath

  def prepareSchema(schema: StructType): StructType =
    StructType(schema.fields.filterNot(f => List("year", "month", "day").contains(f.name)))

  def getTodayCondition: String = {
    val now = LocalDate.now
    s"year=${now.getYear} and month=${now.getMonthValue} and day=${now.getDayOfMonth}"
  }

  def getTodayPartitionCondition: String = {
    val now = LocalDate.now
    s"year=${now.getYear} and month=${now.getMonthValue} and day=${now.getDayOfMonth}"
  }

  abstract class WithSettings(configuration: Config = testConfiguration) {
    implicit val settings: Settings = Settings(configuration, None, None, None, None)
    settings.appConfig.connections.foreach { case (k, v) => v.checkValidity(k) }
    implicit def withSettings: WithSettings = this
    def storageHandler = settings.storageHandler()
    // TestHelper.sparkSessionReset
    @nowarn val mapper: ObjectMapper with ScalaObjectMapper = {
      val mapper = new StarlakeObjectMapper(new YAMLFactory(), (classOf[Settings], settings) :: Nil)
      mapper
    }

    def deliverTestFile(importPath: String, targetPath: Path)(implicit codec: Codec): Unit = {
      val content = loadTextFile(importPath)
      val testContent = applyTestFileSubstitutions(content)
      storageHandler.mkdirs(targetPath.getParent)
      storageHandler.write(testContent, targetPath)(codec.charSet)

      logger.whenTraceEnabled {
        if (content != testContent) {
          logger.trace(s"delivered ${importPath} to ${targetPath.toString}, WITH substitutions")
        } else {
          logger.trace(s"delivered ${importPath} to ${targetPath.toString}")
        }
      }
    }

    def deliverBinaryFile(importPath: String, targetPath: Path)(implicit codec: Codec): Unit = {
      val content: Array[Char] = loadBinaryFile(importPath)
      storageHandler.writeBinary(content.map(_.toByte), targetPath)
    }

    def cleanMetadata(implicit settings: Settings): Try[Unit] =
      Try {
        val fload = new File(starlakeLoadPath)
        val dir = new Directory(fload)
        dir.deleteRecursively()
        fload.mkdirs()

        DatasetArea.initMetadata(storageHandler)
        deliverTypesFiles()
        settings.schemaHandler(reload = true)
      }

    def deliverTypesFiles() = {
      allTypes.foreach { typeToImport =>
        val typesPath = new Path(DatasetArea.types, typeToImport.name)
        deliverTestFile(typeToImport.path, typesPath)
      }
      allExpectations.foreach { assertionToImport =>
        val expectationPath = new Path(DatasetArea.expectations, assertionToImport.name)
        deliverTestFile(assertionToImport.path, expectationPath)
      }
      allExtracts.foreach { extractToImport =>
        val path = extractToImport.folder match {
          case None =>
            DatasetArea.extract.toString
          case Some(folder) =>
            DatasetArea.extract.toString + "/" + folder
        }
        storageHandler.mkdirs(new Path(path))
        val extractPath = new Path(path, extractToImport.name)
        deliverTestFile(extractToImport.path, extractPath)
      }
      allDags.foreach { dagImport =>
        val dagPath = new Path(DatasetArea.dags, dagImport.name)
        deliverTestFile(dagImport.path, dagPath)
      }
    }

    // Init
    new File(starlakeTestRoot).mkdirs()
    new File(starlakeDatasetsPath).mkdir()
    new File(starlakeMetadataPath).mkdir()
    new File(starlakeTestRoot + "/yelp").mkdir()
    new File(starlakeTestRoot + "/DOMAIN").mkdir()
    new File(starlakeTestRoot + "/dream").mkdir()
    new File(starlakeTestRoot + "/json").mkdir()
    new File(starlakeTestRoot + "/position").mkdir()

    DatasetArea.initMetadata(storageHandler)
    deliverTypesFiles()

  }

  // private val sparkSessionInterest = TestHelper.TestSparkSessionInterest()

  /*
  def sparkSession(implicit settings: Settings) = {
    sparkSessionInterest.get(settings)
  }
   */
  protected val _testId: String = UUID.randomUUID().toString

  def sparkSession(implicit isettings: Settings): SparkSession = {
    TestHelper.sparkSession(isettings, _testId)
  }

  def sparkSessionReset(implicit isettings: Settings): SparkSession = {
    TestHelper.sparkSession(isettings, _testId, forceReset = true)
  }

  abstract class SpecTrait(
    val sourceDomainOrJobPathname: String,
    val datasetDomainName: String,
    val sourceDatasetPathName: String,
    val jobFilename: Option[String] = None
  )(implicit withSettings: WithSettings) {

    implicit def settings: Settings = withSettings.settings
    def storageHandler: StorageHandler = settings.storageHandler()
    val domainMetadataRootPath: Path = DatasetArea.load
    val jobMetadataRootPath: Path = DatasetArea.transform

    def cleanDatasets: Try[Unit] =
      Try {
        val fDatasets = new File(starlakeDatasetsPath)
        val dir = new Directory(fDatasets)
        dir.deleteRecursively()
        new File(starlakeDatasetsPath).mkdir()
        jobFilename match {
          case Some(_) =>
            deliverSourceJob()
          case None =>
            deliverSourceDomain()
        }
      }

    def deliverSourceDomain(): Unit = {
      deliverSourceDomain(datasetDomainName, sourceDomainOrJobPathname)
    }

    def deliverSourceDomain(datasetDomainName: String, sourceDomainOrJobPathname: String): Unit = {
      val domainPath = new Path(domainMetadataRootPath, s"$datasetDomainName/_config.sl.yml")

      withSettings.deliverTestFile(sourceDomainOrJobPathname, domainPath)
    }

    def deliverSourceTable(sourceTablePath: String): Unit = {
      deliverSourceTable(datasetDomainName, sourceTablePath)
    }

    def deliverSourceTable(
      datasetDomainName: String,
      sourceTablePath: String,
      tableFileName: Option[String] = None
    ): Unit = {
      val fileName = tableFileName.getOrElse(new Path(sourceTablePath).getName)
      val tablePath = new Path(domainMetadataRootPath, s"$datasetDomainName/$fileName")
      withSettings.deliverTestFile(sourceTablePath, tablePath)
    }

    def deleteSourceDomain(datasetDomainName: String, sourceDomainOrJobPathname: String): Unit = {
      val domainPath = new Path(domainMetadataRootPath, s"$datasetDomainName/_config.sl.yml")
      storageHandler.delete(domainPath)
    }

    def getTablePath(domain: String, table: String): String = {
      val tblMetadata = sparkSession.sessionState.catalog.getTableMetadata(
        new TableIdentifier(table, Some(domain))
      )
      tblMetadata.location.getPath

    }

    def deleteSourceDomains(): Unit = {
      val fload = new File(starlakeLoadPath)
      new Directory(fload).deleteRecursively()
      fload.mkdirs()
      settings.schemaHandler(reload = true)
    }

    def deliverSourceJob(): Unit = {
      jobFilename.foreach(
        deliverSourceJob(sourceDomainOrJobPathname, datasetDomainName, _)
      )
    }

    def deliverSourceJob(
      sourceDomainOrJobPathname: String,
      datasetDomainName: String,
      domainOrJobFilename: String
    ): Unit = {
      val jobPath = new Path(jobMetadataRootPath, s"$datasetDomainName/$domainOrJobFilename")
      withSettings.deliverTestFile(sourceDomainOrJobPathname, jobPath)

    }

    protected def loadWorkflow()(implicit codec: Codec): IngestionWorkflow = {
      loadWorkflow(datasetDomainName, sourceDatasetPathName)
    }

    protected def loadWorkflow(datasetDomainName: String, sourceDatasetPathName: String)(implicit
      codec: Codec
    ): IngestionWorkflow = {
      val targetPath = DatasetArea.path(
        DatasetArea.stage(datasetDomainName),
        new Path(sourceDatasetPathName).getName
      )

      withSettings.deliverTestFile(sourceDatasetPathName, targetPath)

      val schemaHandler = settings.schemaHandler()
      schemaHandler.checkValidity()

      DatasetArea.initMetadata(storageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))

      val validator = new IngestionWorkflow(storageHandler, schemaHandler)
      validator
    }

    def loadPending(implicit codec: Codec): Try[SparkJobResult] = {
      val validator = loadWorkflow()
      validator.load(
        LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
      )
    }

    def secure(config: LoadConfig): Try[Boolean] = {
      val validator = loadWorkflow()
      validator.secure(config)
    }

    def load(config: IngestConfig): Try[JobResult] = {
      val validator = loadWorkflow()
      validator.load(config)
    }

    def getTasks(): List[AutoTaskInfo] = {
      settings.schemaHandler().tasks()
    }

    def getDomains(): List[DomainInfo] = {
      settings.schemaHandler().domains()
    }

    def getDomain(domainName: String): Option[DomainInfo] = {
      settings.schemaHandler().getDomain(domainName)
    }

    def landingPath: String =
      settings
        .schemaHandler()
        .getDomain(datasetDomainName)
        .map(_.resolveDirectory())
        .getOrElse(throw new Exception("Incoming directory must be specified in domain descriptor"))

    def loadLanding(implicit codec: Codec, createAckFile: Boolean = true): Unit = {

      val schemaHandler = settings.schemaHandler()

      DatasetArea.initMetadata(storageHandler)
      DatasetArea.initDomains(storageHandler, schemaHandler.domains().map(_.name))

      // Get incoming directory from Domain descriptor
      val incomingDirectory = schemaHandler.getDomain(datasetDomainName).map(_.resolveDirectory())
      assert(incomingDirectory.isDefined)

      // Deliver file to incoming folder
      val targetPath = new Path(incomingDirectory.get, new Path(sourceDatasetPathName).getName)
      withSettings.deliverBinaryFile(sourceDatasetPathName, targetPath)
      if (createAckFile) {
        storageHandler.touchz(
          new Path(targetPath.getParent, targetPath.getName.replaceFirst("\\.[^.]+$", ""))
            .suffix(".ack")
        )
      }

      // Load landing file
      val validator = new IngestionWorkflow(storageHandler, schemaHandler)
      validator.stage(StageConfig())
    }
  }

  def printDF(df: DataFrame, marker: String) = {
    logger.info(s"Displaying schema for $marker")
    logger.info(df.schemaString())
    logger.info(s"Displaying data for $marker")
    logger.info(df.showString(truncate = 0))
    logger.info("-----")
  }

  // https://scala.monster/testcontainers/
  // We need to start it manually because we need to access the HTTP mapped port
  // in the configuration below before any test get executed.
  lazy val kafkaContainer: KafkaContainer = {
    val kafkaDockerImage = "apache/kafka"
    val kafkaDockerTag = "4.1.1"
    val kafkaDockerImageName = DockerImageName
      .parse(s"$kafkaDockerImage:$kafkaDockerTag")
    KafkaContainer.Def(kafkaDockerImageName).start()
  }

  /*
  lazy val esContainer: ElasticsearchContainer = {
    val esDockerImage = "docker.elastic.co/elasticsearch/elasticsearch"
    val esDockerTag = "7.8.1"
    val esDockerImageName = DockerImageName.parse(s"$esDockerImage:$esDockerTag")
    ElasticsearchContainer.Def(esDockerImageName).start()
  }
   */
  def deepEquals(l1: List[TableAttribute], l2: List[TableAttribute]): Boolean = {
    l1.zip(l2).foreach { case (a1, a2) =>
      a1.name should equal(a2.name)
      a1.`type` should equal(a2.`type`)
      if (a1.`type` == "struct")
        deepEquals(a1.attributes, a2.attributes)
    }
    true
  }
}

object TestHelper extends LazyLogging {
  lazy val mysqlContainer: MySQLContainer = {
    val container = MySQLContainer
      .Def(
        DockerImageName.parse("mysql:8.4"),
        databaseName = "starlake",
        username = "test",
        password = "test"
      )
      .createContainer()
    container.start()
    container
  }

  private var _session: SparkSession = null
  private var _testId: String = null

  def stopSession() = {
    if (_session != null) {

      // _session.stop()
      // _session = null
    }

  }

  def cleanMetastore(path: String): Try[Unit] =
    Try {
      val fMetastore = new File(path + "/metastore_db")
      new Directory(fMetastore).deleteRecursively()
      val fDerby = new File(path + "/derby.log")
      fDerby.delete()
    }

  def cleanDatasets(path: String): Try[Unit] =
    Try {
      val fDatasets = new File(path)
      val dir = new Directory(fDatasets)
      dir.list.foreach { f =>
        if (f.isDirectory) {
          if (f.name != "metastore_db") {
            f.deleteRecursively()
          }
        } else if (f.name != "derby.log") {
          f.delete()
        }
      }
    }

  def closeSession(): Unit = {
    if (_session != null) {
      // _session.stop()
      // _session = null
    }
  }

  def sparkSession(implicit
    isettings: Settings,
    testId: String,
    forceReset: Boolean = false
  ): SparkSession = {
    if (forceReset || _session == null) {
      closeSession()
      logger.info(s"Creating new Spark session for test $testId")
      // BetterFile("metastore_db").delete(swallowIOExceptions = true)
      // val settings: Settings = Settings(Settings.referenceConfig)
      // cleanDatasets(isettings.appConfig.datasets)
      _session = SparkEnv.get(s"test-${UUID.randomUUID()}", settings = isettings).session
      _testId = testId
    } else {
      logger.info(s"Reusing Spark session for test $testId")
    }
    _session
  }

  def sparkSessionReset(implicit isettings: Settings): SparkSession = {
    if (_session != null) {
      // _session.stop()
      // _session = null
      // BetterFile("metastore_db").delete(swallowIOExceptions = true)
    }
    val job = new SparkJob {
      override def name: String = s"test-${UUID.randomUUID()}"
      override implicit def settings: Settings = isettings
      override def run(): Try[JobResult] =
        ??? // we just create a dummy job to get a valid Spark session
    }
    _session = job.session
    _session
  }

  private val runtimeId: String = UUID.randomUUID().toString
}

case class FileToImport(name: String, path: String, folder: Option[String] = None)
