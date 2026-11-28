package ai.starlake.job.transform

import ai.starlake.TestHelper
import com.dimafeng.testcontainers.{GenericContainer, MySQLContainer}
import com.typesafe.config.{Config, ConfigFactory}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/** The engine of a spec that runs its scenario on one engine, through the connection that its
  * configuration names
  */
trait EngineSpec extends TestHelper {

  /** The configuration of the run, whose connectionRef names the connection of the engine */
  protected def engineConfiguration: Config

  /** The schema of the tables */
  protected def domain: String

  /** Whether the engine is available to this run */
  protected def enabled: Boolean = true

  /** The statements that create the schema of the tables, when the engine needs them */
  protected def createSchema: List[String] = List(s"CREATE SCHEMA IF NOT EXISTS $domain")

  protected def stringType: String = "VARCHAR(10)"

  /** Whether the scenario runs the SQL of the write strategy directly, in autocommit, instead of
    * running the task: for an engine whose writes need autocommit, which a task does not use
    */
  protected def runsStrategySqlOnly: Boolean = false
}

/** MySQL, in its TestContainers container. The test user owns the starlake database only, which is
  * the schema of the tables.
  */
trait MySqlEngine extends EngineSpec {
  private lazy val mysql: MySQLContainer = {
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

  override protected def afterAll(): Unit = {
    super.afterAll()
    mysql.stop()
  }

  override protected def domain: String = "starlake"
  override protected def createSchema: List[String] = Nil
  override protected def engineConfiguration: Config =
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-mysql"
           |connections.test-mysql {
           |  type = "jdbc"
           |  options {
           |    "url": "${mysql.jdbcUrl}"
           |    "user": "test"
           |    "password": "test"
           |    "driver": "com.mysql.cj.jdbc.Driver"
           |    "quoteIdentifiers": false
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)
}

/** Trino, in its Docker image, on an Iceberg catalog kept in the container: Trino has no temporary
  * table, and its memory connector deletes no rows. No engine named trino ships in
  * reference-engines.conf: the test declares one, the PostgreSQL engine with the trino templates.
  * Trino writes to Iceberg only in autocommit, so the scenario runs the SQL of the write strategy,
  * not the task.
  */
trait TrinoEngine extends EngineSpec {
  private lazy val trino: GenericContainer = {
    val container = GenericContainer(
      dockerImage = "trinodb/trino:484",
      exposedPorts = Seq(8080),
      waitStrategy = Wait
        .forLogMessage("(?s).*SERVER STARTED.*", 1)
        .withStartupTimeout(java.time.Duration.ofMinutes(4))
    )
    container.container.withClasspathResourceMapping(
      "trino/iceberg.properties",
      "/etc/trino/catalog/iceberg.properties",
      BindMode.READ_ONLY
    )
    container.start()
    container
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    trino.stop()
  }

  override protected def stringType: String = "VARCHAR"
  // the Iceberg catalog writes only in autocommit, and a task writes in a transaction
  override protected def runsStrategySqlOnly: Boolean = true
  override protected def engineConfiguration: Config = {
    val base = ConfigFactory.load().withFallback(super.testConfiguration)
    val trinoEngine = ConfigFactory
      .parseString("strategyBuilder = \"trino\"")
      .withFallback(base.getConfig("jdbcEngines.postgresql"))
      .atPath("jdbcEngines.trino")
    ConfigFactory
      .parseString(
        s"""connectionRef: "test-trino"
           |connections.test-trino {
           |  type = "jdbc"
           |  options {
           |    "url": "jdbc:trino://${trino.host}:${trino.mappedPort(8080)}/iceberg"
           |    "user": "test"
           |    "driver": "io.trino.jdbc.TrinoDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(trinoEngine)
      .withFallback(super.testConfiguration)
  }
}
