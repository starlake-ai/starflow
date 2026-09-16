package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.job.sink.bigquery.{BigQueryJobBase, BigQueryLoadConfig, BigQuerySparkJob}
import ai.starlake.lineage.TaskViewDependency
import ai.starlake.schema.model.*
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

class AutoJobHandlerSpec extends TestHelper with BeforeAndAfterAll {

  lazy val pathBusiness = new Path(starlakeMetadataPath + "/transform/user/user.sl.yml")

  lazy val pathConfigBusiness = new Path(
    starlakeMetadataPath + "/transform/user/_config.sl.yml"
  )

  lazy val pathGraduateProgramBusiness = new Path(
    starlakeMetadataPath + "/transform/user/graduateProgram.sl.yml"
  )

  lazy val pathGraduateDatasetProgramBusiness = new Path(
    starlakeDatasetsPath + "/business/graduateProgram/output"
  )

  val tmpPath = new Path(starlakeTestRoot + "/tmp").toString
  lazy val pathUserAccepted = new Path(tmpPath + "/accepted/user")

  lazy val pathGraduateProgramAccepted = new Path(
    tmpPath + "/accepted/graduateProgram"
  )

  lazy val metadataPath = new Path(starlakeMetadataPath)

  override def beforeAll(): Unit = {
    new WithSettings() {
      TestHelper.sparkSessionReset(settings)
      sparkSession.read
        .option("inferSchema", "true")
        .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
        .write
        .mode("overwrite")
        .parquet(pathUserAccepted.toString)

      sparkSession.read
        .option("inferSchema", "true")
        .json(getResPath("/expected/datasets/accepted/DOMAIN/graduateProgram.json"))
        .write
        .mode("overwrite")
        .parquet(pathGraduateProgramAccepted.toString)
    }
    super.beforeAll()
  }

  "trigger AutoJob by passing parameters on SQL statement" should "generate a dataset in business" in {
    new WithSettings() {
      val userView = pathUserAccepted.toString
      val businessTask1 = AutoTaskInfo(
        name = "",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view where age={{age}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler =
        settings.schemaHandler(Map("age" -> "40"))

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig(name = "user.user"))

      val result = sparkSession
        .table("user.user")
        .select("firstname", "lastname", "age")
        .take(2)

      result.length shouldBe 2

      result
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("test3", "test4", 40),
        ("Elon", "Musk", 40)
      )
    }
  }
  "Extract file and view dependencies" should "report the source file but not the CTE name" in {
    new WithSettings() {

      val userView = pathUserAccepted.toString
      logger.info("************userView:" + userView)
      val businessTask1 = AutoTaskInfo(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view where age={{age}} and lastname={{lastname}} and firstname={{firstname}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        expectations = List(ExpectationItem("is_col_value_not_unique('firstname')")),
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler = settings.schemaHandler()

      val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
      val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)
      // 'user_view' is a CTE declared by this very query, not an upstream object: reporting it
      // would add a phantom node to the generated DAG.
      deps.map(_.parentRef) should contain theSameElementsAs List("parquet." + userView)
    }
  }

  "trigger AutoJob by passing three parameters on SQL statement" should "generate a dataset in business" in {
    new WithSettings() {

      val userView = pathUserAccepted.toString
      val businessTask1 = AutoTaskInfo(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_View where age={{age}} and lastname={{lastname}} and firstname={{firstname}}"
        ),
        database = None,
        domain = "user",
        table = "user",
        expectations = List(
          ExpectationItem("is_col_value_not_unique('firstname')")
        ),
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        sink = Some(FsSink().toAllSinks())
      )
      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler =
        settings.schemaHandler(Map("age" -> "25", "lastname" -> "'Doe'", "firstname" -> "'John'"))

      sparkSession.sql("DROP TABLE IF EXISTS user.user")
      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)
      workflow.autoJob(TransformConfig(name = "user.user", reload = true))

      sparkSession.table("user.user").show()

      val result = sparkSession
        .table("user.user")
        .select("firstname", "lastname", "age")
        .collect()

      result.length shouldBe 1
      result
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("John", "Doe", 25)
      )
      sparkSession.sql("DROP TABLE IF EXISTS user.user")

    }
  }

  "trigger AutoJob with no parameters on SQL statement" should "generate a dataset in business" in {
    new WithSettings() {
      val userView = pathUserAccepted.toString
      val businessTask1 = AutoTaskInfo(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select firstname, lastname, age from user_view"
        ),
        database = None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessJobDef, pathBusiness)

      val schemaHandler = settings.schemaHandler()

      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig(name = "user.user"))

      sparkSession
        .table("user.user")
        .select("firstname", "lastname", "age")
        .take(6)
        .map(r => (r.getString(0), r.getString(1), r.getLong(2)))
        .toList should contain allElementsOf List(
        ("John", "Doe", 25),
        ("fred", "abruzzi", 25),
        ("test3", "test4", 40)
      )
      sparkSession
        .sql(
          "DROP TABLE IF EXISTS user.user"
        )
    }
  }
  "trigger AutoJob using an UDF" should "generate a dataset in business" in {
    new WithSettings() {
      // because we are having a different schema for user in this test
      val userView = pathUserAccepted.toString
      val businessTask1 = AutoTaskInfo(
        name = "user",
        sql = Some(
          s"with user_view as (select * from parquet.`$userView`) select concatWithSpace(firstname, lastname) as fullName from user_View"
        ),
        database = None,
        domain = "user",
        table = "user",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        sink = Some(FsSink().toAllSinks())
      )

      val businessJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessJobDef, pathBusiness)

      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val schemaHandler = settings.schemaHandler()

      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig(name = "user.user"))

      sparkSession
        .table("user.user")
        .select("fullName")
        .take(7)
        .map(r => r.getString(0))
        .toList should contain allElementsOf List(
        "John Doe",
        "fred abruzzi",
        "test3 test4"
      )
    }
  }
  "trigger AutoJob by passing parameters to presql statement" should "generate a dataset in business" in {
    new WithSettings() {
      val businessTask1 = AutoTaskInfo(
        name = "graduateProgram",
        sql = Some(
          s"SELECT * FROM graduate_agg_view"
        ),
        database = None,
        domain = "user",
        table = "output",
        presql = List(
          s"""
               |create or replace temporary view graduate_agg_view as
               |      select degree, department,
               |      school
               |      from parquet.`${pathGraduateProgramAccepted.toString}`
               |      where school={{school}}
               |""".stripMargin
        ),
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite),
        sink = Some(FsSink().toAllSinks())
      )
      val configJob =
        AutoJobInfo(
          "",
          Nil
        )

      val configJobDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, configJob))
      storageHandler.write(configJobDef, pathConfigBusiness)

      val businessTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, businessTask1))
      storageHandler.write(businessTaskDef, pathGraduateProgramBusiness)

      val schemaHandler = settings.schemaHandler(Map("school" -> "'UC_Berkeley'"))
      val workflow =
        new IngestionWorkflow(storageHandler, schemaHandler)

      workflow.autoJob(TransformConfig(name = "user.graduateProgram"))

      val result = sparkSession
        .table("user.output")
        .select("*")

      result
        .take(7)
        .map(r => (r.getString(0), r.getString(1), r.getString(2)))
        .toList should contain allElementsOf List(
        ("Masters", "School_of_Information", "UC_Berkeley"),
        ("Masters", "EECS", "UC_Berkeley"),
        ("Ph.D.", "EECS", "UC_Berkeley")
      )
    }
  }

  "BQ Business Job Definition" should "Prepare correctly against BQ" in {
    val bqConfiguration: Config = {
      val config = ConfigFactory
        .parseString("""
            |connectionRef = "bigquery"
            |""".stripMargin)
      val result = config.withFallback(super.testConfiguration)
      result
    }

    new WithSettings(bqConfiguration) {
      val businessTask1 = AutoTaskInfo(
        name = "",
        sql = Some("select * from domain"),
        database = None,
        domain = "DOMAIN",
        table = "TABLE",
        presql = Nil,
        postsql = Nil,
        None,
        rls = List(RowLevelSecurity("myrls", "TRUE", Set("user:hayssam.saleh@ebiznext.com"))),
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )

      val sink = businessTask1.sink.map(_.asInstanceOf[BigQuerySink])

      val config = BigQueryLoadConfig(
        connectionRef = None,
        outputTableId = Some(
          BigQueryJobBase.extractProjectDatasetAndTable(
            businessTask1.database,
            businessTask1.domain,
            businessTask1.table,
            None
          )
        ),
        sourceFormat = "parquet",
        createDisposition = "CREATE_IF_NEEDED",
        writeDisposition = "WRITE_TRUNCATE",
        outputPartition = sink.flatMap(_.getPartitionColumn()),
        outputClustering = sink.flatMap(_.clustering).getOrElse(Nil),
        days = sink.flatMap(_.days),
        requirePartitionFilter = sink.flatMap(_.requirePartitionFilter).getOrElse(false),
        rls = businessTask1.rls,
        outputDatabase = None,
        accessToken = None
      )
      val job = new BigQuerySparkJob(config)
      val conf = job.prepareConf()

      val delStatement = "DROP ALL ROW ACCESS POLICIES ON `DOMAIN.TABLE`"
      val createStatement =
        """
          | CREATE ROW ACCESS POLICY
          |  myrls
          | ON
          |  `DOMAIN.TABLE`
          | GRANT TO
          |  ("user:hayssam.saleh@ebiznext.com")
          | FILTER USING
          |  (TRUE)
          |""".stripMargin
      val result = BigQueryJobBase
        .buildRLSQueries(
          BigQueryJobBase.extractProjectDatasetAndTable(
            businessTask1.database,
            businessTask1.domain,
            businessTask1.table,
            None
          ),
          businessTask1.rls
        )
        .map(_.replaceAll("`.*`", "`DOMAIN.TABLE`")) // we remove the computed project id
      result should contain theSameElementsInOrderAs List(delStatement, createStatement)
    }
  }
}

/*
      import org.apache.spark.sql.SparkSession
      val config: SparkConf = new SparkConf()
      val master = "local[*]"
      config.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      config.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      val sysProps = System.getProperties
      sysProps.setProperty("derby.system.home", "/Users//tmp/derby")
      config.set("spark.sql.warehouse.dir", "/Users//tmp/warehouse")

      val spark = SparkSession.builder().config(config).master(master).getOrCreate()

 */

object AutoJobHandlerSpec extends AutoJobHandlerSpec {
  def main(args: Array[String]): Unit = {
    new WithSettings() {
      sparkSession.read
        .option("inferSchema", "true")
        .json(getResPath("/expected/datasets/accepted/DOMAIN/User.json"))
        .write
        .mode("overwrite")
        .parquet(pathUserAccepted.toString)

      sparkSession.read
        .option("inferSchema", "true")
        .json(getResPath("/expected/datasets/accepted/DOMAIN/graduateProgram.json"))
        .write
        .mode("overwrite")
        .parquet(pathGraduateProgramAccepted.toString)
    }

    new WithSettings() {
      sparkSessionReset(settings)
      println("go")
      val stmt =
        """
        | CREATE TABLE user1 USING delta
        | AS
        | with user_view as (select * from parquet.`/Users/hayssams/tmp//userdata1.parquet`)
        | select * from user_View;
        |""".stripMargin
      sparkSession.sql(
        stmt
      )
      sparkSession.sql("select * from user1").show()
    }
  }
}
