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

package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.job.ingest.{IngestConfig, LoadConfig}
import ai.starlake.lineage.{AclDependencies, TableDependencies}
import ai.starlake.schema.model.*
import ai.starlake.utils.Formatter.RichFormatter
import better.files.File
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{Metadata as _, *}
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URL
import scala.util.{Failure, Success, Try}

class SchemaInfo2HandlerSpec extends TestHelper {

  override def afterAll(): Unit = {
    super.afterAll()
    // We need to start it manually because we need to access the HTTP mapped port
    // in the configuration below before any test get executed.
    // esContainer.stop()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHelper.closeSession()
  }
  private val playerSchema = StructType(
    Seq(
      StructField("PK", StringType),
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("DOB", DateType),
      StructField("YEAR", IntegerType),
      StructField("MONTH", IntegerType)
    )
  )
  /*
  lazy val esConfiguration: Config = {
    val port = esContainer.httpHostAddress.substring(
      esContainer.httpHostAddress.lastIndexOf(':') + 1
    )
    println(s"--------------------Elasticsearch port: $port-------------------")
    ConfigFactory
      .parseString(s"""
           |connectionRef = "elasticsearch"
           |audit.sink.connectionRef = "spark"
           |connections.elasticsearch {
           |  type = "elasticsearch"
           |  sparkFormat = "elasticsearch"
           |  mode = "Append"
           |  options = {
           |    "es.nodes.wan.only": "true"
           |    "es.nodes": "localhost"
           |    "es.port": $port,

           |    #  net.http.auth.user = ""
           |    #  net.http.auth.pass = ""
           |
           |    "es.net.ssl": "false"
           |    "es.net.ssl.cert.allow.self.signed": "false"
           |
           |    "es.batch.size.entries": "1000"
           |    "es.batch.size.bytes": "1mb"
           |    "es.batch.write.retry.count": "3"
           |    "es.batch.write.retry.wait": "10s"
           |  }
           |}
           |""".stripMargin)
      .withFallback(super.testConfiguration)
  }
   */
  "Ingesting data" should "adapt write based on file attributes" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/adaptiveWrite/simple-adaptive-write.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        cleanDatasets
        deliverSourceDomain()
        deliverSourceTable("/sample/adaptiveWrite/players.sl.yml")
        loadPending

        // We are by  default in ingestion Time strategy
        // Since full is loaded first, it will be the base for the delta

        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-FULL.csv")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-DELTA.csv").load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

        val acceptedFullDelta: Array[Row] = sparkSession
          .sql(
            s"select PK, firstname, lastName, DOB, YEAR, MONTH, title from $datasetDomainName.Players"
          )
          .collect()

        val expectedFullDelta: Array[Row] =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`YEAR` STRING,`MONTH` STRING,`title` STRING"
            )
            .csv(
              getResPath("/expected/datasets/accepted/DOMAIN/Players-adaptive-write-FULL-DELTA.csv")
            )
            .collect()

        acceptedFullDelta should contain theSameElementsAs expectedFullDelta
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-DELTA.csv")
        loadWorkflow("DOMAIN", "/sample/adaptiveWrite/Players-FULL.csv").load(
          LoadConfig(accessToken = None, test = false, files = None, scheduledDate = None)
        )

        val acceptedDeltaFull: Array[Row] = sparkSession
          .sql(
            s"select PK, firstname, lastName, DOB, YEAR, MONTH, title from $datasetDomainName.Players"
          )
          .collect()

        val expectedDeltaFull: Array[Row] =
          sparkSession.read
            .option("encoding", "UTF-8")
            .schema(
              "`PK` STRING,`firstName` STRING,`lastName` STRING,`DOB` DATE,`YEAR` STRING,`MONTH` STRING,`title` STRING"
            )
            .csv(
              getResPath("/expected/datasets/accepted/DOMAIN/Players-adaptive-write-DELTA-FULL.csv")
            )
            .collect()

        acceptedDeltaFull should contain theSameElementsAs expectedDeltaFull
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
      }
    }
  }
  /*
  new WithSettings(esConfiguration) {
    // TODO Helper (to delete)
    "Ingest CSV" should "produce file in accepted" in {
      // ES Load in standby
      ignore
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/elasticsearch/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {

        cleanMetadata
        deliverSourceDomain()

        assert(loadPending.isSuccess)

        // Check Archived
        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/SCHEMA-VALID.dsv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        val rejectedDf = sparkSession.sql("select * from audit.rejected")

        val expectedRejectedF =
          sparkSession.read
            .schema(prepareSchema(rejectedDf.schema))
            .json(getResPath("/expected/datasets/rejected/DOMAIN.json"))

        expectedRejectedF.except(rejectedDf).count() shouldBe 1

        // Accepted should have the same data as input
        val client = HttpClients.createDefault
        val acceptedUri = s"http://${esContainer.httpHostAddress}/domain.user/_search?pretty"
        val acceptedgetRequest = new HttpGet(acceptedUri)
        acceptedgetRequest.setHeader("Content-Type", "application/json")
        val acceptedResponse = client.execute(acceptedgetRequest)
        val responseString: String = EntityUtils.toString(acceptedResponse.getEntity, "UTF-8")
        assert(responseString.indexOf(""""age" : 121""") > 0)
        assert(responseString.indexOf(""""age" : 122""") > 0)
        println(responseString)
        val countUri = s"http://${esContainer.httpHostAddress}/domain.user/_count"
        val getRequest = new HttpGet(countUri)
        getRequest.setHeader("Content-Type", "application/json")
        val response = client.execute(getRequest)

        response.getStatusLine.getStatusCode should be <= 299
        response.getStatusLine.getStatusCode should be >= 200
        EntityUtils.toString(response.getEntity()) contains "\"count\":2"

      }
    }
//    "load to elasticsearch" should "work" in {
//      new SpecTrait(
//        sourceDomainOrJobPathname = s"/sample/simple-json-locations/locations.sl.yml",
//        datasetDomainName = "locations",
//        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
//      ) {
//        sparkSession.sql("DROP DATABASE IF EXISTS locations CASCADE")
//        cleanMetadata
//        deliverSourceDomain()
//        // loadPending
//        val validator = loadWorkflow()
//        val result = validator.esLoad(
//          ESLoadConfig(
//            domain = "DOMAIN",
//            schema = "",
//            format = "json",
//            dataset = Some(
//              Left(new Path(starlakeDatasetsPath + s"/pending/$datasetDomainName/locations.json"))
//            ),
//            options = settings.appConfig.connectionOptions("elasticsearch")
//          )
//        )
//        result.isSuccess shouldBe true
//      }
//    }
  }
   */

  "Ingest empty file with DSV schema" should "be ok " in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/employee-empty.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        loadPending.isSuccess shouldBe true
      }
    }
  }

  "load File" should "work" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {
        val targetPath = DatasetArea.path(
          DatasetArea.stage("DOMAIN.sl.yml"),
          new Path("/sample/SCHEMA-VALID.dsv").getName
        )
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        load(
          IngestConfig(
            "DOMAIN.sl.yml",
            "User",
            List(targetPath),
            accessToken = None,
            scheduledDate = None
          )
        ).isSuccess shouldBe true
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.User")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.Players")
        sparkSession.sql("DROP TABLE IF EXISTS DOMAIN.complexUser")
      }
    }
  }

  "A postsql query" should "update the resulting schema" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/employee.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        loadPending
        val acceptedDf: DataFrame =
          sparkSession.sql(s"select distinct(name) from $datasetDomainName.employee")
        acceptedDf.count() shouldBe 1
        acceptedDf.collect().head.toString() shouldBe "[John]"
      }
    }
  }
  "Ingest Dream Contact CSV" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Contact_20190101_090800_008.psv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List("/sample/dream/client.sl.yml", "/sample/dream/segment.sl.yml").foreach(
          deliverSourceTable
        )
        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/OneClient_Contact_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // If we run this test alone, we do not have rejected, else we have rejected but not accepted ...
        Try {
          printDF(
            sparkSession.sql(s"select * from ${datasetDomainName}_rejected.client"),
            "dream/client"
          )
        }

        // Accepted should have the same data as input
        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.client where $getTodayCondition")
          // Timezone Problem
          .drop("customer_creation_date")
          .drop("year", "month", "day")

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/client.json"))
            // Timezone Problem
            .drop("customer_creation_date")
            .withColumn("truncated_zip_code", substring(col("zip_code"), 0, 3))
            .withColumn("source_file_name", lit("OneClient_Contact_20190101_090800_008.psv"))

        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }
    }
  }

  "Ingest schema with partition" should "produce partitioned output in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/Players.csv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        loadPending
        println(starlakeDatasetsPath)

        val acceptedDf = sparkSession
          .sql(s"select * from $datasetDomainName.Players")

        val exceptedDf = sparkSession.read
          .option("header", "false")
          .schema(playerSchema)
          .csv(getResPath("/sample/Players.csv"))

        acceptedDf.except(exceptedDf).count() shouldBe 0

      }
    }
  }

  "Ingest Dream Segment CSV" should "produce file in accepted" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List("/sample/dream/client.sl.yml", "/sample/dream/segment.sl.yml").foreach(
          deliverSourceTable
        )
        loadPending

        readFileContent(
          starlakeDatasetsPath + s"/archive/$datasetDomainName/OneClient_Segmentation_20190101_090800_008.psv"
        ) shouldBe loadTextFile(
          sourceDatasetPathName
        )

        // Accepted should have the same data as input
        val acceptedDf =
          sparkSession
            .sql(s"select * from $datasetDomainName.segment where $getTodayCondition")
            .drop("year", "month", "day")

        val expectedAccepted =
          sparkSession.read
            .schema(acceptedDf.schema)
            .json(getResPath("/expected/datasets/accepted/dream/segment.json"))

        acceptedDf.except(expectedAccepted).count() shouldBe 0
      }
    }
  }

  "Load Business with Transform Tag" should "load an AutoDesc" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        import org.scalatest.TryValues.*

        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val schemaHandler = settings.schemaHandler()
        val filename = "/sample/metadata/transform/business/business.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val job = schemaHandler.loadJobTasksFromFile(jobPath)
        // FIXME, check why this works on master
        job.success.value.name shouldBe "business2"
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }
  "Load Transform Job" should "not reject tasks without SQL (SQL my be in external file)" in {
    new WithSettings() {
      cleanMetadata
      sparkSession.sql("DROP TABLE IF EXISTS locations.locations")
      sparkSession.sql("DROP TABLE IF EXISTS locations.flat_locations")
      val schemaHandler = settings.schemaHandler()
      val filename = "/sample/job-tasks-without-sql/nosql.sl.yml"
      val jobPath = new Path(getClass.getResource(filename).toURI)

      val job = schemaHandler.loadJobTasksFromFile(jobPath)
      job.isFailure shouldBe false
    }
  }
  "Load Transform Job with taskrefs" should "succeed" in {
    new WithSettings() {
      cleanMetadata
      val schemaHandler = settings.schemaHandler()
      val filename = "/sample/job-with-taskrefs/_config.sl.yml"
      val jobPath = new Path(getClass.getResource(filename).toURI)

      val job = schemaHandler.loadJobTasksFromFile(jobPath)
      job match {
        case Success(job) =>
          val tasks = job.tasks
          tasks.length shouldBe 3
          tasks.map(_.fullName()) should contain theSameElementsAs (List(
            "dream2.client2", // tasks are handled before task refs
            "myjob.task1",
            "myjob.task2"
          ))
        case Failure(e) =>
          throw e
      }
    }
  }

  "Load Transform Job with a commented _config.sl.yml" should "keep the job comment" in {
    new WithSettings() {
      cleanMetadata
      val schemaHandler = settings.schemaHandler()
      val filename = "/sample/job-with-comment/_config.sl.yml"
      val jobPath = new Path(getClass.getResource(filename).toURI)

      val job = schemaHandler.loadJobTasksFromFile(jobPath).get
      job.name shouldBe "job-with-comment"
      job.comment shouldBe Some("Finance KPIs")
      job.tasks.map(_.fullName()) shouldBe List("finance.revenue")
    }
  }

  "Extract Var from Job File" should "find all vars" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val schemaHandler = settings.schemaHandler()
        val filename = "/sample/metadata/transform/business_with_vars/business_with_vars.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val content = storageHandler.read(jobPath)
        val vars = content.extractVars()
        vars should contain theSameElementsAs (Set("DOMAIN", "SCHEMA", "Y", "M"))
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }
  "Load Business with jinja" should "should not run jinja parser" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {

        import org.scalatest.TryValues.*

        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val schemaHandler = settings.schemaHandler()
        val filename = "/sample/metadata/transform/my-jinja-job/_config.sl.yml"
        val jobPath = new Path(getClass.getResource(filename).toURI)
        val job = schemaHandler.loadJobTasksFromFile(jobPath)

        job.success.value.tasks.head.sql.get.trim shouldBe
        """select
          |col1,
          |col2
          |from dream_working.client""".stripMargin // Job renamed to filename and error is logged
      }
    }
  }
  // TODO TOFIX
  //  "Load Business Definition" should "produce business dataset" in {
  //    val sh = new HdfsStorageHandler
  //    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.sl.yml")
  //    sh.write(loadFile("/sample/metadata/business/business.sl.yml"), jobsPath)
  //    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
  //    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
  //    validator.autoJob("business1")
  //  }

  "Writing types" should "work" in {
    new WithSettings() {

      val typesPath = new Path(DatasetArea.types, "types.sl.yml")

      deliverTestFile("/sample/types.sl.yml", typesPath)

      readFileContent(typesPath) shouldBe loadTextFile("/sample/types.sl.yml")
    }
  }
  "Mapping Schema" should "produce valid template" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val schemaHandler = settings.schemaHandler()

        val schema: Option[SchemaInfo] = schemaHandler
          .domains()
          .find(_.name == "locations")
          .flatMap(_.tables.find(_.name == "locations"))
        val expected: String =
          """
              |{
              |  "index_patterns": ["locations.locations", "locations.locations-*"],
              |  "settings": {
              |    "number_of_shards": "1",
              |    "number_of_replicas": "0"
              |  },
              |  "mappings": {
              |      "_source": {
              |        "enabled": true
              |      },
              |
              |"properties": {
              |
              |"id": {
              |  "type": "keyword"
              |},
              |"name": {
              |  "type": "keyword"
              |},
              |"name_upper_case": {
              |  "type": "keyword"
              |},
              |"source_file_name": {
              |  "type": "keyword"
              |},
              |"year":{
              |  "type":"long"
              |},
              |"month":{
              | "type":"long"
              |},
              |"day": {
              |"type":"long"
              |}
              |}
              |  }
              |}
        """.stripMargin.trim
        val mapping =
          schema.map(_.esMapping(None, "locations", schemaHandler)).map(_.trim).getOrElse("")
        logger.info(mapping)
        mapping.replaceAll("\\s", "") shouldBe expected.replaceAll("\\s", "")
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()
      }
    }
  }

  "JSON Schema" should "produce valid template" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/simple-json-locations/locations_domain.sl.yml",
        datasetDomainName = "locations",
        sourceDatasetPathName = "/sample/simple-json-locations/locations.json"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/simple-json-locations/locations.sl.yml",
          "/sample/simple-json-locations/flat_locations.sl.yml"
        ).foreach(deliverSourceTable)
        val schemaHandler = settings.schemaHandler()

        val ds: URL = getClass.getResource("/sample/mapping/dataset")

        logger.info(
          SchemaInfo.mapping(
            "domain",
            "schema",
            StructField("ignore", sparkSession.read.parquet(ds.toString).schema),
            schemaHandler
          )
        )
        sparkSession.sql("DROP TABLE IF EXISTS locations.locations").show()

      }
      // TODO: we aren't actually testing anything here are we?
    }
  }

  "Custom mapping in Metadata" should "be read as a map" in {
    new WithSettings() {
      val sch = settings.schemaHandler()
      val content =
        """withHeader: false
            |encoding: ISO-8859-1
            |format: POSITION
            |sink:
            |  partition: ["_PARTITIONTIME"]
            |writeStrategy:
            |  type: OVERWRITE
            |""".stripMargin

      val metadata = mapper.readValue(content, classOf[Metadata])

      metadata shouldBe Metadata(
        format = Some(ai.starlake.schema.model.Format.POSITION),
        encoding = Some("ISO-8859-1"),
        withHeader = Some(false),
        sink = Some(BigQuerySink(partition = Some(List("_PARTITIONTIME"))).toAllSinks()),
        writeStrategy = Some(WriteStrategy(Some(WriteStrategyType.OVERWRITE)))
      )
    }
  }
  "Exporting domain as Dot" should "create a valid dot file" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        File(starlakeMetadataPath + "/load").delete(swallowIOExceptions = true)
        cleanMetadata
        deliverSourceDomain()
        List("/sample/dream/client.sl.yml", "/sample/dream/segment.sl.yml").foreach(
          deliverSourceTable
        )
        val schemaHandler = settings.schemaHandler()

        val tempFile = File.newTemporaryFile().pathAsString
        new TableDependencies(schemaHandler).run(
          Array("--all", "--output", tempFile)
        )
        val fileContent = readFileContent(tempFile)
        val expectedFileContent = loadTextFile("/expected/dot/output.dot")
        fileContent.trim should equal(expectedFileContent.trim)
        val domains = schemaHandler.domains()
        val result = domains.head.asDot(false, Set("dream.segment", "dream.client"))
        println(result)
        result.trim shouldBe
        """
              |dream_client [label=<
              |<table border="0" cellborder="1" cellspacing="0">
              |<tr>
              |<td port="0" bgcolor="#008B00"><B><FONT color="white"> client </FONT></B></td>
              |</tr>
              |<tr><td port="dream_id"><I> dream_id:long </I></td></tr>
              |</table>>];
              |
              |dream_client:dream_id -> dream_segment:0
              |
              |dream_segment [label=<
              |<table border="0" cellborder="1" cellspacing="0">
              |<tr>
              |<td port="0" bgcolor="#008B00"><B><FONT color="white"> segment </FONT></B></td>
              |</tr>
              |<tr><td port="dreamkey"><B> dreamkey:long </B></td></tr>
              |</table>>];
              |
              |""".stripMargin.trim
      }
    }
  }
  "Exporting domain as ACL Dot" should "create a valid ACL dot file" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = s"/sample/dream/dream.sl.yml",
        datasetDomainName = "dream",
        sourceDatasetPathName = "/sample/dream/OneClient_Segmentation_20190101_090800_008.psv"
      ) {
        File(starlakeMetadataPath + "/load").delete(swallowIOExceptions = true)
        cleanMetadata
        deliverSourceDomain()
        List("/sample/dream/client.sl.yml", "/sample/dream/segment.sl.yml").foreach(
          deliverSourceTable
        )
        val schemaHandler = settings.schemaHandler()

        new AclDependencies(schemaHandler).run(Array("--all"))

        val tempFile = File.newTemporaryFile().pathAsString

        new AclDependencies(schemaHandler).run(
          Array("--all", "--output", tempFile)
        )

        val fileContent = readFileContent(tempFile)
        val expectedFileContent = loadTextFile("/expected/dot/acl-output.dot")
        fileContent.trim shouldBe expectedFileContent.trim
        sparkSession.sql("DROP TABLE IF EXISTS dream.segment").show()
      }
    }
  }
  "Filename Matcher" should "succeed" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/DOMAIN.sl.yml",
        datasetDomainName = "DOMAIN",
        sourceDatasetPathName = "/sample/SCHEMA-VALID.dsv"
      ) {
        cleanMetadata
        deliverSourceDomain()
        List(
          "/sample/User.sl.yml",
          "/sample/Players.sl.yml",
          "/sample/employee.sl.yml",
          "/sample/complexUser.sl.yml"
        ).foreach(deliverSourceTable)
        getDomain("DOMAIN").foreach { domain =>
          val result = domain.tables.map { table =>
            table.finalName -> table.containsArrayOfRecords()
          }
          val expected =
            List(("User", false), ("Players", false), ("employee", false), ("complexUser", true))
          result should contain theSameElementsAs expected
        }

        private val validator = loadWorkflow("DOMAIN", "/sample/Players.csv")
        val ok =
          validator.findAllFilenameMatchers("Players-123.csv", Some("DOMAIN"), Some("Players"))
        assert(ok == List(("DOMAIN", "Players")))
        val ko =
          validator.findAllFilenameMatchers("Player-123.csv", Some("DOMAIN"), Some("Players"))
        assert(ko.isEmpty)
      }
    }
  }
}
