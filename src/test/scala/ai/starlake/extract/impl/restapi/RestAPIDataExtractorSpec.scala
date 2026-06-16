package ai.starlake.extract.impl.restapi

import ai.starlake.TestHelper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class RestAPIDataExtractorSpec extends TestHelper with BeforeAndAfterAll {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private var server: HttpServer = _
  private var port: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress(0), 0)
    port = server.getAddress.getPort

    // Simple endpoint returning a flat array
    server.createContext(
      "/api/users",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val query = Option(exchange.getRequestURI.getQuery).getOrElse("")
          val params = query
            .split("&")
            .filter(_.contains("="))
            .map { p =>
              val parts = p.split("=", 2); parts(0) -> parts(1)
            }
            .toMap

          val offset = params.getOrElse("offset", "0").toInt
          val limit = params.getOrElse("limit", "10").toInt

          val allUsers = (1 to 15).map { i =>
            Map(
              "id"         -> i,
              "name"       -> s"User $i",
              "email"      -> s"user$i@test.com",
              "updated_at" -> s"2024-01-%02d".format(i)
            )
          }
          val page = allUsers.slice(offset, offset + limit)
          sendResponse(exchange, 200, mapper.writeValueAsString(page))
        }
      }
    )

    // Nested response endpoint
    server.createContext(
      "/api/nested",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val data = List(
            Map(
              "id"      -> 1,
              "name"    -> "Test",
              "address" -> Map("city" -> "Paris", "country" -> "France")
            )
          )
          val response = Map("data" -> data, "total" -> 1)
          sendResponse(exchange, 200, mapper.writeValueAsString(response))
        }
      }
    )

    server.setExecutor(null)
    server.start()
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
    super.afterAll()
  }

  private def sendResponse(exchange: HttpExchange, code: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(code, bytes.length)
    val os = exchange.getResponseBody
    os.write(bytes)
    os.close()
  }

  "RestAPISchemaExtractor" should "produce valid DomainInfo with inferred attributes" in {
    withSettings { implicit settings =>
      val config = RestAPIExtractSchema(
        baseUrl = s"http://localhost:$port",
        endpoints = List(
          RestAPIEndpoint(
            path = "/api/users",
            as = Some("users"),
            domain = "test_domain",
            pagination = Some(OffsetPagination("limit", "offset", 5))
          )
        )
      )
      val extractor = new RestAPISchemaExtractor(config)
      val domains = extractor.extract().toList
      domains should have size 1
      domains.head.name shouldBe "test_domain"
      domains.head.tables should have size 1

      val table = domains.head.tables.head
      table.name shouldBe "users"
      val attrNames = table.attributes.map(_.name)
      attrNames should contain allOf ("id", "name", "email", "updated_at")

      // Check type inference
      val idAttr = table.attributes.find(_.name == "id")
      idAttr.get.`type` shouldBe "long"
      val dateAttr = table.attributes.find(_.name == "updated_at")
      dateAttr.get.`type` shouldBe "date"
    }
  }

  it should "infer schema from nested response with responsePath" in {
    withSettings { implicit settings =>
      val config = RestAPIExtractSchema(
        baseUrl = s"http://localhost:$port",
        endpoints = List(
          RestAPIEndpoint(
            path = "/api/nested",
            as = Some("nested_data"),
            domain = "test_domain",
            responsePath = Some("$.data")
          )
        )
      )
      val extractor = new RestAPISchemaExtractor(config)
      val domains = extractor.extract().toList
      domains should have size 1
      val table = domains.head.tables.head
      table.name shouldBe "nested_data"
      val attrNames = table.attributes.map(_.name)
      attrNames should contain allOf ("id", "name", "address")

      // address should be a struct with nested attributes
      val addressAttr = table.attributes.find(_.name == "address")
      addressAttr.get.`type` shouldBe "struct"
      addressAttr.get.attributes.map(_.name) should contain allOf ("city", "country")
    }
  }

  "RestAPIDataExtractor" should "extract data to CSV files" in {
    withSettings { implicit settings =>
      val outputDir = new Path(starlakeTestRoot, "rest-extract-output")
      val config = RestAPIDataExtractConfig(
        extractConfig = RestAPIExtractSchema(
          baseUrl = s"http://localhost:$port",
          endpoints = List(
            RestAPIEndpoint(
              path = "/api/users",
              as = Some("users"),
              domain = "myapi",
              pagination = Some(OffsetPagination("limit", "offset", 10))
            )
          )
        ),
        baseOutputDir = outputDir,
        limit = 0
      )
      val extractor = new RestAPIDataExtractor()
      val result = extractor.run(config)
      result.isSuccess shouldBe true
      result.get shouldBe 15 // 15 total users

      // Verify CSV file was created
      val storageHandler = settings.storageHandler()
      val files = storageHandler.list(new Path(outputDir, "myapi"), recursive = false)
      files should have size 1
      files.head.path.getName should endWith(".csv")

      // Read and verify CSV content
      val csvContent = storageHandler.read(files.head.path)
      val lines = csvContent.split("\n")
      lines.length shouldBe 16 // 1 header + 15 records
      lines.head should include("id")
      lines.head should include("name")
      lines.head should include("email")
    }
  }

  it should "respect limit parameter" in {
    withSettings { implicit settings =>
      val outputDir = new Path(starlakeTestRoot, "rest-extract-limited")
      val config = RestAPIDataExtractConfig(
        extractConfig = RestAPIExtractSchema(
          baseUrl = s"http://localhost:$port",
          endpoints = List(
            RestAPIEndpoint(
              path = "/api/users",
              as = Some("users_limited"),
              domain = "myapi",
              pagination = Some(OffsetPagination("limit", "offset", 10))
            )
          )
        ),
        baseOutputDir = outputDir,
        limit = 5
      )
      val extractor = new RestAPIDataExtractor()
      val result = extractor.run(config)
      result.isSuccess shouldBe true
      result.get shouldBe 5
    }
  }

  it should "support incremental extraction with state tracking" in {
    withSettings { implicit settings =>
      val outputDir = new Path(starlakeTestRoot, "rest-extract-incremental")

      val config = RestAPIDataExtractConfig(
        extractConfig = RestAPIExtractSchema(
          baseUrl = s"http://localhost:$port",
          endpoints = List(
            RestAPIEndpoint(
              path = "/api/users",
              as = Some("users_incr"),
              domain = "myapi",
              pagination = Some(OffsetPagination("limit", "offset", 10)),
              incrementalField = Some("updated_at")
            )
          )
        ),
        baseOutputDir = outputDir,
        incremental = true
      )

      // First run — should extract all and save state
      val extractor = new RestAPIDataExtractor()
      val result1 = extractor.run(config)
      result1.isSuccess shouldBe true
      result1.get shouldBe 15

      // Verify state file was created
      val storageHandler = settings.storageHandler()
      val stateFile = new Path(outputDir, ".state/myapi/users_incr.json")
      storageHandler.exists(stateFile) shouldBe true

      val stateContent = storageHandler.read(stateFile)
      val stateNode = mapper.readTree(stateContent)
      stateNode.has("lastValue") shouldBe true
      stateNode.get("lastValue").asText() shouldBe "2024-01-15" // max of updated_at
    }
  }

  it should "flatten nested JSON to CSV" in {
    withSettings { implicit settings =>
      val outputDir = new Path(starlakeTestRoot, "rest-extract-nested")
      val config = RestAPIDataExtractConfig(
        extractConfig = RestAPIExtractSchema(
          baseUrl = s"http://localhost:$port",
          endpoints = List(
            RestAPIEndpoint(
              path = "/api/nested",
              as = Some("nested"),
              domain = "myapi",
              responsePath = Some("$.data")
            )
          )
        ),
        baseOutputDir = outputDir
      )
      val extractor = new RestAPIDataExtractor()
      val result = extractor.run(config)
      result.isSuccess shouldBe true
      result.get shouldBe 1

      val storageHandler = settings.storageHandler()
      val files = storageHandler.list(new Path(outputDir, "myapi"), recursive = false)
      val csvContent = storageHandler.read(files.head.path)
      val lines = csvContent.split("\n")
      lines.head should include("address.city")
      lines.head should include("address.country")
      lines(1) should include("Paris")
      lines(1) should include("France")
    }
  }
}
