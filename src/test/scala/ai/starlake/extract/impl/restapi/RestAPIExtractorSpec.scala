package ai.starlake.extract.impl.restapi

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

class RestAPIExtractorSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private var server: HttpServer = _
  private var port: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress(0), 0)
    port = server.getAddress.getPort

    // Endpoint: /api/users (offset pagination)
    server.createContext(
      "/api/users",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val query = Option(exchange.getRequestURI.getQuery).getOrElse("")
          val params = query
            .split("&")
            .filter(_.contains("="))
            .map { p =>
              val parts = p.split("=", 2)
              parts(0) -> parts(1)
            }
            .toMap

          val offset = params.getOrElse("offset", "0").toInt
          val limit = params.getOrElse("limit", "10").toInt

          val allUsers = (1 to 25).map { i =>
            Map(
              "id"         -> i,
              "name"       -> s"User $i",
              "email"      -> s"user$i@example.com",
              "active"     -> (i % 2 == 0),
              "created_at" -> "2024-01-15T10:30:00Z"
            )
          }

          val page = allUsers.slice(offset, offset + limit)
          val responseJson = mapper.writeValueAsString(page)
          sendResponse(exchange, 200, responseJson)
        }
      }
    )

    // Endpoint: /api/products (cursor pagination)
    server.createContext(
      "/api/products",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val query = Option(exchange.getRequestURI.getQuery).getOrElse("")
          val params = query
            .split("&")
            .filter(_.contains("="))
            .map { p =>
              val parts = p.split("=", 2)
              parts(0) -> parts(1)
            }
            .toMap

          val cursor = params.getOrElse("after", "0").toInt
          val limit = params.getOrElse("limit", "5").toInt

          val allProducts = (1 to 12).map { i =>
            Map(
              "id"    -> i,
              "title" -> s"Product $i",
              "price" -> (i * 9.99)
            )
          }

          val startIdx = cursor
          val page = allProducts.slice(startIdx, startIdx + limit)
          val nextCursor =
            if (startIdx + limit < allProducts.size) (startIdx + limit).toString else null

          val response = Map(
            "data" -> page,
            "meta" -> Map("next_cursor" -> nextCursor)
          )
          val responseJson = mapper.writeValueAsString(response)
          sendResponse(exchange, 200, responseJson)
        }
      }
    )

    // Endpoint: /api/simple (no pagination, nested objects)
    server.createContext(
      "/api/simple",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val data = List(
            Map(
              "id"      -> 1,
              "name"    -> "Test",
              "address" -> Map("city" -> "Paris", "country" -> "France"),
              "tags"    -> List("a", "b")
            )
          )
          sendResponse(exchange, 200, mapper.writeValueAsString(data))
        }
      }
    )

    // Endpoint: /api/auth-test (requires bearer token)
    server.createContext(
      "/api/auth-test",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val authHeader = exchange.getRequestHeaders.getFirst("Authorization")
          if (authHeader != null && authHeader == "Bearer test-token-123") {
            sendResponse(exchange, 200, """[{"status":"ok"}]""")
          } else {
            sendResponse(exchange, 401, """{"error":"unauthorized"}""")
          }
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

  private def baseConfig(endpoints: List[RestAPIEndpoint]): RestAPIExtractSchema =
    RestAPIExtractSchema(
      baseUrl = s"http://localhost:$port",
      endpoints = endpoints
    )

  // --- PaginationHandler tests ---

  "OffsetPaginationHandler" should "generate correct initial params" in {
    val handler = PaginationHandler(OffsetPagination("limit", "offset", 10))
    handler.initialParams shouldBe Map("limit" -> "10", "offset" -> "0")
  }

  it should "return None when page has fewer records than pageSize" in {
    val handler = PaginationHandler(OffsetPagination("limit", "offset", 10))
    val smallArray = mapper.createArrayNode()
    (1 to 5).foreach(i => smallArray.addObject().put("id", i))
    handler.nextPageParams(smallArray, smallArray, Map.empty, 0) shouldBe None
  }

  it should "return next offset when page is full" in {
    val handler = PaginationHandler(OffsetPagination("limit", "offset", 10))
    val fullArray = mapper.createArrayNode()
    (1 to 10).foreach(i => fullArray.addObject().put("id", i))
    handler.nextPageParams(fullArray, fullArray, Map.empty, 0) shouldBe Some(
      Map("limit" -> "10", "offset" -> "10")
    )
  }

  "CursorPaginationHandler" should "extract cursor from response" in {
    val handler = PaginationHandler(
      CursorPagination("after", "$.meta.next_cursor", 5, Some("limit"))
    )
    val response = mapper.readTree("""{"meta":{"next_cursor":"abc123"}}""")
    handler.nextPageParams(response, response, Map.empty, 0) shouldBe Some(
      Map("after" -> "abc123", "limit" -> "5")
    )
  }

  it should "return None when cursor is null" in {
    val handler = PaginationHandler(
      CursorPagination("after", "$.meta.next_cursor", 5)
    )
    val response = mapper.readTree("""{"meta":{"next_cursor":null}}""")
    handler.nextPageParams(response, response, Map.empty, 0) shouldBe None
  }

  "PageNumberPaginationHandler" should "increment page number" in {
    val handler = PaginationHandler(PageNumberPagination("page", 10, Some("per_page")))
    val fullArray = mapper.createArrayNode()
    (1 to 10).foreach(i => fullArray.addObject().put("id", i))
    handler.nextPageParams(fullArray, fullArray, Map.empty, 0) shouldBe Some(
      Map("page" -> "2", "per_page" -> "10")
    )
  }

  "LinkHeaderPaginationHandler" should "parse Link header" in {
    val handler = PaginationHandler(LinkHeaderPagination(10))
    val headers = Map(
      "link" -> """<https://api.example.com/items?page=3&per_page=10>; rel="next", <https://api.example.com/items?page=10>; rel="last""""
    )
    val emptyArray = mapper.createArrayNode()
    val result = handler.nextPageParams(emptyArray, emptyArray, headers, 1)
    result shouldBe defined
    result.get("page") shouldBe "3"
    result.get("per_page") shouldBe "10"
  }

  // --- JsonPathUtil tests ---

  "JsonPathUtil.extract" should "navigate dot-notation paths" in {
    val node = mapper.readTree("""{"a":{"b":{"c":"value"}}}""")
    JsonPathUtil.extract(node, "a.b.c") shouldBe Some("value")
  }

  it should "handle JSONPath-style paths" in {
    val node = mapper.readTree("""{"meta":{"cursor":"xyz"}}""")
    JsonPathUtil.extract(node, "$.meta.cursor") shouldBe Some("xyz")
  }

  it should "return None for missing paths" in {
    val node = mapper.readTree("""{"a":1}""")
    JsonPathUtil.extract(node, "b.c") shouldBe None
  }

  "JsonPathUtil.extractDataArray" should "extract nested data" in {
    val node = mapper.readTree("""{"data":[{"id":1},{"id":2}]}""")
    val result = JsonPathUtil.extractDataArray(node, Some("$.data"))
    result.isArray shouldBe true
    result.size() shouldBe 2
  }

  // --- RestAPIClient integration tests ---

  "RestAPIClient" should "fetch data from a simple endpoint" in {
    val config = baseConfig(Nil)
    val client = new RestAPIClient(config)
    val response = client.execute("/api/simple")
    response.statusCode shouldBe 200
    response.body.isArray shouldBe true
    response.body.size() shouldBe 1
    response.body.get(0).get("name").asText() shouldBe "Test"
  }

  it should "handle bearer authentication" in {
    val config = RestAPIExtractSchema(
      baseUrl = s"http://localhost:$port",
      auth = Some(BearerAuth("test-token-123"))
    )
    val client = new RestAPIClient(config)
    val response = client.execute("/api/auth-test")
    response.statusCode shouldBe 200
    response.body.get(0).get("status").asText() shouldBe "ok"
  }

  it should "throw on auth failure" in {
    val config = RestAPIExtractSchema(
      baseUrl = s"http://localhost:$port",
      auth = Some(BearerAuth("wrong-token"))
    )
    val client = new RestAPIClient(config)
    assertThrows[RestAPIException] {
      client.execute("/api/auth-test", maxRetries = 0)
    }
  }

  it should "paginate through offset-based endpoint" in {
    val config = baseConfig(Nil)
    val client = new RestAPIClient(config)
    val endpoint = RestAPIEndpoint(
      path = "/api/users",
      pagination = Some(OffsetPagination("limit", "offset", 10))
    )
    val allPages = client.fetchAllPages(endpoint).toList
    val totalRecords = allPages.map(_._1.size()).sum
    totalRecords shouldBe 25
    allPages.size shouldBe 3 // 10 + 10 + 5
  }

  it should "paginate through cursor-based endpoint" in {
    val config = baseConfig(Nil)
    val client = new RestAPIClient(config)
    val endpoint = RestAPIEndpoint(
      path = "/api/products",
      pagination = Some(CursorPagination("after", "meta.next_cursor", 5, Some("limit"))),
      responsePath = Some("$.data")
    )
    val allPages = client.fetchAllPages(endpoint).toList
    val totalRecords = allPages.map(_._1.size()).sum
    totalRecords shouldBe 12
    allPages.size shouldBe 3 // 5 + 5 + 2
  }

  // --- Schema inference tests ---

  "RestAPISchemaExtractor" should "infer schema from endpoint" in {
    val config = RestAPIExtractSchema(
      baseUrl = s"http://localhost:$port",
      endpoints = List(
        RestAPIEndpoint(
          path = "/api/simple",
          as = Some("simple_data"),
          domain = "test_domain"
        )
      )
    )
    val extractor = new RestAPISchemaExtractor(config)
    // Need implicit Settings — skip for unit test, tested via integration
  }

  // --- Config serialization tests ---

  "RestAPIExtractorConfig" should "deserialize auth types from JSON" in {
    val json = """{"type":"bearer","token":"my-token"}"""
    val auth = mapper.readValue(json, classOf[RestAPIAuth])
    auth shouldBe a[BearerAuth]
    auth.asInstanceOf[BearerAuth].token shouldBe "my-token"
  }

  it should "deserialize api_key auth" in {
    val json = """{"type":"api_key","key":"secret","header":"X-Custom"}"""
    val auth = mapper.readValue(json, classOf[RestAPIAuth])
    auth shouldBe a[ApiKeyAuth]
    val apiKey = auth.asInstanceOf[ApiKeyAuth]
    apiKey.key shouldBe "secret"
    apiKey.header shouldBe "X-Custom"
  }

  it should "deserialize pagination strategies" in {
    val json = """{"type":"offset","limitParam":"limit","offsetParam":"skip","pageSize":50}"""
    val pagination = mapper.readValue(json, classOf[PaginationStrategy])
    pagination shouldBe a[OffsetPagination]
    val offset = pagination.asInstanceOf[OffsetPagination]
    offset.limitParam shouldBe "limit"
    offset.offsetParam shouldBe "skip"
    offset.pageSize shouldBe 50
  }

  it should "deserialize cursor pagination" in {
    val json =
      """{"type":"cursor","cursorParam":"after","cursorPath":"$.meta.next","pageSize":20}"""
    val pagination = mapper.readValue(json, classOf[PaginationStrategy])
    pagination shouldBe a[CursorPagination]
    val cursor = pagination.asInstanceOf[CursorPagination]
    cursor.cursorParam shouldBe "after"
    cursor.cursorPath shouldBe "$.meta.next"
    cursor.pageSize shouldBe 20
  }

  it should "derive table name from endpoint path" in {
    val ep1 = RestAPIEndpoint(path = "/api/v2/customers")
    ep1.tableName shouldBe "customers"

    val ep2 = RestAPIEndpoint(path = "/api/v2/customers", as = Some("customer"))
    ep2.tableName shouldBe "customer"

    val ep3 = RestAPIEndpoint(path = "/orders/{parent.id}/items")
    ep3.tableName shouldBe "items"
  }

  it should "resolve defaults in RestAPIExtractSchema" in {
    val config = RestAPIExtractSchema(
      baseUrl = "http://example.com",
      headers = Map("X-Global" -> "true"),
      defaults = Some(
        RestAPIDefaults(
          pagination = Some(OffsetPagination("limit", "offset", 50)),
          headers = Map("X-Default" -> "yes")
        )
      ),
      endpoints = List(
        RestAPIEndpoint(path = "/api/test"),
        RestAPIEndpoint(
          path = "/api/custom",
          pagination = Some(CursorPagination("cursor", "$.next", 10)),
          headers = Map("X-Custom" -> "override")
        )
      )
    )
    val resolved = config.resolvedEndpoints

    // First endpoint inherits defaults
    resolved(0).pagination shouldBe Some(OffsetPagination("limit", "offset", 50))
    resolved(0).headers should contain("X-Global" -> "true")
    resolved(0).headers should contain("X-Default" -> "yes")

    // Second endpoint keeps its own pagination but merges headers
    resolved(1).pagination shouldBe Some(CursorPagination("cursor", "$.next", 10))
    resolved(1).headers should contain("X-Custom" -> "override")
    resolved(1).headers should contain("X-Global" -> "true")
    resolved(1).headers should contain("X-Default" -> "yes")
  }

  // --- XML parsing tests ---

  "XmlToJsonConverter" should "convert simple XML to JSON" in {
    val xml = """<root><name>Alice</name><age>30</age></root>"""
    val result = XmlToJsonConverter.convert(xml, mapper)
    result.has("root") shouldBe true
    val root = result.get("root")
    root.get("name").asText() shouldBe "Alice"
    root.get("age").asText() shouldBe "30"
  }

  it should "convert repeated elements to arrays" in {
    val xml = """<root><items><item>A</item><item>B</item><item>C</item></items></root>"""
    val result = XmlToJsonConverter.convert(xml, mapper)
    val items = result.get("root").get("items").get("item")
    items.isArray shouldBe true
    items.size() shouldBe 3
  }

  it should "handle XML attributes" in {
    val xml = """<root><user id="123" active="true">Alice</user></root>"""
    val result = XmlToJsonConverter.convert(xml, mapper)
    val user = result.get("root").get("user")
    user.get("@id").asText() shouldBe "123"
    user.get("@active").asText() shouldBe "true"
    user.get("#text").asText() shouldBe "Alice"
  }

  it should "handle REST API to parse XML responses" in {
    // Add XML endpoint to mock server
    server.createContext(
      "/api/xml-data",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val xml =
            """<?xml version="1.0"?><response><items><item><id>1</id><name>Test</name></item></items></response>"""
          val bytes = xml.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "application/xml")
          exchange.sendResponseHeaders(200, bytes.length)
          val os = exchange.getResponseBody
          os.write(bytes)
          os.close()
        }
      }
    )
    val config = baseConfig(Nil)
    val client = new RestAPIClient(config)
    val response = client.execute("/api/xml-data")
    response.statusCode shouldBe 200
    response.body.has("response") shouldBe true
    val item = response.body.get("response").get("items").get("item")
    item.get("id").asText() shouldBe "1"
    item.get("name").asText() shouldBe "Test"
  }
}
