package ai.starlake.extract.impl.restapi

import com.fasterxml.jackson.annotation.{
  JsonCreator,
  JsonIgnoreProperties,
  JsonSubTypes,
  JsonTypeInfo
}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonSerializer,
  SerializerProvider
}

import java.util.regex.Pattern

// --- Auth ---

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[BearerAuth], name = "bearer"),
    new JsonSubTypes.Type(value = classOf[ApiKeyAuth], name = "api_key"),
    new JsonSubTypes.Type(value = classOf[BasicAuth], name = "basic"),
    new JsonSubTypes.Type(
      value = classOf[OAuth2ClientCredentials],
      name = "oauth2_client_credentials"
    )
  )
)
@JsonIgnoreProperties(ignoreUnknown = true)
sealed trait RestAPIAuth

case class BearerAuth(token: String) extends RestAPIAuth {
  @JsonCreator
  private def this() = this("")
}

case class ApiKeyAuth(
  key: String,
  header: String = "X-API-Key"
) extends RestAPIAuth {
  @JsonCreator
  private def this() = this("", "X-API-Key")
}

case class BasicAuth(
  username: String,
  password: String
) extends RestAPIAuth {
  @JsonCreator
  private def this() = this("", "")
}

case class OAuth2ClientCredentials(
  tokenUrl: String,
  clientId: String,
  clientSecret: String,
  scope: Option[String] = None
) extends RestAPIAuth {
  @JsonCreator
  private def this() = this("", "", "")
}

// --- Pagination ---

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[OffsetPagination], name = "offset"),
    new JsonSubTypes.Type(value = classOf[CursorPagination], name = "cursor"),
    new JsonSubTypes.Type(value = classOf[LinkHeaderPagination], name = "link_header"),
    new JsonSubTypes.Type(value = classOf[PageNumberPagination], name = "page_number")
  )
)
@JsonIgnoreProperties(ignoreUnknown = true)
sealed trait PaginationStrategy {
  def pageSize: Int
}

case class OffsetPagination(
  limitParam: String = "limit",
  offsetParam: String = "offset",
  pageSize: Int = 100
) extends PaginationStrategy {
  @JsonCreator
  private def this() = this("limit", "offset", 100)
}

case class CursorPagination(
  cursorParam: String = "after",
  cursorPath: String,
  pageSize: Int = 100,
  limitParam: Option[String] = None
) extends PaginationStrategy {
  @JsonCreator
  private def this() = this("after", "", 100, None)
}

case class LinkHeaderPagination(
  pageSize: Int = 100,
  limitParam: Option[String] = None
) extends PaginationStrategy {
  @JsonCreator
  private def this() = this(100, None)
}

case class PageNumberPagination(
  pageParam: String = "page",
  pageSize: Int = 100,
  limitParam: Option[String] = None
) extends PaginationStrategy {
  @JsonCreator
  private def this() = this("page", 100, None)
}

// --- HTTP Method ---

@JsonSerialize(`using` = classOf[RestAPIHttpMethodSerializer])
@JsonDeserialize(`using` = classOf[RestAPIHttpMethodDeserializer])
sealed trait RestAPIHttpMethod

case object RestGet extends RestAPIHttpMethod
case object RestPost extends RestAPIHttpMethod

class RestAPIHttpMethodSerializer extends JsonSerializer[RestAPIHttpMethod] {
  override def serialize(
    value: RestAPIHttpMethod,
    gen: JsonGenerator,
    serializers: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case RestGet  => "GET"
      case RestPost => "POST"
    }
    gen.writeString(strValue)
  }
}

class RestAPIHttpMethodDeserializer extends JsonDeserializer[RestAPIHttpMethod] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): RestAPIHttpMethod = {
    val value = jp.readValueAs[String](classOf[String])
    value.toUpperCase match {
      case "GET"  => RestGet
      case "POST" => RestPost
      case x      => throw new IllegalArgumentException(s"Unsupported HTTP method: $x")
    }
  }
}

// --- Endpoint ---

case class RestAPIEndpoint(
  path: String,
  method: RestAPIHttpMethod = RestGet,
  as: Option[String] = None,
  domain: String = "default",
  headers: Map[String, String] = Map.empty,
  queryParams: Map[String, String] = Map.empty,
  requestBody: Option[String] = None,
  pagination: Option[PaginationStrategy] = None,
  responsePath: Option[String] = None,
  incrementalField: Option[String] = None,
  children: List[RestAPIEndpoint] = Nil,
  excludeFields: List[Pattern] = Nil,
  errorPath: Option[String] = None
) {
  @JsonCreator
  private def this() = this(path = "")

  /** Derive the table name from the endpoint path if `as` is not set */
  def tableName: String = as.getOrElse {
    path
      .split("/")
      .filter(s => s.nonEmpty && !s.startsWith("{"))
      .lastOption
      .getOrElse("unknown")
      .replaceAll("[^a-zA-Z0-9_]", "_")
  }
}

// --- Top-level config ---

case class RateLimitConfig(
  requestsPerSecond: Int = 10
) {
  @JsonCreator
  private def this() = this(10)
}

case class RetryConfig(
  maxRetries: Int = 3,
  initialBackoffMs: Long = 1000,
  maxBackoffMs: Long = 30000
) {
  @JsonCreator
  private def this() = this(3, 1000, 30000)
}

case class TimeoutConfig(
  connectTimeoutMs: Int = 30000,
  readTimeoutMs: Int = 60000
) {
  @JsonCreator
  private def this() = this(30000, 60000)
}

case class ProxyConfig(
  host: String,
  port: Int,
  username: Option[String] = None,
  password: Option[String] = None
) {
  @JsonCreator
  private def this() = this("", 0)
}

case class TlsConfig(
  trustStorePath: Option[String] = None,
  trustStorePassword: Option[String] = None,
  keyStorePath: Option[String] = None,
  keyStorePassword: Option[String] = None,
  insecure: Boolean = false
) {
  @JsonCreator
  private def this() = this(None, None, None, None, false)
}

case class RestAPIDefaults(
  pagination: Option[PaginationStrategy] = None,
  headers: Map[String, String] = Map.empty,
  queryParams: Map[String, String] = Map.empty
) {
  @JsonCreator
  private def this() = this(None)
}

case class RestAPIExtractSchema(
  baseUrl: String,
  auth: Option[RestAPIAuth] = None,
  headers: Map[String, String] = Map.empty,
  rateLimit: Option[RateLimitConfig] = None,
  retry: Option[RetryConfig] = None,
  timeout: Option[TimeoutConfig] = None,
  proxy: Option[ProxyConfig] = None,
  tls: Option[TlsConfig] = None,
  defaults: Option[RestAPIDefaults] = None,
  endpoints: List[RestAPIEndpoint] = Nil
) {
  @JsonCreator
  private def this() = this(baseUrl = "")

  /** Return endpoints with defaults applied */
  def resolvedEndpoints: List[RestAPIEndpoint] = {
    val defaultPagination = defaults.flatMap(_.pagination)
    val defaultHeaders = defaults.map(_.headers).getOrElse(Map.empty)
    val defaultQueryParams = defaults.map(_.queryParams).getOrElse(Map.empty)
    endpoints.map { ep =>
      ep.copy(
        pagination = ep.pagination.orElse(defaultPagination),
        headers = defaultHeaders ++ headers ++ ep.headers,
        queryParams = defaultQueryParams ++ ep.queryParams
      )
    }
  }
}
