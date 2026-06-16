package ai.starlake.extract.impl.restapi

import com.fasterxml.jackson.databind.JsonNode

/** Handles pagination logic for REST API responses. Each implementation knows how to extract the
  * next page request parameters from the current response.
  */
trait PaginationHandler {

  /** Determine the query parameters for the next page based on the current response.
    * @param fullResponseBody
    *   the full JSON response body (before responsePath extraction)
    * @param dataNode
    *   the extracted data array/node (after responsePath extraction)
    * @param responseHeaders
    *   the HTTP response headers from the current page
    * @param currentPageIndex
    *   the 0-based index of the current page
    * @return
    *   Some(params) if there is a next page, None if pagination is complete
    */
  def nextPageParams(
    fullResponseBody: JsonNode,
    dataNode: JsonNode,
    responseHeaders: Map[String, String],
    currentPageIndex: Int
  ): Option[Map[String, String]]

  /** Return the initial query parameters for the first page request */
  def initialParams: Map[String, String]
}

object PaginationHandler {

  def apply(strategy: PaginationStrategy): PaginationHandler = strategy match {
    case s: OffsetPagination     => new OffsetPaginationHandler(s)
    case s: CursorPagination     => new CursorPaginationHandler(s)
    case s: LinkHeaderPagination => new LinkHeaderPaginationHandler(s)
    case s: PageNumberPagination => new PageNumberPaginationHandler(s)
  }

  /** No-op handler for endpoints without pagination — fetches a single page */
  val noPagination: PaginationHandler = new PaginationHandler {
    override def nextPageParams(
      fullResponseBody: JsonNode,
      dataNode: JsonNode,
      responseHeaders: Map[String, String],
      currentPageIndex: Int
    ): Option[Map[String, String]] = None

    override def initialParams: Map[String, String] = Map.empty
  }
}

class OffsetPaginationHandler(config: OffsetPagination) extends PaginationHandler {

  override def initialParams: Map[String, String] = Map(
    config.limitParam  -> config.pageSize.toString,
    config.offsetParam -> "0"
  )

  override def nextPageParams(
    fullResponseBody: JsonNode,
    dataNode: JsonNode,
    responseHeaders: Map[String, String],
    currentPageIndex: Int
  ): Option[Map[String, String]] = {
    val recordCount =
      if (dataNode.isArray) dataNode.size()
      else 0

    if (recordCount < config.pageSize) None
    else {
      val nextOffset = (currentPageIndex + 1) * config.pageSize
      Some(
        Map(
          config.limitParam  -> config.pageSize.toString,
          config.offsetParam -> nextOffset.toString
        )
      )
    }
  }
}

class CursorPaginationHandler(config: CursorPagination) extends PaginationHandler {

  override def initialParams: Map[String, String] = {
    val params = scala.collection.mutable.Map[String, String]()
    config.limitParam.foreach(lp => params += (lp -> config.pageSize.toString))
    params.toMap
  }

  override def nextPageParams(
    fullResponseBody: JsonNode,
    dataNode: JsonNode,
    responseHeaders: Map[String, String],
    currentPageIndex: Int
  ): Option[Map[String, String]] = {
    val cursorValue = JsonPathUtil.extract(fullResponseBody, config.cursorPath)
    cursorValue match {
      case Some(cursor) if cursor.nonEmpty =>
        val params = scala.collection.mutable.Map(config.cursorParam -> cursor)
        config.limitParam.foreach(lp => params += (lp -> config.pageSize.toString))
        Some(params.toMap)
      case _ => None
    }
  }
}

class LinkHeaderPaginationHandler(config: LinkHeaderPagination) extends PaginationHandler {

  override def initialParams: Map[String, String] = {
    config.limitParam
      .map(lp => Map(lp -> config.pageSize.toString))
      .getOrElse(Map.empty)
  }

  override def nextPageParams(
    fullResponseBody: JsonNode,
    dataNode: JsonNode,
    responseHeaders: Map[String, String],
    currentPageIndex: Int
  ): Option[Map[String, String]] = {
    // Parse Link header: <url>; rel="next"
    responseHeaders.get("link").orElse(responseHeaders.get("Link")).flatMap { linkHeader =>
      val nextPattern = """<([^>]+)>;\s*rel="next"""".r
      nextPattern.findFirstMatchIn(linkHeader).map { m =>
        val nextUrl = m.group(1)
        // Extract query params from the next URL
        val queryString = nextUrl.split("\\?", 2) match {
          case Array(_, qs) => qs
          case _            => ""
        }
        queryString
          .split("&")
          .filter(_.contains("="))
          .map { param =>
            val parts = param.split("=", 2)
            java.net.URLDecoder.decode(parts(0), "UTF-8") ->
            java.net.URLDecoder.decode(parts(1), "UTF-8")
          }
          .toMap
      }
    }
  }
}

class PageNumberPaginationHandler(config: PageNumberPagination) extends PaginationHandler {

  override def initialParams: Map[String, String] = {
    val params = scala.collection.mutable.Map(config.pageParam -> "1")
    config.limitParam.foreach(lp => params += (lp -> config.pageSize.toString))
    params.toMap
  }

  override def nextPageParams(
    fullResponseBody: JsonNode,
    dataNode: JsonNode,
    responseHeaders: Map[String, String],
    currentPageIndex: Int
  ): Option[Map[String, String]] = {
    val recordCount =
      if (dataNode.isArray) dataNode.size()
      else 0

    if (recordCount < config.pageSize) None
    else {
      val nextPage = currentPageIndex + 2 // pages are 1-based
      val params = scala.collection.mutable.Map(config.pageParam -> nextPage.toString)
      config.limitParam.foreach(lp => params += (lp -> config.pageSize.toString))
      Some(params.toMap)
    }
  }
}

/** Utility for navigating JSON using simple dot-notation or JSONPath-like expressions */
object JsonPathUtil {

  /** Extract a string value from a JsonNode using a simple path expression. Supports:
    *   - dot notation: "meta.next_cursor"
    *   - JSONPath-style: "$.meta.next_cursor"
    */
  def extract(node: JsonNode, path: String): Option[String] = {
    val cleanPath = if (path.startsWith("$.")) path.substring(2) else path
    val parts = cleanPath.split("\\.")
    var current = node
    for (part <- parts) {
      if (current == null || current.isMissingNode || current.isNull) return None
      current = current.get(part)
    }
    if (current == null || current.isMissingNode || current.isNull) None
    else if (current.isTextual) Some(current.asText())
    else if (current.isNumber) Some(current.asText())
    else Some(current.toString)
  }

  /** Extract all records from a response body at the given path. If responsePath is None, assumes
    * the response body itself is the data array.
    */
  def extractDataArray(responseBody: JsonNode, responsePath: Option[String]): JsonNode = {
    responsePath match {
      case None => responseBody
      case Some(path) =>
        val cleanPath = if (path.startsWith("$.")) path.substring(2) else path
        val parts = cleanPath.split("\\.")
        var current = responseBody
        for (part <- parts) {
          if (current == null || current.isMissingNode || current.isNull) return responseBody
          current = current.get(part)
        }
        if (current == null || current.isMissingNode) responseBody else current
    }
  }
}
