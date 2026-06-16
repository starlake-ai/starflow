package ai.starlake.extract.impl.restapi

import ai.starlake.config.Settings
import ai.starlake.extract.ParUtils
import ai.starlake.schema.handlers.StorageHandler
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.csv.{CsvFormat, CsvWriter, CsvWriterSettings}
import org.apache.hadoop.fs.Path

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Output format for REST API extraction */
sealed trait RestAPIOutputFormat
case object CsvOutput extends RestAPIOutputFormat
case object JsonLinesOutput extends RestAPIOutputFormat

object RestAPIOutputFormat {
  def fromString(s: String): RestAPIOutputFormat = s.toLowerCase match {
    case "jsonl" | "jsonlines" | "json_lines" => JsonLinesOutput
    case _                                    => CsvOutput
  }
}

/** Configuration for REST API data extraction */
case class RestAPIDataExtractConfig(
  extractConfig: RestAPIExtractSchema,
  baseOutputDir: Path,
  limit: Int = 0,
  parallelism: Option[Int] = None,
  incremental: Boolean = false,
  outputFormat: RestAPIOutputFormat = CsvOutput,
  resume: Boolean = false
)

/** Extracts data from REST API endpoints and writes to CSV files. Data is written to
  * {outputDir}/{domain}/{tableName}-{timestamp}.csv and can then be picked up by the standard
  * Starlake ingestion pipeline.
  */
class RestAPIDataExtractor(implicit settings: Settings) extends LazyLogging {

  private val stateMapper = new ObjectMapper()
  stateMapper.registerModule(DefaultScalaModule)

  private val extractionDateTime: String =
    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  def run(config: RestAPIDataExtractConfig): Try[Long] = {
    val client = new RestAPIClient(config.extractConfig)
    val endpoints = config.extractConfig.resolvedEndpoints
    implicit val storageHandler: StorageHandler = settings.storageHandler()

    Try {
      val results = ParUtils.runInParallel(endpoints, config.parallelism) { endpoint =>
        extractEndpointData(client, endpoint, config, None)
      }
      results.sum
    }
  }

  private def extractEndpointData(
    client: RestAPIClient,
    endpoint: RestAPIEndpoint,
    config: RestAPIDataExtractConfig,
    parentRecord: Option[JsonNode]
  )(implicit storageHandler: StorageHandler): Long = {
    val resolvedPath = resolveParentPlaceholders(endpoint.path, parentRecord)
    val resolvedEndpoint = endpoint.copy(path = resolvedPath)
    val tableName = resolvedEndpoint.tableName
    val domainDir = new Path(config.baseOutputDir, resolvedEndpoint.domain)

    // Incremental: read last extracted value and add as query param
    val incrementalParams: Map[String, String] =
      if (config.incremental) {
        endpoint.incrementalField
          .flatMap { field =>
            readIncrementalState(config.baseOutputDir, resolvedEndpoint.domain, tableName)
              .map(lastValue => Map(field -> lastValue))
          }
          .getOrElse(Map.empty)
      } else Map.empty

    // Conditional requests: read ETag/Last-Modified from state and add as headers
    val previousState = if (config.incremental) {
      readState(config.baseOutputDir, resolvedEndpoint.domain, tableName)
    } else Map.empty[String, String]

    val conditionalHeaders = scala.collection.mutable.Map[String, String]()
    previousState.get("etag").foreach(conditionalHeaders += "If-None-Match" -> _)
    previousState.get("lastModified").foreach(conditionalHeaders += "If-Modified-Since" -> _)

    val endpointWithIncrParams = {
      val ep = if (incrementalParams.nonEmpty) {
        resolvedEndpoint.copy(queryParams = resolvedEndpoint.queryParams ++ incrementalParams)
      } else resolvedEndpoint
      if (conditionalHeaders.nonEmpty) {
        ep.copy(headers = ep.headers ++ conditionalHeaders)
      } else ep
    }

    logger.info(s"Extracting data from ${endpointWithIncrParams.path} as $tableName")

    // Ensure output directory exists
    if (!storageHandler.exists(domainDir)) {
      storageHandler.mkdirs(domainDir)
    }

    val fileExtension = config.outputFormat match {
      case CsvOutput       => "csv"
      case JsonLinesOutput => "jsonl"
    }
    val outputFile =
      new Path(domainDir, s"$tableName-$extractionDateTime.$fileExtension")

    var totalRecords = 0L
    var headerWritten = false
    var csvWriter: CsvWriter = null
    var outputWriter: OutputStreamWriter = null
    var firstResponseEtag: Option[String] = None
    var firstResponseLastModified: Option[String] = None
    // Buffer parent records if children need processing (avoids re-fetching)
    val parentRecordBuffer =
      if (endpoint.children.nonEmpty) scala.collection.mutable.ListBuffer[JsonNode]()
      else null
    // Track max incremental field value
    var maxIncrementalValue: Option[String] = None
    val incrementalField = endpoint.incrementalField
    var pageCount = 0

    // Resume: skip pages already extracted in a previous run
    val pagesToSkip = if (config.resume) {
      previousState.get("pagesExtracted").map(_.toInt).getOrElse(0)
    } else 0

    try {
      val pages = client.fetchAllPages(endpointWithIncrParams)

      if (pagesToSkip > 0) {
        logger.info(s"Resuming $tableName: skipping $pagesToSkip already-extracted pages")
      }

      for ((dataNode, pageHeaders) <- pages) {
        pageCount += 1
        if (pageCount <= pagesToSkip) {
          // Skip already-extracted pages on resume
        } else {
          // Capture ETag/Last-Modified from first page for conditional requests
          if (!headerWritten) {
            firstResponseEtag = pageHeaders.get("etag")
            firstResponseLastModified = pageHeaders.get("last-modified")
          }
          val records = if (dataNode.isArray) {
            dataNode.elements().asScala.toList
          } else {
            List(dataNode)
          }

          if (records.nonEmpty) {
            // Lazily initialize writer on first data
            if (!headerWritten) {
              outputWriter = new OutputStreamWriter(
                storageHandler.output(outputFile),
                StandardCharsets.UTF_8
              )
              config.outputFormat match {
                case CsvOutput =>
                  val headers = flattenFieldNames(records.head, "")
                  val writerSettings = new CsvWriterSettings()
                  val format = new CsvFormat()
                  format.setDelimiter(',')
                  writerSettings.setFormat(format)
                  writerSettings.setHeaderWritingEnabled(true)
                  writerSettings.setHeaders(headers: _*)
                  writerSettings.setQuoteAllFields(true)
                  csvWriter = new CsvWriter(outputWriter, writerSettings)
                  csvWriter.writeHeaders()
                case JsonLinesOutput => // outputWriter is enough
              }
              headerWritten = true
            }

            for (record <- records) {
              if (config.limit <= 0 || totalRecords < config.limit) {
                config.outputFormat match {
                  case CsvOutput =>
                    val row = flattenValues(record, "")
                    csvWriter.writeRow(row: _*)
                  case JsonLinesOutput =>
                    outputWriter.write(record.toString)
                    outputWriter.write('\n')
                }
                totalRecords += 1
                // Buffer for children processing
                if (parentRecordBuffer != null) parentRecordBuffer += record
                // Track incremental field max value
                incrementalField.foreach { field =>
                  val fieldNode = record.get(field)
                  if (fieldNode != null && !fieldNode.isNull) {
                    val value = fieldNode.asText()
                    maxIncrementalValue = maxIncrementalValue match {
                      case None                       => Some(value)
                      case Some(prev) if value > prev => Some(value)
                      case existing                   => existing
                    }
                  }
                }
              }
            }
          }
        } // close else (skip on resume)
      }

      // Process children endpoints using buffered parent records
      if (parentRecordBuffer != null && parentRecordBuffer.nonEmpty) {
        for (parentRec <- parentRecordBuffer; child <- endpoint.children) {
          Try(extractEndpointData(client, child, config, Some(parentRec))) match {
            case Success(count) =>
              logger.info(
                s"Extracted $count child records from ${child.path} for parent ${endpointWithIncrParams.path}"
              )
            case Failure(e) =>
              logger.error(
                s"Failed to extract child ${child.path}: ${e.getMessage}"
              )
          }
        }
      }

      // Persist state (incremental value, ETag, pages extracted for resume)
      if (config.incremental || config.resume) {
        val lastValue = maxIncrementalValue.getOrElse("")
        writeIncrementalState(
          config.baseOutputDir,
          endpointWithIncrParams.domain,
          tableName,
          lastValue,
          firstResponseEtag,
          firstResponseLastModified,
          Some(pageCount)
        )
      }

      logger.info(
        s"Extracted $totalRecords records from ${endpointWithIncrParams.path} into $outputFile"
      )
      totalRecords
    } finally {
      if (csvWriter != null) csvWriter.close()
      if (outputWriter != null) outputWriter.close()
    }
  }

  /** Replace {parent.fieldName} placeholders in the path with values from the parent record */
  private def resolveParentPlaceholders(path: String, parentRecord: Option[JsonNode]): String = {
    parentRecord match {
      case None => path
      case Some(parent) =>
        val pattern = """\{parent\.([^}]+)\}""".r
        pattern.replaceAllIn(
          path,
          m => {
            val field = m.group(1)
            val value = parent.get(field)
            if (value == null || value.isNull) {
              throw new RestAPIException(
                s"Parent record does not have field '$field' for path placeholder"
              )
            }
            java.net.URLEncoder.encode(value.asText(), "UTF-8")
          }
        )
    }
  }

  /** Flatten a JSON record's field names using dot notation for nested objects */
  private def flattenFieldNames(node: JsonNode, prefix: String): Array[String] = {
    node.fieldNames().asScala.toArray.flatMap { fieldName =>
      val fullName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"
      val fieldNode = node.get(fieldName)
      if (fieldNode != null && fieldNode.isObject) {
        flattenFieldNames(fieldNode, fullName)
      } else {
        Array(fullName)
      }
    }
  }

  /** Flatten a JSON record's values matching the order from flattenFieldNames */
  private def flattenValues(node: JsonNode, prefix: String): Array[String] = {
    node.fieldNames().asScala.toArray.flatMap { fieldName =>
      val fieldNode = node.get(fieldName)
      if (fieldNode != null && fieldNode.isObject) {
        flattenValues(fieldNode, if (prefix.isEmpty) fieldName else s"$prefix.$fieldName")
      } else {
        Array(nodeToString(fieldNode))
      }
    }
  }

  private def nodeToString(node: JsonNode): String = {
    if (node == null || node.isNull || node.isMissingNode) ""
    else if (node.isTextual) node.asText()
    else if (node.isArray || node.isObject) node.toString // JSON string for complex types
    else node.asText()
  }

  // --- Incremental state management ---

  private def stateFilePath(baseOutputDir: Path, domain: String, tableName: String): Path =
    new Path(new Path(baseOutputDir, s".state/$domain"), s"$tableName.json")

  private def readState(
    baseOutputDir: Path,
    domain: String,
    tableName: String
  )(implicit storageHandler: StorageHandler): Map[String, String] = {
    val path = stateFilePath(baseOutputDir, domain, tableName)
    if (storageHandler.exists(path)) {
      try {
        val content = storageHandler.read(path)
        val node = stateMapper.readTree(content)
        val fields = scala.collection.mutable.Map[String, String]()
        val it = node.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          val value = node.get(key)
          if (value != null && !value.isNull) fields += (key -> value.asText())
        }
        fields.toMap
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to read state from $path: ${e.getMessage}")
          Map.empty
      }
    } else Map.empty
  }

  private def readIncrementalState(
    baseOutputDir: Path,
    domain: String,
    tableName: String
  )(implicit storageHandler: StorageHandler): Option[String] =
    readState(baseOutputDir, domain, tableName).get("lastValue")

  private def writeState(
    baseOutputDir: Path,
    domain: String,
    tableName: String,
    state: Map[String, String]
  )(implicit storageHandler: StorageHandler): Unit = {
    val path = stateFilePath(baseOutputDir, domain, tableName)
    val stateDir = path.getParent
    if (!storageHandler.exists(stateDir)) {
      storageHandler.mkdirs(stateDir)
    }
    val content = stateMapper.writeValueAsString(state)
    storageHandler.write(content, path)
  }

  private def writeIncrementalState(
    baseOutputDir: Path,
    domain: String,
    tableName: String,
    lastValue: String,
    etag: Option[String] = None,
    lastModified: Option[String] = None,
    pagesExtracted: Option[Int] = None
  )(implicit storageHandler: StorageHandler): Unit = {
    val state = scala.collection.mutable.Map(
      "lastValue"   -> lastValue,
      "extractedAt" -> extractionDateTime
    )
    etag.foreach(state += "etag" -> _)
    lastModified.foreach(state += "lastModified" -> _)
    pagesExtracted.foreach(p => state += "pagesExtracted" -> p.toString)
    writeState(baseOutputDir, domain, tableName, state.toMap)
    logger.info(s"Saved state: $tableName lastValue=$lastValue pages=$pagesExtracted")
  }
}
