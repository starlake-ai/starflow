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

package ai.starlake.job.infer

import ai.starlake.config.{Settings, SparkEnv}
import ai.starlake.schema.handlers.{DataTypesToInt, InferSchemaHandler, StorageHandler}
import ai.starlake.schema.model.Format.DSV
import ai.starlake.schema.model._
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.BufferedReader
import java.nio.charset.Charset
import java.util.regex.Pattern
import scala.util.Try

/** * Infers the schema of a given datapath, domain name, schema name.
  */
class InferSchemaJob(implicit settings: Settings) extends LazyLogging {

  def name: String = "InferSchema"

  private val sparkEnv: SparkEnv = SparkEnv.get(name, identity, settings)
  private val session: SparkSession = sparkEnv.session

  /** Read file without specifying the format
    *
    * @param path
    *   : file path
    * @return
    *   a dataset of string that contains data file
    */
  def readFile(path: Path): Dataset[String] = {
    session.read
      .textFile(path.toString)
  }

  /** Get format file by using the first and the last line of the dataset We use
    * mapPartitionsWithIndex to retrieve these information to make sure that the first line really
    * corresponds to the first line (same for the last)
    *
    * @param lines
    *   : list of lines read from file
    * @return
    */
  def getFormatFile(inputPath: String, encoding: Charset)(implicit
    storageHandler: StorageHandler
  ): String = {
    val file = File(inputPath)

    val filePath = new Path(inputPath)

    file.extension(includeDot = false).getOrElse("").toLowerCase() match {
      case "parquet" => "PARQUET"
      case "xml"     => "XML"
      case "json" if filePath.firstLine(encoding).startsWith("[") =>
        "JSON_ARRAY"
      case "json" if filePath.firstLine(encoding).startsWith("{") =>
        "JSON"
      case "csv" | "dsv" | "tsv" | "psv"  => "DSV"
      case _ if hasParquetMagic(filePath) => "PARQUET"
      case _ =>
        val jsonRegexStart = """\{.*""".r
        val jsonArrayRegexStart = """\[.*""".r

        val jsonRegexEnd = """.*\}""".r
        val jsonArrayRegexEnd = """.*\]""".r

        val xmlRegexStart = """<.*""".r
        val xmlRegexEnd = """.*>""".r
        /*
        (filePath.firstLine(encoding), filePath.lastLine(encoding)) match {
          case (jsonRegexStart(), jsonRegexEnd())           => "JSON"
          case (jsonArrayRegexStart(), jsonArrayRegexEnd()) => "JSON_ARRAY"
          case (xmlRegexStart(), xmlRegexEnd())             => "XML"
          case _                                            => "DSV"
        }
         */
        filePath.firstLine(encoding) match {
          case (jsonRegexStart())      => "JSON"
          case (jsonArrayRegexStart()) => "JSON_ARRAY"
          case (xmlRegexStart())       => "XML"
          case _                       => "DSV"
        }
    }
  }

  /** Parquet files start with the "PAR1" magic bytes. Used when the file has no extension (eg.
    * uploaded temp files) so binary content is not parsed as DSV.
    */
  private def hasParquetMagic(filePath: Path)(implicit storageHandler: StorageHandler): Boolean =
    Try {
      storageHandler.readAndExecuteIS(filePath) { is =>
        val magic = new Array[Byte](4)
        is.readNBytes(magic, 0, 4) == 4 && new String(magic, "US-ASCII") == "PAR1"
      }
    }.getOrElse(false)

  /** Get separator file by taking the character that appears the most in 10 lines of the dataset
    *
    * @param lines
    *   : list of lines read from file
    * @return
    *   the file separator
    */
  def getSeparator(firstLine: String): String = {
    val (separator, _) = {
      val chars = firstLine
        .replaceAll("[A-Za-z0-9 \"'()@?!éèîàÀÉÈç+\\-_]", "")
        .toCharArray
        .map((_, 1))
        .groupBy(_._1)
        .view
        .mapValues(_.length)
        .toList

      if (chars.nonEmpty)
        chars.maxBy { case (ch, count) => count }
      else
        // We assume "," is the separator
        (',', 1)
    }
    separator.toString
  }

  /** Get schema pattern
    *
    * @param path
    *   : file path
    * @return
    *   the schema pattern
    */

  /** Create the dataframe with its associated format
    *
    * @param lines
    *   : list of lines read from file
    * @param path
    *   : file path
    * @return
    *   dataframe and rowtag if xml
    */
  private def createDataFrameWithFormat(
    dataPath: String,
    tableName: String,
    rowTag: Option[String],
    encoding: Charset,
    inferSchema: Boolean = true,
    forceFormat: Option[Format] = None
  )(implicit storageHandler: StorageHandler): (DataFrame, Option[String], String) = {
    val formatFile = forceFormat.map(_.toString).getOrElse(getFormatFile(dataPath, encoding))

    formatFile match {
      case "PARQUET" =>
        val df = session.read
          .parquet(dataPath)
        (df, None, formatFile)
      case "JSON_ARRAY" =>
        val df = session.read
          .option("multiLine", true)
          .option("lineSep", "\n")
          .option("encoding", encoding.name())
          .json(dataPath)
        (df, None, formatFile)
      case "JSON" =>
        val isJsonL = storageHandler.readAndExecute(new Path(dataPath), encoding)(isr => {
          val bufferedReader = new BufferedReader(isr)
          (Iterator continually bufferedReader.readLine takeWhile (_ != null))
            .map(_.trim)
            .filter(_.nonEmpty)
            .forall(line => {
              line.length >= 2 && line.startsWith("{") && line.endsWith("}")
            })
        })
        val df = session.read
          .option("encoding", encoding.name())
          .option("multiLine", !isJsonL)
          .option("lineSep", "\n")
          .json(dataPath)
        (df, None, formatFile)
      case "XML" =>
        // find second occurrence of xml tag starting with letter in content
        val tag = {
          rowTag.getOrElse {
            storageHandler.readAndExecute(new Path(dataPath), encoding)(isr => {
              val bufferedReader = new BufferedReader(isr)
              import scala.jdk.CollectionConverters._
              val lineIterator = bufferedReader.lines().iterator().asScala
              var content = ""
              var tag = None: Option[String]
              while (tag.isEmpty && lineIterator.hasNext) {
                val currentLine = lineIterator.next().trim.replace("<?", "")
                content = content + currentLine
                val firstXmlTagStart = content.indexOf("<")
                val secondXmlTagStart = content.indexOf('<', firstXmlTagStart + 1)
                val closingTag = content.indexOf(">", secondXmlTagStart)
                if (firstXmlTagStart != -1 && secondXmlTagStart != -1 && closingTag != -1) {
                  tag = Some(content.substring(secondXmlTagStart + 1, closingTag).split(' ')(0))
                }
              }
              val result = tag match {
                case Some(tagFound) => tagFound
                case None =>
                  tableName
              }
              logger.info(s"Using rowTag: $result")
              result
            })
          }
        }

        val df = session.read
          .format("xml")
          .option("encoding", encoding.name())
          .option("charset", encoding.name())
          .option("rowTag", tag)
          .option("inferSchema", value = inferSchema)
          .load(dataPath)

        (df, Some(tag), formatFile)
      case "DSV" =>
        val df = session.read
          .format("csv")
          .option("header", value = true)
          .option("inferSchema", value = inferSchema)
          .option("encoding", encoding.name())
          .option("delimiter", getSeparator(new Path(dataPath).firstLine(encoding)))
          .load(dataPath)
        (df, None, formatFile)
    }
  }

  private implicit class RichPath(filePath: Path) {
    def firstLine(encoding: Charset)(implicit storageHandler: StorageHandler): String =
      storageHandler.readAndExecute(filePath, encoding)(isr => {
        val bufferedReader = new BufferedReader(isr)
        bufferedReader.readLine()
      })

    def lastLine(encoding: Charset)(implicit storageHandler: StorageHandler): String =
      storageHandler.readAndExecute(filePath, encoding)(isr => {
        val bufferedReader = new BufferedReader(isr)
        (Iterator continually bufferedReader.readLine takeWhile (_ != null)).foldLeft("") {
          case (_, line) => line
        }
      })

    def lines(encoding: Charset)(implicit storageHandler: StorageHandler): List[String] =
      storageHandler.readAndExecute(filePath, encoding)(isr => {
        val bufferedReader = new BufferedReader(isr)
        (Iterator continually bufferedReader.readLine takeWhile (_ != null)).toList
      })
  }

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  def infer(
    domainName: String,
    tableName: String,
    pattern: Option[String],
    comment: Option[String],
    inputPath: String,
    saveDir: String,
    forceFormat: Option[Format],
    writeMode: WriteMode,
    rowTag: Option[String],
    clean: Boolean,
    encoding: Charset,
    variant: Boolean = false,
    fromJsonSchema: Boolean = false
  )(implicit storageHandler: StorageHandler): Try[Path] = {
    Try {
      val path = new Path(inputPath)

      val schema = forceFormat match {
        case Some(Format.POSITION) =>
          val content =
            if (forceFormat.exists(Format.isBinary))
              List("")
            else {
              path.lines(encoding)
            }
          val lines = content.map(_.trim).filter(_.nonEmpty)
          var lastIndex = -1
          val attributes = lines.zipWithIndex.map { case (line, index) =>
            val fieldIndex = line.indexOf(":")
            if (fieldIndex == -1)
              throw new IllegalArgumentException(
                s"""Positional format schema inference requires a colon (:) to separate the field name from its value in line $index.
                   |Example
                   |-------
                   |order_id:00001
                   |customer_id:010203
                   |""".stripMargin
              )
            val fieldName = line.substring(0, fieldIndex).trim
            val field =
              line.substring(fieldIndex + 1) // no trim to keep leading and trailing spaces
            val startPosition = lastIndex + 1
            val endPosition = startPosition + field.length - 1
            lastIndex = endPosition
            TableAttribute(
              name = fieldName,
              position = Some(Position(startPosition, endPosition)),
              sample = Option(field)
            )
          }
          val metadata = InferSchemaHandler.createMetaData(Format.POSITION, encoding)

          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(InferSchemaJob.inferPattern(path.getName))),
            comment,
            attributes,
            Some(metadata),
            None
          )

        case _ if fromJsonSchema =>
          val attributes = inferJsonSchema(inputPath, encoding)
          val metadata = InferSchemaHandler.createMetaData(Format.JSON, encoding)
          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(InferSchemaJob.inferPattern(path.getName))),
            comment,
            attributes,
            Some(metadata),
            None
          )

        case forceFormat =>
          val (dataframeWithFormat, xmlTag, formatStr) =
            createDataFrameWithFormat(
              inputPath,
              tableName,
              rowTag,
              forceFormat = forceFormat,
              encoding = encoding
            )
          val array = formatStr == "JSON_ARRAY"
          val format = Format.fromString(formatStr)
          val targetAttributeTypes =
            InferSchemaHandler.adjustAttributes(dataframeWithFormat.schema, format)(_)

          val dataframeToInfer = format match {
            case Format.DSV =>
              val (rawDataframeWithFormat, _, _) =
                createDataFrameWithFormat(
                  inputPath,
                  tableName,
                  rowTag,
                  inferSchema = false,
                  encoding = encoding,
                  forceFormat = Some(format)
                )
              rawDataframeWithFormat
            case _ =>
              dataframeWithFormat
          }
          val adjustedAttributesType: Option[Row] = dataframeToInfer
            .transform(targetAttributeTypes)
            .collect()
            .headOption

          val adjustedAttributesTypeMap = adjustedAttributesType
            .map(r => {
              r.schema.fields.map { f =>
                if (f.name.endsWith(InferSchemaHandler.sampleColumnSuffix))
                  f.name -> r.getAs[String](f.name)
                else
                  f.name -> DataTypesToInt.typeIntToTypeStr(r.getAs[Int](f.name))
              }.toMap
            })
            .getOrElse(Map.empty)
          val attributes: List[TableAttribute] =
            InferSchemaHandler.createAttributes(
              adjustedAttributesTypeMap,
              dataframeWithFormat.schema
            )
          val preciseFormat =
            format match {
              case Format.JSON =>
                if (attributes.exists(_.attributes.nonEmpty)) Format.JSON else Format.JSON_FLAT
              case _ => format
            }
          val xmlOptions = xmlTag.map(tag => Map("rowTag" -> tag))
          val metadata = InferSchemaHandler.createMetaData(
            preciseFormat,
            encoding,
            Option(array),
            Some(true),
            format match {
              case DSV => Some(getSeparator(path.firstLine(encoding)))
              case _   => None
            },
            xmlOptions
          )

          val strategy = WriteStrategy(
            `type` = Some(WriteStrategyType.fromWriteMode(writeMode))
          )
          val sample =
            metadata.resolveFormat() match {
              case Format.JSON | Format.JSON_FLAT =>
                if (metadata.resolveArray())
                  dataframeToInfer
                    .limit(20)
                    .toJSON
                    .collect()
                    .mkString("[", metadata.resolveSeparator(), "]")
                else
                  dataframeToInfer.limit(20).toJSON.collect().mkString("\n")
              case Format.DSV =>
                dataframeToInfer.limit(20).collect().mkString("\n")
              case _ =>
                dataframeToInfer.limit(20).toJSON.collect().mkString("\n")
            }
          val isFormatVariantCompatible =
            Set(Format.JSON, Format.JSON_FLAT, Format.XML).contains(
              preciseFormat
            )
          val finalAttributes =
            if (variant && isFormatVariantCompatible) {
              List(
                TableAttribute(
                  name = "value",
                  `type` = PrimitiveType.variant.toString,
                  sample = Some(sample)
                )
              )
            } else attributes
          InferSchemaHandler.createSchema(
            tableName,
            Pattern.compile(pattern.getOrElse(InferSchemaJob.inferPattern(path.getName))),
            comment,
            finalAttributes,
            Some(metadata.copy(writeStrategy = Some(strategy))),
            sample = Some(sample)
          )
      }

      val domain: DomainInfo =
        InferSchemaHandler.createDomain(
          domainName,
          Some(
            Metadata(
              directory = Some(s"{{SL_DATASETS}}/incoming/$domainName")
            )
          ),
          schemas = List(schema)
        )

      InferSchemaHandler.generateYaml(domain, saveDir, clean)
    }.flatten
  }

  private def inferJsonSchema(inputPath: String, encoding: Charset): List[TableAttribute] = {
    val content = File(inputPath).contentAsString(encoding)
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val rootNode = objectMapper.readTree(content)
    val propertiesNode = if (rootNode.has("properties")) rootNode.get("properties") else rootNode
    val requiredNode = if (rootNode.has("required")) rootNode.get("required") else null
    val requiredFields =
      if (requiredNode != null && requiredNode.isArray) {
        import scala.jdk.CollectionConverters._
        requiredNode.elements().asScala.map(_.asText()).toSet
      } else Set.empty[String]

    parseJsonSchema(propertiesNode, requiredFields, rootNode)
  }

  private def resolveRef(node: JsonNode, rootNode: JsonNode): JsonNode = {
    if (node.has("$ref")) {
      val ref = node.get("$ref").asText()
      if (ref.startsWith("#/")) {
        val path = ref.substring(2).split("/")
        val resolved = path.foldLeft(rootNode)((currentNode, segment) =>
          if (currentNode != null && currentNode.has(segment)) currentNode.get(segment) else null
        )
        if (resolved != null) resolveRef(resolved, rootNode) else node
      } else {
        val defs = if (rootNode.has("$defs")) rootNode.get("$defs") else rootNode.get("definitions")
        if (defs != null && defs.has(ref)) {
          resolveRef(defs.get(ref), rootNode)
        } else {
          node
        }
      }
    } else {
      node
    }
  }

  private def parseJsonSchema(
    jsonNode: JsonNode,
    requiredFields: Set[String],
    rootNode: JsonNode
  ): List[TableAttribute] = {
    import scala.jdk.CollectionConverters._
    if (jsonNode.isObject) {
      jsonNode
        .fields()
        .asScala
        .map { entry =>
          val name = entry.getKey
          val rawNode = entry.getValue
          val node = resolveRef(rawNode, rootNode)
          val typeNode = node.get("type")
          val typeStr = if (typeNode != null) {
            if (typeNode.isArray) typeNode.get(0).asText().toLowerCase
            else typeNode.asText().toLowerCase
          } else "string"

          val (tpe, subAttributes, isArray) = typeStr match {
            case "object" =>
              val properties = if (node.has("properties")) node.get("properties") else node
              val requiredNode = if (node.has("required")) node.get("required") else null
              val subRequiredFields =
                if (requiredNode != null && requiredNode.isArray) {
                  requiredNode.elements().asScala.map(_.asText()).toSet
                } else Set.empty[String]
              ("struct", parseJsonSchema(properties, subRequiredFields, rootNode), false)
            case "array" =>
              val rawItems = node.get("items")
              val items = if (rawItems != null) resolveRef(rawItems, rootNode) else null
              val itemType =
                if (items != null && items.has("type")) items.get("type").asText().toLowerCase
                else "string"
              if (itemType == "object") {
                val properties = if (items.has("properties")) items.get("properties") else items
                val requiredNode = if (items.has("required")) items.get("required") else null
                val subRequiredFields =
                  if (requiredNode != null && requiredNode.isArray) {
                    requiredNode.elements().asScala.map(_.asText()).toSet
                  } else Set.empty[String]
                ("struct", parseJsonSchema(properties, subRequiredFields, rootNode), true)
              } else {
                val primitiveType = itemType match {
                  case "integer" => "long"
                  case "number"  => "double"
                  case "boolean" => "boolean"
                  case _         => "string"
                }
                (primitiveType, Nil, true)
              }
            case "integer" => ("long", Nil, false)
            case "number"  => ("double", Nil, false)
            case "boolean" => ("boolean", Nil, false)
            case _         => ("string", Nil, false)
          }

          val required = requiredFields.contains(name)
          TableAttribute(
            name = name,
            `type` = tpe,
            array = Some(isArray),
            required = Some(required),
            attributes = subAttributes
          )
        }
        .toList
    } else {
      Nil
    }
  }
}

object InferSchemaJob {
  def inferPattern(filename: String): String = {
    val parts = filename.split("\\.")
    if (parts.length < 2)
      filename
    else {
      // pattern extension
      val extension = parts.last

      // remove extension from filename hello-1234.json => hello-1234
      val prefix = filename.substring(0, filename.length - (extension.length + 1))

      val prefixArray = prefix.toCharArray
      var indexOfNonAlpha = 0
      var continue = true
      do {
        continue = indexOfNonAlpha < prefixArray.length &&
          (prefixArray(indexOfNonAlpha).isLetterOrDigit || prefixArray(
            indexOfNonAlpha
          ) == '_' || prefixArray(indexOfNonAlpha) == '-')

        val end =
          indexOfNonAlpha == prefixArray.length || ((prefixArray(
            indexOfNonAlpha
          ) == '_' || prefixArray(indexOfNonAlpha) == '-') &&
          indexOfNonAlpha + 1 < prefixArray.length &&
          !prefixArray(indexOfNonAlpha + 1).isLetter)

        if (!continue || end) {
          continue = false
        } else {
          indexOfNonAlpha = indexOfNonAlpha + 1
        }
      } while (continue)

      val prefixWithoutNonAlpha =
        if (
          indexOfNonAlpha != 0 &&
          (indexOfNonAlpha + 1) < prefix.length
        )
          prefix.substring(0, indexOfNonAlpha + 1)
        else prefix
      if (prefixWithoutNonAlpha.isEmpty)
        filename
      else
        s"$prefixWithoutNonAlpha.*.$extension"
    }
  }
  def inferTableName(filename: String): String = {
    val LeadingAlnum = "^[_A-Za-z0-9]+".r
    val result = LeadingAlnum.findFirstIn(filename).getOrElse("")
    // we do not allow table name to start with anything else than a letter
    if (result.nonEmpty && !result(0).isLetter) "" else result
  }
}
