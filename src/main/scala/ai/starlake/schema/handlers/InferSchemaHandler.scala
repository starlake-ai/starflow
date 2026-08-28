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

import ai.starlake.config.Settings
import ai.starlake.core.utils.NamingUtils
import ai.starlake.schema.exceptions.InvalidFieldNameException
import ai.starlake.schema.model.*
import ai.starlake.schema.model.Format.{JSON, JSON_FLAT}
import ai.starlake.utils.YamlSerde
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  collect_set,
  lit,
  max,
  reduce,
  regexp_like,
  transform,
  trim,
  udf,
  when
}
import org.apache.spark.sql.types.{
  ArrayType,
  ByteType,
  DataType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

import java.nio.charset.Charset
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import scala.util.Try

object DataTypesToInt extends Enumeration {
  type DataTypeToInt = Value
  // The id of the enum corresponds to the precedence. Higher int is higher precedence
  val NULL, BYTE, BOOLEAN, DATE, BASIC_ISO_DATE, ISO_OFFSET_DATE, ISO_DATE_TIME, ISO_ORDINAL_DATE,
    ISO_WEEK_DATE, RFC_1123_DATE_TIME, TIMESTAMP, SHORT, INT, LONG, DOUBLE, DECIMAL, STRING, STRUCT,
    VARIANT = Value

  def dataTypeToTypeInt(dataType: DataType) = PrimitiveType.from(dataType) match {
    case PrimitiveType.string    => DataTypesToInt.STRING.id
    case PrimitiveType.variant   => DataTypesToInt.VARIANT.id
    case PrimitiveType.long      => DataTypesToInt.LONG.id
    case PrimitiveType.int       => DataTypesToInt.INT.id
    case PrimitiveType.short     => DataTypesToInt.SHORT.id
    case PrimitiveType.double    => DataTypesToInt.DOUBLE.id
    case PrimitiveType.decimal   => DataTypesToInt.DECIMAL.id
    case PrimitiveType.boolean   => DataTypesToInt.BOOLEAN.id
    case PrimitiveType.byte      => DataTypesToInt.BYTE.id
    case PrimitiveType.struct    => DataTypesToInt.STRUCT.id
    case PrimitiveType.date      => DataTypesToInt.DATE.id
    case PrimitiveType.timestamp => DataTypesToInt.TIMESTAMP.id
  }

  def typeIntToTypeStr(primitiveTypeInt: Int): String = DataTypesToInt(primitiveTypeInt) match {
    case DataTypesToInt.STRING             => PrimitiveType.string.value
    case DataTypesToInt.VARIANT            => PrimitiveType.variant.value
    case DataTypesToInt.LONG               => PrimitiveType.long.value
    case DataTypesToInt.INT                => PrimitiveType.int.value
    case DataTypesToInt.SHORT              => PrimitiveType.short.value
    case DataTypesToInt.DOUBLE             => PrimitiveType.double.value
    case DataTypesToInt.DECIMAL            => PrimitiveType.decimal.value
    case DataTypesToInt.BOOLEAN            => PrimitiveType.boolean.value
    case DataTypesToInt.BYTE               => PrimitiveType.byte.value
    case DataTypesToInt.STRUCT             => PrimitiveType.struct.value
    case DataTypesToInt.DATE               => PrimitiveType.date.value
    case DataTypesToInt.TIMESTAMP          => PrimitiveType.timestamp.value
    case DataTypesToInt.ISO_DATE_TIME      => "iso_date_time"
    case DataTypesToInt.BASIC_ISO_DATE     => "basic_iso_date"
    case DataTypesToInt.ISO_OFFSET_DATE    => "iso_offset_date"
    case DataTypesToInt.ISO_ORDINAL_DATE   => "iso_ordinal_date"
    case DataTypesToInt.ISO_WEEK_DATE      => "iso_week_date"
    case DataTypesToInt.RFC_1123_DATE_TIME => "rfc_1123_date_time"
    case DataTypesToInt.NULL => PrimitiveType.string.value // null type default to string
  }

  def originalTypeIntToTypeStr(primitiveTypeInt: Int): String = DataTypesToInt(
    primitiveTypeInt
  ) match {
    case DataTypesToInt.STRING             => PrimitiveType.string.value
    case DataTypesToInt.VARIANT            => PrimitiveType.variant.value
    case DataTypesToInt.LONG               => PrimitiveType.long.value
    case DataTypesToInt.INT                => PrimitiveType.int.value
    case DataTypesToInt.SHORT              => PrimitiveType.short.value
    case DataTypesToInt.DOUBLE             => PrimitiveType.double.value
    case DataTypesToInt.DECIMAL            => PrimitiveType.decimal.value
    case DataTypesToInt.BOOLEAN            => PrimitiveType.boolean.value
    case DataTypesToInt.BYTE               => PrimitiveType.byte.value
    case DataTypesToInt.STRUCT             => PrimitiveType.struct.value
    case DataTypesToInt.DATE               => PrimitiveType.date.value
    case DataTypesToInt.TIMESTAMP          => PrimitiveType.timestamp.value
    case DataTypesToInt.ISO_DATE_TIME      => "iso_date_time"
    case DataTypesToInt.BASIC_ISO_DATE     => "basic_iso_date"
    case DataTypesToInt.ISO_OFFSET_DATE    => "iso_offset_date"
    case DataTypesToInt.ISO_ORDINAL_DATE   => "iso_ordinal_date"
    case DataTypesToInt.ISO_WEEK_DATE      => "iso_week_date"
    case DataTypesToInt.RFC_1123_DATE_TIME => "rfc_1123_date_time"
    case DataTypesToInt.NULL               => "null"
  }

  private val patterns: Seq[(Object, Int)] = Seq(
    "yyyy-MM-dd HH:mm:ss" -> DataTypesToInt.TIMESTAMP.id,
    // the data type "ISO_LOCAL_DATE" and ISO_DATE exist but will never be inferred since date have a higher precedence
    DateTimeFormatter.ISO_LOCAL_DATE -> DataTypesToInt.DATE.id,
    DateTimeFormatter.BASIC_ISO_DATE -> DataTypesToInt.BASIC_ISO_DATE.id,
    // ISO_OFFSET_DATE is preferred over ISO_DATE since the date without offset is already inferred from date.
    DateTimeFormatter.ISO_OFFSET_DATE -> DataTypesToInt.ISO_OFFSET_DATE.id,
    // ISO_DATE_TIME is preferred over ISO_ZONED_DATE_TIME, ISO_OFFSET_DATE_TIME, ISO_LOCAL_DATE_TIME and ISO_INSTANT since it covers their pattern
    DateTimeFormatter.ISO_DATE_TIME      -> DataTypesToInt.ISO_DATE_TIME.id,
    DateTimeFormatter.ISO_ORDINAL_DATE   -> DataTypesToInt.ISO_ORDINAL_DATE.id,
    DateTimeFormatter.ISO_WEEK_DATE      -> DataTypesToInt.ISO_WEEK_DATE.id,
    DateTimeFormatter.RFC_1123_DATE_TIME -> DataTypesToInt.RFC_1123_DATE_TIME.id
  )

  def guessTemporalType: UserDefinedFunction = udf((maybeTimestamp: String) => {
    Option(maybeTimestamp).flatMap { input =>
      patterns
        .collectFirst {
          case (pattern: String, typeId)
              if Try(DateTimeFormatter.ofPattern(pattern).parse(input)).isSuccess =>
            typeId
          case (formatter: DateTimeFormatter, typeId) if Try(formatter.parse(input)).isSuccess =>
            typeId
        }
    }
  })

  def coerceDataType(element1Type: Column, element2Type: Column): Column = {
    when(element1Type.isNull, element2Type)
      .when(element2Type.isNull, element1Type)
      .otherwise(
        coalesce(
          when(element1Type === lit(DataTypesToInt.NULL.id), element2Type)
            .when(element2Type === lit(DataTypesToInt.NULL.id), element1Type)
            .when(element1Type === element2Type, element1Type)
            .when(
              element1Type === lit(DataTypesToInt.SHORT.id),
              when(
                element2Type.isin(
                  DataTypesToInt.INT.id,
                  DataTypesToInt.LONG.id,
                  DataTypesToInt.DOUBLE.id,
                  DataTypesToInt.DECIMAL.id
                ),
                element2Type
              )
            )
            .when(
              element1Type === lit(DataTypesToInt.INT.id),
              when(element2Type.isin(DataTypesToInt.SHORT.id), element1Type)
                .when(
                  element2Type
                    .isin(
                      DataTypesToInt.LONG.id,
                      DataTypesToInt.DOUBLE.id,
                      DataTypesToInt.DECIMAL.id
                    ),
                  element2Type
                )
            )
            .when(
              element1Type === lit(DataTypesToInt.LONG.id),
              when(element2Type.isin(DataTypesToInt.SHORT.id, DataTypesToInt.INT.id), element1Type)
                .when(
                  element2Type.isin(DataTypesToInt.DOUBLE.id, DataTypesToInt.DECIMAL.id),
                  element2Type
                )
            )
            .when(
              element1Type === lit(DataTypesToInt.DOUBLE.id),
              when(
                element2Type
                  .isin(DataTypesToInt.SHORT.id, DataTypesToInt.INT.id, DataTypesToInt.LONG.id),
                element1Type
              )
                .when(element2Type.isin(DataTypesToInt.DECIMAL.id), element2Type)
            )
            .when(
              element1Type === lit(DataTypesToInt.DECIMAL.id),
              when(
                element2Type
                  .isin(DataTypesToInt.SHORT.id, DataTypesToInt.INT.id, DataTypesToInt.LONG.id),
                element1Type
              )
            ),
          lit(DataTypesToInt.STRING.id) // this means we can't coerce so we default to string
        )
      )
  }
}

object InferSchemaHandler extends LazyLogging {

  val identifierRegex = "^([a-zA-Z_][a-zA-Z\\d_:.-]*)$".r

  val sampleColumnSuffix = "_$SL_SAMPLE"

  def convertToValidXMLSchema(
    currentSchema: DataType
  ): DataType = {
    val result =
      currentSchema match {
        case st: StructType =>
          val updatedFields = st.fields.map { originalField =>
            val updatedField =
              originalField.copy(name = originalField.name.replaceAll("[:.-]", "_"))
            updatedField.dataType match {
              case st: StructType =>
                updatedField.copy(dataType = convertToValidXMLSchema(st))
              case dt: ArrayType =>
                updatedField.copy(dataType =
                  dt.copy(elementType = convertToValidXMLSchema(dt.elementType))
                )
              case _ => updatedField
            }
          }
          st.copy(fields = updatedFields)
        case dt: ArrayType =>
          dt.copy(elementType = convertToValidXMLSchema(dt.elementType))
        // if the datatype is a simple Attribute
        case simpleType =>
          simpleType
      }
    result
  }

  def adjustAttributes(schema: StructType, format: Format)(inputDF: DataFrame): DataFrame = {

    def isNumberStringLike(strColumn: Column) = {
      strColumn.startsWith(lit("0")).and(regexp_like(strColumn, lit("^0[0-9]+.*")))
    }

    // tuple1: relative transform and the final column name
    // tuple2: relative transform to get sample and the final column name which is suffixed with _$SL_SAMPLE
    def adjustAttributes(
      currentSchema: DataType,
      currentPath: String
    ): List[((Column => Column, String), (Column => Column, String))] = {
      currentSchema match {
        case st: StructType =>
          st.fields.flatMap { field =>
            adjustAttributes(
              field.dataType,
              if (currentPath.isEmpty) field.name else currentPath + "." + field.name
            ).map { case ((fAttribute, attributeColumnName), (fSample, sampleColumnName)) =>
              (
                (
                  (currentColumn: Column) => fAttribute(currentColumn.getField(field.name))
                ) -> attributeColumnName,
                (
                  (currentColumn: Column) => fSample(currentColumn.getField(field.name))
                ) -> sampleColumnName
              )
            }
          }.toList
        case dt: ArrayType =>
          dt.elementType match {
            case _: ArrayType =>
              throw new RuntimeException(
                s"Starflow doesn't support array of array. Rejecting field path $currentPath"
              )
            case _ =>
              adjustAttributes(dt.elementType, currentPath + "[]").map {
                case ((fAttribute, attributeColumnName), (fSample, sampleColumnName)) =>
                  (
                    ((arrayColumn: Column) => {
                      reduce(
                        transform(arrayColumn, elementColumn => fAttribute(elementColumn)),
                        lit(DataTypesToInt.NULL.id),
                        (finalType, elementColumn) => {
                          DataTypesToInt.coerceDataType(finalType, elementColumn)
                        }
                      )
                    }) -> attributeColumnName,
                    ((arrayColumn: Column) => {
                      reduce(
                        arrayColumn,
                        lit(null).cast(StringType),
                        (finalSample, elementColumn) => {
                          when(finalSample.isNotNull, finalSample)
                            .when(trim(fSample(elementColumn)).isNotNull, fSample(elementColumn))
                        }
                      )
                    }) -> sampleColumnName
                  )
              }
          }
        case StringType =>
          List(
            (
              ((strColumn: Column) => {
                when(strColumn.isNull, lit(DataTypesToInt.NULL.id))
                  .when(
                    DataTypesToInt.guessTemporalType(strColumn).isNotNull,
                    DataTypesToInt.guessTemporalType(strColumn)
                  )
                  .when(
                    isNumberStringLike(strColumn),
                    lit(DataTypesToInt.STRING.id)
                  )
                  .when(
                    strColumn.cast(LongType).isNotNull,
                    when(strColumn.contains("."), DataTypesToInt.DOUBLE.id)
                      .otherwise(DataTypesToInt.LONG.id)
                  )
                  .otherwise(lit(DataTypesToInt.STRING.id))
              }) -> currentPath,
              (
                (strColumn: Column) => strColumn.cast(StringType)
              ) -> (currentPath + sampleColumnSuffix)
            )
          )
        case TimestampType
            if Set(
              Format.DSV,
              Format.POSITION,
              Format.JSON_FLAT
            ).contains(format) =>
          List(
            (
              ((currentColumn: Column) => {
                val strColumn = currentColumn.cast(StringType)
                when(strColumn.isNull, lit(DataTypesToInt.NULL.id))
                  .when(
                    DataTypesToInt.guessTemporalType(strColumn).isNotNull,
                    DataTypesToInt.guessTemporalType(strColumn)
                  )
                  .otherwise(lit(DataTypesToInt.STRING.id))
              }) -> currentPath,
              (
                (currentColumn: Column) => currentColumn.cast(StringType)
              ) -> (currentPath + sampleColumnSuffix)
            )
          )
        case ByteType | IntegerType | LongType | ShortType | FloatType | DoubleType |
            _: DecimalType =>
          List(
            (
              ((currentColumn: Column) => {
                val strColumn = currentColumn.cast(StringType)
                when(strColumn.isNull, lit(DataTypesToInt.NULL.id))
                  .when(
                    isNumberStringLike(strColumn),
                    lit(DataTypesToInt.STRING.id)
                  )
                  .otherwise(lit(DataTypesToInt.dataTypeToTypeInt(currentSchema)))
              }) -> currentPath,
              (
                (currentColumn: Column) => currentColumn.cast(StringType)
              ) -> (currentPath + sampleColumnSuffix)
            )
          )
        case _ =>
          List(
            (
              ((valueColumn: Column) => {
                when(valueColumn.isNull, lit(DataTypesToInt.NULL.id))
                  .otherwise(lit(DataTypesToInt.dataTypeToTypeInt(currentSchema)))
              }) -> currentPath,
              (
                (currentColumn: Column) => currentColumn.cast(StringType)
              ) -> (currentPath + sampleColumnSuffix)
            )
          )
      }
    }
    val (projectionsList, columnNames) = (for {
      field <- schema.fields.toList
      ((fAttribute, attributeColumnName), (fSample, sampleColumnName)) <- adjustAttributes(
        field.dataType,
        field.name
      )
    } yield {
      (
        List(
          fAttribute(col(field.name)).as(attributeColumnName),
          fSample(col(field.name)).as(sampleColumnName)
        ),
        (attributeColumnName, sampleColumnName)
      )
    }).unzip

    val lineWithColumnTypesDF = inputDF.select(projectionsList.flatten: _*)
    val (attributeTypeColumnNames, sampleColumnNames) = columnNames.unzip
    val reduceAttributeColumnTypes = attributeTypeColumnNames.map { column =>
      reduce(
        collect_set(lineWithColumnTypesDF("`" + column + "`")),
        lit(DataTypesToInt.NULL.id),
        (finalType, elementColumn) => {
          DataTypesToInt.coerceDataType(finalType, elementColumn)
        }
      ).as(column)
    }
    val maxSampleColumn = sampleColumnNames.map { column =>
      max(lineWithColumnTypesDF("`" + column + "`")).as(column)
    }
    logger.whenDebugEnabled {
      lineWithColumnTypesDF.show(truncate = false)
      DataTypesToInt.values.foreach { v =>
        println(s"${v.id} -> ${v.toString}")
      }
    }
    val aggregations = reduceAttributeColumnTypes ++ maxSampleColumn
    lineWithColumnTypesDF.agg(
      aggregations.head,
      aggregations.tail: _*
    )
  }

  /** * Traverses the schema and returns a list of attributes.
    *
    * @param schema
    *   Schema so that we find all Attributes
    * @return
    *   List of Attributes
    */
  def createAttributes(
    adjustedAttributes: Map[String, String], // field path, type
    schema: StructType,
    forcePattern: Boolean = true
  ): List[TableAttribute] = {

    def createAttribute(
      currentSchema: DataType,
      container: StructField,
      fieldPath: String,
      forcePattern: Boolean = true
    ): TableAttribute = {
      val result =
        currentSchema match {
          case st: StructType =>
            val rename = NamingUtils.normalizeAttributeName(container.name, toSnakeCase = false)
            val renamedField = if (rename != container.name) Some(rename) else None
            try {
              val attributes = st.map { field =>
                createAttribute(
                  st(field.name).dataType,
                  field,
                  if (fieldPath.isEmpty) field.name else fieldPath + "." + field.name,
                  forcePattern
                )
              }.toList

              TableAttribute(
                name = container.name,
                `type` = st.typeName,
                rename = renamedField,
                required = if (!container.nullable) Some(true) else None,
                array = Some(false),
                attributes = attributes,
                sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
              )
            } catch {
              case e: InvalidFieldNameException =>
                logger.error(e.getMessage, e)
                TableAttribute(
                  name = container.name,
                  `type` = PrimitiveType.variant.value,
                  rename = renamedField,
                  required = if (!container.nullable) Some(true) else None,
                  array = Some(false),
                  attributes = Nil,
                  sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
                )
            }
          case dt: ArrayType =>
            dt.elementType match {
              case _: ArrayType =>
                throw new RuntimeException(
                  s"Starflow doesn't support array of array. Rejecting field path $fieldPath"
                )
              case _ =>
                val stAttributes = createAttribute(
                  dt.elementType,
                  container,
                  fieldPath + "[]",
                  forcePattern
                )
                stAttributes.copy(
                  array = Some(true),
                  required = if (!container.nullable) Some(true) else None,
                  sample = None // currentLines.map(Option(_)).headOption.flatten.map(_.toString)
                )
            }
          // if the datatype is a simple Attribute
          case _ =>
            val cellType =
              adjustedAttributes.getOrElse(fieldPath, PrimitiveType.from(currentSchema).value)
            val rename = NamingUtils.normalizeAttributeName(container.name, toSnakeCase = false)
            val renamedField = if (rename != container.name) Some(rename) else None
            TableAttribute(
              name = container.name,
              `type` = cellType,
              rename = renamedField,
              array = Some(false),
              required = if (!container.nullable) Some(true) else None,
              sample = adjustedAttributes.get(fieldPath + sampleColumnSuffix).flatMap(Option(_))
            )
        }

      if (
        !forcePattern || identifierRegex.pattern
          .matcher(result.rename.getOrElse(result.name))
          .matches
      )
        result
      else
        throw new InvalidFieldNameException(result.name)
    }
    val attributes = createAttribute(
      schema,
      StructField("_ROOT_", StructType(Nil)),
      "",
      forcePattern
    ).attributes
    if (attributes.isEmpty) {
      throw new RuntimeException("Inferred schema is empty, please check logs")
    } else {
      attributes
    }
  }

  /** * builds the Metadata case class. check case class metadata for attribute definition
    *
    * @param format
    *   : DSV by default
    * @param array
    *   : Is a json stored as a single object array ? false by default
    * @param withHeader
    *   : does the dataset has a header ? true bu default
    * @param separator
    *   : the column separator, ';' by default
    * @return
    */

  def createMetaData(
    format: Format,
    encoding: Charset,
    array: Option[Boolean] = None,
    withHeader: Option[Boolean] = None,
    separator: Option[String] = None,
    options: Option[Map[String, String]] = None
  ): Metadata = {
    val finalFormat = if (format == JSON_FLAT) JSON else format
    Metadata(
      format = Some(finalFormat),
      encoding = Some(encoding.name()),
      multiline = None,
      array = if (array.contains(true)) array else None,
      withHeader = withHeader,
      separator = separator,
      options = options
    )
  }

  /** * builds the Schema case class
    *
    * @param name
    *   : Schema name, must be unique in the domain. Will become the hive table name
    * @param pattern
    *   : filename pattern to which this schema must be applied
    * @param attributes
    *   : datasets columns
    * @param metadata
    *   : Dataset metadata
    * @return
    */

  def createSchema(
    name: String,
    pattern: Pattern,
    comment: Option[String],
    attributes: List[TableAttribute],
    metadata: Option[Metadata],
    sample: Option[String]
  ): SchemaInfo =
    SchemaInfo(
      name = name,
      pattern = pattern,
      attributes = attributes,
      metadata = metadata,
      comment = comment,
      sample = sample
    )

  /** * Builds the Domain case class
    *
    * @param name
    *   : Domain name
    * @param directory
    *   : Folder on the local filesystem where incomping files are stored. This folder will be
    *   scanned regurlaly to move the dataset to the cluster
    * @param metadata
    *   : Default Schema meta data.
    * @param schemas
    *   : List of schema for each dataset in this domain
    * @return
    */

  def createDomain(
    name: String,
    metadata: Option[Metadata] = None,
    schemas: List[SchemaInfo] = Nil
  ): DomainInfo =
    DomainInfo(name = name, metadata = metadata, tables = schemas)

  /** * Generates the YAML file using the domain object and a savepath
    *
    * @param domain
    *   Domain case class
    * @param savePath
    *   path to save files.
    */
  def generateYaml(domain: DomainInfo, saveDir: String, clean: Boolean)(implicit
    settings: Settings
  ): Try[Path] = Try {
    implicit val storageHandler: StorageHandler = settings.storageHandler()

    /** load: metadata: directory: "{{incoming_path}}"
      */
    val domainFolder = new Path(saveDir, domain.name)
    storageHandler.mkdirs(domainFolder)
    val configPath = new Path(domainFolder, "_config.sl.yml")
    if (!storageHandler.exists(configPath)) {
      YamlSerde.serializeToPath(configPath, domain.copy(tables = Nil))
    }
    val table = domain.tables.head
    val tablePath = new Path(domainFolder, s"${table.name}.sl.yml")
    if (storageHandler.exists(tablePath) && !clean) {
      throw new Exception(
        s"Could not write table ${domain.tables.head.name} already defined in file $tablePath"
      )
    }
    YamlSerde.serializeToPath(tablePath, table)
    tablePath
  }
}
