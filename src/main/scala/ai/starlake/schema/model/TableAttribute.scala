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

package ai.starlake.schema.model

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Severity.*
import ai.starlake.transpiler.diff.{Attribute as DiffAttribute, AttributeStatus}
import ai.starlake.utils.DataTypeEx.*
import ai.starlake.utils.Utils
import ai.starlake.utils.conversion.BigQueryType
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.*

import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name
  *   : Attribute name as defined in the source dataset and as received in the file
  * @param `type`
  *   : semantic type of the attribute.
  * @param array
  *   : Is it an array ?
  * @param required
  *   : Should this attribute always be present in the source
  * @param privacy
  *   : Should this attribute be applied a privacy transformation at ingestion time
  * @param comment
  *   : free text for attribute description
  * @param rename
  *   : If present, the attribute is renamed with this name
  * @param metricType
  *   : If present, what kind of stat should be computed for this field
  * @param attributes
  *   : List of sub-attributes (valid for JSON and XML files only)
  * @param position
  *   : Valid only when file format is POSITION
  * @param default
  *   : Default value for this attribute when it is not present.
  * @param tags
  *   : Tags associated with this attribute
  * @param trim
  *   : Should we trim the attribute value ?
  * @param script
  *   : Scripted field : SQL request on renamed column
  */

case class TableAttribute(
  name: String,
  `type`: String = "string",
  array: Option[Boolean] = None,
  required: Option[Boolean] = None,
  privacy: Option[TransformInput] = None,
  comment: Option[String] = None,
  rename: Option[String] = None,
  metricType: Option[MetricType] = None,
  attributes: List[TableAttribute] = Nil,
  position: Option[Position] = None,
  default: Option[String] = None,
  tags: Set[String] = Set.empty,
  trim: Option[Trim] = None,
  script: Option[String] = None,
  foreignKey: Option[String] = None, // [domain.]table.attribute
  ignore: Option[Boolean] = None,
  accessPolicy: Option[String] = None,
  sample: Option[String] = None
) extends Named {
  def this() = this("") // Should never be called. Here for Jackson deserialization only

  def asMap(): Map[String, Any] = {
    Map(
      "name"         -> name,
      "type"         -> `type`,
      "array"        -> resolveArray().toString,
      "required"     -> resolveRequired().toString,
      "privacy"      -> resolvePrivacy(),
      "comment"      -> comment.orNull,
      "rename"       -> getFinalName(),
      "attributes"   -> attributes.map(_.asMap()),
      "position"     -> position.map(_.asMap()).orNull,
      "default"      -> default.orNull,
      "tags"         -> tags.toList,
      "trim"         -> trim.orNull,
      "script"       -> resolveScript(),
      "foreignKey"   -> foreignKey.orNull,
      "ignore"       -> ignore.map(_.toString).orNull,
      "accessPolicy" -> accessPolicy.orNull,
      "sample"       -> sample.orNull
    )
  }

  override def toString: String =
    // we pretend the "settings" field does not exist
    s"TableAttribute(${name},${`type`},${resolveArray()},${resolveRequired()},${resolvePrivacy()},${comment},${rename},${metricType},${attributes},${position},${default},${tags})"

  def `type`(schemaHandler: SchemaHandler): Option[Type] =
    schemaHandler.types().find(_.name == `type`)

  def primitiveType(schemaHandler: SchemaHandler): Option[PrimitiveType] =
    schemaHandler.types().find(_.name == `type`).map(_.primitiveType)

  /** Spark Type if this attribute is a primitive type of array of primitive type
    *
    * @return
    *   Primitive type if attribute is a leaf node or array of primitive type, None otherwise
    */
  def primitiveSparkType(schemaHandler: SchemaHandler): DataType = {
    `type`(schemaHandler)
      .map { tpe =>
        if (resolveArray())
          ArrayType(tpe.primitiveType.sparkType(tpe.zone), !resolveRequired())
        else
          tpe.primitiveType.sparkType(tpe.zone)
      }
      .getOrElse(PrimitiveType.struct.sparkType(None))
  }

  def toDiffAttribute(): DiffAttribute = {
    new DiffAttribute(getFinalName(), `type`, this.resolveArray(), null, AttributeStatus.UNCHANGED)
  }
  @JsonIgnore
  def isNestedOrRepeatedField(): Boolean = {
    attributes.nonEmpty || array.getOrElse(false)
  }

  /** Check attribute validity An attribute is valid if :
    *   - Its name is a valid identifier
    *   - its type is defined
    *   - When a privacy function is defined its primitive type is a string
    *
    * @return
    *   true if attribute is valid
    */
  def checkValidity(
    schemaHandler: SchemaHandler,
    domainName: String,
    schema: SchemaInfo
  ): Either[List[ValidationMessage], Boolean] = {
    val tableName = schema.getName()
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    if (`type` == null)
      errorList +=
        ValidationMessage(Error, "Attribute.type", s"$this : unspecified type")

    val colNamePattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{1,767}")
    // We do not check if the name is valid. We only need to check that once renamed, the name is valid.
    if (!rename.forall(colNamePattern.matcher(_).matches()))
      errorList += ValidationMessage(
        Error,
        s"Attribute.rename in table $domainName.$tableName",
        s"rename: renamed attribute with renamed name '$rename' should respect the pattern ${colNamePattern.pattern()}"
      )

    val primitiveType = `type`(schemaHandler).map(_.primitiveType)

    primitiveType match {
      case Some(tpe) =>
        if (tpe == PrimitiveType.struct && attributes.isEmpty)
          errorList += ValidationMessage(
            Error,
            s"Attribute.primitiveType in table $domainName.$tableName",
            s"Attribute $this : Struct types must have at least one attribute."
          )
        if (tpe != PrimitiveType.struct && attributes.nonEmpty)
          errorList += ValidationMessage(
            Error,
            s"Attribute.primitiveType in table $domainName.$tableName",
            s" $this : Simple attributes cannot have sub-attributes"
          )
      case None if attributes.isEmpty =>
        errorList +=
          ValidationMessage(
            Error,
            s"Attribute.primitiveType in table $domainName.$tableName",
            s"Invalid Type ${`type`}"
          )
      case _ => // good boy
    }

    default.foreach { default =>
      if (resolveRequired())
        errorList += ValidationMessage(
          Error,
          s"Attribute.default in table $domainName.$tableName",
          s"attribute with name $name - default value valid for optional fields only"
        )
      primitiveType.foreach { primitiveType =>
        if (primitiveType == PrimitiveType.struct)
          errorList += ValidationMessage(
            Error,
            s"Attribute.default in table $domainName.$tableName",
            s"attribute with name $name - default value not valid for struct type fields"
          )
        `type`(schemaHandler).foreach { someTpe =>
          Try(someTpe.sparkValue(default)) match {
            case Success(_) =>
            case Failure(e) =>
              errorList += ValidationMessage(
                Error,
                s"Attribute.default in table $domainName.$tableName",
                s"attribute with name $name - Invalid default value for this attribute type ${e.getMessage()}"
              )
          }
        }
      }
      array.foreach { isArray =>
        if (isArray)
          errorList += ValidationMessage(
            Error,
            s"Attribute.array in table $domainName.$tableName",
            s"attribute with name $name: default value not valid for array fields"
          )
      }
    }

    (script, resolveRequired()) match {
      case (Some(_), true) =>
        ValidationMessage(
          Warning,
          s"Attribute.script in table $domainName.$tableName",
          s"script: Attribute $name : Scripted attributed cannot be required. It will be forced to optional"
        )
      case (_, _) =>
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def samePrimitiveType(other: TableAttribute)(implicit schemaHandler: SchemaHandler): Boolean = {
    this.primitiveType(schemaHandler).map(_.value) == other
      .primitiveType(schemaHandler)
      .map(_.value)
  }

  /** Go get recursively the Spark tree type of this object
    *
    * @return
    *   Spark type of this attribute
    */
  def sparkType(
    schemaHandler: SchemaHandler,
    structFieldModifier: (TableAttribute, StructField) => StructField = (_, sf) => sf
  ): DataType = {
    def buildStruct(): List[StructField] = {
      if (attributes.isEmpty)
        throw new Exception(
          s"Attribute `$name` of type ${`type`} is considered as struct but doesn't have any attributes. Please check the types you defined or add attributes to it."
        )
      val fields = attributes.map { attr =>
        val structField =
          structFieldModifier(
            attr,
            StructField(
              attr.name,
              attr.sparkType(schemaHandler, structFieldModifier),
              !attr.resolveRequired()
            )
          )
        attr.comment.map(structField.withComment).getOrElse(structField)
      }
      fields
    }

    val tpe = primitiveSparkType(schemaHandler)
    tpe match {
      case ArrayType(s: StructType, b: Boolean) =>
        val fields = buildStruct()
        ArrayType(StructType(fields))
      case _: StructType =>
        val fields = buildStruct()
        StructType(fields)
      case simpleType => simpleType
    }
  }

  def ddlMapping(
    isPrimaryKey: Boolean,
    datawarehouse: String,
    schemaHandler: SchemaHandler
  ): DDLField = {
    attributes match {
      // TODO: Support NoSQL Documents
      case attr :: tail =>
        DDLNode(
          this.getFinalName(),
          attributes.map(_.ddlMapping(false, datawarehouse, schemaHandler)),
          resolveRequired(),
          resolveArray(),
          comment,
          Utils.labels(tags)
        )
      case Nil =>
        val tpe = `type`(schemaHandler)
        tpe.map { tpe =>
          val ddlMapping = tpe.ddlMapping.flatMap { mapping => mapping.get(datawarehouse) }
          ddlMapping match {
            case None =>
              val sameTypes = schemaHandler.types().filter(_.primitiveType == tpe.primitiveType)
              // The built-in types of the newer primitive types come after the project types: the
              // first type of the same primitive type with a mapping
              val newPrimitiveTypes: Set[PrimitiveType] =
                Set(PrimitiveType.datetime, PrimitiveType.time, PrimitiveType.binary)
              // A type of the parameters of a column, which extract-bq-schema may add to the
              // project, is not a default type
              val fallbackType =
                if (newPrimitiveTypes.contains(tpe.primitiveType)) {
                  val mapped = sameTypes.filter(_.ddlMapping.exists(_.contains(datawarehouse)))
                  mapped
                    .find(sameType =>
                      !sameType
                        .getDdlMappingForDB("bigquery")
                        .flatMap(BigQueryType.parse)
                        .exists(
                          _.hasParameters
                        )
                    )
                    .orElse(mapped.headOption)
                } else sameTypes.headOption
              if (PrimitiveType.decimal.isWide(tpe))
                fallbackType.foreach { defaultType =>
                  logger.warn(
                    s"The decimal type ${tpe.name} of $name is wider than ${PrimitiveType.decimal.maxSparkPrecision} digits and has no mapping for $datawarehouse: its column takes the one of ${defaultType.name}, which may not hold its values"
                  )
                }
              fallbackType match {
                case None =>
                  throw new Exception(
                    s"No mapping found for ${tpe.name} with primitive type " +
                    s"${tpe.primitiveType} for datawarehouse $datawarehouse"
                  )
                case Some(defaultType) =>
                  DDLLeaf(
                    this.getFinalName(),
                    defaultType.ddlMapping
                      .flatMap { mapping => mapping.get(datawarehouse) }
                      .getOrElse(
                        throw new Exception(
                          s"No mapping found for ${tpe.name} with primitive type " +
                          s"${tpe.primitiveType} for datawarehouse $datawarehouse"
                        )
                      ),
                    this.resolveRequired(),
                    this.comment,
                    isPrimaryKey,
                    Utils.labels(tags)
                  )

              }
            case Some(mapping) =>
              DDLLeaf(
                this.getFinalName(),
                mapping,
                this.resolveRequired(),
                this.comment,
                isPrimaryKey,
                Utils.labels(tags)
              )
          }
        } getOrElse (throw new Exception(s"Unknown type ${`type`}"))
    }
  }

  def indexMapping(schemaHandler: SchemaHandler): String = {
    attributes match {
      case attr :: head =>
        s"""
           |"$name": {
           |  "properties" : {
           |  ${attributes.map(_.indexMapping(schemaHandler)).mkString(",")}
           |  }
           |}""".stripMargin

      case Nil =>
        `type`(schemaHandler).map { tpe =>
          val typeMapping = tpe.getIndexMapping().toString
          tpe.primitiveType match {
            case PrimitiveType.date =>
              //  "format" : "${tpe.pattern}"
              s"""
                 |"$name": {
                 |  "type": "$typeMapping"
                 |}""".stripMargin
            case PrimitiveType.timestamp | PrimitiveType.datetime =>
              val format = tpe.pattern match {
                case "epoch_milli"  => Some("epoch_millis")
                case "epoch_second" => Some("epoch_second")
                case x if PrimitiveType.dateFormatters.keys.exists(_ == x) =>
                  Some("date") // ISO formatters are now supported by ES
                case y => None // unknown to ES
              }
              format match {
                case Some(_) =>
                  // "format" : "$fmt"
                  s"""
                     |"$name": {
                     |"type": "$typeMapping"
                     |}""".stripMargin
                case None => // Not Supported date format for ES TODO : needs to be implemented
                  s"""
                     |"$name": {
                     |"type": "keyword"
                     |}""".stripMargin
              }

            case _ =>
              s"""
                 |"$name": {
                 |  "type": "$typeMapping"
                 |}""".stripMargin
          }
        } getOrElse (throw new Exception("Cannot map unknown type"))
    }
  }

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  def getFinalName(): String = rename.getOrElse(name)

  def resolveIgnore(): Boolean = ignore.getOrElse(false)

  def resolvePrivacy(): TransformInput = privacy.getOrElse(TransformInput.None)

  def resolveArray(): Boolean = array.getOrElse(false)

  def resolveRequired(): Boolean = required.getOrElse(false)

  def resolveScript(): String = {
    val result =
      script.map(_.trim).map { script =>
        if (script.toUpperCase().startsWith("SQL:")) script.substring("SQL:".length) else script
      }
    result.getOrElse("")
  }

  @JsonIgnore
  val transform: Option[String] = privacy.filter(_.sql).map(_.value)

  @JsonIgnore
  def getMetricType(schemaHandler: SchemaHandler): MetricType = {
    val sparkType = `type`(schemaHandler).map(tpe => tpe.primitiveType.sparkType(tpe.zone))
    logger.info(s"Attribute Metric ==> $name, $metricType, $sparkType")
    (sparkType, metricType) match {
      case (Some(sparkType), Some(MetricType.DISCRETE)) =>
        if (sparkType.isOfValidDiscreteType()) {
          MetricType.DISCRETE
        } else
          MetricType.NONE
      case (Some(sparkType), Some(MetricType.CONTINUOUS)) =>
        if (sparkType.isOfValidContinuousType()) {
          MetricType.CONTINUOUS
        } else
          MetricType.NONE
      case (_, _) =>
        MetricType.NONE
    }
  }

  def compare(other: Metadata): ListDiff[Named] =
    AnyRefDiff.diffAnyRef(name, this, other)

  def containsArrayOfRecords(): Boolean = {
    val isArrayOfRecords = this.array.getOrElse(false) && this.`type` == "struct"
    if (isArrayOfRecords) {
      true
    } else {
      attributes.exists(_.containsArrayOfRecords())
    }
  }

  def containsVariant(): Boolean = {
    val isVariant = this.`type` == PrimitiveType.variant.value
    if (isVariant) {
      true
    } else {
      attributes.exists(_.containsVariant())
    }
  }

  def deepForeignKey(): Option[String] = {
    this.foreignKey match {
      case Some(_) => this.foreignKey
      case None =>
        attributes.flatMap(_.deepForeignKey()).headOption
    }
  }
}

object TableAttribute {
  def apply(sparkField: StructField): TableAttribute = {
    val sparkType = sparkField.dataType
    val fieldName = sparkField.name
    val required = if (!sparkField.nullable) Some(true) else None
    val isArray = sparkType.isInstanceOf[ArrayType]
    sparkType match {
      case st: StructType =>
        val subFields = st.fields.map(field => apply(field))
        new TableAttribute(
          fieldName,
          PrimitiveType.struct.toString,
          Some(isArray),
          required,
          attributes = subFields.toList
        )
      case at: ArrayType =>
        at.elementType match {
          case _: ArrayType =>
            throw new RuntimeException("Don't support array of array")
          case nestedSt: StructType =>
            val subFields = nestedSt.fields.map(field => apply(field))
            new TableAttribute(
              fieldName,
              PrimitiveType.struct.toString,
              Some(isArray),
              required,
              attributes = subFields.toList
            )
          case nestedDt =>
            val tpe = PrimitiveType.from(nestedDt)
            new TableAttribute(
              fieldName,
              tpe.toString,
              Some(isArray),
              required
            )
        }
      case _ =>
        val tpe = PrimitiveType.from(sparkType)
        new TableAttribute(fieldName, tpe.toString, Some(isArray), required)
    }
  }

  // Merge operations extracted to AttributeMerger.scala — delegated for backward compatibility
  def mergeAll(
    refAttrs: List[TableAttribute],
    sourceAttrs: List[TableAttribute],
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit schemaHandler: SchemaHandler): List[TableAttribute] =
    AttributeMerger.mergeAll(refAttrs, sourceAttrs, attributeMergeStrategy)
}

object Attributes {
  def from(structType: StructType): List[TableAttribute] = {
    structType.fields.map(field => TableAttribute(field)).toList
  }
}
