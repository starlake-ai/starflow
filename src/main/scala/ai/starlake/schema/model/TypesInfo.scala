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

import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.schema.model.PrimitiveType.{boolean, date, timestamp}
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.StructField

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import scala.collection.mutable
import scala.util.Try
import ai.starlake.schema.model.Severity._

/** Semantic Type
  *
  * @param name
  *   : Type name
  * @param pattern
  *   : Pattern use to check that the input data matches the pattern
  * @param primitiveType
  *   : Spark Column Type of the attribute
  */
case class Type(
  name: String,
  pattern: String,
  primitiveType: PrimitiveType = PrimitiveType.string,
  zone: Option[String] = None,
  sample: Option[String] = None,
  comment: Option[String] = None,
  ddlMapping: Option[Map[String, String]] = None
) {
  def this() = this("", "") // Should never be called. Here for Jackson deserialization only

  def getDdlMappingForDB(db: String): Option[String] = {
    ddlMapping.flatMap(_.get(db.toLowerCase()))
  }
  @JsonIgnore
  val isString: Boolean = name == "string"

  @JsonIgnore
  def getIndexMapping(): IndexMapping = {
    require(PrimitiveType.primitiveTypes.contains(primitiveType))
    IndexMapping.fromType(primitiveType)
  }

  @JsonIgnore
  val textPattern: Option[Pattern] = {
    name match {
      case "string" => None
      case _ =>
        primitiveType match {
          case PrimitiveType.struct | PrimitiveType.date | PrimitiveType.timestamp |
              PrimitiveType.boolean =>
            None
          case _ =>
            Some(Pattern.compile(pattern, Pattern.MULTILINE))
        }
    }
  }

  @JsonIgnore
  val booleanPattern: Option[(Pattern, Pattern)] = {
    name match {
      case "string" => None
      case _ =>
        primitiveType match {
          case PrimitiveType.struct | PrimitiveType.date | PrimitiveType.timestamp => None
          case PrimitiveType.boolean =>
            val tf = pattern.split("<-TF->")
            Some(
              (
                Pattern
                  .compile(tf(0), Pattern.MULTILINE),
                Pattern
                  .compile(tf(1), Pattern.MULTILINE)
              )
            )
          case _ =>
            None
        }
    }
  }

  def matches(value: String): Boolean = {
    if (isString) // optimization for strings
      true
    else {
      primitiveType match {
        case PrimitiveType.struct => true
        case PrimitiveType.date =>
          Try(date.fromString(value, pattern, zone)).isSuccess
        case PrimitiveType.timestamp =>
          Try(timestamp.fromString(value, pattern, zone)).isSuccess
        case PrimitiveType.boolean =>
          // We can get the pattern safely since checkValidity has been called by now
          booleanPattern match {
            case Some((truePattern, falsePattern)) =>
              boolean.matches(value, truePattern, falsePattern)
            case _ => false
          }
        case _ =>
          // We can get the pattern safely since checkValidity has been called by now
          textPattern match {
            case Some(textPattern) =>
              textPattern.matcher(value).matches()
            case _ => false
          }
      }
    }
  }

  def sparkValue(value: String): Any = {
    primitiveType.fromString(value, pattern, zone)
  }

  def sparkType(fieldName: String, nullable: Boolean, comment: Option[String]): StructField = {
    StructField(fieldName, primitiveType.sparkType(zone), nullable)
      .withComment(comment.getOrElse(""))
  }

  def checkValidity(): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty

    val patternIsValid = Try {
      primitiveType match {
        case PrimitiveType.struct => // ignore
        case PrimitiveType.date =>
          new SimpleDateFormat(pattern)
        case PrimitiveType.timestamp =>
          pattern match {
            case "epoch_second" | "epoch_milli"                                  =>
            case _ if PrimitiveType.dateFormatters.keys.toList.contains(pattern) =>
            case _ =>
              DateTimeFormatter.ofPattern(pattern)
          }
        case PrimitiveType.boolean =>
          val tf = pattern.split("<-TF->")
          require(tf.size == 2)
          Pattern.compile(tf(0), Pattern.MULTILINE)
          Pattern.compile(tf(1), Pattern.MULTILINE)
        case PrimitiveType.decimal =>
          zone match {
            case None =>
            case Some(zone) =>
              val precisionScale = zone.split(",")
              require(precisionScale.length == 2)
              precisionScale(0).toInt
              precisionScale(0).toInt
          }
        case _ =>
          Pattern.compile(pattern)
      }
    }
    if (patternIsValid.isFailure)
      errorList += ValidationMessage(
        Error,
        "Type.pattern",
        s"Type $name - invalid Pattern $pattern"
      )
    val ok = sample.forall { sample =>
      this.matches(sample)
    }
    if (!ok)
      errorList += ValidationMessage(
        Error,
        "Type.sample",
        s"Type $name - sample $sample does not match pattern $pattern"
      )

    val notLowerCaseOnlyMapping =
      this.ddlMapping
        .getOrElse(Map.empty)
        .keys
        .find(mapping => !mapping.equals(mapping.toLowerCase()))

    notLowerCaseOnlyMapping match {
      case Some(mapping) =>
        errorList += ValidationMessage(
          Error,
          "Type.ddlMapping",
          s"Type $name - ddlMapping key $mapping must be lowercase"
        )
      case None =>
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

}

/** List of globally defined types
  *
  * @param types
  *   : Type list
  */
case class TypesInfo(version: Int, types: List[Type]) {
  def this() =
    this(latestSchemaVersion, Nil) // Should never be called. Here for Jackson deserialization only

  def checkValidity(): Either[List[ValidationMessage], Boolean] = {
    val typeNames = types.map(_.name)
    val dup: Either[List[ValidationMessage], Boolean] =
      Utils.duplicates(
        "Type name",
        typeNames,
        "%s is defined %d times. A type can only be defined once."
      )
    val result = Utils.combine(dup, types.map(_.checkValidity()): _*)
    result
  }

  def getTypeForSqlType(db: String, sqlType: String): Option[Type] =
    types.find(t => t.getDdlMappingForDB(db).contains(sqlType))

  def getApproximateTypeForSqlType(db: String, sqlType: String): Option[Type] = {
    val exactMatch = getTypeForSqlType(db, sqlType)
    exactMatch.orElse {
      // Try to find an approximate match by prefix ignoring precision and scale
      types.find(t =>
        t.getDdlMappingForDB(db).exists { ddlType =>
          ddlType.toLowerCase().startsWith(sqlType)
        }
      )
    }
  }
}
