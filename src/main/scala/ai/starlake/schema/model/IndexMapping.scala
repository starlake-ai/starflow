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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

/** This attribute property let us know what statistics should be computed for this field when
  * analyze is active.
  *
  * @param value
  *   : DISCRETE or CONTINUOUS or TEXT or NONE
  */
@JsonSerialize(`using` = classOf[ToStringSerializer])
@JsonDeserialize(`using` = classOf[IndexMappingDeserializer])
sealed case class IndexMapping(value: String) {
  override def toString: String = value
}

object IndexMapping {

  def fromString(value: String): IndexMapping = {
    value.toUpperCase() match {
      case "text"    => IndexMapping.Text
      case "keyword" => IndexMapping.Keyword

      case "long"         => IndexMapping.Long
      case "integer"      => IndexMapping.Integer
      case "short"        => IndexMapping.Short
      case "byte"         => IndexMapping.Byte
      case "double"       => IndexMapping.Double
      case "float"        => IndexMapping.Float
      case "half_float"   => IndexMapping.HalfFloat
      case "scaled_float" => IndexMapping.ScaledFloat

      case "date"    => IndexMapping.Date
      case "boolean" => IndexMapping.Boolean

      case "binary" => IndexMapping.Binary

      case "integer_range" => IndexMapping.IntegerRange
      case "float_range"   => IndexMapping.FloatRange
      case "long_range"    => IndexMapping.LongRange
      case "double_range"  => IndexMapping.DoubleRange
      case "date_range"    => IndexMapping.DateRange

      case "geo_point"   => IndexMapping.GeoPoint
      case "geo_shape"   => IndexMapping.GeoShape
      case "ip"          => IndexMapping.Ip
      case "completion"  => IndexMapping.Completion
      case "token_count" => IndexMapping.TokenCount

      case "object" => IndexMapping.Object
      case "array"  => IndexMapping.Array
      case _ =>
        throw new Exception(
          s"Invalid value for index type: $value not in $indexMappings"
        )
    }
  }

  def fromType(primitiveType: PrimitiveType) = {
    primitiveType match {
      case PrimitiveType.string    => IndexMapping.Keyword
      case PrimitiveType.long      => IndexMapping.Long
      case PrimitiveType.short     => IndexMapping.Short
      case PrimitiveType.int       => IndexMapping.Integer
      case PrimitiveType.double    => IndexMapping.Double
      case PrimitiveType.boolean   => IndexMapping.Boolean
      case PrimitiveType.byte      => IndexMapping.Byte
      case PrimitiveType.date      => IndexMapping.Date
      case PrimitiveType.timestamp => IndexMapping.Date
      case PrimitiveType.datetime  => IndexMapping.Date
      case PrimitiveType.time      => IndexMapping.Keyword
      case PrimitiveType.binary    => IndexMapping.Binary
      case PrimitiveType.decimal   => IndexMapping.Long
      case PrimitiveType.struct    => IndexMapping.Object
      case _ =>
        throw new Exception(
          s"Invalid primitive type: $primitiveType not in ${PrimitiveType.primitiveTypes}"
        )
    }
  }

  object Text extends IndexMapping("text")

  object Keyword extends IndexMapping("keyword")

  object Long extends IndexMapping("long")

  object Integer extends IndexMapping("integer")

  object Short extends IndexMapping("short")

  object Byte extends IndexMapping("byte")

  object Double extends IndexMapping("double")

  object Float extends IndexMapping("float")

  object HalfFloat extends IndexMapping("half_float")

  object ScaledFloat extends IndexMapping("scaled_float")

  object Date extends IndexMapping("date")

  object DateRange extends IndexMapping("date_range")

  object Boolean extends IndexMapping("boolean")

  object Binary extends IndexMapping("binary")

  object IntegerRange extends IndexMapping("integer_range")

  object FloatRange extends IndexMapping("float_range")

  object LongRange extends IndexMapping("long_range")

  object DoubleRange extends IndexMapping("double_range")

  object GeoPoint extends IndexMapping("geo_point")

  object GeoShape extends IndexMapping("geo_shape")

  object Ip extends IndexMapping("ip")

  object Completion extends IndexMapping("completion")

  object TokenCount extends IndexMapping("token_count")

  object Object extends IndexMapping("object")

  object Array extends IndexMapping("array")

  val indexMappings: Set[IndexMapping] = Set(
    Text,
    Keyword,
    Long,
    Integer,
    Short,
    Byte,
    Double,
    Float,
    HalfFloat,
    ScaledFloat,
    Date,
    DateRange,
    Boolean,
    Binary,
    IntegerRange,
    FloatRange,
    LongRange,
    DoubleRange,
    GeoPoint,
    GeoShape,
    Ip,
    Completion,
    TokenCount
  )
}

class IndexMappingDeserializer extends JsonDeserializer[IndexMapping] {

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): IndexMapping = {
    val value = jp.readValueAs[String](classOf[String])
    IndexMapping.fromString(value)
  }
}
