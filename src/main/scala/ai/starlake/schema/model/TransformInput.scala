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

import ai.starlake.utils.TransformEngine
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.functions.udf

import scala.util.Try

/** How (the attribute should be transformed at ingestion time ?
  *
  * @param value
  *   algorithm to use : NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES
  */
@JsonSerialize(`using` = classOf[ToStringSerializer])
@JsonDeserialize(`using` = classOf[TransformInputDeserializer])
sealed case class TransformInput(value: String, sql: Boolean) {
  override def toString: String = if (sql) s"SQL:$value" else value

  def crypt(
    s: String,
    colMap: Map[String, Option[String]],
    transformAlgo: TransformEngine,
    transformParams: List[String]
  ): String = {
    // val ((privacyAlgo, privacyParams), _) = allPrivacyLevels(value)
    transformAlgo.crypt(s, colMap, transformParams)
  }

  /** @param colMap
    * @param transformAlgo
    * @param transformParams
    * @return
    */
  def cryptUDF(transformAlgo: TransformEngine, transformParams: List[String]) = {
    // A failing engine, or one returning null, gives null: Some(null) cannot be encoded by Spark
    udf((column: String) =>
      Option(column).flatMap(c =>
        Try(transformAlgo.crypt(c, Map.empty, transformParams)).toOption.flatMap(Option(_))
      )
    ).withName("crypt_it") // TODO: remove colMap
  }
}

object TransformInput {

  val None: TransformInput = TransformInput("NONE", false)
}

class TransformInputDeserializer extends JsonDeserializer[TransformInput] {

  override def getNullValue(ctxt: DeserializationContext): TransformInput = TransformInput.None

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): TransformInput = {
    val value = jp.readValueAs[String](classOf[String]).toUpperCase()
    val isSQL =
      if (value.startsWith("SQL:")) {
        true
      } else {
        // the value contains space before the '(' and is not a function
        val parIndex = value.indexOf('(')
        if (parIndex > 0)
          value.substring(0, parIndex).trim.contains(' ')
        else
          value.trim.contains(' ')
      }
    val finalValue = if (isSQL) value.substring("SQL:".length) else value
    TransformInput(
      finalValue,
      isSQL
    )
  }
}
