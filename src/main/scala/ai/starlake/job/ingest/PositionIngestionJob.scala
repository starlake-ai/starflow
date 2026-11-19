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

package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/** Main class to ingest delimiter separated values file
  *
  * @param domain
  *   : Input Dataset Domain
  * @param schema
  *   : Input Dataset Schema
  * @param types
  *   : List of globally defined types
  * @param path
  *   : Input dataset path
  * @param storageHandler
  *   : Storage Handler
  */
class PositionIngestionJob(
  domain: DomainInfo,
  schema: SchemaInfo,
  types: List[Type],
  path: List[Path],
  storageHandler: StorageHandler,
  schemaHandler: SchemaHandler,
  options: Map[String, String],
  accessToken: Option[String],
  test: Boolean,
  scheduledDate: Option[String]
)(implicit settings: Settings)
    extends DsvIngestionJob(
      domain,
      schema,
      types,
      path,
      storageHandler,
      schemaHandler,
      options,
      accessToken,
      test,
      scheduledDate
    ) {

  /** Load dataset using spark csv reader and all metadata. Does not infer schema. columns not
    * defined in the schema are dropped from the dataset (require datsets with a header)
    *
    * @return
    *   Spark DataFrame where each row holds a single string
    */
  override def loadDataSet(): Try[DataFrame] = {
    Try {
      val dfInNonFiltered = PositionIngestionUtil.loadLines(
        session,
        path,
        mergedMetadata.resolveEncoding(),
        mergedMetadata.resolveWithHeader(),
        sparkOptions
      )
      val dfIn = schema.filter
        .map { filterExpr =>
          logger.info(s"Applying filter expression: $filterExpr")
          dfInNonFiltered.filter(filterExpr)
        }
        .getOrElse(dfInNonFiltered)

      logger.whenDebugEnabled {
        logger.debug(dfIn.schemaString())
      }
      val preparedDF =
        PositionIngestionUtil.prepare(session, dfIn, schema.attributesWithoutScriptedFields)
      preparedDF
        .withColumn(
          CometColumns.cometInputFileNameColumn,
          org.apache.spark.sql.functions.input_file_name()
        )
    }
  }

  override def defineOutputAsOriginalFormat(rejectedLines: DataFrame): DataFrameWriter[Row] = {
    rejectedLines
      .select(
        functions
          .concat(
            schema.attributesWithoutScriptedFields.map(attr => col(attr.name).cast(StringType)): _*
          )
          .as("input_line")
      )
      .write
      .format("csv")
      .option("encoding", mergedMetadata.resolveEncoding())
      // A header line, which a reload with withHeader skips, as the native replay file has (#1946)
      .option("header", mergedMetadata.resolveWithHeader().booleanValue())
      .option("delimiter", "\n")
      .option("quote", "\n")
  }

}

/** The Spark task that run on each worker
  */
object PositionIngestionUtil {

  /** @param withHeader
    *   true if the first line of each file is a header, which is skipped
    * @return
    *   the lines of the files, in a single `value` column
    */
  def loadLines(
    session: SparkSession,
    path: List[Path],
    encoding: String,
    withHeader: Boolean,
    sparkOptions: Map[String, String]
  ): DataFrame =
    encoding.toUpperCase match {
      // The text reader cannot skip the header line of each file
      case "UTF-8" if !withHeader =>
        session.read.options(sparkOptions).text(path.map(_.toString): _*)
      case _ =>
        loadDfWithEncoding(session, path, encoding, withHeader, sparkOptions)
    }

  def loadDfWithEncoding(
    session: SparkSession,
    path: List[Path],
    encoding: String,
    withHeader: Boolean,
    sparkOptions: Map[String, String]
  ): DataFrame = {
    // This is an hack of how to use CSV source as a text source and have the ability to specify an encoding.
    // Using wholeTextFile requires the input to be in UTF-8. It drops the blank lines.
    // The options of the table apply, except those that would parse the lines
    session.read
      .options(sparkOptions)
      .schema(StructType(Array(StructField("value", StringType))))
      .option("encoding", encoding)
      // Read before `delimiter`
      .option("sep", "\n")
      // No quoting, as with the separator, which a line never holds: a line keeps its quotes, and
      // its positions. An empty quote would make a line starting with NUL a quoted value.
      .option("quote", "\n")
      .option("comment", "")
      .option("ignoreLeadingWhiteSpace", false)
      .option("ignoreTrailingWhiteSpace", false)
      .option("nullValue", "")
      .option("multiLine", false)
      .option("enforceSchema", true)
      .option("maxCharsPerColumn", -1)
      // Skips the first line of each file
      .option("header", withHeader)
      .csv(path.map(_.toString): _*)
  }

  def prepare(
    session: SparkSession,
    input: DataFrame,
    attributes: List[TableAttribute]
  ): DataFrame = {
    import org.apache.spark.sql.functions._
    val attributesProjection = attributes.foldLeft(List[Column]()) { (projection, attribute) =>
      val attributePosition = attribute.position.getOrElse(
        throw new RuntimeException(s"Attribute ${attribute.name} does not have position set")
      )
      // spark substring is 1-based and last is the length in spark and not the position to exclude
      projection :+ substring(
        col("value"),
        attributePosition.first + 1,
        attributePosition.last - attributePosition.first + 1
      ).as(attribute.name)
    }

    input
      .select(attributesProjection: _*)
      .withColumn(
        CometColumns.cometInputFileNameColumn,
        org.apache.spark.sql.functions.input_file_name()
      )
  }
}
