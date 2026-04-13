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

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Severity._

import scala.collection.mutable
import scala.util.Try

case class TransformDesc(version: Int, transform: AutoJobInfo)

/** A job is a set of transform tasks executed using the specified engine.
  *
  * @param name:
  *   Job logical name
  * @param tasks
  *   List of transform tasks to execute
  * @param format
  *   output file format when using Spark engine. Ingored for BigQuery. Default value is "parquet"
  * @param coalesce
  *   When outputting files, should we coalesce it to a single file. Useful when CSV is the output
  *   format.
  * @param udf
  *   : Register UDFs written in this JVM class when using Spark engine Register UDFs stored at this
  *   location when using BigQuery engine
  * @param views
  *   : Create temporary views using where the key is the view name and the map the SQL request
  *   corresponding to this view using the SQL engine supported syntax.
  * @param engine
  *   : SPARK or BQ. Default value is SPARK.
  */
case class AutoJobInfo(
  name: String,
  tasks: List[AutoTaskInfo] = Nil,
  comment: Option[String] = None,
  default: Option[AutoTaskInfo] = None
) extends Named {
  def this() = this("", Nil) // Should never be called. Here for Jackson deserialization only
  // TODO
  def checkValidity(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty

    // Check Domain name validity
    val forceJobPrefixRegex = settings.appConfig.forceJobPattern.r
    // TODO: name doesn't need to respect the pattern because it may be renamed. Restriction is based on target database restriction.
    // We may check depending on the sink type but we may sink differently for each table.
    // It would be better to assume a starlake pattern to describe a dataset and the container of the dataset such as the bigquery syntax project:dataset
    // and then apply this syntax to all databases even if natively they don't accept that.
    // Therefore, it means that we need to adapt on writing to the database, the target name.
    // The same applies to table name.
    if (!forceJobPrefixRegex.pattern.matcher(name).matches())
      errorList += ValidationMessage(
        Error,
        s"Transform $name",
        s"name: Job with name $name should respect the pattern ${forceJobPrefixRegex.regex}"
      )

    // Check Tasks validity
    tasks.foreach { task =>
      task.checkValidity() match {
        case Left(errors) => errorList ++= errors
        case Right(_)     =>
      }
    }

    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList.toList)

  }
}

object AutoJobInfo {
  private def diffTasks(
    existing: List[AutoTaskInfo],
    incoming: List[AutoTaskInfo]
  ): (List[AutoTaskInfo], List[AutoTaskInfo], List[AutoTaskInfo]) = {
    val (commonTasks, deletedTasks) =
      existing
        // .filter(_.name.nonEmpty)
        .partition(task => incoming.map(_.name.toLowerCase).contains(task.name.toLowerCase))
    val addedTasks =
      incoming.filter(task =>
        task.name.nonEmpty && !existing.map(_.name.toLowerCase).contains(task.name.toLowerCase)
      )
    (addedTasks, deletedTasks, commonTasks)
  }

  def compare(existing: AutoJobInfo, incoming: AutoJobInfo): Try[TransformsDiff] = {
    Try {
      val (addedTasks, deletedTasks, existingCommonTasks) =
        diffTasks(existing.tasks, incoming.tasks)

      val commonTasks: List[(AutoTaskInfo, AutoTaskInfo)] = existingCommonTasks.map { task =>
        (
          task,
          incoming.tasks
            .find(_.name.toLowerCase() == task.name.toLowerCase())
            .getOrElse(throw new IllegalStateException("Should not happen"))
        )
      }

      val updatedTasksDiff = commonTasks.map { case (existing, incoming) =>
        AutoTaskInfo.compare(existing, incoming)
      }
      TransformsDiff(
        existing.name,
        TasksDiff(addedTasks.map(_.name), deletedTasks.map(_.name), updatedTasksDiff)
      )
    }
  }
}
