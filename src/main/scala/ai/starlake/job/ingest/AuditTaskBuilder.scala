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

import ai.starlake.config.Settings
import ai.starlake.job.transform.{AutoTask, TransformContext}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, Engine}

/** Builds the AutoTask that writes a literal SELECT ... UNION ALL into an audit table
  * (`audit.audit` or `audit.rejected`). Shared by `AuditLog`, which logs load and transform
  * outcomes, and by `NativeRejectedSink`, which logs rejected input lines for the DuckDB native
  * loader. Both sites only ever differ by the task name, the audit table name, the SELECT SQL, the
  * application id, the scheduled date and the access token. Everything else about the task comes
  * from `settings.appConfig.audit`, except the task timeout, which comes from
  * `settings.appConfig.shortJobTimeoutMs`.
  */
object AuditTaskBuilder {

  /** @param name
    *   Name of the AutoTask, unique per caller.
    * @param auditTableName
    *   Name of the audit table to write to, e.g. "audit" or "rejected".
    * @param selectSql
    *   The literal SELECT ... UNION ALL statement to run.
    * @param applicationId
    *   Application id used as both the TransformContext appId and the task run id.
    * @param scheduledDate
    *   Scheduled date to record on the task run, if any.
    * @param accessToken
    *   Optional access token forwarded to the underlying connection.
    */
  def buildTask(
    name: String,
    auditTableName: String,
    selectSql: String,
    applicationId: String,
    scheduledDate: Option[String],
    accessToken: Option[String]
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): AutoTask = {
    val taskDesc = AutoTaskInfo(
      name = name,
      sql = Some(selectSql),
      database = settings.appConfig.audit.getDatabase(),
      domain = settings.appConfig.audit.getDomain(),
      table = auditTableName,
      presql = Nil,
      postsql = Nil,
      connectionRef = settings.appConfig.audit.sink.connectionRef,
      sink = Some(settings.appConfig.audit.sink),
      parseSQL = Some(true),
      _auditTableName = Some(auditTableName),
      taskTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
    )
    // When sparkFormat is true, we do not want to use spark to write the logs
    val engine =
      if (taskDesc.getSinkConnection().isJdbcUrl()) Engine.JDBC
      else taskDesc.getSinkConnection().getEngine()
    val context = TransformContext(
      appId = Option(applicationId),
      taskDesc = taskDesc,
      commandParameters = Map.empty,
      interactive = None,
      truncate = false,
      test = false,
      logExecution = false, // We do not log the job that writes the audit logs :)
      accessToken = accessToken,
      resultPageSize = 200,
      resultPageNumber = 1,
      dryRun = false,
      scheduledDate = scheduledDate,
      syncSchema = false
    )
    context.toTask(engine)
  }
}
