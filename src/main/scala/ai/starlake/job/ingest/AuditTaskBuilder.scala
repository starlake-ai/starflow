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

  /** Neutralizes a value that is inlined into an audit SELECT. Quotes and newlines would break the
    * literal, a backslash would break it on the dialects that treat it as an escape character
    * inside string literals (Snowflake, BigQuery and MySQL among them, where a value ending in a
    * backslash escapes the closing quote and aborts the load), and a Jinja delimiter would be
    * picked up by the Jinja pass that AutoTask runs on the SQL before executing it, since the task
    * is built with `parseSQL = true`. Backslashes are replaced rather than doubled because doubling
    * is not dialect safe either: a standard conforming dialect would then store the doubled
    * backslash. All three delimiter pairs have to go, not only `{{ }}`: an unknown expression such
    * as `{{ANUM}}` merely renders to the empty string and mangles the recorded value, but an
    * unknown tag such as `{%ANUM%}` is a FATAL error whatever `failOnUnknownTokens` says, and
    * `Jinjava.render` throws on it, which would fail the whole job over a single value. Shared by
    * every caller of `buildTask` so the escapings cannot drift apart.
    *
    * The invariant is that the result contains no single quote (given a `quoteReplacement` that
    * carries none itself), no backslash and no brace at all, so no Jinja delimiter can survive the
    * escaping and none can be assembled out of what does. Every Jinja delimiter needs a brace (`{{
    * }}`, `{% %}`, `{# #}`), and it also means the result is immune to the `{{key}}` substitution
    * `Formatter.richFormat` runs over the same template. This is why the braces go wholesale and
    * not pairwise. Stripping the six delimiters as pairs looks tighter but is not a fixpoint: each
    * `replaceAll` pass is non overlapping and left to right, so a deletion joins the characters on
    * either side of it and the join is never rescanned, which lets the escaping SYNTHESIZE a live
    * delimiter out of an input that carried none. Pairwise stripping turned `{}}%ANUM%#}}` into
    * `{%ANUM%}` and `{}}{ANUM}%}}` into `{{ANUM}}`, so a DSV line carrying the first still aborted
    * the whole load, which is precisely what the caller uses this for. Removing every brace can
    * neither leave a delimiter nor build one, and applying it twice changes nothing, so please do
    * not "improve" it back into pairwise stripping.
    *
    * @param quoteReplacement
    *   What a single quote becomes. Defaults to a dash, which is what every caller that inlines a
    *   value into a quoted SQL literal wants. A caller that inlines SQL text of its own, such as
    *   the expectations sink, wants that text to stay readable and passes a value that stays safe
    *   inside the enclosing single quoted literal instead, for example a double quote. The value is
    *   inserted literally (`Matcher.quoteReplacement` neutralizes the `$` and `\` regex replacement
    *   metacharacters), and a replacement carrying a brace or backslash is cleaned up by the later
    *   passes, so the one thing a caller must never pass is a single quote, which no later pass
    *   removes and which would reopen the enclosing literal.
    */
  def escapeLiteral(value: String, quoteReplacement: String = "-"): String =
    value
      .replaceAll("'", java.util.regex.Matcher.quoteReplacement(quoteReplacement))
      .replaceAll("\\\\", "-")
      .replaceAll("\\n", " ")
      .replaceAll("[{}]", "")

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
