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

package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{AutoTaskInfo, Engine}

/** Encapsulates all common parameters needed to create an AutoTask.
  *
  * ==Design Pattern==
  * This class implements the '''Parameter Object''' pattern to reduce the number of parameters
  * passed to AutoTask constructors. Instead of passing 13+ individual parameters, callers create a
  * single TransformContext instance.
  *
  * ==Usage==
  * {{{
  * // Create context with all parameters
  * val context = TransformContext(
  *   appId = Some("my-app"),
  *   taskDesc = myTaskInfo,
  *   commandParameters = Map("key" -> "value"),
  *   interactive = None,
  *   truncate = false,
  *   test = false,
  *   logExecution = true,
  *   accessToken = None,
  *   resultPageSize = 200,
  *   resultPageNumber = 1,
  *   dryRun = false,
  *   scheduledDate = None,
  *   syncSchema = false
  * )
  *
  * // Or use convenience factory methods
  * val execContext = TransformContext.forExecution(appId, taskDesc)
  * val interactiveContext = TransformContext.forInteractive(taskDesc, "json")
  *
  * // Create appropriate task using factory
  * val task = TransformContext.createTask(context, Engine.SPARK)
  * }}}
  *
  * ==Implicit Parameters==
  * The context captures implicit Settings, StorageHandler, and SchemaHandler at creation time,
  * ensuring they are consistently available when creating AutoTask instances.
  *
  * @param appId
  *   Application ID for tracking and audit logging
  * @param taskDesc
  *   Task description containing SQL, domain, table, and configuration
  * @param commandParameters
  *   SQL parameters to substitute in SQL statements (e.g., date ranges, filters)
  * @param interactive
  *   If Some(format), task returns results without materializing; None means full execution
  * @param truncate
  *   Whether to truncate the target table before writing (WRITE_TRUNCATE disposition)
  * @param test
  *   Whether this is a test run (enables DuckDB transpilation and test macros)
  * @param logExecution
  *   Whether to log execution details to the audit table
  * @param accessToken
  *   Optional OAuth access token for BigQuery or other authenticated services
  * @param resultPageSize
  *   Number of rows per page for interactive query results
  * @param resultPageNumber
  *   Which page of results to return (1-based) for interactive queries
  * @param dryRun
  *   Whether to validate the query without executing it (BigQuery only)
  * @param scheduledDate
  *   Optional scheduled date for the job (used in audit logging)
  * @param syncSchema
  *   Whether to synchronize the target table schema with the YAML definition
  */
case class TransformContext(
  appId: Option[String],
  taskDesc: AutoTaskInfo,
  commandParameters: Map[String, String],
  interactive: Option[String],
  truncate: Boolean,
  test: Boolean,
  logExecution: Boolean,
  accessToken: Option[String] = None,
  resultPageSize: Int,
  resultPageNumber: Int,
  dryRun: Boolean,
  scheduledDate: Option[String],
  syncSchema: Boolean
)(implicit
  val settings: Settings,
  val storageHandler: StorageHandler,
  val schemaHandler: SchemaHandler
) {

  /** Creates the appropriate AutoTask subclass based on the engine type and sink configuration.
    *
    * This instance method provides a fluent API for task creation:
    * {{{
    * val context = TransformContext(...)
    * val task = context.toTask(Engine.BQ)  // Returns BigQueryAutoTask
    * task.run()
    * }}}
    *
    * ==Engine Selection Logic==
    *   - Engine.BQ with BigQuerySink or interactive mode → BigQueryAutoTask
    *   - Engine.JDBC with JdbcSink matching run connection → JdbcAutoTask
    *   - FsSink with export mode → SparkExportTask
    *   - All other cases → SparkAutoTask
    *
    * @param engine
    *   The execution engine (BQ, JDBC, or SPARK)
    * @return
    *   The appropriate AutoTask subclass instance
    */
  def toTask(engine: Engine): AutoTask = {
    import ai.starlake.schema.model.{BigQuerySink, FsSink, JdbcSink}

    val sinkConfig = taskDesc.getSinkConfig()
    val runConnectionRef = taskDesc.getRunConnectionRef()
    engine match {
      case Engine.BQ if sinkConfig.isInstanceOf[BigQuerySink] || interactive.isDefined =>
        TransformContext.createBigQueryTask(this)
      case Engine.JDBC
          if sinkConfig
            .isInstanceOf[JdbcSink] && sinkConfig
            .getConnectionRef() == runConnectionRef || interactive.isDefined =>
        TransformContext.createJdbcTask(this)
      case _ =>
        sinkConfig match {
          case fs: FsSink if fs.isExport() && interactive.isEmpty =>
            TransformContext.createSparkExportTask(this)
          case _ =>
            TransformContext.createSparkTask(this)
        }
    }
  }
}

/** Companion object providing factory methods for creating TransformContext and AutoTask instances.
  *
  * ==Factory Pattern==
  * This object implements the '''Factory Pattern''' to centralize AutoTask creation logic. Instead
  * of duplicating 15-20 lines of constructor calls throughout the codebase, callers use single-line
  * factory method calls.
  *
  * ==Available Factory Methods==
  *   - `forExecution()` - Creates context for batch/scheduled execution
  *   - `forInteractive()` - Creates context for interactive queries (no materialization)
  *   - `createBigQueryTask()` - Creates BigQueryAutoTask from context
  *   - `createJdbcTask()` - Creates JdbcAutoTask from context
  *   - `createSparkTask()` - Creates SparkAutoTask from context
  *   - `createSparkExportTask()` - Creates SparkExportTask from context
  *   - `createTask()` - Creates appropriate AutoTask based on engine type
  */
object TransformContext {

  /** Creates a TransformContext with common defaults for non-interactive (batch) execution.
    *
    * Use this when running scheduled or batch jobs where results are materialized to a table. Sets
    * sensible defaults: no truncation, logging enabled, no dry run.
    */
  def forExecution(
    appId: Option[String],
    taskDesc: AutoTaskInfo,
    commandParameters: Map[String, String] = Map.empty,
    accessToken: Option[String] = None,
    scheduledDate: Option[String] = None
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): TransformContext = {
    TransformContext(
      appId = appId,
      taskDesc = taskDesc,
      commandParameters = commandParameters,
      interactive = None,
      truncate = false,
      test = false,
      logExecution = true,
      accessToken = accessToken,
      resultPageSize = 200,
      resultPageNumber = 1,
      dryRun = false,
      scheduledDate = scheduledDate,
      syncSchema = false
    )
  }

  /** Creates a TransformContext for interactive/query execution.
    *
    * Use this when running ad-hoc queries where results are returned to the caller without being
    * materialized to a table. The `format` parameter specifies the output format (e.g., "json",
    * "json-array", "csv").
    */
  def forInteractive(
    taskDesc: AutoTaskInfo,
    format: String,
    commandParameters: Map[String, String] = Map.empty,
    accessToken: Option[String] = None,
    resultPageSize: Int = 200,
    resultPageNumber: Int = 1
  )(implicit
    settings: Settings,
    storageHandler: StorageHandler,
    schemaHandler: SchemaHandler
  ): TransformContext = {
    TransformContext(
      appId = None,
      taskDesc = taskDesc,
      commandParameters = commandParameters,
      interactive = Some(format),
      truncate = false,
      test = false,
      logExecution = false,
      accessToken = accessToken,
      resultPageSize = resultPageSize,
      resultPageNumber = resultPageNumber,
      dryRun = false,
      scheduledDate = None,
      syncSchema = false
    )
  }

  /** Creates a BigQueryAutoTask from a TransformContext.
    *
    * This factory method eliminates the need to repeat 15+ constructor parameters every time a
    * BigQueryAutoTask is instantiated. The context's implicit parameters (Settings, StorageHandler,
    * SchemaHandler) are automatically passed to the constructor.
    *
    * @param context
    *   The TransformContext containing all task parameters
    * @return
    *   A new BigQueryAutoTask instance configured with the context's parameters
    */
  def createBigQueryTask(context: TransformContext): BigQueryAutoTask = {
    new BigQueryAutoTask(
      appId = context.appId,
      taskDesc = context.taskDesc,
      commandParameters = context.commandParameters,
      interactive = context.interactive,
      truncate = context.truncate,
      test = context.test,
      logExecution = context.logExecution,
      accessToken = context.accessToken,
      resultPageSize = context.resultPageSize,
      resultPageNumber = context.resultPageNumber,
      dryRun = context.dryRun,
      scheduledDate = context.scheduledDate,
      syncSchema = context.syncSchema
    )(context.settings, context.storageHandler, context.schemaHandler)
  }

  /** Creates a JdbcAutoTask from a TransformContext.
    *
    * This factory method supports an optional JDBC connection parameter for cases where a
    * connection is already established (e.g., within a transaction or connection pool).
    *
    * @param context
    *   The TransformContext containing all task parameters
    * @param conn
    *   Optional existing JDBC connection to reuse (avoids creating a new connection)
    * @return
    *   A new JdbcAutoTask instance configured with the context's parameters
    */
  def createJdbcTask(
    context: TransformContext,
    conn: Option[java.sql.Connection] = None
  ): JdbcAutoTask = {
    new JdbcAutoTask(
      appId = context.appId,
      taskDesc = context.taskDesc,
      commandParameters = context.commandParameters,
      interactive = context.interactive,
      truncate = context.truncate,
      test = context.test,
      logExecution = context.logExecution,
      accessToken = context.accessToken,
      resultPageSize = context.resultPageSize,
      resultPageNumber = context.resultPageNumber,
      conn = conn,
      scheduledDate = context.scheduledDate,
      syncSchema = context.syncSchema
    )(context.settings, context.storageHandler, context.schemaHandler)
  }

  /** Creates a SparkAutoTask from a TransformContext.
    *
    * SparkAutoTask is the most versatile task type, capable of reading from and writing to various
    * sources (Hive, JDBC, BigQuery, filesystem). It can also delegate to BigQueryAutoTask or
    * JdbcAutoTask when the sink requires native execution.
    *
    * @param context
    *   The TransformContext containing all task parameters
    * @param schema
    *   Optional Starflow schema definition for the target table
    * @return
    *   A new SparkAutoTask instance configured with the context's parameters
    */
  def createSparkTask(
    context: TransformContext,
    schema: Option[ai.starlake.schema.model.SchemaInfo] = None
  ): SparkAutoTask = {
    new SparkAutoTask(
      appId = context.appId,
      taskDesc = context.taskDesc,
      commandParameters = context.commandParameters,
      interactive = context.interactive,
      truncate = context.truncate,
      test = context.test,
      logExecution = context.logExecution,
      accessToken = context.accessToken,
      resultPageSize = context.resultPageSize,
      resultPageNumber = context.resultPageNumber,
      schema = schema,
      scheduledDate = context.scheduledDate,
      syncSchema = context.syncSchema
    )(context.settings, context.storageHandler, context.schemaHandler)
  }

  /** Creates a SparkExportTask from a TransformContext.
    *
    * SparkExportTask is used when exporting data to the filesystem in formats like CSV, JSON,
    * Parquet, etc. It differs from SparkAutoTask in that it writes to files rather than tables.
    *
    * @param context
    *   The TransformContext containing all task parameters
    * @return
    *   A new SparkExportTask instance configured with the context's parameters
    */
  def createSparkExportTask(context: TransformContext): SparkExportTask = {
    new SparkExportTask(
      appId = context.appId,
      taskDesc = context.taskDesc,
      commandParameters = context.commandParameters,
      interactive = context.interactive,
      truncate = context.truncate,
      test = context.test,
      logExecution = context.logExecution,
      accessToken = context.accessToken,
      resultPageSize = context.resultPageSize,
      resultPageNumber = context.resultPageNumber,
      scheduledDate = context.scheduledDate
    )(context.settings, context.storageHandler, context.schemaHandler)
  }

  /** Creates an AutoTask of the appropriate type based on the engine.
    *
    * This is the '''preferred factory method''' for creating AutoTask instances. It uses
    * `context.toTask(engine)` which contains the logic for selecting the appropriate task type
    * based on:
    *   - The engine type (SPARK, BQ, JDBC)
    *   - The sink configuration (FsSink, BigQuerySink, JdbcSink)
    *   - Whether the task is interactive or batch
    *
    * @param context
    *   The TransformContext containing all task parameters
    * @param engine
    *   The execution engine (Engine.SPARK, Engine.BQ, Engine.JDBC)
    * @return
    *   The appropriate AutoTask subclass instance
    */
  def createTask(context: TransformContext, engine: Engine): AutoTask = {
    context.toTask(engine)
  }
}
