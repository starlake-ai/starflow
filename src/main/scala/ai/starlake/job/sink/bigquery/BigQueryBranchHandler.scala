package ai.starlake.job.sink.bigquery

import ai.starlake.config.Settings
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.JSQLReplacer
import com.google.cloud.bigquery.{BigQuery, DatasetId, DatasetInfo, QueryJobConfiguration, TableId}
import com.typesafe.scalalogging.LazyLogging
import net.sf.jsqlparser.parser.CCJSqlParserUtil

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

object BigQueryBranchHandler extends LazyLogging {

  case class BranchContext(
    branchDataset: String,
    tableMappings: Map[String, String],
    branchTargetTableId: TableId
  )

  /** Prepare branching for a BigQuery native transform.
    *
    * Creates the branch dataset, clones source and target tables, and returns the mappings needed
    * for SQL rewriting.
    *
    * @param branchName
    *   the branch/session name (becomes the dataset name)
    * @param bqService
    *   BigQuery client
    * @param originalTargetTableId
    *   the original target table
    * @param sql
    *   the source SQL (SELECT statement) to extract source table references from
    * @param location
    *   BigQuery location (e.g., europe-west1)
    * @param connectionOptions
    *   connection options for job execution
    * @return
    *   BranchContext with mappings and redirected target
    */
  def prepareBranch(
    branchName: String,
    bqService: BigQuery,
    originalTargetTableId: TableId,
    sql: String,
    location: String,
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): BranchContext = {
    val projectId = Option(originalTargetTableId.getProject)
      .getOrElse(
        BigQueryJobBase.projectId(connectionOptions.get("projectId"), None)
      )

    // 1. Create branch dataset if it doesn't exist
    getOrCreateBranchDataset(bqService, branchName, projectId, location)

    // 2. Extract source table names from the SQL
    logger.info(s"Extracting table names from SQL: $sql")
    val sourceTableNames = Try(SQLUtils.extractTableNames(sql)) match {
      case scala.util.Success(names) =>
        logger.info(s"Extracted source table names: $names")
        names
      case scala.util.Failure(e) =>
        logger.warn(s"Failed to extract table names from SQL: ${e.getMessage}")
        Nil
    }

    // 3. Build mappings: clone source tables into branch dataset
    val tableMappings = scala.collection.mutable.Map[String, String]()
    val clonedTableNames = scala.collection.mutable.Set[String]()

    sourceTableNames.foreach { sourceTableRef =>
      val sourceTableId = resolveTableId(sourceTableRef, projectId)
      val branchTableName =
        uniqueTableName(sourceTableId.getTable, clonedTableNames.toSet)
      clonedTableNames += branchTableName

      val branchTableId = TableId.of(projectId, branchName, branchTableName)
      cloneTableIfNotExists(bqService, sourceTableId, branchTableId, location, connectionOptions)

      val originalFullName = formatTableRef(sourceTableId)
      val branchFullName = formatTableRef(branchTableId)
      tableMappings += (originalFullName -> branchFullName)
    }

    // 4. Clone target table into branch dataset (for MERGE/UPSERT on existing table)
    val targetBranchTableName =
      uniqueTableName(originalTargetTableId.getTable, clonedTableNames.toSet)
    val branchTargetTableId = TableId.of(projectId, branchName, targetBranchTableName)

    val originalTargetExists =
      Option(bqService.getTable(originalTargetTableId)).isDefined
    if (originalTargetExists) {
      cloneTableIfNotExists(
        bqService,
        originalTargetTableId,
        branchTargetTableId,
        location,
        connectionOptions
      )
    }

    val originalTargetFullName = formatTableRef(originalTargetTableId)
    val branchTargetFullName = formatTableRef(branchTargetTableId)
    tableMappings += (originalTargetFullName -> branchTargetFullName)

    logger.info(
      s"Branch '$branchName' prepared with ${tableMappings.size} table mappings: ${tableMappings.mkString(", ")}"
    )

    BranchContext(
      branchDataset = branchName,
      tableMappings = tableMappings.toMap,
      branchTargetTableId = branchTargetTableId
    )
  }

  /** Rewrite SQL table references using the branch mappings.
    *
    * Uses JSQLReplacer from jsqltranspiler to parse and rewrite all table references in the SQL.
    */
  def rewriteSql(sql: String, tableMappings: Map[String, String]): String = {
    if (tableMappings.isEmpty) return sql

    try {
      val replacer = new JSQLReplacer(Array.empty[Array[String]])
      replacer.putReplaceTables(tableMappings.asJava)
      val statements = CCJSqlParserUtil.parseStatements(sql)
      val rewritten = statements.asScala.map { st =>
        replacer.replace(st, tableMappings.asJava).toString
      }
      val result = rewritten.mkString(";\n")
      logger.info(s"Rewritten SQL for branch:\n$result")
      result
    } catch {
      case e: Exception =>
        logger.warn(
          s"JSQLReplacer failed, falling back to string replacement: ${e.getMessage}"
        )
        // Fallback: simple string replacement for each mapping
        tableMappings.foldLeft(sql) { case (currentSql, (original, replacement)) =>
          currentSql.replace(original, replacement)
        }
    }
  }

  private def getOrCreateBranchDataset(
    bqService: BigQuery,
    branchName: String,
    projectId: String,
    location: String
  ): Unit = {
    val datasetId = DatasetId.of(projectId, branchName)
    val existing = Option(bqService.getDataset(datasetId))
    if (existing.isEmpty) {
      val datasetInfo = DatasetInfo
        .newBuilder(datasetId)
        .setLocation(location)
        .setDescription(s"Starlake data branch: $branchName")
        .build()
      bqService.create(datasetInfo)
      logger.info(s"Created branch dataset: $projectId.$branchName in $location")
    }
  }

  private def cloneTableIfNotExists(
    bqService: BigQuery,
    sourceTableId: TableId,
    branchTableId: TableId,
    location: String,
    connectionOptions: Map[String, String]
  ): Unit = {
    val existing = Option(bqService.getTable(branchTableId))
    if (existing.isDefined) {
      logger.info(s"Branch table already exists: ${formatTableRef(branchTableId)}")
      return
    }

    val sourceExists = Option(bqService.getTable(sourceTableId)).isDefined
    if (!sourceExists) {
      logger.info(
        s"Source table does not exist, skipping clone: ${formatTableRef(sourceTableId)}"
      )
      return
    }

    val cloneSql =
      s"CREATE TABLE ${formatTableRef(branchTableId)} CLONE ${formatTableRef(sourceTableId)}"
    logger.info(s"Cloning table: $cloneSql")

    val jobId = com.google.cloud.bigquery.JobId
      .newBuilder()
      .setJob(UUID.randomUUID().toString)
      .setProject(
        Option(sourceTableId.getProject)
          .getOrElse(Option(branchTableId.getProject).getOrElse(""))
      )
      .setLocation(location)
      .build()

    val queryConfig = QueryJobConfiguration
      .newBuilder(cloneSql)
      .setUseLegacySql(false)
      .build()

    val job = bqService.create(
      com.google.cloud.bigquery.JobInfo.newBuilder(queryConfig).setJobId(jobId).build()
    )
    job.waitFor()

    if (job.getStatus.getError != null) {
      throw new RuntimeException(
        s"Failed to clone table: ${job.getStatus.getError.getMessage}"
      )
    }
  }

  private def resolveTableId(tableRef: String, defaultProject: String): TableId = {
    // Handle formats: project.dataset.table, dataset.table, `project.dataset.table`
    val cleaned = tableRef.replace("`", "").trim
    val parts = cleaned.split("\\.")
    parts.length match {
      case 3 => TableId.of(parts(0), parts(1), parts(2))
      case 2 => TableId.of(defaultProject, parts(0), parts(1))
      case 1 => TableId.of(defaultProject, parts(0), parts(0)) // shouldn't happen
      case _ => throw new IllegalArgumentException(s"Invalid table reference: $tableRef")
    }
  }

  private def formatTableRef(tableId: TableId): String = {
    val project = Option(tableId.getProject)
    project match {
      case Some(p) => s"`$p.${tableId.getDataset}.${tableId.getTable}`"
      case None    => s"${tableId.getDataset}.${tableId.getTable}"
    }
  }

  /** Generate a unique table name, appending _1, _2 etc. on collision. Same logic as starlakeJDBC's
    * StarlakeSession.
    */
  private def uniqueTableName(baseName: String, existingNames: Set[String]): String = {
    if (!existingNames.contains(baseName)) {
      baseName
    } else {
      var i = 1
      var candidate = s"${baseName}_$i"
      while (existingNames.contains(candidate)) {
        i += 1
        candidate = s"${baseName}_$i"
      }
      candidate
    }
  }
}
