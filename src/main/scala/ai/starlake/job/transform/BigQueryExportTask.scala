package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.metrics.{BigQueryExpectationAssertionHandler, ExpectationAssertionHandler}
import ai.starlake.job.sink.bigquery.{BigQueryLoadConfig, BigQueryNativeJob}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.{
  AutoTaskInfo,
  Engine,
  FsSink,
  Severity,
  Sink,
  ValidationMessage,
  WriteStrategyType
}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{JobResult, Utils}
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{
  BigQuery,
  Field,
  Job,
  QueryJobConfiguration,
  Schema as BQSchema,
  StandardSQLTypeName,
  TableId
}
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.{BlobId, Storage}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Runs a BigQuery transform whose sink is a set of files on Cloud Storage as an `EXPORT DATA`
  * statement, without Spark (#1834).
  *
  * The files are written to the domain folder of the sink path, named after the table, the run and
  * their number, or to the URI of the sink path when it holds the wildcard of the file names. The
  * files of a run are named after it, so that no run overwrites the files of another:
  *   - '''OVERWRITE''' deletes the files of the previous runs once the export succeeded;
  *   - '''APPEND''' keeps them, except the files of a previous run of the same scheduled date,
  *     which a successful run replaces.
  *
  * A failed run deletes the files it wrote. The task's presql and postsql run in the same script as
  * the export. Its audit log counts the rows that the export wrote.
  */
class BigQueryExportTask(
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
  scheduledDate: Option[String]
)(implicit settings: Settings, storageHandler: StorageHandler, schemaHandler: SchemaHandler)
    extends AutoTask(
      appId,
      taskDesc,
      commandParameters,
      interactive,
      test,
      logExecution,
      truncate,
      resultPageSize,
      resultPageNumber,
      accessToken,
      None,
      scheduledDate,
      syncSchema = false
    ) {
  import BigQueryExportTask.*

  private lazy val fsSink: FsSink = sinkConfig match {
    case fs: FsSink => fs
    case other =>
      throw new IllegalStateException(
        s"BigQueryExportTask requires FsSink but got ${other.getClass.getSimpleName}"
      )
  }

  private lazy val format: String =
    exportFormat(fsSink).getOrElse(
      throw new IllegalStateException(
        s"BigQuery does not export files of format ${fsSink.format.getOrElse("")}"
      )
    )

  private lazy val (options, ignoredOptions) = exportOptions(format, fsSink.getOptions())

  private lazy val target: ExportTarget = {
    val path = Utils.parseJinja(fsSink.path.getOrElse(""), allVars).trim
    val extension = fsSink.extension
      .filter(_.nonEmpty)
      .map(ext => if (ext.startsWith(".")) ext else s".$ext")
      .getOrElse(defaultExtension(format, options))
    exportTarget(path, taskDesc.domain, taskDesc.table, extension)
  }

  private lazy val strategy: WriteStrategyType =
    if (truncate) WriteStrategyType.OVERWRITE else writeStrategy.getEffectiveType()

  private lazy val runId: String = newRunId(scheduledDate)

  private lazy val uri: String = target.uri(runId)

  lazy val fullTableName: String = s"${taskDesc.domain}.${taskDesc.table}"

  /** The jobs of the run connection. A BigQuery job is named after a table: the export has none,
    * the name of the task stands for it.
    */
  private[transform] lazy val bqConfig = BigQueryLoadConfig(
    connectionRef = Some(taskDesc.getRunConnectionRef()),
    outputTableId = Some(TableId.of(taskDesc.domain, taskDesc.table)),
    outputDatabase = taskDesc.getDatabase(),
    accessToken = accessToken
  )

  private def bqJob(sql: String): BigQueryNativeJob =
    new BigQueryNativeJob(bqConfig, sql, resultPageSize, taskDesc.taskTimeoutMs)

  override protected def expectationAssertionHandler: ExpectationAssertionHandler =
    new BigQueryExpectationAssertionHandler(bqJob(""))

  override def createAuditTable(): Boolean = false

  override def tableExists: Boolean = false

  override def buildRLSQueries(): List[String] = Nil

  /** The SELECT of the task, as the BigQuery native transforms run it */
  private def select(sql: Option[String]): String = {
    val inputSQL = sql.getOrElse(SQLUtils.stripComments(taskDesc.getSql()))
    val statement =
      if (taskDesc.parseSQL.getOrElse(true))
        schemaHandler.transpileAndSubstituteSelectStatement(
          SQLUtils.instantiateMacrosInSql(inputSQL, schemaHandler.allMacros, allVars),
          taskDesc.getRunConnection(),
          allVars,
          this.test
        )
      else
        SQLUtils.instantiateMacrosInSql(inputSQL, schemaHandler.allMacros, allVars)
    statement.trim.stripSuffix(";")
  }

  /** The columns of the SELECT that BigQuery does not export in this format, read from a dry run.
    * The dry run fails when the SELECT needs the presql, which then keeps all the columns.
    */
  private def columnsAsText(select: String): List[(String, String)] =
    if (!formatsRefusingTypes.contains(format)) Nil
    else
      bqJob(select).runBigQueryJob(dryRun = true) match {
        case Success(result) =>
          result.job
            .flatMap(job => Option(job.getStatistics[QueryStatistics]()))
            .flatMap(statistics => Option(statistics.getSchema))
            .map(textColumns(format, _))
            .getOrElse(Nil)
        case Failure(e) =>
          logger.warn(
            s"Could not read the schema of the SELECT of $name (${e.getMessage}): its JSON, INTERVAL or RANGE columns, which BigQuery does not export to $format, are not exported as text"
          )
          Nil
      }

  /** @return
    *   the EXPORT DATA statement of the task
    */
  private def exportStatement(sql: Option[String]): String = {
    val selectStatement = select(sql)
    val asText = columnsAsText(selectStatement)
    if (asText.nonEmpty)
      logger.info(
        s"$name exports the columns ${asText.map(_._1).mkString(", ")} as text: BigQuery does not export their type to $format"
      )
    exportDataSql(uri, format, options, selectWithTextColumns(selectStatement, asText))
  }

  /** The EXPORT DATA statement is the main SQL of the task: `transform --compile` prints it */
  override def buildAllSQLQueries(
    sql: Option[String],
    tableExistsForcedValue: Option[Boolean],
    forceNative: Boolean
  ): (Option[String], String) = {
    unsupported(name, strategy, fsSink).foreach(message =>
      throw new IllegalArgumentException(message)
    )
    (None, exportStatement(sql))
  }

  private def warnIgnoredFeatures(): Unit = {
    // The options of the connection are the default options of the sink: only the sink's own are reported
    val sinkOptions = taskDesc.sink.flatMap(_.options).getOrElse(Map.empty).keySet
    val ignored =
      List(
        Option(ignoredOptions.filter(sinkOptions.contains))
          .filter(_.nonEmpty)
          .map(_.mkString("the sink options ", ", ", "")),
        fsSink.clustering.filter(_.nonEmpty).map(_ => "the sink clustering"),
        fsSink.coalesce.filter(identity).map(_ => "the sink coalesce"),
        Option(taskDesc.acl).filter(_.nonEmpty).map(_ => "the acl"),
        Option(taskDesc.rls).filter(_.nonEmpty).map(_ => "the rls"),
        Option(taskDesc.expectations)
          .filter(_.nonEmpty && settings.appConfig.expectations.active)
          .map(_ => "the expectations")
      ).flatten
    if (ignored.nonEmpty)
      logger.warn(
        s"The BigQuery export of $name to files ignores ${ignored.mkString(", ")}"
      )
  }

  /** @return
    *   the files of the target folder that the exports to this target wrote, by name
    */
  private def exportedFiles(storage: Storage): List[(String, BlobId)] = {
    val (bucket, folder) = target.bucketAndFolder
    storage
      .list(
        bucket,
        Storage.BlobListOption.prefix(folder + target.prefix),
        Storage.BlobListOption.currentDirectory()
      )
      .iterateAll()
      .asScala
      .toList
      .filter(!_.isDirectory)
      .map(blob => blob.getName.stripPrefix(folder) -> blob.getBlobId)
      .filter { case (name, _) => target.runIdOf(name).isDefined }
  }

  /** @return
    *   the names of the files that could not be deleted
    */
  private def delete(storage: Storage, files: List[(String, BlobId)]): List[String] =
    files
      .grouped(100)
      .flatMap { group =>
        group.zip(storage.delete(group.map(_._2).asJava).asScala).collect {
          case ((name, _), deleted) if !deleted.booleanValue() => name
        }
      }
      .toList

  /** Deletes the files of the previous runs that this successful run replaces */
  private def deleteReplacedFiles(storage: Storage): Unit = {
    val replaced = exportedFiles(storage).filter { case (name, _) =>
      replacesFile(target, runId, strategy, scheduledDate.isDefined, name)
    }
    if (replaced.nonEmpty) {
      logger.info(
        s"Deleting the ${replaced.size} file(s) of the previous exports that $uri replaces"
      )
      val kept = delete(storage, replaced)
      if (kept.nonEmpty)
        throw new IllegalStateException(
          s"The export of $name to $uri succeeded, but ${kept.size} file(s) of its previous exports could not be deleted: ${kept
              .mkString(", ")}"
        )
    }
  }

  /** Deletes the files that this failed run wrote */
  private def deleteFilesOfFailedRun(storage: => Storage): Unit =
    Try {
      val written = exportedFiles(storage).filter { case (name, _) =>
        target.runIdOf(name).contains(runId)
      }
      if (written.nonEmpty) {
        logger.info(s"Deleting the ${written.size} file(s) that the failed export wrote to $uri")
        delete(storage, written)
      } else Nil
    } match {
      case Success(Nil) =>
      case Success(kept) =>
        logger.warn(
          s"Could not delete the files that the failed export wrote: ${kept.mkString(", ")}"
        )
      case Failure(e) =>
        logger.warn(
          s"Could not delete the files that the failed export wrote to $uri: ${e.getMessage}"
        )
    }

  /** @return
    *   the job of the EXPORT DATA statement: the job itself, or its child job when it ran in a
    *   script with the presql and postsql
    */
  private def exportJob(job: Job, script: Boolean): Option[Job] =
    if (!script) Some(job)
    else {
      val bigquery = bqJob("").bigquery(accessToken = accessToken)
      val exports = bigquery
        .listJobs(BigQuery.JobListOption.parentJobId(job.getJobId.getJob))
        .iterateAll()
        .asScala
        .toList
        .flatMap(child => Option(bigquery.getJob(child.getJobId)))
        .filter { child =>
          Option(child.getStatistics[QueryStatistics]())
            .flatMap(statistics => Option(statistics.getStatementType))
            .contains(QueryStatistics.StatementType.EXPORT_DATA)
        }
      exports
        .find(
          _.getConfiguration[QueryJobConfiguration]().getQuery.contains(stringLiteral(uri))
        )
        .orElse(exports.headOption.filter(_ => exports.size == 1))
    }

  override def run(): Try[JobResult] = {
    val start = Timestamp.from(Instant.now())
    val result = Try {
      unsupported(name, strategy, fsSink).foreach(message =>
        throw new IllegalArgumentException(message)
      )
      warnIgnoredFeatures()
      val statement = exportStatement(None)
      val script = preSql.nonEmpty || postSql.nonEmpty
      val sql =
        if (script) BigQueryAutoTask.nativeScript(preSql, statement, postSql) else statement
      Utils.printOut(s"""
           |Export: $uri
           |Connection: ${taskDesc.getRunConnectionRef()}(${taskDesc.getRunConnectionType()})
           |""".stripMargin)
      val job = bqJob(sql)
      val jobResult = job.runBigQueryJob(dryRun = dryRun) match {
        case Success(jobResult) => jobResult
        case Failure(e) =>
          if (!dryRun) deleteFilesOfFailedRun(job.storageClient(accessToken))
          throw e
      }
      if (dryRun)
        Utils.printOut(s"Dry run of $name: ${jobResult.totalBytesProcessed} bytes processed")
      else {
        deleteReplacedFiles(job.storageClient(accessToken))
        if (logExecution) {
          val statistics = Try {
            jobResult.job
              .flatMap(exportJob(_, script))
              .flatMap(job => Option(job.getStatistics[QueryStatistics]()))
              .flatMap(statistics => Option(statistics.getExportDataStats))
          }.recover { case e =>
            logger.warn(s"Could not read the export statistics of $name: ${e.getMessage}")
            None
          }.toOption
            .flatten
          val rows =
            statistics.flatMap(s => Option(s.getRowCount)).map(_.longValue).getOrElse(-1L)
          val files =
            statistics.flatMap(s => Option(s.getFileCount)).map(_.longValue).getOrElse(-1L)
          logAuditSuccess(
            start,
            Timestamp.from(Instant.now()),
            rows,
            test,
            exportAuditMessage(rows, files, uri)
          )
        }
      }
      jobResult
    }
    result match {
      case Failure(e) =>
        Utils.logException(logger, e)
        if (logExecution) logAuditFailure(start, Timestamp.from(Instant.now()), e, test)
      case Success(_) =>
    }
    result
  }
}

object BigQueryExportTask {

  /** The EXPORT DATA formats of the file sinks that BigQuery writes */
  private val formats = Map("parquet" -> "PARQUET", "avro" -> "AVRO", "json" -> "JSON")

  /** The EXPORT DATA options that a sink may set, with the formats they apply to */
  private val sinkOptionFormats = Map(
    "compression"            -> Set("PARQUET", "AVRO", "JSON"),
    "use_avro_logical_types" -> Set("AVRO")
  )

  /** The types that EXPORT DATA refuses in each format: "Type JSON is not currently supported for
    * parquet exports", RANGE "not supported in Avro export", and "Unsupported type for JSON export:
    * RANGE<DATE>"
    */
  private val refusedTypes: Map[String, Set[StandardSQLTypeName]] = Map(
    "PARQUET" -> Set(
      StandardSQLTypeName.JSON,
      StandardSQLTypeName.INTERVAL,
      StandardSQLTypeName.RANGE
    ),
    "AVRO" -> Set(StandardSQLTypeName.RANGE),
    "JSON" -> Set(StandardSQLTypeName.RANGE)
  )

  private val formatsRefusingTypes: Set[String] = refusedTypes.keySet

  /** A run id: the scheduled date of the run, or its start time, and a random suffix */
  private val runIdPattern = "(\\d{17})_[0-9a-f]{8}"

  private val runIdTime =
    DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC)

  /** @return
    *   a new run id, whose time is the digits of the scheduled date when there is one: the runs of
    *   the same scheduled date have the same time
    */
  def newRunId(scheduledDate: Option[String]): String = {
    val time = scheduledDate
      .map(_.filter(_.isDigit))
      .filter(_.length >= 8)
      .map(digits => (digits + "0" * 17).take(17))
      .getOrElse(runIdTime.format(Instant.now()))
    s"${time}_${UUID.randomUUID().toString.take(8)}"
  }

  /** @return
    *   the EXPORT DATA format of the files of this sink, when BigQuery writes them
    */
  def exportFormat(sink: FsSink)(implicit settings: Settings): Option[String] =
    formats.get(sink.format.getOrElse(settings.appConfig.defaultWriteFormat).toLowerCase)

  /** @return
    *   the Cloud Storage path of the sink, rendered with these variables, when the task writes its
    *   files with an EXPORT DATA statement
    */
  def nativeExportPath(sink: Sink, vars: => Map[String, Any])(implicit
    settings: Settings
  ): Option[String] =
    sink match {
      case fs: FsSink if exportFormat(fs).isDefined =>
        fs.path
          .map(path => Try(Utils.parseJinja(path, vars)).getOrElse(path).trim)
          .filter(_.startsWith("gs://"))
      case _ => None
    }

  /** @return
    *   true when the task runs on BigQuery and writes its files with an EXPORT DATA statement
    */
  def isNativeExport(task: AutoTaskInfo, sink: Sink, vars: => Map[String, Any])(implicit
    settings: Settings
  ): Boolean =
    task.python.isEmpty && task.getRunEngine() == Engine.BQ &&
    nativeExportPath(sink, vars).isDefined

  /** @return
    *   why EXPORT DATA cannot write the files of the sink with this write strategy
    */
  def unsupported(task: String, strategy: WriteStrategyType, sink: FsSink): Option[String] =
    if (!Set(WriteStrategyType.APPEND, WriteStrategyType.OVERWRITE).contains(strategy))
      Some(
        s"The BigQuery export of $task writes files: its write strategy must be APPEND or OVERWRITE, not $strategy"
      )
    else if (sink.partition.exists(_.nonEmpty))
      Some(
        s"The BigQuery export of $task cannot partition its files by ${sink.partition.getOrElse(Nil).mkString(", ")}: EXPORT DATA writes no partition folders"
      )
    else None

  /** The validation of a task that BigQuery exports: its write strategy and its sink partition */
  def checkValidity(task: AutoTaskInfo)(implicit
    settings: Settings
  ): Either[List[ValidationMessage], Boolean] = {
    val errors = Try {
      task.getSinkConfig() match {
        case fs: FsSink if isNativeExport(task, fs, Map.empty) =>
          unsupported(
            task.name,
            task.writeStrategy.map(_.getEffectiveType()).getOrElse(WriteStrategyType.APPEND),
            fs
          ).toList
        case _ => Nil
      }
    }.getOrElse(Nil)
    if (errors.isEmpty) Right(true)
    else Left(errors.map(ValidationMessage(Severity.Error, s"Task ${task.name}", _)))
  }

  /** Where an export writes: a folder, and the names of the files it writes there, a prefix and a
    * suffix around the run id and the number that BigQuery gives each file
    */
  case class ExportTarget(folder: String, prefix: String, suffix: String) {

    def uri(runId: String): String = s"$folder$prefix$runId-*$suffix"

    private val exportedFile = Pattern.compile(
      s"${Pattern.quote(prefix)}($runIdPattern)-\\d{12,}${Pattern.quote(suffix)}"
    )

    /** @return
      *   the run id of a file of the folder that an export to this target wrote
      */
    def runIdOf(name: String): Option[String] = {
      val matcher = exportedFile.matcher(name)
      if (matcher.matches()) Some(matcher.group(1)) else None
    }

    /** @return
      *   the bucket, and the name prefix of the objects of the folder
      */
    def bucketAndFolder: (String, String) = {
      val Array(bucket, folderPath) = folder.stripPrefix("gs://").split("/", 2)
      (bucket, folderPath)
    }
  }

  /** @param path
    *   a Cloud Storage folder, whose domain folder holds the files of the tables, or the URI of the
    *   files of the task with the single wildcard that EXPORT DATA replaces with the number of each
    *   file
    */
  def exportTarget(path: String, domain: String, table: String, extension: String): ExportTarget = {
    require(
      path.startsWith("gs://"),
      s"The path of a BigQuery export must be on Cloud Storage: $path"
    )
    val target =
      if (path.contains('*')) {
        val wildcard = path.indexOf('*')
        val slash = path.lastIndexOf('/')
        require(
          path.lastIndexOf('*') == wildcard && wildcard > slash,
          s"The path of a BigQuery export may hold a single wildcard, in the name of the files: $path"
        )
        ExportTarget(
          path.substring(0, slash + 1),
          path.substring(slash + 1, wildcard),
          path.substring(wildcard + 1)
        )
      } else ExportTarget(s"${path.stripSuffix("/")}/$domain/", s"$table-", extension)
    require(
      target.folder.matches("gs://[^/]+/.*"),
      s"The path of a BigQuery export must name a bucket: $path"
    )
    target
  }

  /** @return
    *   true for the file of a previous run that a successful run replaces: any for an OVERWRITE,
    *   one of the same scheduled date for an APPEND
    */
  def replacesFile(
    target: ExportTarget,
    runId: String,
    strategy: WriteStrategyType,
    scheduled: Boolean,
    name: String
  ): Boolean =
    target.runIdOf(name).exists { fileRunId =>
      fileRunId != runId && (strategy == WriteStrategyType.OVERWRITE ||
      scheduled && fileRunId.take(17) == runId.take(17))
    }

  /** @return
    *   the EXPORT DATA options of the sink options, as SQL values, and the names of the sink
    *   options that are not EXPORT DATA options of this format. Parquet and Avro files are
    *   compressed with Snappy by default, as Spark writes them.
    */
  def exportOptions(
    format: String,
    sinkOptions: Map[String, String]
  ): (List[(String, String)], List[String]) = {
    val (known, ignored) =
      sinkOptions.partition { case (key, _) =>
        sinkOptionFormats.get(key.toLowerCase).exists(_.contains(format))
      }
    val values = known.map { case (key, value) => key.toLowerCase -> value.trim }
    val compression = values
      .get("compression")
      .map(_.toUpperCase)
      .orElse(if (format == "JSON") None else Some("SNAPPY"))
      .filterNot(Set("", "NONE", "UNCOMPRESSED"))
      .map(codec => "compression" -> stringLiteral(codec))
    val logicalTypes = values.get("use_avro_logical_types").map { value =>
      require(
        Set("true", "false").contains(value.toLowerCase),
        s"use_avro_logical_types must be true or false: $value"
      )
      "use_avro_logical_types" -> value.toLowerCase
    }
    (compression.toList ++ logicalTypes, ignored.keys.toList.sorted)
  }

  def defaultExtension(format: String, options: List[(String, String)]): String =
    format match {
      case "PARQUET" => ".parquet"
      case "AVRO"    => ".avro"
      case _ =>
        if (options.contains("compression" -> stringLiteral("GZIP"))) ".json.gz" else ".json"
    }

  private def stringLiteral(value: String): String =
    "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

  /** @return
    *   the statement exporting the result of the select to the files of this URI. Overwrite is
    *   always set: the files of a run have names of their own, and the files of the same names are
    *   the only files that BigQuery overwrites.
    */
  def exportDataSql(
    uri: String,
    format: String,
    options: List[(String, String)],
    select: String
  ): String = {
    val allOptions =
      ("uri"       -> stringLiteral(uri)) :: ("format" -> stringLiteral(format)) ::
      ("overwrite" -> "true") :: options
    s"""EXPORT DATA OPTIONS (${allOptions.map { case (k, v) => s"$k = $v" }.mkString(", ")}) AS
       |$select""".stripMargin
  }

  /** @return
    *   the top level columns of this schema, not repeated, that BigQuery does not export in this
    *   format, with the expression of their text: the JSON text of the JSON values, the canonical
    *   text of the INTERVAL and RANGE values, which the native loader reads back. A null stays
    *   null: the JSON text of a SQL NULL is `null`.
    */
  def textColumns(format: String, schema: BQSchema): List[(String, String)] = {
    val refused = refusedTypes.getOrElse(format, Set.empty)
    Option(schema.getFields).map(_.asScala.toList).getOrElse(Nil).collect {
      case field
          if field.getMode != Field.Mode.REPEATED &&
          Option(field.getType.getStandardType).exists(refused.contains) =>
        val column = s"`${field.getName}`"
        field.getName -> (
          if (field.getType.getStandardType == StandardSQLTypeName.JSON)
            s"IF($column IS NULL, NULL, TO_JSON_STRING($column))"
          else s"CAST($column AS STRING)"
        )
    }
  }

  def selectWithTextColumns(select: String, columns: List[(String, String)]): String =
    if (columns.isEmpty) select
    else
      s"SELECT * REPLACE (${columns
          .map { case (name, text) => s"$text AS `$name`" }
          .mkString(", ")}) FROM (\n$select\n)"

  def exportAuditMessage(rows: Long, files: Long, uri: String): String =
    s"success: $rows rows exported to $files file(s) $uri"
}
