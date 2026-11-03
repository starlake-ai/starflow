package ai.starlake.workflow

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobResult
import ai.starlake.job.transform.{
  AutoTask,
  TransformAction,
  TransformBatchSummary,
  TransformConfig,
  TransformContext,
  TransformTaskResult,
  TransformTaskStatus
}
import ai.starlake.lineage.{
  AutoTaskDependencies,
  AutoTaskDependenciesConfig,
  TaskViewDependencyNode
}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.schema.model.Engine.BQ
import ai.starlake.utils.{JdbcJobResult, SparkJobResult, StarlakeNotFoundException}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.Utils
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

trait TransformWorkflow extends LazyLogging {
  this: IngestionWorkflow =>

  protected def storageHandler: StorageHandler
  protected def schemaHandler: SchemaHandler
  implicit protected def settings: Settings

  def buildTask(
    config: TransformConfig
  ): AutoTask = {
    val taskDesc: AutoTaskInfo = buildTaskDesc(config)
    logger.debug(taskDesc.toString)
    val context = TransformContext(
      appId = None,
      taskDesc = taskDesc,
      commandParameters = config.options,
      interactive = config.interactive,
      truncate = config.truncate,
      test = config.test,
      logExecution = true,
      accessToken = config.accessToken,
      resultPageSize = config.pageSize,
      resultPageNumber = config.pageNumber,
      dryRun = config.dryRun,
      scheduledDate = config.scheduledDate,
      syncSchema = true
    )(settings, storageHandler, schemaHandler)
    context.toTask(taskDesc.getRunEngine())
  }

  private def buildTaskDesc(config: TransformConfig): AutoTaskInfo = {
    val taskDesc =
      if (config.name == "__ignore__.__ignore__") {
        AutoTaskInfo(
          name = "__ignore__.__ignore__",
          sql = config.query,
          domain = "__ignore__",
          table = "__ignore__",
          database = None,
          connectionRef = None,
          parseSQL = Some(false)
        )
      } else {
        val taskDesc = schemaHandler
          .taskOnly(config.name, reload = true)
          .getOrElse(throw new StarlakeNotFoundException(s"Invalid task name ${config.name}"))
        config.query match {
          case Some(_) =>
            taskDesc.copy(sql = config.query)
          case None =>
            taskDesc
        }
      }
    taskDesc
  }

  // TODO
  def updateTaskAttributes(
    config: TransformConfig
  ): Try[Unit] = Try {
    val task = buildTask(config)
  }

  def compileAutoJob(config: TransformConfig): Try[(String, String)] = Try {
    val action = buildTask(config)
    val sqlWhenTableDoesNotExist = action.buildAllSQLQueriesMerged(None, Some(false))
    val sqlWhenTableExist = action.buildAllSQLQueriesMerged(None, Some(true))
    val tableExists = Try(action.tableExists)

    def addPrePostSql(sql: String): String = {
      val preSql = if (action.preSql.isEmpty) "" else action.preSql.mkString(";\n") + ";\n"
      val postSql = if (action.postSql.isEmpty) "" else ";\n" + action.postSql.mkString(";\n")
      preSql + sql + postSql
    }

    val (formattedDontExist, formattedExist) =
      if (config.format) {
        val finalSqlDontExist =
          SQLUtils.format(sqlWhenTableDoesNotExist, JSQLFormatter.OutputFormat.PLAIN)

        val finalSqlExist = SQLUtils.format(sqlWhenTableExist, JSQLFormatter.OutputFormat.PLAIN)
        (
          addPrePostSql(finalSqlDontExist),
          addPrePostSql(finalSqlExist)
        )
      } else {
        (
          addPrePostSql(sqlWhenTableDoesNotExist),
          addPrePostSql(sqlWhenTableExist)
        )
      }

    val result =
      (
        s"""
         |----------------------------------------
         |-- SQL when table does not already exist
         |----------------------------------------
         |$formattedDontExist
         |""".stripMargin,
        s"""
         |---------------------------------
         |-- SQL when table already exists
         |---------------------------------
         |$formattedExist
         |""".stripMargin
      )
    Utils.printOut(result._1)
    Utils.printOut(result._2)
    result
  }

  /** Runs the selected tasks of the trees, upstream first, in this JVM. A task runs as many times
    * as it is a root of the trees, and once when other tasks only read it.
    *
    * Serial on purpose: the transform paths are not safe to run concurrently in one JVM (#1785).
    *
    * A task behind a failure is skipped, and tasks on other branches still run, so that one run
    * reports all the failures it can find. Nodes that are not run (tables, tasks not selected)
    * still carry the order and the failures of what is upstream of them.
    *
    * @param selected
    *   receives the name of a task as the trees spell it
    * @param taskConfig
    *   the config each task runs with, once given its name
    */
  private[workflow] def runTaskTrees(
    trees: List[TaskViewDependencyNode],
    selected: String => Boolean,
    taskConfig: TransformConfig
  ): TransformBatchSummary = {
    // Node name -> the failed task blocking whatever reads this node. The trees repeat what several
    // tasks read, so this is also what runs only once a task that several tasks read.
    val visited = mutable.Map.empty[String, Option[String]]
    val results = mutable.ListBuffer.empty[TransformTaskResult]

    def runTask(name: String): Option[String] = {
      logger.info(s"Transforming $name")
      val start = System.nanoTime()
      val status = Try(transform(taskConfig.copy(name = name))).flatten match {
        case Success(_) => TransformTaskStatus.Succeeded
        case Failure(e) => TransformTaskStatus.Failed(e)
      }
      results += TransformTaskResult(name, status, (System.nanoTime() - start) / 1000000L)
      if (status == TransformTaskStatus.Succeeded) None else Some(name)
    }

    def runOrSkip(node: TaskViewDependencyNode, failedUpstream: Option[String]): Option[String] =
      if (node.isTask() && selected(node.data.name))
        failedUpstream match {
          case Some(failed) =>
            results += TransformTaskResult(node.data.name, TransformTaskStatus.Skipped(failed), 0L)
            failedUpstream
          case None => runTask(node.data.name)
        }
      else
        failedUpstream

    def visit(node: TaskViewDependencyNode): Option[String] = {
      val key = node.data.name.toLowerCase
      visited.getOrElse(
        key, {
          val outcome = runOrSkip(node, node.children.flatMap(visit).headOption)
          visited(key) = outcome
          outcome
        }
      )
    }

    // There is one tree per listed task, so a root seen before is a task listed again. It runs again
    // at this place in the list, while what it reads is already settled and does not.
    val listed = mutable.Set.empty[String]
    trees.foreach { root =>
      if (listed.add(root.data.name.toLowerCase))
        visit(root)
      else
        visited(root.data.name.toLowerCase) =
          runOrSkip(root, root.children.flatMap(visit).headOption)
    }
    TransformBatchSummary(results.toList)
  }

  /** The tasks a multi task config asks for, spelled as the project declares them. Fails before
    * anything runs, so that a typo in a list does not leave half of it executed.
    */
  private def selectBatchTasks(config: TransformConfig): List[String] = {
    val tasks = schemaHandler.tasks(config.reload)
    if (config.tags.nonEmpty) {
      val tags = config.tags.toSet
      val tagged = tasks.filter(_.tags.intersect(tags).nonEmpty).map(_.fullName())
      if (tagged.isEmpty)
        throw new IllegalArgumentException(s"No task has any of the tags ${tags.mkString(", ")}")
      tagged
    } else {
      val declared = tasks.map(task => task.fullName().toLowerCase -> task.fullName()).toMap
      val unknown = config.names.filterNot(name => declared.contains(name.toLowerCase))
      if (unknown.nonEmpty)
        throw new IllegalArgumentException(s"Unknown task(s): ${unknown.mkString(", ")}")
      config.names.map(name => declared(name.toLowerCase))
    }
  }

  private def transformBatch(config: TransformConfig): Try[String] =
    Try {
      config.requireCompatibleFlags()
      val tasks = selectBatchTasks(config)
      Utils.printOut(s"Building dependency tree for ${tasks.mkString(", ")}...")
      val dependencies = new AutoTaskDependencies(settings, schemaHandler, storageHandler)
      // Computed once for the project: asking for the tasks by name computes it once per name
      val lineage = dependencies.tasks(AutoTaskDependenciesConfig(all = true))
      val trees =
        dependencies.jobsDependencyTree(lineage, AutoTaskDependenciesConfig(tasks = Some(tasks)))
      val summary =
        if (config.recursive)
          runTaskTrees(
            trees,
            _ => true,
            TransformConfig(
              TransformAction.Run,
              options = config.options,
              scheduledDate = config.scheduledDate
            )
          )
        else {
          val selected = tasks.map(_.toLowerCase).toSet
          runTaskTrees(
            trees,
            name => selected.contains(name.toLowerCase),
            config.copy(tags = Seq.empty)
          )
        }
      Utils.printOut(summary.render)
      summary
    }.flatMap(_.toTry)

  def autoJob(config: TransformConfig): Try[String] =
    if (config.recursive || config.isMultiTask)
      transformBatch(config)
    else
      transform(config)

  /** Successively run each task of a job
    *
    * @param transformConfig
    *   : job name as defined in the YML file and sql parameters to pass to SQL statements.
    */
  // scalastyle:off println
  def transform(transformConfig: TransformConfig): Try[String] = {
    if (transformConfig.reload)
      schemaHandler.tasks(transformConfig.reload)
    val action = buildTask(transformConfig)
    logger.info(s"Transforming with config $transformConfig")
    logger.info(s"Entering ${action.taskDesc.getRunEngine()} engine")
    val runEngine = action.taskDesc.getRunEngine()

    if (!transformConfig.dryRunSupportedOn(runEngine))
      return Failure(
        new IllegalArgumentException(
          s"--dry-run is only supported on BigQuery: ${transformConfig.name} runs on $runEngine " +
          "and would really write"
        )
      )

    runEngine match {
      case BQ =>
        val result = action.run()
        Utils.logFailure(result, logger)
        result match {
          case Success(res) =>
            transformConfig.interactive match {
              case Some(format) =>
                val bqJobResult = res.asInstanceOf[BigQueryJobResult]
                val pretty = bqJobResult.prettyPrint(format, transformConfig.dryRun)
                Utils.printOut(pretty)
                Success(pretty)
              case None =>
                Success("")
            }
          case Failure(e) =>
            Failure(e)
        }
      case Engine.JDBC =>
        (action.run(), transformConfig.interactive) match {
          case (Success(jdbcJobResult: JdbcJobResult), Some(format)) =>
            val pretty = jdbcJobResult.prettyPrint(format)
            Utils.printOut(pretty)
            Success(pretty) // Sink already done in JDBC
          case (Success(_), _) =>
            Success("")
          case (Failure(exception), _) =>
            exception.printStackTrace()
            Failure(exception)
        }
      case _ =>
        (action.run(), transformConfig.interactive) match {
          case (Success(jobResult: SparkJobResult), Some(format)) =>
            val result = jobResult.prettyPrint(format)
            Utils.printOut(result)
            Success(result)
          case (Success(_), None) =>
            Success("")
          case (Failure(exception), _) =>
            exception.printStackTrace()
            Failure(exception)
          case (Success(_), _) =>
            throw new Exception("Should never happen")
        }
    }
  }
}
