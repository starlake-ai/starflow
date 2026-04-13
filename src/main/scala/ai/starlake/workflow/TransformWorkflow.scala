package ai.starlake.workflow

import ai.starlake.config.Settings
import ai.starlake.extract.ParUtils
import ai.starlake.job.sink.bigquery.BigQueryJobResult
import ai.starlake.job.transform.{AutoTask, TransformAction, TransformConfig, TransformContext}
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

  def transform(
    dependencyTree: List[TaskViewDependencyNode],
    options: Map[String, String],
    scheduledDate: Option[String]
  ): Try[String] = {
    val executed = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    transform(dependencyTree, options, scheduledDate, executed)
  }

  private def transform(
    dependencyTree: List[TaskViewDependencyNode],
    options: Map[String, String],
    scheduledDate: Option[String],
    executed: java.util.Set[String]
  ): Try[String] = {

    implicit val forkJoinTaskSupport: Option[scala.collection.parallel.ForkJoinTaskSupport] =
      ParUtils.createForkSupport(Some(settings.appConfig.maxParTask))

    val parJobs =
      ParUtils.makeParallel(dependencyTree)
    val res = parJobs.iterator.map { jobContext =>
      val ok = transform(jobContext.children, options, scheduledDate, executed)
      if (ok.isSuccess) {
        if (jobContext.isTask() && executed.add(jobContext.data.name)) {
          logger.info(s"Transforming ${jobContext.data.name}")
          val res =
            transform(
              TransformConfig(
                TransformAction.Run,
                jobContext.data.name,
                options,
                scheduledDate = scheduledDate
              )
            )
          res
        } else {
          if (jobContext.isTask()) {
            logger.info(s"Skipping already executed transform ${jobContext.data.name}")
          }
          Success("")
        }
      } else
        ok
    }
    forkJoinTaskSupport.foreach(_.forkJoinPool.shutdown())
    val allIsSuccess = res.iterator.forall(_.isSuccess)
    if (res.iterator.isEmpty) {
      Success("")
    } else if (allIsSuccess) {
      res.iterator.next()
    } else {
      res.iterator
        .find(_.isFailure)
        .getOrElse(throw new IllegalStateException("Should never happen"))
    }
  }

  def autoJob(config: TransformConfig): Try[String] = {
    val result =
      if (config.recursive) {
        Utils.printOut(s"Building dependency tree for task ${config.name}...")
        val taskConfig = AutoTaskDependenciesConfig(tasks = Some(List(config.name)))
        val dependencyTree = new AutoTaskDependencies(settings, schemaHandler, storageHandler)
          .jobsDependencyTree(taskConfig)
        Utils.printOut(s"Dependency tree built for task ${config.name}")
        dependencyTree.foreach(_.print())
        transform(dependencyTree, config.options, scheduledDate = config.scheduledDate)
      } else if (config.tags.nonEmpty) {
        val jobs =
          schemaHandler.jobs().flatMap { job =>
            val tasks = job.tasks.filter { task =>
              task.tags.intersect(config.tags.toSet).nonEmpty
            }
            if (tasks.isEmpty)
              None
            else
              Some(job.copy(tasks = tasks))
          }
        Try {
          jobs
            .flatMap { job =>
              job.tasks.map { task =>
                val result = transform(config.copy(name = s"${job.name}.${task.name}"))
                result match {
                  case Success(res) =>
                    res
                  case Failure(e) =>
                    throw e
                }
              }
            }
            .mkString("\n")
        }
      } else {
        transform(config)
      }
    result
  }

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
