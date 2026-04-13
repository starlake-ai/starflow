package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.core.utils.CaseClassToPojoConverter
import ai.starlake.job.ingest.DummyIngestionJob
import ai.starlake.job.transform.{AutoTaskQueries, TransformContext}
import ai.starlake.lineage.{
  AutoTaskDependencies,
  AutoTaskDependenciesConfig,
  TaskViewDependencyNode
}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.*
import ai.starlake.utils.{JsonSerializer, StarlakeConfigException, Utils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.Try

class DagGenerateJob(schemaHandler: SchemaHandler) extends LazyLogging {

  case class TableWithDagConfig(
    domain: DomainInfo,
    table: SchemaInfo,
    dagConfigName: String,
    dagConfig: DagInfo,
    schedule: Option[String]
  )

  private def tableWithDagConfigs(
    dagConfigs: Map[String, DagInfo],
    tags: Set[String]
  )(implicit settings: Settings): List[TableWithDagConfig] = {
    logger.info("Starting to generate dags")
    val tableWithDagConfigAndSchedule = schemaHandler.domains().flatMap { domain =>
      domain.tables.filter(tags.isEmpty || _.tags.intersect(tags).nonEmpty).flatMap { table =>
        val mergedMetadata = table.mergedMetadata(domain.metadata)
        val dagConfigRef = mergedMetadata.dagRef
          .orElse(settings.appConfig.dagRef.flatMap(_.load))
        val schedule = mergedMetadata.schedule

        dagConfigRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${domain.name}.${table.name}. Dag configs found ${dagConfigs.keys
                  .mkString(",")}"
            )
          )
          TableWithDagConfig(domain, table, dagRef, dagConfig, schedule)
        }
      }
    }
    tableWithDagConfigAndSchedule
  }

  case class TaskWithDagConfig(
    taskDesc: AutoTaskInfo,
    dagConfigName: String,
    dagConfig: DagInfo,
    schedule: Option[String]
  )

  private def taskWithDagConfigs(
    dagConfigs: Map[String, DagInfo],
    tags: Set[String]
  )(implicit settings: Settings): List[TaskWithDagConfig] = {
    logger.info("Starting to task generate dags")
    val tasks = schemaHandler.tasks()
    val taskWithDagConfigAndSchedule: List[TaskWithDagConfig] =
      tasks.filter(tags.isEmpty || _.tags.intersect(tags).nonEmpty).flatMap { taskWithDag =>
        val dagConfigRef = taskWithDag.dagRef
          .orElse(settings.appConfig.dagRef.flatMap(_.transform))
        val schedule = taskWithDag.schedule
        dagConfigRef.map { dagRef =>
          val dagConfig = dagConfigs.getOrElse(
            dagRef,
            throw new Exception(
              s"Could not find dag config $dagRef referenced in ${taskWithDag.name}. Dag config found ${dagConfigs.keys
                  .mkString(",")}"
            )
          )
          TaskWithDagConfig(taskWithDag, dagRef, dagConfig, schedule)
        }
      }
    taskWithDagConfigAndSchedule
  }

  def getScheduleName(schedule: String, currentScheduleIndex: Int): (String, Int) = {
    if (schedule.contains(" ")) {
      ("cron" + currentScheduleIndex.toString, currentScheduleIndex + 1)
    } else {
      (schedule, currentScheduleIndex)
    }
  }

  def orchestratorFromTemplate(template: String): String = {
    val pathIndex = template.lastIndexOf('/')
    val templateName =
      if (pathIndex > 0)
        template.substring(pathIndex + 1)
      else
        template
    val oIndex = templateName.indexOf("__")
    if (oIndex > 0) {
      templateName.substring(0, oIndex)
    } else
      "unknown"
  }

  private def allTaskDepsFullNames(deps: List[TaskViewDependencyNode]): List[String] = {
    val immediateDeps = deps.flatMap { dep =>
      if (dep.isTask()) List(dep.data.name) else Nil
    }
    immediateDeps ++
    deps.flatMap { dep =>
      allTaskDepsFullNames(dep.children)
    }.distinct // avoid duplicates
  }
  def generateTaskDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.build.toString + "/dags/")
    )

    val dagConfigs = schemaHandler.loadDagGenerationConfigs(instantiateVars = true)
    val taskConfigs = taskWithDagConfigs(dagConfigs, config.tags.toSet)
    val env = schemaHandler.activeEnvVars(root = Option(settings.appConfig.root))
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList
    val depsEngine = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
    val taskConfigsGroupedByFilename = taskConfigs
      .map { taskConfig =>
        val envVars =
          schemaHandler.activeEnvVars(root = Option(taskConfig.dagConfig.getSlRoot())) ++ Map(
            "table"         -> taskConfig.taskDesc.table,
            "domain"        -> taskConfig.taskDesc.domain,
            "name"          -> taskConfig.taskDesc.fullName(), // deprecated
            "task"          -> taskConfig.taskDesc.fullName(),
            "schedule"      -> taskConfig.schedule.getOrElse("None"),
            "sl_project_id" -> config.masterProjectId.getOrElse("-1")
          )
        val filename = Utils
          .parseJinja(
            taskConfig.dagConfig.filename,
            envVars
          )
          .replace("_-1", "")

        val comment = Utils.parseJinja(
          taskConfig.dagConfig.comment,
          envVars
        )
        val options =
          taskConfig.dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
        (
          filename,
          taskConfig
            .copy(dagConfig = taskConfig.dagConfig.copy(options = options, comment = comment))
        )
      }
      .groupBy { case (filename, _) => filename }
    taskConfigsGroupedByFilename
      .foreach { case (filename, taskConfigs) =>
        val headConfig = taskConfigs.head match { case (_, config) => config }
        val dagConfig = headConfig.dagConfig
        val dagTemplateName = dagConfig.template
        val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
        val cron = settings.appConfig.schedulePresets.getOrElse(
          headConfig.schedule.getOrElse("None"),
          headConfig.schedule.getOrElse("None")
        )
        val cronIfNone = if (cron == "None") null else cron
        val configs = taskConfigs.map { case (_, config) => config.taskDesc.fullName() }
        val autoTaskDepsConfig = AutoTaskDependenciesConfig(tasks = Some(configs))
        val deps = depsEngine.jobsDependencyTree(autoTaskDepsConfig)
        val orchestratorName = orchestratorFromTemplate(dagConfig.template).toLowerCase()
        val nativeOrchestrator = Set("snowflake", "databricks").contains(orchestratorName)

        val depsFullNames = allTaskDepsFullNames(deps)
        val configFullNames = taskConfigs.map { case (_, config) => config.taskDesc.fullName() }

        val allTaskFullNames = depsFullNames ++ configFullNames
        val allTaskDeps = allTaskFullNames
          .map(_.toLowerCase())
          .distinct
          .flatMap { taskFullName =>
            schemaHandler.taskOnly(taskFullName).toOption
          }

        val taskStatements: List[AutoTaskQueries] =
          if (nativeOrchestrator) {
            allTaskDeps.map { taskDep =>
              val transformContext = TransformContext(
                appId = None,
                taskDesc = taskDep,
                commandParameters = Map.empty,
                interactive = None,
                truncate = false,
                test = false,
                logExecution = false,
                accessToken = None,
                resultPageSize = 1,
                resultPageNumber = 1,
                dryRun = false,
                scheduledDate = None, // No scheduled date for task dags
                syncSchema = false
              )(settings, settings.storageHandler(), schemaHandler)
              val task = transformContext.toTask(taskDep.getRunEngine())
              Try {
                AutoTaskQueries(task)
              } match {
                case scala.util.Success(value) => value
                case scala.util.Failure(exception) =>
                  throw new Exception(
                    s"Failed to build statements for task ${task.name}: ${exception.getMessage}",
                    exception
                  )
              }
            }
          } else {
            Nil
          }

        val context = TransformDagGenerationContext(
          config = dagConfig,
          deps = deps,
          cron = Option(cronIfNone),
          statements = taskStatements
        )

        applyJ2AndSave(
          outputDir,
          jEnv,
          dagTemplateContent,
          optionsWithProjectIdAndName(config, context.asMap()),
          filename
        )
      }
  }

  private def airflowAccessControl(projectId: Long): String = {
    require(projectId > 0)

    // To reduce memory footprint in the SaaS version, we access global variables
    // from the application configuration object instead of the user session
    // configuration object.
    val viewerConfig = Settings.referenceConfig.getString("dagAccess.airflow.viewer")
    val userConfig = Settings.referenceConfig.getString("dagAccess.airflow.user")
    val opsConfig = Settings.referenceConfig.getString("dagAccess.airflow.ops")
    val result =
      s"""{
         |  "SL_${projectId}_VIEWER": $viewerConfig,
         |  "SL_${projectId}_USER": $userConfig,
         |  "SL_${projectId}_OP": $opsConfig
         |}
         |""".stripMargin
    result
  }

  private def optionsWithProjectIdAndName(
    config: DagGenerateConfig,
    options: util.HashMap[String, Object]
  ): util.HashMap[String, Object] = {
    config.masterProjectId match {
      case Some(masterProjectId) =>
        options.put("sl_project_id", masterProjectId)
        options.put("sl_project_name", config.masterProjectName.getOrElse("[noname]"))
        if (config.includeRoles)
          options.put("sl_airflow_access_control", airflowAccessControl(masterProjectId.toLong))
        else
          options.put("sl_airflow_access_control", "None")
      case None =>
        options.put("sl_project_id", "-1")
        options.put("sl_project_name", "[noname]")
        options.put("sl_airflow_access_control", "None")
    }
    options
  }

  def clean(dir: Option[String])(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      dir.getOrElse(DatasetArea.build.toString + "/dags/")
    )
    settings.storageHandler().mkdirs(outputDir)
    logger.info(s"Cleaning output directory $outputDir")
    settings.storageHandler().delete(outputDir)
  }

  def publishDags(folder: String)(implicit settings: Settings): Unit = {
    val outputDir = new Path(folder)
    val dagBuildDir = new Path(DatasetArea.build, "dags")
    val dagFiles =
      settings
        .storageHandler()
        .list(
          path = dagBuildDir,
          recursive = true
        )
    dagFiles.foreach { file =>
      val fileName = file.path.getName
      val newPath = new Path(outputDir, fileName)
      settings.storageHandler().move(file.path, newPath)
    }
  }

  def generateDomainDags(
    config: DagGenerateConfig
  )(implicit settings: Settings): Unit = {
    val outputDir = new Path(
      config.outputDir.getOrElse(DatasetArea.build.toString + "/dags/")
    )

    val dagConfigs = schemaHandler.loadDagGenerationConfigs(instantiateVars = true)
    val tableConfigs = tableWithDagConfigs(dagConfigs, config.tags.toSet)
    val groupedDags = groupByDagConfigScheduleDomain(tableConfigs)

    val env = schemaHandler.activeEnvVars(root = Some(settings.appConfig.root))
    val jEnv = env.map { case (k, v) =>
      DagPair(k, v)
    }.toList

    groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
      val dagConfig = dagConfigs(dagConfigName)
      val errors = dagConfig.checkValidity().filter(_.severity == Severity.Error)
      errors.foreach { error =>
        logger.error(error.toString())
      }
      if (errors.nonEmpty) {
        throw new Exception(s"Dag config ${dagConfigName} is invalid: ${errors.mkString(",")}")
      }
      val dagTemplateName = dagConfig.template
      val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
      val filenameVars = dagConfig.getfilenameVars()
      if (filenameVars.exists(List("table", "finalTable", "renamedTable").contains)) {
        // filename: "{{domain}}_{{table}}.py"
        if (!filenameVars.exists(List("domain", "finalDomain", "renamedDomain").contains))
          logger.warn(
            s"Dag Config $dagConfigName: filename contains table but not domain, this will generate multiple dags with the same name if the same table name appear in multiple domains"
          )
        // one dag per table
        var scheduleIndex = 1
        groupedBySchedule.foreach { case (schedule, groupedByDomain) =>
          groupedByDomain.foreach { case (domain, tables) =>
            tables.foreach { table =>
              val rawDomains = CaseClassToPojoConverter.asJava(domain.copy(tables = List(table)))
              val dagDomain = DagDomain(
                domain.name,
                domain.finalName,
                java.util.List.of[TableDomain](TableDomain(table.name, table.finalName))
              )
              val (scheduleName, nextScheduleIndex) = getScheduleName(schedule, scheduleIndex)
              scheduleIndex = nextScheduleIndex
              val cronValue = settings.appConfig.schedulePresets.getOrElse(
                scheduleName,
                schedule
              )
              val cronIfNone = if (cronValue == "None") null else cronValue
              val schedules =
                List(
                  DagSchedule(
                    scheduleName,
                    cronIfNone,
                    java.util.List.of[DagDomain](dagDomain),
                    java.util.List.of(rawDomains)
                  )
                )

              val envVars =
                schemaHandler.activeEnvVars(root = Option(dagConfig.getSlRoot())) ++ Map(
                  "schedule"      -> scheduleName,
                  "domain"        -> domain.name,
                  "finalDomain"   -> domain.finalName,
                  "renamedDomain" -> domain.finalName,
                  "table"         -> table.name,
                  "finalTable"    -> table.finalName,
                  "renamedTable"  -> table.finalName,
                  "rawDomains"    -> rawDomains,
                  "sl_project_id" -> config.masterProjectId.getOrElse("-1")
                )
              val options = dagConfig.options.map { case (k, v) =>
                k -> Utils.parseJinja(v, envVars)
              }
              val comment = Utils.parseJinja(dagConfig.comment, envVars)
              val filename = Utils
                .parseJinja(dagConfig.filename, envVars)
                .replace("_-1", "")
              // we remove the reference to the project id if we are using starlake core only
              val orchestratorName = orchestratorFromTemplate(dagConfig.template).toLowerCase()
              val nativeOrchestrator = Set("snowflake", "databricks").contains(orchestratorName)
              if (nativeOrchestrator) {
                val statements =
                  new DummyIngestionJob(
                    domain = domain,
                    schema = table,
                    types = schemaHandler.types(),
                    path = Nil,
                    storageHandler = settings.storageHandler(),
                    schemaHandler = schemaHandler,
                    options = options,
                    accessToken = None,
                    test = false,
                    scheduledDate = None // No scheduled date for domain dags
                  ).buildListOfSQLStatementsAsMap(orchestratorName)
                val context = LoadDagGenerationContext(
                  config = dagConfig.copy(options = options, comment = comment),
                  schedules,
                  workflowStatementsIn = List(statements)
                )
                applyJ2AndSave(
                  outputDir,
                  jEnv,
                  dagTemplateContent,
                  optionsWithProjectIdAndName(config, context.asMap()),
                  filename
                )
              } else {
                val context = LoadDagGenerationContext(
                  config = dagConfig.copy(options = options, comment = comment),
                  schedules,
                  workflowStatementsIn = List(Map.empty)
                )
                applyJ2AndSave(
                  outputDir,
                  jEnv,
                  dagTemplateContent,
                  optionsWithProjectIdAndName(config, context.asMap()),
                  filename
                )
              }
            }
          }
        }
      } else if (filenameVars.exists(List("domain", "finalDomain", "renamedDomain").contains)) {
        // filename: "{{domain}}.py"

        // one dag per domain
        val domains = groupedBySchedule
          .flatMap { case (schedule, groupedByDomain) =>
            groupedByDomain.keys
          }
          .toList
          .sortBy(_.name)
        var scheduleIndex = 1
        domains.foreach { domain =>
          val schedules = groupedBySchedule
            .flatMap { case (schedule, groupedByDomain) =>
              val tablesOfDomain = groupedByDomain.get(domain)
              tablesOfDomain.map { tables =>
                val rawDomains = CaseClassToPojoConverter.asJava(domain.copy(tables = tables))
                val dagDomain = DagDomain(
                  domain.name,
                  domain.finalName,
                  tables.map(t => TableDomain(t.name, t.finalName)).asJava
                )
                val (scheduleName, nextScheduleIndex) = getScheduleName(schedule, scheduleIndex)
                scheduleIndex = nextScheduleIndex
                val cronValue = settings.appConfig.schedulePresets.getOrElse(
                  scheduleName,
                  schedule
                )
                val cronIfNone = if (cronValue == "None") null else cronValue
                DagSchedule(
                  scheduleName,
                  cronIfNone,
                  java.util.List.of[DagDomain](dagDomain),
                  java.util.List.of(rawDomains)
                )
              }
            }
            .toList
            .sortBy(_.schedule)
          val rawDomain = CaseClassToPojoConverter.asJava(domain)
          if (filenameVars.contains("schedule")) {
            var scheduleIndex = 1
            schedules.foreach { schedule =>
              val (scheduleValue, nextScheduleIndex) =
                getScheduleName(schedule.schedule, scheduleIndex)
              scheduleIndex = nextScheduleIndex
              val envVars =
                schemaHandler.activeEnvVars(root = Option(dagConfig.getSlRoot())) ++ Map(
                  "schedule"      -> scheduleValue,
                  "domain"        -> domain.name,
                  "renamedDomain" -> domain.finalName,
                  "finalDomain"   -> domain.finalName,
                  "rawDomain"     -> rawDomain,
                  "sl_project_id" -> config.masterProjectId.getOrElse("-1")
                )
              val options = dagConfig.options.map { case (k, v) =>
                k -> Utils.parseJinja(v, envVars)
              }
              val comment = Utils.parseJinja(dagConfig.comment, envVars)

              val context = LoadDagGenerationContext(
                config = dagConfig.copy(options = options, comment = comment),
                schedules,
                workflowStatementsIn = List.empty[Map[String, Object]]
              )
              val filename = Utils
                .parseJinja(dagConfig.filename, envVars)
                .replace("_-1", "")
              applyJ2AndSave(
                outputDir,
                jEnv,
                dagTemplateContent,
                optionsWithProjectIdAndName(config, context.asMap()),
                filename
              )
            }
          } else {
            val envVars =
              schemaHandler.activeEnvVars(root = Option(dagConfig.getSlRoot())) ++ Map(
                "domain"        -> domain.name,
                "renamedDomain" -> domain.finalName,
                "finalDomain"   -> domain.finalName,
                "rawDomain"     -> rawDomain,
                "sl_project_id" -> config.masterProjectId.getOrElse("-1")
              )
            val options = dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
            val comment = Utils.parseJinja(dagConfig.comment, envVars)
            val context = LoadDagGenerationContext(
              config = dagConfig.copy(options = options, comment = comment),
              schedules,
              workflowStatementsIn = List.empty[Map[String, Object]]
            )
            val filename = Utils
              .parseJinja(dagConfig.filename, envVars)
              .replace("_-1", "")
            applyJ2AndSave(
              outputDir,
              jEnv,
              dagTemplateContent,
              optionsWithProjectIdAndName(config, context.asMap()),
              filename
            )
          }
        }
      } else {
        // one dag per config
        groupedDags.foreach { case (dagConfigName, groupedBySchedule) =>
          var scheduleIndex = 1
          val dagConfig = dagConfigs(dagConfigName)
          val dagTemplateName = dagConfig.template
          val dagTemplateContent = new Yml2DagTemplateLoader().loadTemplate(dagTemplateName)
          val dagSchedules = groupedBySchedule
            .map { case (schedule, groupedByDomain) =>
              val (scheduleName, nextScheduleIndex) =
                getScheduleName(schedule, scheduleIndex)
              scheduleIndex = nextScheduleIndex
              val (dagDomains, rawDomains) = groupedByDomain
                .map { case (domain, tables) =>
                  DagDomain(
                    domain.name,
                    domain.finalName,
                    tables.map(t => TableDomain(t.name, t.finalName)).asJava
                  ) -> CaseClassToPojoConverter.asJava(domain.copy(tables = tables))
                }
                .toList
                .sortBy { case (d, r) => d.name }
                .unzip
              val cronValue = settings.appConfig.schedulePresets.getOrElse(
                scheduleName,
                schedule
              )
              val cronIfNone = if (cronValue == "None") null else cronValue
              DagSchedule(scheduleName, cronIfNone, dagDomains.asJava, rawDomains.asJava)
            }
            .toList
            .sortBy(_.schedule)
          if (filenameVars.contains("schedule")) {
            dagSchedules.foreach { schedule =>
              val envVars =
                schemaHandler.activeEnvVars(root = Option(dagConfig.getSlRoot())) ++ Map(
                  "schedule"      -> schedule.schedule,
                  "sl_project_id" -> config.masterProjectId.getOrElse("-1")
                )
              val options = dagConfig.options.map { case (k, v) =>
                k -> Utils.parseJinja(v, envVars)
              }
              val comment = Utils.parseJinja(dagConfig.comment, envVars)
              val context = LoadDagGenerationContext(
                config = dagConfig.copy(options = options, comment = comment),
                List(schedule),
                workflowStatementsIn = List.empty[Map[String, Object]]
              )
              val filename = Utils
                .parseJinja(dagConfig.filename, envVars)
                .replace("_-1", "")
              applyJ2AndSave(
                outputDir,
                jEnv,
                dagTemplateContent,
                optionsWithProjectIdAndName(config, context.asMap()),
                filename
              )
            }
          } else {
            val envVars =
              schemaHandler.activeEnvVars(root = Option(dagConfig.getSlRoot())) ++ Map(
                "sl_project_id" -> config.masterProjectId.getOrElse("-1")
              )
            val options = dagConfig.options.map { case (k, v) => k -> Utils.parseJinja(v, envVars) }
            val comment = Utils.parseJinja(dagConfig.comment, envVars)
            val context = LoadDagGenerationContext(
              config = dagConfig.copy(options = options, comment = comment),
              schedules = dagSchedules,
              workflowStatementsIn = List.empty[Map[String, Object]]
            )
            val filename = Utils
              .parseJinja(dagConfig.filename, envVars)
              .replace("_-1", "")
            applyJ2AndSave(
              outputDir,
              jEnv,
              dagTemplateContent,
              optionsWithProjectIdAndName(config, context.asMap()),
              filename
            )
          }

        }
      }
    }
  }

  private def applyJ2AndSave(
    outputDir: Path,
    jEnv: List[DagPair],
    dagTemplateContent: String,
    context: util.HashMap[String, Object],
    filename: String
  )(implicit settings: Settings): Unit = {
    val context2 = context.clone().asInstanceOf[util.HashMap[String, Object]]

    DagInfo.externalKeys.foreach { key =>
      context2.remove(key)
    }
    val json = JsonSerializer.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(context2)

    val paramMap = Map(
      "context"     -> context,
      "env"         -> jEnv.asJava,
      "json"        -> json,
      "pyjson"      -> json.replace("\\", "\\\\"),
      "sl_datasets" -> settings.appConfig.datasets,
      "sl_env"      -> settings.appConfig.env,
      "sl_root"     -> settings.appConfig.root
    )
    val jinjaOutput = Utils.parseJinjaTpl(dagTemplateContent, paramMap)
    val dagPath = new Path(outputDir, filename)
    logger.info(s"Writing dag to $dagPath")
    settings.storageHandler().write(jinjaOutput, dagPath)(StandardCharsets.UTF_8)
  }

  /** group tables by dag config name, schedule and domain
    *
    * @return
    *   Map[dagConfigName, Map[schedule, Map[domainName, List[tableName]]]]
    */
  private def groupByDagConfigScheduleDomain(
    tableWithDagConfigs: List[TableWithDagConfig]
  ): Map[String, Map[String, Map[DomainInfo, List[SchemaInfo]]]] = {
    val groupByDagConfigName = tableWithDagConfigs.groupBy(_.dagConfigName)
    val groupDagConfigNameAndSchedule = groupByDagConfigName.map {
      case (dagConfigName, tableWithDagConfigs) =>
        val groupedBySchedule = tableWithDagConfigs.groupBy(_.schedule.getOrElse("None"))
        val groupedByScheduleAndDomain = groupedBySchedule.map {
          case (scheduleName, tableWithDagConfigs) =>
            val groupedByDomain = tableWithDagConfigs.groupBy(_.domain)
            val groupedTableNames = groupedByDomain.map { case (domain, tableWithConfig) =>
              domain -> tableWithConfig.map(_.table).sortBy(_.name)
            }
            scheduleName -> groupedTableNames
        }
        (dagConfigName, groupedByScheduleAndDomain)
    }
    groupDagConfigNameAndSchedule
  }

  def normalizeDagNames(config: DagGenerateConfig)(implicit settings: Settings): List[Path] = {
    config.masterProjectId match {
      case Some(projectId) =>
        val outputDir = new Path(
          config.outputDir.getOrElse(throw new StarlakeConfigException("outputDir is required"))
        )
        val dagFiles =
          settings
            .storageHandler()
            .list(
              path = outputDir,
              extension = ".py",
              exclude = Some("_.*".r.pattern),
              recursive = false
            )
        /*
        dagFiles.map { file =>
          val fileName = file.path.getName
          val newFileName = s"SL_${projectId}_$fileName"
          val newPath =
            new Path(
              file.path.getParent,
              newFileName.toLowerCase()
            ) // should be lowercase like dag ids
          settings.storageHandler().move(file.path, newPath)
          newPath
        }
         */
        dagFiles.map(_.path)
      case None =>
        Nil

    }
  }
}

object DagGenerateJob {
  val SCHEDULE = "schedule"
  val name = "generate"
}
