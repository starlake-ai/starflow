package ai.starlake.job.site

import ai.starlake.config.Settings
import ai.starlake.core.utils.StringUtils
import ai.starlake.job.Main
import ai.starlake.lineage.*
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{
  AutoJobInfo,
  AutoTaskInfo,
  DomainInfo,
  SchemaInfo,
  TableAttribute,
  WriteStrategy
}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.{JinjaUtils, Utils}
import better.files.File
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}
import com.manticore.jsqlformatter.JSQLFormatter
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class SiteHandler(config: SiteConfig, schemaHandler: SchemaHandler)(implicit val settings: Settings)
    extends LazyLogging {

  def run(): Try[Unit] = Try {
    if (config.clean.getOrElse(false))
      config.outputPath.delete(swallowIOExceptions = true)
    config.outputPath.createDirectoryIfNotExists()

    val format = config.format.getOrElse("markdown").toLowerCase()

    if (format == "json") {
      buildJsonSite()
    } else {
      logger.info(
        s"Generating standalone site in ${config.outputPath.pathAsString}"
      )
      buildStandaloneSite()
    }

    if (Main.cliMode)
      println(s"Site generated in ${config.outputPath.pathAsString}")
  }

  // ===== Standalone site generation (Jinjava) =====

  private lazy val jinjaEngine: Jinjava = {
    val jConfig = JinjavaConfig
      .newBuilder()
      .withFailOnUnknownTokens(false)
      .withNestedInterpretationEnabled(false)
      .build()
    new Jinjava(jConfig)
  }

  private def buildStandaloneSite(): Unit = {
    val allDomains = schemaHandler.domains(reload = true).sortBy(_.finalName)
    val allJobs = schemaHandler.jobs(reload = true).sortBy(_.name)

    writeStaticAssets()
    buildStandaloneIndex(allDomains, allJobs)
    buildStandaloneDomains(allDomains, allJobs)
    buildStandaloneJobs(allDomains, allJobs)
  }

  private def writeStaticAssets(): Unit = {
    val cssContent = loadTemplateResource("style.css")
    (config.outputPath / "style.css").writeText(cssContent)
    val flowJs = loadTemplateResource("flow.js")
    (config.outputPath / "flow.js").writeText(flowJs)
  }

  private def loadTemplateResource(name: String): String = {
    val templateName = config.templateName.getOrElse("standalone")
    val resourcePath = s"/templates/site/$templateName/$name"
    val stream = getClass.getResourceAsStream(resourcePath)
    if (stream != null) {
      try scala.io.Source.fromInputStream(stream).mkString
      finally stream.close()
    } else {
      val file = File(templateName) / name
      if (file.exists) file.contentAsString
      else throw new IllegalArgumentException(s"Template resource not found: $resourcePath")
    }
  }

  private def renderJinja(
    templateContent: String,
    context: java.util.Map[String, Object]
  ): String = {
    jinjaEngine.render(templateContent, context)
  }

  private def ctx(entries: (String, Any)*): java.util.Map[String, Object] =
    JinjaUtils.ctx(entries: _*)

  // --- Navigation data builders ---

  private def domainNavData(domain: DomainInfo): Map[String, Any] = Map(
    "name"           -> domain.finalName,
    "normalizedName" -> StringUtils.replaceNonAlphanumericWithUnderscore(domain.finalName),
    "comment"        -> domain.comment.getOrElse(""),
    "tables"         -> domain.tables.sortBy(_.finalName).map(t => Map("name" -> t.finalName))
  )

  private def jobNavData(job: AutoJobInfo): Map[String, Any] = Map(
    "name"           -> job.name,
    "normalizedName" -> StringUtils.replaceNonAlphanumericWithUnderscore(job.name),
    "comment"        -> job.comment.getOrElse(""),
    "tasks"          -> job.tasks.map(t => Map("name" -> t.name))
  )

  private def attrData(attr: TableAttribute): Map[String, Any] = Map(
    "name"       -> attr.name,
    "rename"     -> attr.rename.getOrElse(""),
    "type"       -> attr.`type`,
    "comment"    -> attr.comment.getOrElse(""),
    "foreignKey" -> attr.foreignKey.getOrElse(""),
    "required"   -> attr.resolveRequired(),
    "default"    -> attr.default.getOrElse(""),
    "tags"       -> attr.tags.toList.sorted,
    "script"     -> attr.resolveScript(),
    "position"   -> attr.position.map(p => s"${p.first},${p.last}").getOrElse(""),
    "metricType" -> attr.metricType.map(_.value).getOrElse("")
  )

  private def writeStrategyData(ws: WriteStrategy): Map[String, Any] = Map(
    "type"        -> ws.`type`.map(_.toString).getOrElse(""),
    "key"         -> ws.key.mkString(", "),
    "timestamp"   -> ws.timestamp.getOrElse(""),
    "queryFilter" -> ws.queryFilter.getOrElse(""),
    "on"          -> ws.on.map(_.toString).getOrElse(""),
    "startTs"     -> ws.startTs.getOrElse(""),
    "endTs"       -> ws.endTs.getOrElse("")
  )

  // --- Index page ---

  private def buildStandaloneIndex(
    allDomains: List[DomainInfo],
    allJobs: List[AutoJobInfo]
  ): Unit = {
    val template = loadTemplateResource("index.j2")
    val context = ctx(
      "basePath"    -> "",
      "domains"     -> allDomains.map(domainNavData),
      "jobs"        -> allJobs.map(jobNavData),
      "totalTables" -> allDomains.flatMap(_.tables).size,
      "totalTasks"  -> allJobs.flatMap(_.tasks).size
    )
    val html = renderJinja(template, context)
    (config.outputPath / "index.html").writeText(html)
  }

  // --- Domain / Table pages ---

  private def buildStandaloneDomains(
    allDomains: List[DomainInfo],
    allJobs: List[AutoJobInfo]
  ): Unit = {
    val loadPath = config.outputPath / "load"
    loadPath.createDirectoryIfNotExists()
    allDomains.foreach { domain =>
      val normalizedName =
        StringUtils.replaceNonAlphanumericWithUnderscore(domain.finalName)
      val domainFolder = File(loadPath, normalizedName)
      domainFolder.createDirectoryIfNotExists()
      domain.tables.sortBy(_.finalName).foreach { table =>
        val tableFile = File(domainFolder, s"${table.finalName}.html")
        buildStandaloneTable(domain, table, tableFile, allDomains, allJobs)
      }
    }
  }

  private def buildStandaloneTable(
    domain: DomainInfo,
    table: SchemaInfo,
    outputFile: File,
    allDomains: List[DomainInfo],
    allJobs: List[AutoJobInfo]
  ): Unit = {
    val mapper = Utils.newJsonMapper()
    val tableFQN = s"${domain.finalName}.${table.finalName}"

    val relationsJson =
      try {
        val diagram = new TableDependencies(schemaHandler).relationsAsDiagram(
          TableDependenciesConfig(
            tables = Some(List(tableFQN)),
            related = true,
            reload = false
          )
        )
        if (diagram.items.nonEmpty) mapper.writeValueAsString(diagram) else ""
      } catch {
        case e: Exception =>
          logger.warn(
            s"Could not generate relations JSON for table ${table.finalName}: ${e.getMessage}"
          )
          ""
      }

    val aclJson =
      try {
        val diagram = new AclDependencies(schemaHandler).aclsAsDiagram(
          AclDependenciesConfig(tables = List(tableFQN), all = true)
        )
        if (diagram.items.nonEmpty) mapper.writeValueAsString(diagram) else ""
      } catch {
        case e: Exception =>
          logger.warn(s"Could not generate ACL JSON for table ${table.finalName}: ${e.getMessage}")
          ""
      }

    val template = loadTemplateResource("table.j2")
    val context = ctx(
      "basePath"      -> "../../",
      "pageTitle"     -> s"${table.finalName} - ${domain.finalName}",
      "relationsJson" -> relationsJson,
      "aclJson"       -> aclJson,
      "domain" -> Map(
        "name"           -> domain.finalName,
        "normalizedName" -> StringUtils.replaceNonAlphanumericWithUnderscore(domain.finalName)
      ),
      "table" -> Map(
        "name"          -> table.name,
        "finalName"     -> table.finalName,
        "comment"       -> table.comment.getOrElse("No description provided"),
        "pattern"       -> table.pattern.pattern(),
        "patternSample" -> table.patternSample.getOrElse(""),
        "tags"          -> table.tags.toList.sorted,
        "primaryKey"    -> table.primaryKey,
        "attributes"    -> table.attributes.map(attrData),
        "expectations"  -> table.expectations.map(_.asMap())
      ),
      "domains" -> allDomains.map(domainNavData),
      "jobs"    -> allJobs.map(jobNavData)
    )
    val html = renderJinja(template, context)
    outputFile.writeText(html)
  }

  // --- Job / Task pages ---

  private def buildStandaloneJobs(
    allDomains: List[DomainInfo],
    allJobs: List[AutoJobInfo]
  ): Unit = {
    val transformPath = config.outputPath / "transform"
    transformPath.createDirectoryIfNotExists()
    allJobs.foreach { job =>
      val normalizedName =
        StringUtils.replaceNonAlphanumericWithUnderscore(job.name)
      val jobFolder = File(transformPath, normalizedName)
      jobFolder.createDirectoryIfNotExists()
      job.tasks.foreach { task =>
        val taskFile = File(jobFolder, s"${task.name}.html")
        buildStandaloneTask(job, task, taskFile, allDomains, allJobs)
      }
    }
  }

  private def buildStandaloneTask(
    job: AutoJobInfo,
    task: AutoTaskInfo,
    outputFile: File,
    allDomains: List[DomainInfo],
    allJobs: List[AutoJobInfo]
  ): Unit = {
    val sql = task.sql.getOrElse("")
    val formattedSql = SQLUtils.format(sql, JSQLFormatter.OutputFormat.PLAIN)
    val pythonCode = task.python.getOrElse("")

    val mapper = Utils.newJsonMapper()
    val taskFQN = s"${job.name}.${task.name}"

    val lineageJson =
      try {
        val lineage = new ColLineage(settings, schemaHandler).colLineage(
          ColLineageConfig(task = task.fullName())
        )
        lineage.filter(_.tables.nonEmpty).map(l => mapper.writeValueAsString(l)).getOrElse("")
      } catch {
        case e: Exception =>
          logger.warn(s"Could not generate lineage for task ${task.name}: ${e.getMessage}")
          ""
      }

    val aclJson =
      try {
        val diagram = new AclDependencies(schemaHandler).aclsAsDiagram(
          AclDependenciesConfig(tables = List(taskFQN), all = true)
        )
        if (diagram.items.nonEmpty) mapper.writeValueAsString(diagram) else ""
      } catch {
        case e: Exception =>
          logger.warn(s"Could not generate ACL JSON for task ${task.name}: ${e.getMessage}")
          ""
      }

    val relationsJson =
      try {
        val diagram = new AutoTaskDependencies(settings, schemaHandler, settings.storageHandler())
          .jobAsDiagram(
            AutoTaskDependenciesConfig(tasks = Some(List(task.fullName())))
          )
        if (diagram.items.nonEmpty) mapper.writeValueAsString(diagram) else ""
      } catch {
        case e: Exception =>
          logger.warn(
            s"Could not generate relations JSON for task ${task.name}: ${e.getMessage}"
          )
          ""
      }

    val template = loadTemplateResource("task.j2")
    val context = ctx(
      "basePath"      -> "../../",
      "pageTitle"     -> s"${task.name} - ${job.name}",
      "relationsJson" -> relationsJson,
      "sql"           -> formattedSql,
      "python"        -> pythonCode.toString,
      "lineageJson"   -> lineageJson,
      "aclJson"       -> aclJson,
      "job" -> Map(
        "name"           -> job.name,
        "normalizedName" -> StringUtils.replaceNonAlphanumericWithUnderscore(job.name),
        "comment"        -> job.comment.getOrElse("")
      ),
      "task" -> Map(
        "name"          -> task.name,
        "tableName"     -> task.getTableName(),
        "domain"        -> task.domain,
        "comment"       -> task.comment.getOrElse("No description provided"),
        "tags"          -> task.tags.toList.sorted,
        "writeStrategy" -> task.writeStrategy.map(writeStrategyData).getOrElse(null),
        "attributes"    -> task.attributes.map(attrData),
        "expectations"  -> task.expectations.map(_.asMap())
      ),
      "domains" -> allDomains.map(domainNavData),
      "jobs"    -> allJobs.map(jobNavData)
    )
    val html = renderJinja(template, context)
    outputFile.writeText(html)
  }

  // ===== JSON site generation =====

  private def buildJsonSite(): Unit = {
    logger.info(s"Generating site in ${config.outputPath.pathAsString} in JSON format")
    val mapper = Utils.newJsonMapper().writerWithDefaultPrettyPrinter()
    val domains = schemaHandler.domains(reload = true)
    val tablesDir = File(config.outputPath, "tables")
    tablesDir.createDirectoryIfNotExists()
    mapper.writeValue(
      File(tablesDir, "domains.json").toJava,
      domains
    )
    domains.foreach { domain =>
      domain.tables.foreach { table =>
        mapper.writeValue(
          File(tablesDir, s"${domain.finalName}.${table.finalName}.json").toJava,
          table
        )
      }
    }
    val tablesAclDir = File(config.outputPath, "table-acls")
    tablesAclDir.createDirectoryIfNotExists()
    domains.foreach { domain =>
      domain.tables.foreach { table =>
        new AclDependencies(schemaHandler).aclsAsDiagram(
          AclDependenciesConfig(
            tables = List(s"${domain.finalName}.${table.finalName}"),
            outputFile =
              Some(File(tablesAclDir, s"${domain.finalName}.${table.finalName}-acl.json")),
            json = true,
            all = true
          )
        )
      }
    }

    val tablesRelationsDir = File(config.outputPath, "table-relations")
    tablesRelationsDir.createDirectoryIfNotExists()
    domains.foreach { domain =>
      domain.tables.foreach { table =>
        new TableDependencies(schemaHandler).relationsAsDiagram(
          TableDependenciesConfig(
            tables = Some(List(s"${domain.finalName}.${table.finalName}")),
            outputFile = Some(
              File(tablesRelationsDir, s"${domain.finalName}.${table.finalName}-relations.json")
            ),
            json = true,
            related = true
          )
        )
      }
    }

    val jobs = schemaHandler.jobs(reload = true)
    val tasksDir = File(config.outputPath, "tasks")
    tasksDir.createDirectoryIfNotExists()
    mapper.writeValue(
      File(tasksDir, "tasks.json").toJava,
      jobs.map(j => j.copy(tasks = Nil))
    )
    jobs.foreach { job =>
      job.tasks.foreach { task =>
        mapper.writeValue(
          File(tasksDir, s"${job.name}.${task.name}.json").toJava,
          task
        )
      }
    }

    val tasksLineageDir = File(config.outputPath, "tasks-lineage")
    tasksLineageDir.createDirectoryIfNotExists()
    jobs.foreach { job =>
      job.tasks.foreach { task =>
        val lineage = new ColLineage(settings, schemaHandler).colLineage(
          ColLineageConfig(task = task.fullName())
        )
        mapper.writeValue(
          File(tasksLineageDir, s"${task.fullName()}-lineage.json").toJava,
          lineage
        )
      }
    }
  }
}
