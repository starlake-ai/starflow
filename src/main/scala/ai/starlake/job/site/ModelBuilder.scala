package ai.starlake.job.site

import ai.starlake.buildinfo.BuildInfo
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.transform.TransformContext
import ai.starlake.lineage.ColLineage
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.time.Instant
import scala.collection.immutable.TreeMap
import scala.util.{Failure, Success, Try}

class ModelBuilder(
  schemaHandler: SchemaHandler,
  compileSql: AutoTaskInfo => Try[String],
  colLineage: AutoTaskInfo => Try[Option[ColLineage.Lineage]]
)(implicit settings: Settings)
    extends LazyLogging {

  private val storage = settings.storageHandler()

  def build(): SiteModel = {
    val (docBlocks, docWarnings) =
      DocBlocks.load(storage, new Path(DatasetArea.metadata, "docs"))
    docWarnings.foreach(w => logger.warn(w))

    val domains = schemaHandler.domains(reload = true).sortBy(_.finalName)
    val jobs = schemaHandler.jobs(reload = true).sortBy(_.name)

    val loadNodes = domains.flatMap(d => d.tables.map(t => loadNode(d, t, docBlocks)))
    val tasks = jobs.flatMap(j => j.tasks.map(t => j -> t))
    val transformNodes = tasks.map { case (_, t) => transformNode(t, docBlocks) }
    val declared = TreeMap((loadNodes ++ transformNodes).map(n => n.id -> n): _*)

    val streams = schemaHandler.streams()
    val autoTasks = Try(
      ai.starlake.job.transform.AutoTask
        .unauthenticatedTasks(reload = false)(settings, storage, schemaHandler)
    ) match {
      case Success(tasks) => tasks
      case Failure(e) =>
        logger.warn(s"Table lineage unavailable, could not load tasks: ${e.getMessage}")
        Nil
    }
    val (taskDeps, depWarnings) = ModelBuilder.extractTaskDeps(
      autoTasks.map(at => NodeId.transform(at.taskDesc.fullName()) -> Try(at.dependencies(streams)))
    )

    val lineageWarnings = scala.collection.mutable.Map.empty[String, List[String]]
    val colLineages: Map[String, ColLineage.Lineage] = tasks.flatMap { case (_, t) =>
      val id = NodeId.transform(t.fullName())
      colLineage(t) match {
        case Success(Some(l)) => Some(id -> l)
        case Success(None)    => None
        case Failure(e) =>
          lineageWarnings(id) = List(s"Column lineage failed: ${e.getMessage}")
          None
      }
    }.toMap

    val lineage = LineageBuilder.build(declared, taskDeps, colLineages)
    val nodes = lineage.nodes.map { case (id, n) =>
      id -> n.copy(warnings =
        n.warnings ++ depWarnings.getOrElse(id, Nil) ++ lineageWarnings.getOrElse(id, Nil)
      )
    }

    SiteModel(
      schemaVersion = 1,
      generatedAt = Instant.now().toString,
      starlakeVersion = BuildInfo.version,
      project = SiteProject(
        name = new Path(settings.appConfig.root).getName,
        env = settings.appConfig.env,
        defaultConnection = settings.appConfig.connectionRef,
        overview = docBlocks.blocks.get("__overview__").map { b =>
          val (text, ws) = docBlocks.resolve(b)
          ws.foreach(w => logger.warn(w))
          text
        }
      ),
      nodes = nodes,
      edges = lineage.edges.sortBy(e => (e.kind, e.from, e.to)),
      columnEdges = lineage.columnEdges.sortBy(e => (e.task, e.from, e.to)),
      groups = groups(domains, jobs, nodes, docBlocks),
      docs = TreeMap(docBlocks.blocks.toSeq: _*)
    )
  }

  // ---------- nodes ----------

  private def describe(text: Option[String], docs: DocBlocks): (Option[String], List[String]) =
    text match {
      case None => (None, Nil)
      case Some(t) =>
        val (resolved, warnings) = docs.resolve(t)
        (Some(resolved), warnings)
    }

  private def engineOf(connectionRef: Option[String]): Option[String] =
    connectionRef.flatMap(ref =>
      Try(settings.appConfig.connection(ref).map(_.getJdbcEngineName().toString)).toOption.flatten
    )

  private def sqlTypeOf(attr: TableAttribute, engine: Option[String]): Option[String] =
    engine.flatMap { key =>
      Try(attr.ddlMapping(isPrimaryKey = false, key, schemaHandler)).toOption.collect {
        case leaf: DDLLeaf => leaf.tpe
      }
    }

  private def column(
    attr: TableAttribute,
    pk: List[String],
    engine: Option[String],
    docs: DocBlocks
  ): (SiteColumn, List[String]) = {
    val (desc, w) = describe(attr.comment, docs)
    val children = attr.attributes.map(a => column(a, Nil, engine, docs))
    (
      SiteColumn(
        name = attr.name,
        rename = attr.rename,
        `type` = attr.`type`,
        sqlType = if (attr.attributes.isEmpty) sqlTypeOf(attr, engine) else None,
        required = attr.resolveRequired(),
        primaryKey = pk.exists(_.equalsIgnoreCase(attr.getFinalName())),
        foreignKey = attr.foreignKey,
        privacy = attr.privacy.map(_.toString).filterNot(_.equalsIgnoreCase("none")),
        script = attr.script,
        default = attr.default,
        metricType = attr.metricType.map(_.value),
        description = desc,
        tags = attr.tags.toList.sorted,
        columns = children.map(_._1)
      ),
      w ++ children.flatMap(_._2)
    )
  }

  private def siteRls(r: RowLevelSecurity, docs: DocBlocks): (SiteRls, List[String]) = {
    val (desc, w) = docs.resolve(Option(r.description).getOrElse(""))
    (SiteRls(r.name, r.predicate, r.grants.toList.sorted, desc), w)
  }

  private def existingPath(candidates: String*): Option[String] =
    candidates.find(rel =>
      Try(storage.exists(new Path(DatasetArea.metadata, rel))).getOrElse(false)
    )

  private def dagName(ref: String): String =
    ref.split('/').last.stripSuffix(".sl.yml").stripSuffix(".yml")

  private def loadNode(domain: DomainInfo, table: SchemaInfo, docs: DocBlocks): SiteNode = {
    val merged = table.mergedMetadata(domain.metadata)
    val connectionRef = Try(merged.getSinkConnectionRef()).toOption
    val engine = engineOf(connectionRef)
    val (desc, descWarnings) = describe(table.comment, docs)
    val cols = table.attributes.map(a => column(a, table.primaryKey, engine, docs))
    val rls = table.rls.map(r => siteRls(r, docs))
    val dag = table.metadata
      .flatMap(_.dagRef)
      .orElse(domain.metadata.flatMap(_.dagRef))
      .orElse(settings.appConfig.dagRef.flatMap(_.load))
      .map(dagName)
    SiteNode(
      id = NodeId.load(domain.finalName, table.finalName),
      kind = "load",
      name = table.finalName,
      domain = domain.finalName,
      table = table.finalName,
      database = domain.database,
      connectionRef = connectionRef,
      engine = engine,
      description = desc,
      tags = table.tags.toList.sorted,
      filePath = existingPath(s"load/${domain.name}/${table.name}.sl.yml"),
      columns = cols.map(_._1),
      expectations = table.expectations.map(e => SiteExpectation(e.expect, e.failOnError)),
      acl = table.acl.map(a => SiteAcl(a.role, a.grants.toList.sorted)),
      rls = rls.map(_._1),
      sink = merged.sink,
      dag = dag,
      primaryKey = table.primaryKey,
      presql = table.presql,
      postsql = table.postsql,
      writeStrategy = merged.writeStrategy,
      pattern = Some(table.pattern.pattern()),
      patternSample = table.patternSample,
      metadata = Some(merged),
      schedule = merged.schedule,
      freshness = merged.freshness,
      warnings = (descWarnings ++ cols.flatMap(_._2) ++ rls.flatMap(_._2)).distinct
    )
  }

  private def transformNode(task: AutoTaskInfo, docs: DocBlocks): SiteNode = {
    val connectionRef = Try(task.getRunConnectionRef()).toOption
    val engine = engineOf(connectionRef)
    val (desc, descWarnings) = describe(task.comment, docs)
    val cols = task.attributes.map(a => column(a, task.primaryKey, engine, docs))
    val rls = task.rls.map(r => siteRls(r, docs))
    val (compiled, compileWarnings) = task.sql match {
      case None => (None, Nil)
      case Some(_) =>
        compileSql(task) match {
          case Success(sql) => (Some(sql), Nil)
          case Failure(e)   => (None, List(s"SQL compilation failed: ${e.getMessage}"))
        }
    }
    val dag = task.dagRef.orElse(settings.appConfig.dagRef.flatMap(_.transform)).map(dagName)
    SiteNode(
      id = NodeId.transform(task.fullName()),
      kind = "transform",
      name = task.name,
      domain = task.domain,
      table = task.table,
      database = task.database,
      connectionRef = connectionRef,
      engine = engine,
      description = desc,
      tags = task.tags.toList.sorted,
      filePath = existingPath(
        s"transform/${task.domain}/${task.name}.sl.yml",
        s"transform/${task.domain}/${task.name}.sql",
        s"transform/${task.domain}/${task.name}.py"
      ),
      columns = cols.map(_._1),
      expectations = task.expectations.map(e => SiteExpectation(e.expect, e.failOnError)),
      acl = task.acl.map(a => SiteAcl(a.role, a.grants.toList.sorted)),
      rls = rls.map(_._1),
      sink = task.sink,
      dag = dag,
      primaryKey = task.primaryKey,
      presql = task.presql,
      postsql = task.postsql,
      writeStrategy = task.writeStrategy,
      sql = task.sql.map(raw => SiteSql(Some(raw), compiled)),
      python = task.python.map(_.toString),
      freshness = task.freshness,
      schedule = task.schedule,
      warnings =
        (descWarnings ++ cols.flatMap(_._2) ++ rls.flatMap(_._2) ++ compileWarnings).distinct
    )
  }

  // ---------- groups ----------

  private def groups(
    domains: List[DomainInfo],
    jobs: List[AutoJobInfo],
    nodes: TreeMap[String, SiteNode],
    docs: DocBlocks
  ): SiteGroups = {
    def resolved(owner: String, text: Option[String]): Option[String] = {
      val (desc, warnings) = describe(text, docs)
      warnings.foreach(w => logger.warn(s"$owner: $w"))
      desc
    }
    val domainGroups = domains.map { d =>
      SiteGroup(
        d.finalName,
        resolved(s"Domain ${d.finalName}", d.comment),
        d.tables.map(t => NodeId.load(d.finalName, t.finalName)).sorted
      )
    }
    val jobGroups = jobs.map { j =>
      SiteGroup(
        j.name,
        resolved(s"Job ${j.name}", j.comment),
        j.tasks.map(t => NodeId.transform(t.fullName())).sorted
      )
    }
    val dagConfigs = Try(schemaHandler.loadDagGenerationConfigs(instantiateVars = false))
      .getOrElse(Map.empty)
      .map { case (k, v) => dagName(k) -> v }
    val byDag = nodes.values.flatMap(n => n.dag.map(_ -> n.id)).groupBy(_._1)
    val dagGroups = (dagConfigs.keySet ++ byDag.keySet).toList.sorted.map { name =>
      val info = dagConfigs.get(name)
      SiteDagGroup(
        name,
        info.map(_.template).getOrElse(""),
        info.flatMap(i => resolved(s"DAG $name", Option(i.comment))).getOrElse(""),
        byDag.getOrElse(name, Nil).map(_._2).toList.sorted
      )
    }
    SiteGroups(domainGroups, jobGroups, dagGroups)
  }
}

object ModelBuilder {

  /** Splits per-task dependency results into the dependency map and per-node warnings. */
  private[site] def extractTaskDeps(
    results: List[(String, Try[List[String]])]
  ): (Map[String, List[String]], Map[String, List[String]]) = {
    val deps = results.collect { case (id, Success(d)) => id -> d }.toMap
    val warnings = results.collect { case (id, Failure(e)) =>
      id -> List(s"Table lineage failed: ${e.getMessage}")
    }.toMap
    (deps, warnings)
  }

  def defaultCompiler(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): AutoTaskInfo => Try[String] = taskDesc =>
    Try {
      val context = TransformContext(
        appId = None,
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = false,
        logExecution = false,
        accessToken = None,
        resultPageSize = 200,
        resultPageNumber = 1,
        dryRun = true,
        scheduledDate = None,
        syncSchema = false
      )(settings, settings.storageHandler(), schemaHandler)
      context.toTask(taskDesc.getRunEngine()).buildAllSQLQueriesMerged(None, Some(true))
    }

  def defaultColLineage(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): AutoTaskInfo => Try[Option[ColLineage.Lineage]] = taskDesc =>
    Try {
      taskDesc.sql.flatMap { sql =>
        new ColLineage(settings, schemaHandler).sqlColLineage(
          None,
          sql,
          taskDesc.domain,
          taskDesc.name,
          taskDesc.getRunConnection(),
          None
        )
      }
    }

  def apply(schemaHandler: SchemaHandler)(implicit settings: Settings): ModelBuilder =
    new ModelBuilder(
      schemaHandler,
      defaultCompiler(schemaHandler),
      defaultColLineage(schemaHandler)
    )
}
