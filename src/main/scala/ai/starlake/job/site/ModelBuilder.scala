package ai.starlake.job.site

import ai.starlake.buildinfo.BuildInfo
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.transform.{AutoTask, TransformContext}
import ai.starlake.lineage.ColLineage
import ai.starlake.schema.handlers.{SchemaHandler, TableWithNameAndType}
import ai.starlake.schema.model._
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.SparkUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

import java.time.Instant
import scala.collection.immutable.TreeMap
import scala.util.{Failure, Success, Try}

class ModelBuilder(
  schemaHandler: SchemaHandler,
  compileSql: AutoTaskInfo => Try[String],
  colLineage: AutoTaskInfo => Try[Option[ColLineage.Lineage]],
  taskDeps: AutoTaskInfo => Try[List[String]]
)(implicit settings: Settings)
    extends LazyLogging {

  private val storage = settings.storageHandler()

  def build(): SiteModel = {
    val (docBlocks, docWarnings) =
      DocBlocks.load(storage, new Path(DatasetArea.metadata, "docs"))
    docWarnings.foreach(w => logger.warn(w))

    val domains = schemaHandler.domains(reload = true).sortBy(_.finalName)
    val jobs = schemaHandler.jobs(reload = true).sortBy(_.name)
    // externals() only: objects() also appends the audit tables, which are not declared
    val externals = schemaHandler.externals().sortBy(_.finalName)

    val loadNodes = domains.flatMap(d => d.tables.map(t => loadNode(d, t, docBlocks)))
    val externalNodes = externals.flatMap(d => d.tables.map(t => externalNode(d, t, docBlocks)))
    val tasks = jobs.flatMap(j => j.tasks.map(t => j -> t))
    val transformNodes = tasks.map { case (j, t) => transformNode(j, t, docBlocks) }
    val declared =
      TreeMap((loadNodes ++ externalNodes ++ transformNodes).map(n => n.id -> n): _*)

    val (deps, depWarnings) = ModelBuilder.extractTaskDeps(
      tasks.map { case (_, t) => NodeId.transform(t.fullName()) -> taskDeps(t) }
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

    val lineage = LineageBuilder.build(declared, deps, colLineages)
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
      groups = groups(domains, externals, jobs, nodes, docBlocks),
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
    engine.flatMap(ModelBuilder.declaredSqlType(attr, _, schemaHandler))

  private[site] def column(
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
        sqlType = if (ModelBuilder.hasLeafSqlType(attr, engine)) sqlTypeOf(attr, engine) else None,
        required = attr.resolveRequired(),
        primaryKey = pk.exists(_.equalsIgnoreCase(attr.getFinalName())),
        foreignKey = attr.foreignKey,
        privacy = attr.privacy.map(_.toString).filterNot(_.equalsIgnoreCase("none")),
        script = attr.script,
        default = attr.default,
        metricType = attr.metricType.map(_.value),
        description = desc,
        tags = attr.tags.toList.sorted,
        columns = children.map(_._1),
        ignore = if (attr.resolveIgnore()) Some(true) else None
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

  /** Path relative to the metadata folder, so no local absolute path reaches the site. */
  private def relativeToMetadata(path: Path): String = {
    val root = DatasetArea.metadata.toUri.getPath.stripSuffix("/") + "/"
    val full = path.toUri.getPath
    if (full.startsWith(root)) full.stripPrefix(root) else path.getName
  }

  private def dagName(ref: String): String =
    ref.split('/').last.stripSuffix(".sl.yml").stripSuffix(".yml")

  /** The fields a domain table carries whether Starlake loads it (metadata/load) or only reads it
    * (metadata/external): columns, description, connection, file path, security and expectations.
    * The connection is the one core writes or reads the table on: the sink connection, else the
    * load connection, else the default one (Metadata.getSinkConnectionRef).
    */
  private def domainTableNode(
    id: String,
    kind: String,
    area: String,
    domain: DomainInfo,
    table: SchemaInfo,
    docs: DocBlocks
  ): SiteNode = {
    val merged = table.mergedMetadata(domain.metadata)
    val connectionRef = Try(merged.getSinkConnectionRef()).toOption
    val engine = engineOf(connectionRef)
    val (desc, descWarnings) = describe(table.comment, docs)
    val cols = table.attributes.map(a => column(a, table.primaryKey, engine, docs))
    val rls = table.rls.map(r => siteRls(r, docs))
    SiteNode(
      id = id,
      kind = kind,
      name = table.finalName,
      domain = domain.finalName,
      table = table.finalName,
      database = domain.database,
      connectionRef = connectionRef,
      engine = engine,
      description = desc,
      tags = table.tags.toList.sorted,
      filePath = existingPath(s"$area/${domain.name}/${table.name}.sl.yml"),
      columns = cols.map(_._1),
      expectations = table.expectations.map(e => SiteExpectation(e.expect, e.failOnError)),
      acl = table.acl.map(a => SiteAcl(a.role, a.grants.toList.sorted)),
      rls = rls.map(_._1),
      primaryKey = table.primaryKey,
      warnings = (descWarnings ++ cols.flatMap(_._2) ++ rls.flatMap(_._2)).distinct
    )
  }

  private def loadNode(domain: DomainInfo, table: SchemaInfo, docs: DocBlocks): SiteNode = {
    val merged = table.mergedMetadata(domain.metadata)
    val dag = table.metadata
      .flatMap(_.dagRef)
      .orElse(domain.metadata.flatMap(_.dagRef))
      .orElse(settings.appConfig.dagRef.flatMap(_.load))
      .map(dagName)
    domainTableNode(
      NodeId.load(domain.finalName, table.finalName),
      "load",
      "load",
      domain,
      table,
      docs
    ).copy(
      sink = merged.sink,
      dag = dag,
      presql = table.presql,
      postsql = table.postsql,
      writeStrategy = merged.writeStrategy,
      pattern = Some(table.pattern.pattern()),
      patternSample = table.patternSample,
      metadata = Some(merged),
      schedule = merged.schedule,
      freshness = merged.freshness
    )
  }

  /** A table declared under metadata/external: documented like a load table, without what only
    * loading uses (pattern, write strategy, DAG, schedule).
    */
  private def externalNode(domain: DomainInfo, table: SchemaInfo, docs: DocBlocks): SiteNode =
    domainTableNode(
      NodeId.external(s"${domain.finalName}.${table.finalName}"),
      "external",
      "external",
      domain,
      table,
      docs
    ).copy(freshness = table.mergedMetadata(domain.metadata).freshness)

  private def transformNode(job: AutoJobInfo, task: AutoTaskInfo, docs: DocBlocks): SiteNode = {
    val connectionRef = Try(task.getRunConnectionRef()).toOption
    val engine = engineOf(connectionRef)
    val (desc, descWarnings) = describe(task.comment, docs)
    // Core creates the table on its sink connection, so declared types follow that engine.
    val sinkEngine = Try(task.getSinkConnectionRef()).toOption.flatMap(r => engineOf(Some(r)))
    val cols = task.attributes.map(a => column(a, task.primaryKey, sinkEngine, docs))
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
      filePath = {
        val prefix = if (task._filenamePrefix.isEmpty) task.name else task._filenamePrefix
        existingPath(
          s"transform/${job.name}/$prefix.sl.yml",
          s"transform/${job.name}/$prefix.sql",
          s"transform/${job.name}/$prefix.py"
        )
      },
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
      python = task.python.map(relativeToMetadata),
      freshness = task.freshness,
      schedule = task.schedule,
      warnings =
        (descWarnings ++ cols.flatMap(_._2) ++ rls.flatMap(_._2) ++ compileWarnings).distinct
    )
  }

  // ---------- groups ----------

  private def groups(
    domains: List[DomainInfo],
    externals: List[DomainInfo],
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
    val externalGroups = externals.map { d =>
      SiteGroup(
        d.finalName,
        resolved(s"External domain ${d.finalName}", d.comment),
        d.tables.map(t => NodeId.external(s"${d.finalName}.${t.finalName}")).sorted
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
    SiteGroups(domainGroups, jobGroups, dagGroups, externalGroups)
  }
}

object ModelBuilder {

  /** Whether the declared SQL type of the attribute can be compared with the warehouse: only for
    * leaves, and for array leaves only on Spark, whose type keeps the array wrapper. On the other
    * engines the ddlMapping of an array leaf is its element type, which the warehouse never
    * reports.
    */
  private[site] def hasLeafSqlType(attr: TableAttribute, engine: Option[String]): Boolean =
    attr.attributes.isEmpty && (!attr.resolveArray() || engine.contains("spark"))

  /** The SQL type core gives the column of a declared leaf attribute on this engine.
    *   - postgresql, redshift: like SparkUtils.sqlSchema, the ddlMapping of the attribute's own
    *     type under the engine name (the url subprotocol), else the type that the Spark JDBC
    *     dialect gives its Spark type. The built-in types only map `postgres`, so they take the
    *     dialect type.
    *   - spark: the Spark SQL type, as SparkAutoTask writes it (`dataType.sql`).
    *   - any other engine: the engine's ddlMapping (TableAttribute.ddlMapping).
    */
  private[site] def declaredSqlType(
    attr: TableAttribute,
    engine: String,
    schemaHandler: SchemaHandler
  ): Option[String] =
    engine match {
      case "spark" => Try(attr.sparkType(schemaHandler).sql).toOption
      case "postgresql" | "redshift" =>
        Try {
          attr
            .`type`(schemaHandler)
            .flatMap(_.ddlMapping)
            .flatMap(_.get(engine))
            .getOrElse(
              JdbcUtils
                .getJdbcType(
                  attr.sparkType(schemaHandler),
                  SparkUtils.dialectForUrl(s"jdbc:$engine:")
                )
                .databaseTypeDefinition
            )
        }.toOption
      case _ =>
        Try(attr.ddlMapping(isPrimaryKey = false, engine, schemaHandler)).toOption.collect {
          case leaf: DDLLeaf => leaf.tpe
        }
    }

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

  /** Builds the task's AutoTask and reads its input tables, each step in its own Try so one bad
    * task only fails its own table lineage.
    */
  private[site] def taskDependencies(
    task: AutoTaskInfo,
    build: AutoTaskInfo => AutoTask,
    streams: => CaseInsensitiveMap[String]
  ): Try[List[String]] =
    Try(build(task)).flatMap(autoTask => Try(autoTask.dependencies(streams)))

  /** Same TransformContext as AutoTask.unauthenticatedTasks, built per task. */
  def defaultTaskDeps(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): AutoTaskInfo => Try[List[String]] = {
    lazy val streams = schemaHandler.streams()
    taskDesc =>
      taskDependencies(
        taskDesc,
        t =>
          TransformContext(
            appId = None,
            taskDesc = t,
            commandParameters = Map.empty,
            interactive = None,
            truncate = false,
            test = false,
            logExecution = true,
            accessToken = None,
            resultPageSize = 200,
            resultPageNumber = 1,
            dryRun = false,
            scheduledDate = None,
            syncSchema = false
          )(settings, settings.storageHandler(), schemaHandler).toTask(Engine.SPARK),
        streams
      )
  }

  /** Compiles the SQL of a task as `transform` builds it, from the project files alone. Without
    * `--stats` the site connects to no warehouse: the compilation neither aligns the schema of the
    * target table, which on BigQuery reads the dataset, creates it when it is missing and alters
    * the table, nor reads the target table, nor runs a query to resolve the columns of the SELECT.
    */
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
      context
        .toTask(taskDesc.getRunEngine())
        .buildAllSQLQueriesMerged(None, Some(true), offline = true)
    }

  /** Columns of the referenced tables, read from the declared project only. */
  private[site] def declaredTablesWithColumns(
    schemaHandler: SchemaHandler,
    tableNames: List[String]
  ): List[(String, TableWithNameAndType)] =
    schemaHandler.getTablesWithColumnNames(tableNames, None, declaredOnly = true)

  /** Same steps as ColLineage.sqlColLineage, with the referenced tables read from the declared
    * project only: site generation never queries the warehouse.
    */
  def defaultColLineage(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): AutoTaskInfo => Try[Option[ColLineage.Lineage]] = taskDesc =>
    Try {
      taskDesc.sql.flatMap { sql =>
        val sqlSubst = schemaHandler.substituteRefTaskMainSQL(sql, taskDesc.getRunConnection())
        if (sqlSubst.isEmpty) None
        else {
          val tableNames =
            SQLUtils.extractTableNames(sqlSubst).map(SQLUtils.quoteFreeTableName)
          Some(
            new ColLineage(settings, schemaHandler).colLineage(
              None,
              taskDesc.domain,
              taskDesc.name,
              sqlSubst,
              declaredTablesWithColumns(schemaHandler, tableNames)
            )
          )
        }
      }
    }

  def apply(schemaHandler: SchemaHandler)(implicit settings: Settings): ModelBuilder =
    new ModelBuilder(
      schemaHandler,
      defaultCompiler(schemaHandler),
      defaultColLineage(schemaHandler),
      defaultTaskDeps(schemaHandler)
    )
}
