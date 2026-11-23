package ai.starlake.impact

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{
  AllSinks,
  AutoTaskInfo,
  DomainInfo,
  Metadata,
  SchemaInfo,
  TableAttribute
}
import ai.starlake.serve.CaffeineSettingsManager
import ai.starlake.utils.{Utils, YamlSerde}
import better.files.File
import com.hubspot.jinjava.interpret.FatalTemplateErrorsException
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.regex.{Matcher, Pattern}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Turns one version of a project on disk into a [[SnapshotView]], fully offline: no connection is
  * opened and no table is read from a database. Column lists come from the YAML declarations and
  * from the SQL of the tasks only.
  */
object ProjectSnapshot {

  /** Stands for the project root in rendered SQL, so that the same SQL loaded from two directories
    * (a temporary checkout and the working tree) renders the same.
    */
  val RootToken = "__SL_ROOT__"

  /** A YAML file this version cannot read: the project may predate the current layout. */
  private final class UnreadableYaml(message: String, cause: Throwable)
      extends Exception(message, cause)

  /** Loads the project from a temporary copy of its `metadata` folder: the schema handler writes
    * while loading (stage directories of each domain, a domain `_config.sl.yml` without a name is
    * rewritten), and `impact` must leave the project untouched. The copy is deleted afterwards.
    *
    * The Jinja engine is a singleton that resolves `{% include %}` against the metadata folder of
    * the settings it was first used with: it is reset before, so that includes resolve against the
    * copy, and after, so that the caller gets an engine built from its own settings again. That
    * engine is global: two loads must not run concurrently in one JVM.
    *
    * @param dateVars
    *   the `sl_*` date variables the YAML and SQL are rendered with. Both sides of a comparison
    *   must get the same map, otherwise SQL using `{{sl_datetime}}` changes between the two loads.
    *   They replace the date variables of the schema handler everywhere, env files included; an env
    *   file that defines one of them still wins, as in a run.
    */
  def load(
    projectDir: File,
    label: String,
    env: Option[String],
    dateVars: Map[String, String] = SchemaHandler.slDateVars(LocalDateTime.now)
  )(implicit
    settings: Settings
  ): Try[SnapshotView] =
    Try {
      val workDir = File.newTemporaryDirectory("starlake-impact-")
      Utils.resetJinjaClassLoader()
      try {
        val metadata = projectDir / "metadata"
        if (metadata.isDirectory) metadata.copyTo(workDir / "metadata")
        else (workDir / "metadata").createDirectories()
        // the schema handler fails on a missing load folder: a project with transforms only, or
        // the empty old side of a commit that predates the project, has none
        List("load", "transform").foreach(d => (workDir / "metadata" / d).createDirectories())
        // An empty default env file reads as no variable, and avoids a warning per variable lookup
        val defaultEnv = workDir / "metadata" / "env.sl.yml"
        if (!defaultEnv.exists) defaultEnv.createFile()
        val local = localSettings(workDir, env)
        val handler = schemaHandler(dateVars)(local)
        build(projectDir, workDir, label, env, handler)(local)
      } finally {
        Utils.resetJinjaClassLoader()
        workDir.delete(swallowIOExceptions = true)
      }
    }.recover {
      case e: UnreadableYaml =>
        throw new IllegalStateException(
          s"Could not load project at $projectDir: ${e.getMessage}. If it predates the current layout, run 'starlake migrate' on it.",
          e
        )
      case e: Throwable =>
        throw new IllegalStateException(
          s"Could not load project at $projectDir: ${e.getMessage}",
          e
        )
    }

  /** A schema handler for the work copy whose date variables are `dateVars`, registered like the
    * one `Settings.schemaHandler(reload = true)` builds, for the code that looks it up.
    */
  private def schemaHandler(dateVars: Map[String, String])(implicit
    settings: Settings
  ): SchemaHandler = {
    val handler = new SchemaHandler(settings.storageHandler()) {
      override lazy val slDateVars: Map[String, String] = dateVars
    }
    CaffeineSettingsManager.setSchemaHandler(
      settings.appConfig.root,
      Option(settings.appConfig.env),
      handler
    )
    handler
  }

  /** Every path of the work copy, not only root and metadata: types, macros, DAGs, strategies,
    * datasets and the areas would point at the current project otherwise, which the snapshot must
    * neither read nor write (the schema handler creates the stage directories of each domain).
    */
  private[impact] def localSettings(workDir: File, env: Option[String])(implicit
    settings: Settings
  ): Settings = {
    val root = workDir.pathAsString
    val metadata = new Path(root, "metadata").toString
    val area = settings.appConfig.area
    def relative(default: String, value: String): String =
      if (value.contains("://") || java.nio.file.Paths.get(value).isAbsolute) default else value
    settings.copy(appConfig =
      settings.appConfig.copy(
        root = root,
        metadata = metadata,
        datasets = new Path(root, "datasets").toString,
        types = new Path(metadata, "types").toString,
        macros = new Path(metadata, "macros").toString,
        dags = new Path(metadata, "dags").toString,
        writeStrategies = new Path(metadata, "write-strategies").toString,
        loadStrategies = new Path(metadata, "load-strategies").toString,
        area = area.copy(
          incoming = relative("incoming", area.incoming),
          stage = relative("stage", area.stage),
          unresolved = relative("unresolved", area.unresolved),
          archive = relative("archive", area.archive),
          ingesting = relative("ingesting", area.ingesting),
          replay = relative("replay", area.replay)
        ),
        // the schema handler renders the task SQL with the variables of this env
        env = env.getOrElse(settings.appConfig.env)
      )
    )
  }

  /** Replaces the root of the work copy and of the project, as written and as real paths, with
    * [[RootToken]]. Every other path variable (`SL_METADATA`, `SL_DATASETS` ...) lies under the
    * work copy root, so it is covered too. A root is replaced only as a whole path: not followed by
    * a character of a path name (`/x/proj` is not replaced in `/x/proj2` nor in `/x/proj.bak`), so
    * whatever follows it in a fingerprint (`)`, `,`, `|` ...) does not matter. Longest first: a
    * real path may contain the other one (`/private/var/x` on macOS).
    */
  private def neutralizeRoots(dirs: List[File]): String => String = {
    val patterns = dirs
      .flatMap(d => List(Some(d.pathAsString), Try(d.path.toRealPath().toString).toOption).flatten)
      .filter(_.length > 1)
      .distinct
      .sortBy(-_.length)
      .map(root => Pattern.compile(Pattern.quote(root) + "(?![A-Za-z0-9._-])"))
    val token = Matcher.quoteReplacement(RootToken)
    text => patterns.foldLeft(text)((acc, p) => p.matcher(acc).replaceAll(token))
  }

  /** The messages of `e` and of its causes: a Jinja error says "Error rendering tag", its cause
    * says which template is missing.
    */
  private[impact] def messages(e: Throwable): String = {
    // identity, not equality: a cause chain may loop back to an exception already visited
    val seen = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[Throwable, java.lang.Boolean]()
    )
    def walk(e: Throwable): List[String] =
      Iterator
        .iterate(e)(_.getCause)
        .takeWhile(c => c != null && seen.add(c))
        .toList
        .flatMap {
          case fatal: FatalTemplateErrorsException =>
            Option(fatal.getMessage).toList ++ fatal.getErrors.asScala.toList.flatMap { error =>
              Option(error.getMessage).toList ++ Option(error.getException).toList.flatMap(walk)
            }
          case c => Option(c.getMessage).toList
        }
    walk(e).distinct.mkString(": ")
  }

  /** The schema handler logs and skips the YAML files it cannot read, and fails a whole transform
    * when one of its SQL files does not render. Before it loads anything, every domain, table,
    * transform and task YAML file of the copy is rendered and deserialized here: a render error
    * fails the load naming the file, a deserialization error fails it with the migrate hint. A task
    * SQL file that does not render is emptied in the copy, so that only this task is lost, and its
    * error and raw content are returned to mark the task unresolved.
    */
  private def checkFiles(metadataDir: File, handler: SchemaHandler)(implicit
    settings: Settings
  ): Map[String, (String, String)] = {
    val vars = handler.activeEnvVars()
    def subDirs(dir: File): List[File] =
      if (dir.isDirectory) dir.list.filter(_.isDirectory).toList.sortBy(_.name) else Nil
    def yamlFiles(dir: File): List[File] =
      dir.listRecursively
        .filter(f =>
          f.isRegularFile && f.name.endsWith(".sl.yml") && !f.name.startsWith("_config.")
        )
        .toList
        .sortBy(_.pathAsString)
    // "/" whatever the platform: the same key as the command file of the task, see build
    def name(f: File): String = relativePath(metadataDir, f)
    def check(f: File)(deserialize: String => Any): Unit = {
      val content = Try(Utils.parseJinja(f.contentAsString, vars)) match {
        case Success(content) => content
        case Failure(e) =>
          throw new IllegalStateException(s"${name(f)} does not render: ${messages(e)}", e)
      }
      Try(deserialize(content)).failed.foreach { e =>
        throw new UnreadableYaml(s"${name(f)} could not be read: ${e.getMessage}", e)
      }
    }
    subDirs(metadataDir / "load").foreach { domain =>
      val config = domain / "_config.sl.yml"
      if (config.exists)
        check(config)(YamlSerde.deserializeYamlLoadConfig(_, config.pathAsString, false).get)
      yamlFiles(domain).foreach(f => check(f)(YamlSerde.deserializeYamlTables(_, f.pathAsString)))
    }
    subDirs(metadataDir / "transform").flatMap { job =>
      val config = job / "_config.sl.yml"
      if (config.exists)
        check(config)(YamlSerde.deserializeYamlTransform(_, config.pathAsString).get)
      yamlFiles(job).foreach(f => check(f)(YamlSerde.deserializeYamlTask(_, f.pathAsString)))
      job.list
        .filter(f => f.isRegularFile && (f.name.endsWith(".sql") || f.name.endsWith(".sql.j2")))
        .toList
        .flatMap { sqlFile =>
          val raw = sqlFile.contentAsString
          Try(Utils.parseJinja(raw, vars)).failed.toOption.map { e =>
            sqlFile.writeText("")
            name(sqlFile) -> (messages(e), raw)
          }
        }
    }.toMap
  }

  /** The tasks of each transform folder of the copy, with that folder. A transform the handler
    * cannot load (all YAML files were checked before, so not a YAML error) fails the load.
    */
  private def readableTasks(handler: SchemaHandler)(implicit
    settings: Settings
  ): List[(Path, AutoTaskInfo)] = {
    val storage = settings.storageHandler()
    val jobs =
      if (storage.exists(DatasetArea.transform))
        storage.listDirectories(DatasetArea.transform).map { dir =>
          dir -> handler.loadJobTasksFromFile(new Path(dir, "_config.sl.yml"))
        }
      else Nil
    val errors = jobs.collect { case (dir, Failure(e)) => s"${dir.getName}: ${e.getMessage}" }
    if (errors.nonEmpty)
      throw new IllegalStateException(s"transforms not loaded: ${errors.mkString("; ")}")
    jobs.collect { case (dir, Success(job)) => job.tasks.map(dir -> _) }.flatten
  }

  private def build(
    projectDir: File,
    workDir: File,
    label: String,
    env: Option[String],
    handler: SchemaHandler
  )(implicit settings: Settings): SnapshotView = {
    // First, so that the variables cached by the handler, with which it renders the YAML and the
    // task SQL while loading them, define SL_ROOT as the work copy root (otherwise it is the
    // SL_ROOT of the environment, unrelated to the project).
    handler.activeEnvVars(reload = true, env = env, root = Some(workDir.pathAsString))
    val neutralize = neutralizeRoots(List(workDir, projectDir))
    val renderErrors = checkFiles(workDir / "metadata", handler)
    val domains: List[DomainInfo] = handler.domains(reload = true)
    // sorted, so that the memoized shapes, cycles included, do not depend on the listing order
    val tasks: List[(Path, AutoTaskInfo)] =
      readableTasks(handler).sortBy { case (_, t) => NodeRef.of(t.domain, t.table).name }

    val loadNodes: List[NodeView] = for {
      domain <- domains
      table  <- domain.tables
    } yield NodeView(
      NodeRef.of(domain.finalName, table.finalName),
      NodeKind.LoadTable,
      table.attributes.map(attributeView(neutralize)),
      None,
      neutralize(structural(table, domain)),
      neutralize(metadataFingerprint(table, domain))
    )

    // the maps below keep one definition per node: the others are reported, not analyzed
    val collisions: Map[NodeRef, String] = {
      val loads = loadNodes.groupBy(_.ref).map { case (ref, ns) => ref -> ns.size }
      val taskCounts =
        tasks.groupBy { case (_, t) => NodeRef.of(t.domain, t.table) }.map { case (ref, ts) =>
          ref -> ts.size
        }
      def count(n: Int, what: String): Option[String] = n match {
        case 0 => None
        case 1 => Some(what)
        case 2 => Some(s"two ${what}s")
        case _ => Some(s"$n ${what}s")
      }
      (loads.keySet ++ taskCounts.keySet).toList.flatMap { ref =>
        val l = loads.getOrElse(ref, 0)
        val t = taskCounts.getOrElse(ref, 0)
        if (l + t > 1)
          Some(ref -> (count(l, "load table").toList ++ count(t, "task")).mkString(" and "))
        else None
      }.toMap
    }

    val taskByRef: Map[NodeRef, AutoTaskInfo] =
      tasks.map { case (_, t) => NodeRef.of(t.domain, t.table) -> t }.toMap
    val taskDirs: Map[NodeRef, Path] =
      tasks.map { case (dir, t) => NodeRef.of(t.domain, t.table) -> dir }.toMap

    /** The file the handler reads the command of the task from (same lookup order), relative to
      * `metadata/` like the keys of `renderErrors`. Transform folders are one level under
      * `transform/`, and their name is all that is taken from the Hadoop path.
      */
    def commandFile(ref: NodeRef, task: AutoTaskInfo): Option[String] =
      taskDirs.get(ref).flatMap { dir =>
        List("sql", "sql.j2", "py")
          .map(ext => s"transform/${dir.getName}/${task._filenamePrefix}.$ext")
          .find(relative => (workDir / "metadata" / relative).exists)
      }

    val loadByRef = loadNodes.map(n => n.ref -> n).toMap
    val allRefs = loadByRef.keySet ++ taskByRef.keySet

    def resolveName(name: String): NodeRef = {
      val parts = name.toLowerCase.split('.').toList
      parts.takeRight(2) match {
        case d :: t :: Nil => NodeRef(d, t)
        case t :: Nil =>
          allRefs.filter(_.table == t).toList match {
            case single :: Nil => single
            case _             => NodeRef("", t)
          }
        case _ => NodeRef("", name.toLowerCase)
      }
    }

    val shapes = mutable.Map.empty[NodeRef, TaskShape]
    val inProgress = mutable.Set.empty[NodeRef]

    def knownColumns(ref: NodeRef): Option[List[String]] =
      loadByRef.get(ref).map(_.attributes.map(_.finalName)).filter(_.nonEmpty).orElse {
        taskByRef.get(ref).flatMap { t =>
          if (t.attributes.nonEmpty) Some(t.attributes.map(finalNameOf))
          else if (inProgress.contains(ref)) None
          else Some(shapeOf(ref, t).columns.map(_.name)).filter(_.nonEmpty)
        }
      }

    def shapeOf(ref: NodeRef, task: AutoTaskInfo): TaskShape =
      shapes.getOrElse(
        ref, {
          inProgress += ref
          val shape =
            try {
              task.python match {
                case Some(path) =>
                  TaskShape
                    .unresolved(TaskShape.PythonTask, Set.empty, Set.empty)
                    .copy(rowFingerprint = sha256(settings.storageHandler().read(path)))
                case None =>
                  commandFile(ref, task).flatMap(renderErrors.get) match {
                    case Some((error, raw)) =>
                      TaskShape
                        .unresolved(s"SQL render failed: $error", Set.empty, Set.empty)
                        .copy(rowFingerprint = TaskShapeBuilder.normalize(raw))
                    case None =>
                      // already rendered by the handler with the variables of the snapshot
                      TaskShapeBuilder.build(
                        neutralize(task.sql.getOrElse("")),
                        resolveName,
                        knownColumns
                      )
                  }
              }
            } finally inProgress -= ref
          shapes(ref) = shape
          shape
        }
      )

    val taskNodes = taskByRef.toList.sortBy(_._1.name).map { case (ref, t) =>
      NodeView(
        ref,
        NodeKind.Task,
        t.attributes.map(attributeView(neutralize)),
        Some(shapeOf(ref, t)),
        neutralize(structural(t)),
        neutralize(metadataFingerprint(t))
      )
    }

    SnapshotView(
      label,
      (loadNodes ++ taskNodes).map(n => n.ref -> n).toMap,
      otherFiles(projectDir),
      definitionFiles(
        projectDir,
        loadByRef.keySet,
        taskByRef.map { case (ref, t) => ref -> (taskDirs(ref).getName -> t._filenamePrefix) }
      ),
      collisions
    )
  }

  /** Column names are compared lower case everywhere in the impact analysis. */
  private def finalNameOf(a: TableAttribute): String = a.getFinalName().toLowerCase

  private def attributeView(neutralize: String => String)(a: TableAttribute): AttributeView =
    AttributeView(a.name, finalNameOf(a), a.`type`, neutralize(attributeData(a)))

  /** What changes how the files of a load table are parsed. */
  private def parsing(m: Metadata): String =
    List(
      m.format,
      m.encoding,
      m.multiline,
      m.array,
      m.withHeader,
      m.separator,
      m.quote,
      m.escape,
      m.options.map(_.toList.sorted),
      m.loader,
      m.emptyIsNull,
      m.nullValue,
      m.fillWithDefaultValue
    ).mkString(",")

  /** The fields of an attribute that change the values of its column, nested attributes included:
    * the `dataFingerprint` of its [[AttributeView]], a column-level change. Nested attributes also
    * carry their name and type, which [[AttributeView]] (top level only) does not see. `required`
    * is not here: it rejects rows, see [[requiredAttributes]].
    */
  private def attributeData(a: TableAttribute): String = {
    val fields = List(
      "array"    -> a.array,
      "default"  -> a.default,
      "trim"     -> a.trim,
      "script"   -> a.script,
      "ignore"   -> a.ignore,
      "position" -> a.position,
      "privacy"  -> a.privacy
    ).collect { case (key, Some(value)) => s"$key=$value" }
    val nested =
      if (a.attributes.isEmpty) Nil
      else
        List(
          a.attributes
            .map(n => s"${n.name}:${n.getFinalName()}:${n.`type`}{${attributeData(n)}}")
            .mkString("attributes=[", ";", "]")
        )
    (fields ++ nested).mkString(",")
  }

  /** Final names (`parent.child` when nested, lower case) of the required attributes: a row with no
    * value for one of them is rejected, so which rows land depends on this set.
    */
  private def requiredAttributes(attrs: List[TableAttribute], prefix: String = ""): List[String] =
    attrs.flatMap { a =>
      val name = prefix + finalNameOf(a)
      (if (a.required.contains(true)) List(name) else Nil) ++
      requiredAttributes(a.attributes, s"$name.")
    }

  /** The sink fields that change where the data lands or how it is laid out. */
  private def sinkStructural(sink: Option[AllSinks]): String = {
    // no sink and an empty one are the same
    val s = sink.getOrElse(AllSinks())
    List(
      s.connectionRef,
      s.sharding,
      s.clustering,
      s.days,
      s.requirePartitionFilter,
      s.materializedView,
      s.id,
      s.format,
      s.extension,
      s.partition,
      s.path,
      s.options.map(_.toList.sorted)
    ).mkString(",")
  }

  /** The other sink fields: they change neither the rows nor where they land. */
  private def sinkMetadata(sink: Option[AllSinks]): String = {
    val s = sink.getOrElse(AllSinks())
    List(s.enableRefresh, s.refreshIntervalMs, s.coalesce).mkString(",")
  }

  private def structural(table: SchemaInfo, domain: DomainInfo): String = {
    val metadata = table.mergedMetadata(domain.metadata)
    List(
      metadata.writeStrategy.map(_.toString).getOrElse(""),
      sinkStructural(metadata.sink),
      table.filter.getOrElse(""),
      parsing(metadata),
      requiredAttributes(table.attributes).sorted.mkString("required=[", ",", "]"),
      table.presql.mkString(";"),
      table.postsql.mkString(";"),
      // which files are loaded
      table.pattern.toString,
      metadata.directory,
      metadata.ack,
      // the merge key, when the write strategy does not name one
      table.primaryKey.mkString(","),
      domain.database
    ).mkString("|")
  }

  private def structural(task: AutoTaskInfo): String =
    List(
      task.writeStrategy.map(_.toString).getOrElse(""),
      sinkStructural(task.sink),
      task.presql.mkString(";"),
      task.postsql.mkString(";"),
      // where the table is written
      task.connectionRef,
      task.database,
      task.parseSQL,
      task.syncStrategy,
      task.primaryKey.mkString(",")
    ).mkString("|")

  /** Privacy is left out: it changes the values of the column, see [[attributeData]]. */
  private def attributeMetadata(attrs: List[TableAttribute]): String =
    attrs.map(a => s"${a.name}:${a.comment}:${a.tags.toList.sorted}").mkString(";")

  private def metadataFingerprint(table: SchemaInfo, domain: DomainInfo): String = {
    val metadata = table.mergedMetadata(domain.metadata)
    List(
      table.comment,
      table.tags.toList.sorted,
      table.acl,
      table.rls,
      table.expectations,
      attributeMetadata(table.attributes),
      table.streams,
      table.sample,
      table.patternSample,
      metadata.dagRef,
      metadata.freshness,
      metadata.schedule,
      sinkMetadata(metadata.sink),
      domain.comment,
      domain.tags.toList.sorted
    ).mkString("|")
  }

  private def metadataFingerprint(task: AutoTaskInfo): String =
    List(
      task.comment,
      task.tags.toList.sorted,
      task.acl,
      task.rls,
      task.expectations,
      attributeMetadata(task.attributes),
      task.schedule,
      task.dagRef,
      task.freshness,
      task.taskTimeoutMs,
      task.streams,
      task.assetTriggeringStrategy,
      sinkMetadata(task.sink)
    ).mkString("|")

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

  private def sha256(text: String): String = sha256(text.getBytes(StandardCharsets.UTF_8))

  /** `f` relative to `base`, its path components joined with "/" whatever the platform: the keys of
    * the snapshot are compared across sides and tested as such.
    */
  private[impact] def relativePath(base: File, f: File): String =
    base.path.relativize(f.path).iterator().asScala.map(_.toString).mkString("/")

  private def filesUnder(dir: File): List[File] =
    if (dir.isDirectory) dir.listRecursively.filter(_.isRegularFile).toList else Nil

  /** Every file under `metadata/` but `load/` and `transform/`, which the nodes cover. */
  private def otherFiles(projectDir: File): Map[String, String] = {
    val metadataDir = projectDir / "metadata"
    filesUnder(metadataDir).flatMap { f =>
      val path = relativePath(metadataDir, f)
      if (path.startsWith("load/") || path.startsWith("transform/")) None
      else Some(path -> sha256(f.byteArray))
    }.toMap
  }

  /** Every file under `metadata/load` and `metadata/transform`, hashed, with the nodes it defines
    * by the file name convention (see [[DefinitionFile]]). A table whose domain or name differs
    * from its folder or file name falls back on the nodes of that table name.
    */
  private def definitionFiles(
    projectDir: File,
    loadRefs: Set[NodeRef],
    taskFiles: Map[NodeRef, (String, String)]
  ): Map[String, DefinitionFile] = {
    val metadataDir = projectDir / "metadata"
    def baseName(name: String): String = name.takeWhile(_ != '.')
    def nodesOf(path: String): Set[NodeRef] = path.split('/').toList match {
      case "load" :: folder :: rest if rest.nonEmpty =>
        val domain = folder.toLowerCase
        if (rest.last.startsWith("_config.")) loadRefs.filter(_.domain == domain)
        else {
          val table = baseName(rest.last).toLowerCase
          val exact = loadRefs.filter(r => r.domain == domain && r.table == table)
          if (exact.nonEmpty) exact else loadRefs.filter(_.table == table)
        }
      case "transform" :: folder :: rest if rest.nonEmpty =>
        val inFolder = taskFiles.filter { case (_, (dir, _)) => dir == folder }
        if (rest.last.startsWith("_config.")) inFolder.keySet
        else {
          val prefix = baseName(rest.last)
          val exact = inFolder.collect {
            case (ref, (_, p)) if p.equalsIgnoreCase(prefix) => ref
          }.toSet
          if (exact.nonEmpty) exact else taskFiles.keySet.filter(_.table == prefix.toLowerCase)
        }
      case _ => Set.empty
    }
    List("load", "transform")
      .flatMap(d => filesUnder(metadataDir / d))
      .map { f =>
        val path = relativePath(metadataDir, f)
        path -> DefinitionFile(sha256(f.byteArray), nodesOf(path))
      }
      .toMap
  }
}
