package ai.starlake.impact

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{AutoTaskInfo, DomainInfo, SchemaInfo, TableAttribute}
import ai.starlake.utils.Utils
import better.files.File
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.mutable
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

  /** Loads the project from a temporary copy of its `metadata` folder: the schema handler writes
    * while loading (stage directories of each domain, a domain `_config.sl.yml` without a name is
    * rewritten), and `impact` must leave the project untouched. The copy is deleted afterwards.
    */
  def load(projectDir: File, label: String, env: Option[String])(implicit
    settings: Settings
  ): Try[SnapshotView] =
    Try {
      val workDir = File.newTemporaryDirectory("starlake-impact-")
      try {
        val metadata = projectDir / "metadata"
        if (metadata.isDirectory) metadata.copyTo(workDir / "metadata")
        else (workDir / "metadata").createDirectories()
        // An empty default env file reads as no variable, and avoids a warning per variable lookup
        val defaultEnv = workDir / "metadata" / "env.sl.yml"
        if (!defaultEnv.exists) defaultEnv.createFile()
        val local = localSettings(workDir, env)
        val handler = local.schemaHandler(reload = true)
        build(projectDir, workDir, label, env, handler)(local)
      } finally workDir.delete(swallowIOExceptions = true)
    }.recover { case e: Throwable =>
      throw new IllegalStateException(
        s"Could not load project at $projectDir: ${e.getMessage}. If it predates the current layout, run 'starlake migrate' on it.",
        e
      )
    }

  /** Every path of the work copy, not only root and metadata: types, macros, DAGs, strategies,
    * datasets and the areas would point at the current project otherwise, which the snapshot must
    * neither read nor write (the schema handler creates the stage directories of each domain).
    */
  private def localSettings(workDir: File, env: Option[String])(implicit
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
    * work copy root, so it is covered too. Longest first: a real path may contain the other one
    * (`/private/var/x` on macOS).
    */
  private def neutralizeRoots(dirs: List[File]): String => String = {
    val roots = dirs
      .flatMap(d => List(Some(d.pathAsString), Try(d.path.toRealPath().toString).toOption).flatten)
      .filter(_.length > 1)
      .distinct
      .sortBy(-_.length)
    sql => roots.foldLeft(sql)((acc, root) => acc.replace(root, RootToken))
  }

  /** The schema handler logs and skips a domain or a transform whose `_config.sl.yml` it cannot
    * read: a project where some of them are unreadable must fail instead of yielding a partial
    * snapshot. `SchemaHandler.checkValidity` is not used: it also validates the connections of the
    * settings and requires a types file, which has nothing to do with reading the project offline.
    * Returns the tasks of the project, so the transforms are read only once.
    */
  private def readableTasks(handler: SchemaHandler)(implicit
    settings: Settings
  ): List[AutoTaskInfo] = {
    val storage = settings.storageHandler()
    val domainErrors =
      handler.deserializedDomains(DatasetArea.load).collect { case (path, Failure(e)) =>
        s"$path: ${e.getMessage}"
      }
    val jobs =
      if (storage.exists(DatasetArea.transform))
        storage.listDirectories(DatasetArea.transform).map { dir =>
          val configPath = new Path(dir, "_config.sl.yml")
          configPath -> handler.loadJobTasksFromFile(configPath)
        }
      else Nil
    val jobErrors = jobs.collect { case (path, Failure(e)) => s"$path: ${e.getMessage}" }
    val errors = domainErrors ++ jobErrors
    if (errors.nonEmpty)
      throw new IllegalStateException(s"unreadable files: ${errors.mkString("; ")}")
    jobs.collect { case (_, Success(job)) => job.tasks }.flatten
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
    val vars =
      handler.activeEnvVars(reload = true, env = env, root = Some(workDir.pathAsString))
    val neutralize = neutralizeRoots(List(workDir, projectDir))
    val domains: List[DomainInfo] = handler.domains(reload = true)
    val tasks: List[AutoTaskInfo] = readableTasks(handler)

    val loadNodes: List[NodeView] = for {
      domain <- domains
      table  <- domain.tables
    } yield NodeView(
      NodeRef.of(domain.finalName, table.finalName),
      NodeKind.LoadTable,
      table.attributes.map(attributeView),
      None,
      structural(table, domain),
      metadataFingerprint(table)
    )

    val taskByRef: Map[NodeRef, AutoTaskInfo] =
      tasks.map(t => NodeRef.of(t.domain, t.table) -> t).toMap
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
                    .unresolved("python task", Set.empty, Set.empty)
                    .copy(rowFingerprint = sha256(settings.storageHandler().read(path)))
                case None =>
                  Try(Utils.parseJinja(task.sql.getOrElse(""), vars)) match {
                    case Success(sql) =>
                      TaskShapeBuilder.build(neutralize(sql), resolveName, knownColumns)
                    case Failure(e) =>
                      TaskShape.unresolved(
                        s"SQL render failed: ${e.getMessage}",
                        Set.empty,
                        Set.empty
                      )
                  }
              }
            } finally inProgress -= ref
          shapes(ref) = shape
          shape
        }
      )

    val taskNodes = taskByRef.toList.map { case (ref, t) =>
      NodeView(
        ref,
        NodeKind.Task,
        t.attributes.map(attributeView),
        Some(shapeOf(ref, t)),
        structural(t),
        metadataFingerprint(t)
      )
    }

    SnapshotView(
      label,
      (loadNodes ++ taskNodes).map(n => n.ref -> n).toMap,
      otherFiles(projectDir)
    )
  }

  /** Column names are compared lower case everywhere in the impact analysis. */
  private def finalNameOf(a: TableAttribute): String = a.getFinalName().toLowerCase

  private def attributeView(a: TableAttribute): AttributeView =
    AttributeView(a.name, finalNameOf(a), a.`type`)

  private def structural(table: SchemaInfo, domain: DomainInfo): String = {
    val metadata = table.mergedMetadata(domain.metadata)
    List(
      metadata.writeStrategy.map(_.toString).getOrElse(""),
      metadata.sink.map(s => s"${s.partition}|${s.clustering}").getOrElse(""),
      table.filter.getOrElse("")
    ).mkString("|")
  }

  private def structural(task: AutoTaskInfo): String =
    List(
      task.writeStrategy.map(_.toString).getOrElse(""),
      task.sink.map(s => s"${s.partition}|${s.clustering}").getOrElse("")
    ).mkString("|")

  private def attributeMetadata(attrs: List[TableAttribute]): String =
    attrs.map(a => s"${a.name}:${a.comment}:${a.tags.toList.sorted}:${a.privacy}").mkString(";")

  private def metadataFingerprint(table: SchemaInfo): String =
    List(
      table.comment,
      table.tags.toList.sorted,
      table.acl,
      table.rls,
      table.expectations,
      attributeMetadata(table.attributes)
    ).mkString("|")

  private def metadataFingerprint(task: AutoTaskInfo): String =
    List(
      task.comment,
      task.tags.toList.sorted,
      task.acl,
      task.rls,
      task.expectations,
      attributeMetadata(task.attributes)
    ).mkString("|")

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

  private def sha256(text: String): String = sha256(text.getBytes(StandardCharsets.UTF_8))

  private def otherFiles(projectDir: File): Map[String, String] = {
    val metadataDir = projectDir / "metadata"
    if (!metadataDir.exists) Map.empty
    else {
      val dirs = List(metadataDir / "dags", metadataDir / "expectations").filter(_.exists)
      val topLevel = metadataDir.list.filter { f =>
        f.isRegularFile && (f.name == "application.sl.yml" ||
        (f.name.startsWith("env") && f.name.endsWith(".sl.yml")))
      }.toList
      val files = dirs.flatMap(_.listRecursively.filter(_.isRegularFile).toList) ++ topLevel
      files.map { f =>
        metadataDir.relativize(f).toString -> sha256(f.byteArray)
      }.toMap
    }
  }
}
