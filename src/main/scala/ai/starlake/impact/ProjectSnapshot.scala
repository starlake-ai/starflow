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

  def load(projectDir: File, label: String, env: Option[String])(implicit
    settings: Settings
  ): Try[SnapshotView] =
    Try {
      val local = localSettings(projectDir, env)
      val handler = local.schemaHandler(reload = true)
      build(projectDir, label, env, handler)(local)
    }.recover { case e: Throwable =>
      throw new IllegalStateException(
        s"Could not load project at $projectDir: ${e.getMessage}. If it predates the current layout, run 'starlake migrate' on it.",
        e
      )
    }

  /** Every path of the project, not only root and metadata: types, macros, DAGs, strategies and
    * datasets default to the current project otherwise, which the snapshot must neither read nor
    * write (the schema handler creates the stage directories of each domain under datasets).
    */
  private def localSettings(projectDir: File, env: Option[String])(implicit
    settings: Settings
  ): Settings = {
    val root = projectDir.pathAsString
    val metadata = new Path(root, "metadata").toString
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
        // the schema handler renders the task SQL with the variables of this env
        env = env.getOrElse(settings.appConfig.env)
      )
    )
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

  private def build(projectDir: File, label: String, env: Option[String], handler: SchemaHandler)(
    implicit settings: Settings
  ): SnapshotView = {
    val domains: List[DomainInfo] = handler.domains(reload = true)
    val tasks: List[AutoTaskInfo] = readableTasks(handler)
    val vars =
      handler.activeEnvVars(reload = true, env = env, root = Some(projectDir.pathAsString))

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
                    case Success(sql) => TaskShapeBuilder.build(sql, resolveName, knownColumns)
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
