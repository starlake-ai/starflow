package ai.starlake.job.transform

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import com.typesafe.scalalogging.LazyLogging
import scopt.OParser

import scala.util.Try

/** Command to run a transformation task.
  *
  * Usage: starlake transform [options]
  */
trait TransformCmd extends Cmd[TransformConfig] with LazyLogging {

  def command = "transform"

  override def pageDescription: String =
    "Execute SQL-based transformation tasks with dry-run, recursive dependency resolution, and interactive output formats."
  override def pageKeywords: Seq[String] =
    Seq("starlake transform", "SQL transformation", "data pipeline", "ELT", "BigQuery")

  val parser: OParser[Unit, TransformConfig] = {
    val builder = OParser.builder[TransformConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Execute a SQL or Python transformation task. Starlake resolves dependencies, compiles queries with variable substitutions, and writes results to the target table. Use `--recursive` for upstream dependencies. See [Transform Guide](/guides/transform/tutorial)."""
      ),
      builder
        .opt[String]("action")
        .action((x, c) => c.copy(action = TransformAction.fromString(x)))
        .optional()
        .text(
          "May take one of run (default) or create. When action is set to 'create', only parameters 'name' and 'query' are meaningful"
        ),
      builder
        .opt[String]("name")
        .action((x, c) => c.copy(name = x))
        .required()
        .text("Task Name in the form domain.task"),
      builder
        .opt[Unit]("compile")
        .action((_, c) => c.copy(compile = true))
        .optional()
        .text("Return final query only"),
      builder
        .opt[Unit]("sync-apply")
        .action((_, c) => c.copy(syncApply = true))
        .optional()
        .text("Update YAML attributes to match SQL query"),
      builder
        .opt[Unit]("sync-preview")
        .action((_, c) => c.copy(syncPreview = true))
        .optional()
        .text("Preview YAML attributes to match SQL query"),
      builder
        .opt[Unit]("refresh-inherited-descriptions")
        .action((_, c) => c.copy(refreshInheritedDescriptions = true))
        .optional()
        .text(
          "With --sync-apply or --sync-preview, replace the description of every column that copies a described column with the current one, even a description written in the task"
        ),
      builder
        .opt[String]("query")
        .action((x, c) => c.copy(query = Some(x)))
        .optional()
        .text(
          "if action is 'create', this is the query used to initialize th SQL file. If action is set to 'run', run this query instead of the one in the task"
        ),
      builder
        .opt[Unit]("dry-run")
        .action((_, c) => c.copy(dryRun = true))
        .optional()
        .text("Dry run only (supported on BigQuery only)"),
      builder
        .opt[Seq[String]]("tags")
        .action((x, c) => c.copy(tags = x))
        .optional()
        .text("Return final query only"),
      builder
        .opt[Unit]("format")
        .action((_, c) => c.copy(format = true))
        .optional()
        .text("pretty print the final query and update sql file"),
      builder
        .opt[String]("interactive")
        .action((x, c) => c.copy(interactive = Some(x)))
        .optional()
        .text(
          "Run query without sinking the result. Valid parameters are: `csv`, `json`, `table`, `json-array`"
        ),
      builder
        .opt[Unit]("reload")
        .action((_, c) => c.copy(reload = true))
        .optional()
        .text("Reload YAML  files. Used in server mode"),
      builder
        .opt[Boolean]("truncate")
        .action((x, c) => c.copy(truncate = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Int]("pageSize")
        .action((x, c) => c.copy(pageSize = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Int]("pageNumber")
        .action((x, c) => c.copy(pageNumber = x))
        .optional()
        .text(
          s"Force table to be truncated before insert. Default value is false"
        ),
      builder
        .opt[Unit]("recursive")
        .action((_, c) => c.copy(recursive = true))
        .optional()
        .text(
          s"Execute all dependencies recursively. Default value is false"
        ),
      builder
        .opt[Unit]("test")
        .action((_, c) => c.copy(test = true))
        .optional()
        .text(
          s"Should we run this transform as a test ? Default value is false"
        ),
      builder
        .opt[String]("accessToken")
        .action((x, c) => c.copy(accessToken = Some(x)))
        .text(s"Access token to use for authentication")
        .optional(),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(options = c.options ++ x))
        .unbounded()
        .text("Job arguments to be used as substitutions"),
      builder
        .opt[String]("scheduledDate")
        .optional()
        .action((x, c) => c.copy(scheduledDate = Some(x.replace('T', ' '))))
        .text("Scheduled date for the job, in format yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x)),
      builder.checkConfig { config =>
        if (config.refreshInheritedDescriptions && !config.syncApply && !config.syncPreview)
          builder.failure("--refresh-inherited-descriptions needs --sync-apply or --sync-preview")
        else builder.success
      }
    )
  }

  def parse(args: Seq[String]): Option[TransformConfig] = {
    val result = OParser.parse(parser, args, TransformConfig(scheduledDate = None), setup)
    result.foreach { config =>
      require(config.name.isEmpty || config.tags.isEmpty, "Cannot specify both tags and task name")
    }
    result
  }

  override def run(config: TransformConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    if (config.syncPreview) {
      syncPreview(config, schemaHandler) map { _ =>
        JobResult.empty
      }

    } else if (config.syncApply) {
      Try {
        val list = schemaHandler.syncPreviewSqlWithDb(config.name, config.query, config.accessToken)
        val result = schemaHandler.inheritDescriptions(
          config.name,
          list,
          config.query,
          config.refreshInheritedDescriptions
        )
        result.inherited.foreach { case (attribute, inherited) =>
          logger.info(s"Attribute $attribute inherits the description of ${inherited.declaredBy}")
        }
        result.leftOut.foreach { case (attribute, inherited) =>
          logger.warn(
            s"Attribute $attribute does not inherit the description of ${inherited.declaredBy}, since it contains Jinja or a variable"
          )
        }
        if (list.isEmpty && result.attributes.nonEmpty)
          // Only inherited descriptions change: a task whose YAML cannot be updated keeps it
          Try(schemaHandler.syncApplySqlWithYaml(config.name, result.attributes, None)).recover {
            case exception: IllegalStateException =>
              logger.warn(
                s"The inherited descriptions of task ${config.name} are not written: ${exception.getMessage}"
              )
          }.get
        else
          schemaHandler.syncApplySqlWithYaml(config.name, result.attributes, None)
      } map { _ =>
        JobResult.empty
      }
    } else if (config.compile) {
      workflow(schemaHandler).compileAutoJob(config).map(_ => JobResult.empty)
    } else {
      val res = workflow(schemaHandler).autoJob(config)
      res.map(_ => JobResult.empty)
    }
  }

  private def syncPreview(config: TransformConfig, schemaHandler: SchemaHandler): Try[Unit] = {
    Try {
      val list = schemaHandler.syncPreviewSqlWithDb(config.name, config.query, config.accessToken)
      val result = schemaHandler.inheritDescriptions(
        config.name,
        list,
        config.query,
        config.refreshInheritedDescriptions
      )
      println(s"Previewing SQL attributes for task: ${config.name}")
      result.attributes.foreach { case (attribute, status) =>
        val typ = if (attribute.resolveArray()) s"${attribute.`type`}[]" else attribute.`type`
        val inherited = (result.inherited ++ result.written).collectFirst {
          case (name, inherited) if name == attribute.name =>
            s", Description: inherited from ${inherited.declaredBy}"
        }
        println(s"\tAttribute: ${attribute.name}: $typ, Status: $status${inherited.getOrElse("")}")
      }
      if (result.attributes.isEmpty && result.written.nonEmpty)
        println(
          s"Inherited descriptions, already written: ${result.written
              .map { case (name, inherited) => s"$name (${inherited.declaredBy})" }
              .mkString(", ")}"
        )
      if (result.leftOut.nonEmpty)
        println(
          s"Inherited descriptions left out, since they contain Jinja or a variable: ${result.leftOut
              .map { case (name, inherited) => s"$name (${inherited.declaredBy})" }
              .mkString(", ")}"
        )
      if (result.undescribed.nonEmpty)
        println(s"Attributes without a description: ${result.undescribed.mkString(", ")}")
      result.error.foreach { error =>
        println(
          s"The column lineage of the task is not read, its columns inherit no description: $error"
        )
      }

    }
  }
}

object TransformCmd extends TransformCmd
