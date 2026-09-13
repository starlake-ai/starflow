package ai.starlake.job.run

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.job.ingest.LoadConfig
import ai.starlake.job.transform.{AutoTask, TransformConfig}
import ai.starlake.lineage.TaskViewDependency
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.JobResult
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.scalalogging.LazyLogging
import scopt.OParser

import scala.util.{Success, Try}

/** In-process DAG runner: executes the project's tasks in dependency order, in parallel,
  * inside this JVM. A runner, not an orchestrator: scheduling stays with the caller.
  */
trait RunCmd extends Cmd[RunConfig] with LazyLogging {

  def command = "run"

  val parser: OParser[Unit, RunConfig] = {
    val builder = OParser.builder[RunConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        "Execute the project's tasks in dependency order, in parallel, inside this JVM."
      ),
      builder
        .opt[Int]("parallelism")
        .optional()
        .validate(p =>
          if (p >= 1) builder.success
          else builder.failure("parallelism must be >= 1")
        )
        .action((x, c) => c.copy(parallelism = Some(x)))
        .text("Max concurrently executing tasks. Default: min(8, available processors)"),
      builder
        .opt[Unit]("fail-fast")
        .optional()
        .action((_, c) => c.copy(failFast = true))
        .text("Abort the whole run on first failure instead of only the failed branch"),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Variables passed to the templating path"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[RunConfig] =
    OParser.parse(parser, args, RunConfig())

  override def run(config: RunConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = Try(runProject(config, schemaHandler))

  def runProject(config: RunConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): RunJobResult = {
    implicit val storageHandler: ai.starlake.schema.handlers.StorageHandler =
      settings.storageHandler()
    implicit val sh: SchemaHandler = schemaHandler

    val tasks = AutoTask.unauthenticatedTasks(reload = false)
    val deps = TaskViewDependency.dependencies(tasks)
    val loadTables: Set[String] =
      schemaHandler
        .domains()
        .flatMap { domain =>
          domain.tables.map(table => s"${domain.finalName}.${table.finalName}".toLowerCase)
        }
        .toSet

    DagBuilder.build(deps, loadTables) match {
      case Left(cycle) =>
        val message = s"Cycle detected in the task graph: ${cycle.mkString(" -> ")}"
        System.err.println(message)
        RunJobResult(exitCode = 2, summary = None, errorMessage = Some(message))
      case Right(dag) =>
        val ingestionWorkflow = workflow(schemaHandler)
        val parallelism =
          config.parallelism.getOrElse(math.min(8, Runtime.getRuntime.availableProcessors()))
        val scheduler = new RunScheduler(
          dag = dag,
          parallelism = parallelism,
          failFast = config.failFast,
          executor = executeNode(ingestionWorkflow, config, _),
          listener = event => System.err.println(TextRenderer.progressLine(event))
        )
        val summary = scheduler.run()
        // Machine-consumable surface of --output text: the summary table on stdout
        println(TextRenderer.summaryTable(summary))
        RunJobResult(exitCode = summary.exitCode, summary = Some(summary), errorMessage = None)
    }
  }

  private def executeNode(
    ingestionWorkflow: IngestionWorkflow,
    config: RunConfig,
    node: RunNode
  ): Try[Unit] =
    node.typ match {
      case RunNodeType.Task =>
        ingestionWorkflow
          .autoJob(TransformConfig(name = node.displayName, options = config.options))
          .map(_ => ())
      case RunNodeType.LoadTable =>
        val parts = node.displayName.split('.').takeRight(2)
        ingestionWorkflow
          .load(
            LoadConfig(
              domains = Seq(parts(0)),
              tables = Seq(parts(1)),
              options = config.options,
              accessToken = None,
              test = false,
              scheduledDate = None
            )
          )
          .map(_ => ())
      case RunNodeType.Boundary =>
        Success(())
    }
}

object RunCmd extends RunCmd
