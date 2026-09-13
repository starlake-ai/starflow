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

import scala.util.{Failure, Success, Try}

/** In-process DAG runner: executes the project's tasks in dependency order, in parallel, inside
  * this JVM. A runner, not an orchestrator: scheduling stays with the caller.
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
    // Lineage names tables by their final name, but the load path filters on the declared name
    // (IngestionWorkflow.predicate matches table.name). Keep both: the final name to recognize a
    // node, the declared pair to execute it.
    val loadTables: Map[String, (String, String)] =
      schemaHandler
        .domains()
        .flatMap { domain =>
          domain.tables.map { table =>
            s"${domain.finalName}.${table.finalName}".toLowerCase -> (domain.name, table.name)
          }
        }
        .toMap

    DagBuilder.build(deps, loadTables.keySet) match {
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
          executor = executeNode(ingestionWorkflow, config, loadTables, _),
          listener = event => System.err.println(TextRenderer.progressLine(event))
        )
        // Ingestion prints a human-readable block to stdout (Utils.printOut) on every load node,
        // which would interleave with the summary table below. Console.out is a DynamicVariable
        // backed by an InheritableThreadLocal and RunScheduler allocates its thread pool inside
        // run(), so worker threads inherit this redirection. Hoisting that pool out of run() would
        // silently send node output back to stdout.
        val summary = Console.withOut(System.err) { scheduler.run() }
        // Machine-consumable surface of --output text: the summary table on stdout
        println(TextRenderer.summaryTable(summary))
        RunJobResult(exitCode = summary.exitCode, summary = Some(summary), errorMessage = None)
    }
  }

  /** Executes a single DAG node.
    *
    * Known limitation on load nodes: the `domains` filter below is currently ignored downstream.
    * IngestionWorkflow.domainsToWatch forwards it to SchemaHandler.domains, which returns the
    * cached, unfiltered domain list as soon as `_domains` is populated, and runProject populates it
    * when it builds `loadTables`. A load node therefore scans every domain's stage area, filtered
    * only by table name, so a table name shared by two domains is ingested by whichever node runs
    * first. This is pre-existing platform behavior shared with LoadCmd; fixing it here alone would
    * leave the two commands inconsistent, so it is deferred until both can be fixed together.
    * RunCmdSpec pins the current behavior so that a later fix has to flip the assertion
    * deliberately.
    */
  private def executeNode(
    ingestionWorkflow: IngestionWorkflow,
    config: RunConfig,
    loadTables: Map[String, (String, String)],
    node: RunNode
  ): Try[Unit] =
    node.typ match {
      case RunNodeType.Task =>
        ingestionWorkflow
          .autoJob(TransformConfig(name = node.displayName, options = config.options))
          .map(_ => ())
      case RunNodeType.LoadTable =>
        // Same normalization DagBuilder used to classify this node as a load table
        val key = node.id.split('.').takeRight(2).mkString(".")
        loadTables.get(key) match {
          case Some((domainName, tableName)) =>
            ingestionWorkflow
              .load(
                LoadConfig(
                  domains = Seq(domainName),
                  tables = Seq(tableName),
                  options = config.options,
                  accessToken = None,
                  test = false,
                  scheduledDate = None
                )
              )
              .map(_ => ())
          case None =>
            // Unreachable by construction: a LoadTable node exists only because this key matched
            // at build time. Fail loudly rather than ingest nothing and report success.
            Failure(
              new IllegalStateException(
                s"Load node '${node.displayName}' (key '$key') has no declared table in the project" +
                " metadata. The node was classified as a load table, so this is an inconsistency" +
                " between DAG construction and execution."
              )
            )
        }
      case RunNodeType.Boundary =>
        Success(())
    }
}

object RunCmd extends RunCmd
