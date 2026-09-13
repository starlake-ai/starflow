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
        "Execute the project's tasks in dependency order, in parallel, inside this JVM." +
        " The graph is built from transform lineage, so a load table is executed only when some" +
        " transform reads it: tables no transform references are never ingested by this command."
      ),
      builder
        .opt[Int]("parallelism")
        .optional()
        .validate(p =>
          if (p >= 1) builder.success
          else builder.failure("parallelism must be >= 1")
        )
        .action((x, c) => c.copy(parallelism = Some(x)))
        .text(
          "Max concurrently executing tasks. Defaults to the maxParTask setting (SL_MAX_PAR_TASK)." +
          " Raising it runs tasks concurrently through code paths that are not concurrency-safe in" +
          " all engines, so treat it as an explicit opt-in."
        ),
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

  /** Builds the task graph from transform lineage and executes it in dependency order.
    *
    * A load table becomes a node only because a transform reads it: the graph has no other source
    * of load nodes. A table that no transform references is therefore not part of the run at all
    * and is never ingested, even when files are waiting for it in the stage area. Loading such a
    * table needs `starlake load`.
    *
    * Parallelism defaults to the `maxParTask` setting (1 unless the project raises it). `run` does
    * not widen that contract on its own: `--parallelism N` is an explicit opt-in, because several
    * engines share process-wide state across concurrently executing tasks.
    */
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
    // Two declared tables can share a final name (a `rename:` resolving onto another table, or two
    // case variants). The value here selects what a node executes, so silently keeping the
    // last-written pair would make a node ingest a different table than the one it names.
    val declaredByFinalName: List[(String, (String, String))] =
      schemaHandler
        .domains()
        .flatMap { domain =>
          domain.tables.map { table =>
            s"${domain.finalName}.${table.finalName}".toLowerCase -> (domain.name, table.name)
          }
        }
    val loadTables: Map[String, (String, String)] =
      checkNoFinalNameCollision(declaredByFinalName)

    DagBuilder.build(deps, loadTables.keySet) match {
      case Left(cycle) =>
        val message = s"Cycle detected in the task graph: ${cycle.mkString(" -> ")}"
        System.err.println(message)
        RunJobResult(exitCode = 2, summary = None, errorMessage = Some(message))
      case Right(dag) =>
        val ingestionWorkflow = workflow(schemaHandler)
        // Same contract as `transform --recursive`: serial unless the project opted into
        // parallelism. Several engines keep process-wide state per task, so concurrency is a
        // deliberate choice, not a default.
        // max(1) because maxParTask is free-form config while the scheduler requires >= 1
        val parallelism = config.parallelism.getOrElse(math.max(1, settings.appConfig.maxParTask))
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
        // The inheritance happens at thread creation, so this covers the runner's own threads and
        // any ParUtils pool a node spawns, but not threads that already existed: Spark's driver and
        // executor threads keep writing to the real stdout.
        val summary = Console.withOut(System.err) { scheduler.run() }
        // The summary table is this command's machine-consumable output and always goes to stdout.
        // RunConfig.reportFormat is parsed for CLI consistency but deliberately not read here, as
        // elsewhere in the codebase.
        println(TextRenderer.summaryTable(summary))
        RunJobResult(exitCode = summary.exitCode, summary = Some(summary), errorMessage = None)
    }
  }

  /** Indexes declared tables by their final name, rejecting any final name claimed by more than one
    * declared table. Picking one of them would make the corresponding node ingest a table other
    * than the one it displays, so the run stops instead.
    */
  private[run] def checkNoFinalNameCollision(
    declaredByFinalName: List[(String, (String, String))]
  ): Map[String, (String, String)] = {
    val collisions = declaredByFinalName
      .groupBy { case (finalName, _) => finalName }
      .collect {
        case (finalName, entries) if entries.map(_._2).distinct.sizeIs > 1 =>
          finalName -> entries.map(_._2).distinct.sorted
      }
      .toList
      .sortBy { case (finalName, _) => finalName }
    if (collisions.nonEmpty) {
      val detail = collisions
        .map { case (finalName, declared) =>
          val names = declared.map { case (domain, table) => s"$domain.$table" }.mkString(", ")
          s"'$finalName' is claimed by $names"
        }
        .mkString("; ")
      throw new IllegalStateException(
        s"Ambiguous load tables: $detail. Two declared tables resolve to the same final name, so a" +
        " run node cannot tell which one to ingest. Rename one of them."
      )
    }
    declaredByFinalName.toMap
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
