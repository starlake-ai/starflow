package ai.starlake.job.ingest

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, FailedJobResult, JobResult, PreLoadJobResult}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import scopt.OParser

import scala.util.{Success, Try}

/** Command to check for files to load.
  *
  * Usage: starlake preload [options]
  */
trait PreLoadCmd extends Cmd[PreLoadConfig] with LazyLogging {

  def command = "preload"

  override def pageDescription: String =
    "Pre-load domains and tables using a configurable strategy before the main ingestion step, with global ack file support."
  override def pageKeywords: Seq[String] =
    Seq("starlake preload", "data pre-loading", "ingestion preparation", "ETL pipeline")

  val parser: OParser[Unit, PreLoadConfig] = {
    val builder = OParser.builder[PreLoadConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(
        """Check for new files in the landing area and prepare them for loading."""
      ),
      builder
        .opt[String]("domain")
        .action((x, c) => c.copy(domain = x))
        .valueName("domain1")
        .required()
        .text("Domain to pre load"),
      builder
        .opt[Seq[String]]("tables")
        .valueName("table1,table2,table3 ...")
        .optional()
        .action((x, c) => c.copy(tables = x))
        .text("Tables to pre load"),
      builder
        .opt[String]("strategy")
        .optional()
        .action((x, c) => c.copy(strategy = PreLoadStrategy.fromString(x)))
        .text("pre load strategy"),
      builder
        .opt[String]("globalAckFilePath")
        .optional()
        .action((x, c) => c.copy(globalAckFilePath = Some(x)))
        .text("Global ack file path"),
      builder
        .opt[String]("notReadySentinel")
        .optional()
        .action((x, c) => c.copy(notReadySentinel = Some(x)))
        .text(
          "If set, a zero-byte marker is written at this path when the pre-load decides files are not yet ready. " +
          "Used by orchestrators to distinguish 'not ready, retry later' from real failures. " +
          "In this mode, 'not ready' outcomes always exit with code 0 (the sentinel is the signal), " +
          "while real exceptions still exit non-zero. " +
          "Supports any StorageHandler-compatible URI (gs://, s3://, file://, hdfs://)."
        ),
      builder
        .opt[Map[String, String]]("options")
        .valueName("k1=v1,k2=v2...")
        .optional()
        .action((x, c) => c.copy(options = x))
        .text("Pre load arguments to be used as substitutions"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  def parse(args: Seq[String]): Option[PreLoadConfig] =
    OParser.parse(parser, args, PreLoadConfig(domain = ""))

  private def writeSentinelIfRequested(
    config: PreLoadConfig
  )(implicit settings: Settings): Unit = {
    config.notReadySentinel.foreach { sentinelPath =>
      val path = new Path(sentinelPath)
      settings.storageHandler().touchz(path) match {
        case scala.util.Success(_) =>
          logger.info(s"Wrote not-ready sentinel to $sentinelPath")
        case scala.util.Failure(e) =>
          // Best-effort: never mask the real pre-load outcome if sentinel write fails.
          logger.warn(s"Failed to write not-ready sentinel to $sentinelPath: ${e.getMessage}")
      }
    }
  }

  override def run(config: PreLoadConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    logger.info(
      s"Pre loading domain ${config.domain} with strategy ${config.strategy.map(_.value).getOrElse("none")}"
    )
    val sentinelMode = config.notReadySentinel.isDefined

    config.strategy match {
      case Some(PreLoadStrategy.Imported) =>
        val result =
          schemaHandler.domains(List(config.domain), config.tables.toList).headOption match {
            case Some(domain) =>
              PreLoadJobResult(
                config.domain,
                workflow(schemaHandler)
                  .listStageFiles(domain, dryMode = true)
                  .map(kv => kv._1 -> kv._2.size)
              )
            case None =>
              PreLoadJobResult(config.domain, config.tables.map(t => t -> 0).toMap)
          }
        if (result.empty && sentinelMode) {
          writeSentinelIfRequested(config)
          Success(EmptyJobResult)
        } else {
          Success(result)
        }

      case Some(PreLoadStrategy.Pending) =>
        val pendingArea = DatasetArea.stage(config.domain)
        val files = settings.storageHandler().list(pendingArea, recursive = false)
        val results =
          for (table <- config.tables) yield {
            schemaHandler.table(config.domain, table) match {
              case Some(schema) =>
                table -> files.count(file => schema.pattern.matcher(file.path.getName).matches())
              case None =>
                table -> 0
            }
          }
        val result = PreLoadJobResult(config.domain, results.toMap)
        if (result.empty && sentinelMode) {
          writeSentinelIfRequested(config)
          Success(EmptyJobResult)
        } else {
          Success(result)
        }

      case Some(PreLoadStrategy.Ack) =>
        config.globalAckFilePath match {
          case Some(globalAckFilePath) =>
            val storageHandler = settings.storageHandler()
            val path = new Path(globalAckFilePath)
            if (storageHandler.exists(path)) {
              storageHandler.delete(path)
              Success(EmptyJobResult)
            } else if (sentinelMode) {
              writeSentinelIfRequested(config)
              Success(EmptyJobResult)
            } else {
              Success(FailedJobResult)
            }
          case None =>
            if (sentinelMode) {
              writeSentinelIfRequested(config)
              Success(EmptyJobResult)
            } else {
              Success(FailedJobResult)
            }
        }

      case _ =>
        Success(PreLoadJobResult(config.domain, config.tables.map(t => t -> 1).toMap))
    }
  }
}

object PreLoadCmd extends PreLoadCmd
