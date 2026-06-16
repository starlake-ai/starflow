package ai.starlake.job

import ai.starlake.buildinfo.BuildInfo
import ai.starlake.config.{DatasetArea, Settings, SettingsCmd}
import ai.starlake.console.ConsoleCmd
import ai.starlake.extract.*
import ai.starlake.extract.freshness.FreshnessExtractCmd
import ai.starlake.extract.impl.restapi.{ExtractRestAPIDataCmd, ExtractRestAPISchemaCmd}
import ai.starlake.job.bootstrap.BootstrapCmd
import ai.starlake.job.convert.Parquet2CSVCmd
import ai.starlake.job.infer.InferSchemaCmd
import ai.starlake.job.ingest.*
import ai.starlake.job.metrics.MetricsCmd
import ai.starlake.job.sink.es.{ESLoadCmd, IndexCmd}
import ai.starlake.job.sink.jdbc.JdbcConnectionLoadCmd
import ai.starlake.job.sink.kafka.KafkaJobCmd
import ai.starlake.job.site.SiteCmd
import ai.starlake.job.tools.SummarizeCmd
import ai.starlake.job.gizmo.GizmoCmd
import ai.starlake.job.quack.QuackCmd
import ai.starlake.job.transform.{JobCmd, TransformCmd}
import ai.starlake.lineage.{
  AclCmd,
  AclDependenciesCmd,
  AutoTaskDependenciesCmd,
  ColLineageCmd,
  TableDependenciesCmd
}
import ai.starlake.migration.MigrateCmd
import ai.starlake.schema.ProjectCompareCmd
import ai.starlake.schema.generator.*
import ai.starlake.schema.handlers.{SchemaHandler, ValidateCmd}
import ai.starlake.serve.MainServerCmd
import ai.starlake.sql.StarlakeJdbcDialects
import ai.starlake.tests.StarlakeTestCmd
import ai.starlake.utils.*
import com.typesafe.scalalogging.LazyLogging

import java.io.ByteArrayOutputStream
import java.time.format.DateTimeFormatter
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

/** The root of all things.
  *   - importing from landing
  *   - submitting requests to the cron manager
  *   - ingesting the datasets
  *   - running an auto job All these things are launched from here. See printUsage below to
  *     understand the CLI syntax.
  */

object Main extends LazyLogging {
  var cliMode = false
  var currentCommand = ""

  final val shell: String = "starlake"

  /** @param args
    *   depends on the action required to run a job:
    *   - call "starlake transform jobname" where jobname is the name of the job as defined in one
    *     of the definition files present in the metadata/jobs folder. to import files from a local
    *     file system
    *   - call "starlake import", this will move files in the landing area to the pending area to
    *     watch for files waiting to be processed
    *   - call"starlake watch [{+|–}domain1,domain2,domain3]" with a optional domain list separated
    *     by a ','. When called without any domain, will watch for all domain folders in the landing
    *     area When called with a '+' sign, will look only for this domain folders in the landing
    *     area When called with a '-' sign, will look for all domain folder in the landing area
    *     except the ones in the command lines.
    *   - call "starlake ingest domain schema hdfs://datasets/domain/pending/file.dsv" to ingest a
    *     file defined by its schema in the specified domain
    * -call "starlake infer-schema --domain domainName --schema schemaName --input datasetpath
    * --output outputPath --with-header
    *   - call "starlake metrics --domain domain-name --schema schema-name " to compute all metrics
    *     on specific schema in a specific domain
    */
  @nowarn
  def main(args: Array[String]): Unit = {
    if (new Main().run(args))
      System.exit(0)
    else
      System.exit(1)
  }
  val commands: List[Cmd[_]] = List(
    BootstrapCmd,
    StageCmd,
    LoadCmd,
    TransformCmd,
    ValidateCmd,
    AutoLoadCmd,
    IngestCmd,
    ESLoadCmd,
    IndexCmd,
    KafkaJobCmd,
    JdbcConnectionLoadCmd,
    Yml2DDLCmd,
    InferSchemaCmd,
    MetricsCmd,
    Parquet2CSVCmd,
    SiteCmd,
    SecureCmd,
    IamPoliciesCmd,
    Xls2YmlCmd,
    Yml2XlsCmd,
    Yml2XlsIamPolicyTagsCmd,
    Xls2YmlAutoJobCmd,
    TableDependenciesCmd,
    AclDependenciesCmd,
    AclCmd,
    AutoTaskDependenciesCmd,
    ExtractSchemaCmd,
    ExtractDataCmd,
    ExtractScriptCmd,
    BigQueryTableInfoCmd,
    ExtractBigQuerySchemaCmd,
    FreshnessExtractCmd,
    ExtractRestAPISchemaCmd,
    ExtractRestAPIDataCmd,
    ProjectCompareCmd,
    MainServerCmd,
    DagGenerateCmd,
    DagDeployCmd,
    ConsoleCmd,
    StarlakeTestCmd,
    MigrateCmd,
    ColLineageCmd,
    PreLoadCmd,
    SettingsCmd,
    SummarizeCmd,
    JobCmd,
    GizmoCmd,
    QuackCmd
  )
}

class Main extends LazyLogging {
  Main.cliMode = true

  import Main.commands
  def printUsage(): Unit = {
    // scalastyle:off println

    // Format the output (optional)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    println(s"Starlake Version ${BuildInfo.version} built on ${BuildInfo.buildTime}")
    println("Usage:")
    println(s"\t${Main.shell} [command]")
    println("Available commands =>")
    commands.foreach { cmd =>
      println(s"\t${cmd.command}")
    }
  }
  def printUsage(command: String): Unit = {
    // scalastyle:off println
    commands.find(_.command == command) match {
      case None =>
        println(s"ERROR: Unknown command --> $command")
      case Some(cmd) =>
        println(cmd.usage())
    }
  }
  // scalastyle:on println
  def checkPrerequisites(args: List[String]): Unit = {
    args match {
      case Nil =>
        printUsage()
        System.exit(0)
      case "help" :: Nil =>
        printUsage()
        System.exit(0)
      case "--help" :: Nil =>
        printUsage()
        System.exit(0)
      case "help" :: command :: _ =>
        printUsage(command)
        System.exit(0)
      case command :: "--help" :: _ =>
        printUsage(command)
        System.exit(0)
      case _ =>
    }

    sys.env.get("SL_ROOT") match {
      case None =>
        logger.warn(
          "Define and set the SL_ROOT env variable to your starlake project folder"
        )
      case Some(rootDir) =>
        logger.info(s"Project located in $rootDir")
    }

  }

  def run(args: Array[String]): Boolean = {
    ProxySettings.setProxy()
    val currentEnv = Option(System.getenv("SL_ENV"))
    implicit val settings: Settings =
      Settings(Settings.referenceConfig, currentEnv, None, None, None)
    logger.debug(settings.toString)
    logger.info(s"Starlake Version ${BuildInfo.version}")
    val argList = args.toList
    checkPrerequisites(argList)

    StarlakeJdbcDialects.registerDialects()

    import settings.storageHandler
    DatasetArea.initMetadata(storageHandler())

    // extract any env var passed as --options argument
    val cliEnv = CliEnvConfig.parse(args.drop(1).toIndexedSeq) match {
      case Some(env) => env.options
      case None      => Map.empty[String, String]
    }

    val schemaHandler = settings.schemaHandler(cliEnv)
    val executedCommand = argList.mkString(" ")
    logger.info(s"Running Starlake $executedCommand")
    val errCapture = new ByteArrayOutputStream()
    Console.withErr(errCapture) {
      val result =
        try {
          run(args, schemaHandler)
        } catch {
          case e: Throwable =>
            Failure(e)
        }
      // We raise an exception only on command failure not on parse args failure
      result match {
        case Failure(e: IllegalArgumentException) =>
          // scalastyle:off println
          val errMessage = s"""
             |--------------------------------------------------
             |${errCapture.toString().trim}
             |--------------------------------------------------
             |${e.getMessage}""".stripMargin

          System.err.print(errMessage)
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(1)
          }

        case Failure(exception) =>
          val message = s"""Starlake failed to execute command with args $executedCommand"""
          System.err.print(message)
          Utils.logException(logger, exception)
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(1)
          } else {
            throw exception
          }
        case Success(FailedJobResult) =>
          val message = s"""Starlake failed to execute command with args $executedCommand"""
          System.err.print(message)
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(1)
          }
        case Success(result: PreLoadJobResult) =>
          logger.info(s"Preload Successfully $executedCommand")
          logger.info("\n" + result.prettyPrint("table"))
          val status =
            if (result.empty)
              1
            else
              0
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(status)
          }
        case Success(_) =>
          logger.info(s"Successfully $executedCommand")
          if (settings.appConfig.forceHalt) {
            Runtime.getRuntime.halt(0)
          }
      }
      // FailedJobResult and empty PreLoadJobResult are considered as a soft failure
      // so we remove them from success possibility
      result.filter {
        case r: PreLoadJobResult if r.empty => false
        case FailedJobResult                => false
        case _                              => true
      }.isSuccess
    }

  }

  def run(args: Array[String], schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[Any] = {

    val command = args.headOption.getOrElse("")

    val result =
      commands.find(_.command == command) match {
        case None =>
          printUsage(command)
          Failure(new IllegalArgumentException(s"Unknown command $command"))
        case Some(cmd) =>
          Main.currentCommand = cmd.command
          if (cmd.command != BootstrapCmd.command && settings.appConfig.validateOnLoad) {
            val checkResult = schemaHandler.checkValidity()
            checkResult match {
              case Failure(e) =>
                return throw e
              case Success((Nil, _, _)) =>
                // scalastyle:off println
                println(s"No errors found. Project loaded successfully.")
              case Success((errorsAndWarning, 0, warningCount)) =>
                // scalastyle:off println
                println(s"Found $warningCount warning(s)")
                errorsAndWarning.foreach(println)
              case Success((errorsAndWarning, errorCount, warningCount)) =>
                if (warningCount == 0) {
                  // scalastyle:off println
                  println(s"$errorCount error(s)")
                } else {
                  // scalastyle:off println
                  println(s"Found $warningCount warning(s) and $errorCount error(s)")
                }
                errorsAndWarning.foreach(println)
            }
          }
          val r = cmd.run(args.drop(1).toIndexedSeq, schemaHandler)
          if (cmd.command == BootstrapCmd.command) {
            if (args.length == 1 || args(1) != "--no-exit") {
              System.exit(0)
            }
          }
          r
      }
    result
  }
}
