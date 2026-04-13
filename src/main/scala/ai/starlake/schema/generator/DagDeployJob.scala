package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, JobResult, StarlakeConfigException, Utils}
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Success, Try}

class DagDeployJob(schemaHandler: SchemaHandler) extends StrictLogging {

  private def handleAirflowIgnore(path: Path)(implicit settings: Settings): Unit = {
    // check if .airflowignore file exists
    val airflowIgnorePath = new Path(path, ".airflowignore")
    if (!settings.storageHandler().exists(airflowIgnorePath)) {
      logger.info(s"Creating .airflowignore file at: $airflowIgnorePath")
      // add ai folder to .airflowignore
      val content = "ai/\n"
      settings.storageHandler().write(content, airflowIgnorePath)
    } else {
      logger.info(s".airflowignore file already exists at: $airflowIgnorePath")
      // check if ai folder exists, if not add it
      val inputStream = settings.storageHandler().read(airflowIgnorePath)
      if (!inputStream.contains("ai/")) {
        logger.info(s"Adding ai/ to .airflowignore file at: $airflowIgnorePath")
        val content = inputStream + "\nai/\n"
        settings.storageHandler().write(content, airflowIgnorePath)
      }
    }
  }

  def deployDags(config: DagDeployConfig)(implicit settings: Settings): Try[JobResult] = Try {
    val inputDir = config.inputDir.getOrElse(new Path(DatasetArea.build, "dags").toString)

    val orchestratorName =
      schemaHandler
        .orchestratorName()
        .getOrElse(throw new StarlakeConfigException("Orchestrator not configured"))

    logger.info(s"Orchestrator Name: $orchestratorName")

    val baseOutputPath = new Path(config.outputDir)
    val dagOutputPath =
      config.dagDir.map(it => new Path(config.outputDir, it)).getOrElse(baseOutputPath)
    if (config.clean)
      settings.storageHandler().delete(dagOutputPath)

    handleAirflowIgnore(baseOutputPath)
    val libPath = new Path(baseOutputPath, "ai/starlake/orchestration/starlake_orchestration.py")
    if (settings.storageHandler().exists(libPath)) {
      logger.info(s"Orchestration library already deployed at: $libPath")
    } else {
      logger.info(s"Deploying orchestration library at: $libPath")
      //    --find-links /path/to/your/dist/folder \

      val pythonCmd =
        if (settings.appConfig.pythonLibsDir.isEmpty)
          s"python3 -m pip install --break-system-packages --no-cache-dir --target ${baseOutputPath.toString} starlake-orchestration starlake-$orchestratorName"
        else {
          val libsDir = File(settings.appConfig.pythonLibsDir)
          val paths =
            libsDir
              .list(f =>
                f.name.endsWith(".whl") &&
                (f.name.contains("starlake_orchestration") || f.name
                  .contains(s"starlake_$orchestratorName"))
              )
              .flatMap { file =>
                Some(file.pathAsString)
              }
              .toList
          require(
            paths.length == 2,
            s"Expected 2 packages in ${libsDir.pathAsString} but found ${paths}"
          )
          val pathsAsString = paths.mkString(" ")
          s"python3 -m pip install --break-system-packages --no-cache-dir --target ${baseOutputPath.toString} $pathsAsString"
        }

      val cmdResult = Utils.runCommand(pythonCmd)
      cmdResult match {
        case Success(result) =>
          logger.info(s"DAG deployment command output: ${result.output}")
          if (result.exit != 0) {
            logger.error(s"DAG deployment command error (if any): ${result.error}")
            throw new Exception(s"DAG deployment command failed: $pythonCmd\n${result.error}")
          } else {
            if (result.error.nonEmpty)
              logger.info(result.error)
          }
          logger.info(s"DAG deployment command succeeded: $pythonCmd")
        case _ =>
          throw new Exception(s"DAG deployment command failed: $pythonCmd")
      }
    }

    val inputFileFolder = File(inputDir)
    inputFileFolder.listRecursively
      .filter(f =>
        !f.isDirectory &&
        !f.pathAsString.contains(".dist-info/") &&
        !f.pathAsString.contains("/__pycache__/")
      ) // Exclude python package metadata
      .foreach { file =>
        val relativePath = {
          val path = file.pathAsString.replace(inputFileFolder.pathAsString, "")
          if (path.startsWith("/"))
            path.substring(1)
          else path
        }
        val targetPath = new Path(dagOutputPath, relativePath)
        settings
          .storageHandler()
          .mkdirs(targetPath.getParent)
        settings
          .storageHandler()
          .copyFromLocal(new Path(file.pathAsString), targetPath)
      }
    EmptyJobResult
  }
}
