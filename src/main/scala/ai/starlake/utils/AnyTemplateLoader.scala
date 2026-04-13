package ai.starlake.utils

import ai.starlake.config.Settings
import better.files.Resource
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class DagTemplateInfo(
  fullName: String,
  dagType: String,
  orchestrator: String,
  template: String,
  runner: String,
  options: List[DagTemplateOption]
)

case class DagTemplateOption(
  key: String,
  dfault: String,
  description: String,
  required: Boolean
)

object DagTemplateOption {
  private def keyAndDefault(
    key: String
  ): (String, String) = {
    val parts = key.split('(')
    if (parts.length == 2) {
      (parts(0), parts(1).stripSuffix(")"))
    } else {
      (key, "")
    }

  }
  def fromLine(line: String): Option[DagTemplateOption] = {
    if (line.startsWith("# - ")) {
      val parts = line.stripPrefix("# - ").split(":")
      val required = parts(1).contains("[REQUIRED]")
      val optional = parts(1).contains("[OPTIONAL]")
      val option = {
        if (parts.length != 2) {
          None
        } else if (required || optional) {
          val (key, dfault) = keyAndDefault(parts(0).trim)
          val option = DagTemplateOption(
            key = key,
            dfault = dfault,
            description = parts(1).trim,
            required = required
          )
          Some(option)
        } else {
          None
        }
      }
      option
    } else
      None
  }
}

object DagTemplateInfo {
  def fromFile(
    pythonFile: Path
  )(implicit settings: Settings): DagTemplateInfo = {
    val name = pythonFile.getName
    val dagType = pythonFile.getParent.getName
    require(
      dagType == "load" || dagType == "transform",
      s"Invalid dag type: ${dagType}, only 'load' and 'transform' are supported."
    )
    val parts = name.split("__")
    require(parts.length == 3, s"Invalid dag template name: ${name}")
    val orchestrator = parts(0)
    val template = parts(1)
    val runner = parts(2).stripSuffix(".py.j2")
    val options =
      settings
        .storageHandler()
        .read(pythonFile)
        .split("\n")
        .flatMap(DagTemplateOption.fromLine)
        .toList

    val fullName = orchestrator + "__" + template + "__" + runner
    DagTemplateInfo(fullName, dagType, orchestrator, template, runner, options)
  }
  def fromResource(
    resourceName: String
  ): DagTemplateInfo = {
    val name = resourceName.substring(resourceName.lastIndexOf("/") + 1)
    val parent = resourceName.substring(0, resourceName.lastIndexOf("/"))
    val dagType = parent.substring(parent.lastIndexOf("/") + 1)
    require(
      dagType == "load" || dagType == "transform",
      s"Invalid dag type: ${dagType}, only 'load' and 'transform' are supported."
    )
    val parts = name.split("__")
    require(parts.length == 3, s"Invalid dag template name: ${name}")
    val orchestrator = parts(0)
    val template = parts(1)
    val runner = parts(2).stripSuffix(".py.j2")
    val source = Source.fromResource(resourceName)
    if (source == null)
      throw new Exception(s"Resource $resourceName not found in assembly")
    val options =
      source
        .getLines()
        .flatMap(DagTemplateOption.fromLine)
        .toList

    val fullName = orchestrator + "__" + template + "__" + runner
    DagTemplateInfo(fullName, dagType, orchestrator, template, runner, options)
  }
}

abstract class AnyTemplateLoader extends LazyLogging {

  protected val TEMPLATE_FOLDER = "templates"
  protected def RESOURCE_TEMPLATE_FOLDER: String
  protected def EXTERNAL_TEMPLATE_BASE_PATH(implicit settings: Settings): Path

  def allPaths(implicit settings: Settings): List[String] = {
    List(
      EXTERNAL_TEMPLATE_BASE_PATH.toString,
      RESOURCE_TEMPLATE_FOLDER
    )
  }

  private def externalTemplates()(
    settings: Settings,
    templateType: String
  ): List[DagTemplateInfo] = {
    val appPath =
      new Path(EXTERNAL_TEMPLATE_BASE_PATH(settings), TEMPLATE_FOLDER + "/" + templateType)
    val paths =
      if (settings.storageHandler().exists(appPath)) {
        settings
          .storageHandler()
          .list(path = appPath, recursive = false)
          .filter { p =>
            val name = p.path.getName
            !name.startsWith("_") && name.endsWith(".j2")
          }
          .map(_.path)
      } else {
        Nil
      }
    val templates =
      paths.map { p =>
        DagTemplateInfo.fromFile(p)(settings)
      }
    templates
  }

  private def internalTemplates()(
    settings: Settings,
    templateType: String
  ): List[DagTemplateInfo] = {
    val resourceNames =
      JarUtil
        .getResourceFiles(s"${RESOURCE_TEMPLATE_FOLDER}/${templateType}")
        .filter { p =>
          val name = p.substring(p.lastIndexOf("/") + 1)
          !name.startsWith("_") && name.endsWith(".j2")
        }
    val templates =
      resourceNames.map { res =>
        DagTemplateInfo.fromResource(res)
      }
    templates
  }

  def internalLoadTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    internalTemplates()(settings, "load")
  }

  def internalTransformTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    internalTemplates()(settings, "transform")
  }

  def externalLoadTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    externalTemplates()(settings, "load")
  }

  def externalTransformTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    externalTemplates()(settings, "transform")
  }

  def allLoadTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    val all = externalLoadTemplates() ++ internalLoadTemplates()
    val distinct = all.groupBy(_.fullName).map(_._2.head).toList
    distinct
  }

  def allTransformTemplates()(implicit settings: Settings): List[DagTemplateInfo] = {
    val all = externalTransformTemplates() ++ internalTransformTemplates()
    val distinct = all.groupBy(_.fullName).map(_._2.head).toList
    distinct
  }

  def loadTemplate(templatePathname: String)(implicit settings: Settings): String = {
    loadTemplateFromAbsolutePath(templatePathname)
      .orElse(loadTemplateFromAppPath(templatePathname))
      .orElse(loadTemplateFromResources(templatePathname))
      .getOrElse(
        throw new RuntimeException(
          s"Template `${templatePathname}` not found. Please provide an absolute path or a template returned by `list` command. This strategy may also not be supported by your platform."
        )
      )
  }

  def loadTemplateFromAbsolutePath(
    template: String
  )(implicit settings: Settings): Try[String] = {
    val templatePath = new Path(template)
    Try {
      // exists may throw an exception
      if (settings.storageHandler().exists(templatePath)) {
        Success(settings.storageHandler().read(templatePath))
      } else {
        Failure(new RuntimeException(s"No absolute template found for ${templatePath}."))
      }
    }.flatten
  }

  def loadTemplateFromAppPath(template: String)(implicit settings: Settings): Try[String] = {
    val appPath = new Path(EXTERNAL_TEMPLATE_BASE_PATH, TEMPLATE_FOLDER + "/" + template)
    if (settings.storageHandler().exists(appPath)) {
      Success(settings.storageHandler().read(appPath))
    } else {
      Failure(new RuntimeException(s"No relative template found for ${appPath}."))
    }
  }

  def loadTemplateFromResources(
    domainTemplate: String
  ): Try[String] = {
    Resource.asString(s"$RESOURCE_TEMPLATE_FOLDER/${domainTemplate}") match {
      case Some(value) => Success(value)
      case None =>
        Failure(new RuntimeException(s"Relative template not found in for ${domainTemplate}"))
    }
  }

  def loadMacros(): Try[String] = {
    Resource.asString(s"$RESOURCE_TEMPLATE_FOLDER/default.j2") match {
      case Some(value) => Success(value)
      case None =>
        Failure(
          new RuntimeException(
            s"default macros not found in for ${RESOURCE_TEMPLATE_FOLDER}/default.j2"
          )
        )
    }
  }

}
