/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */
/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.handlers

import ai.starlake.config.Settings.{latestSchemaVersion, ConnectionInfo}
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractSchema, JdbcDbUtils}
import ai.starlake.job.ingest.{AuditLog, RejectedRecord}
import ai.starlake.job.metrics.ExpectationReport
import ai.starlake.schema.model.*
import ai.starlake.schema.model.Severity.*
import ai.starlake.sql.SQLUtils
import ai.starlake.transpiler.diff.Attribute
import ai.starlake.transpiler.schema.{JdbcColumn, JdbcMetaData}
import ai.starlake.utils.Formatter.*
import ai.starlake.utils.{StarlakeConfigException, StarlakeNotFoundException, Utils, YamlSerde}
import better.files.Resource
import com.databricks.spark.xml.util.XSDToSchema
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class TableWithNameAndType(name: String, attrs: List[(String, String, Option[String])]) {
  def schemaString(fullName: Option[String] = None): String = {
    val schemaName = fullName.getOrElse(name)
    val schema = attrs
      .map { case (attrName, attrType, comment) =>
        val commentStr = comment.map(c => s" COMMENT '$c'").getOrElse("")
        s"$attrName $attrType$commentStr"
      }
      .mkString(",\n")
    s"CREATE TABLE $schemaName (\n$schema\n)"
  }
}

case class DomainWithNameOnly(name: String, tables: List[TableWithNameAndType]) {
  def asSchemaDefinition(jdbcMetadata: JdbcMetaData): Unit = {
    tables.foreach { table =>
      val jdbcColumns = table.attrs.map { case (attrName, attrType, comment) =>
        new JdbcColumn(attrName)
      }
      jdbcMetadata.addTable("", name, table.name, jdbcColumns.asJava)
      jdbcMetadata.addTable("", "", table.name, jdbcColumns.asJava)
    }
  }
}

object SchemaHandler {
  private val watcherMap: scala.collection.concurrent.TrieMap[String, Settings] =
    scala.collection.concurrent.TrieMap.empty

}

/** Handles access to datasets metadata, eq. domains / types / schemas.
  *
  * @param storage
  *   : Underlying filesystem manager
  */
class SchemaHandler(storage: StorageHandler, cliEnv: Map[String, String] = Map.empty)(implicit
  settings: Settings
) extends LazyLogging {

  private def forceJobPrefixRegex: Regex = settings.appConfig.forceJobPattern.r
  private def forceTaskPrefixRegex: Regex = settings.appConfig.forceTablePattern.r

  @throws[Exception]
  private def checkTypeDomainsJobsValidity(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    reload: Boolean = false
  )(implicit storage: StorageHandler): List[ValidationMessage] = {

    val domainStructureValidity = DomainInfo.checkFilenamesValidity()(storage, settings)
    val types = this.types(reload)
    if (types.isEmpty) {
      throw new Exception(
        "No types defined. Please define types in metadata/types/default.sl.yml"
      )
    }

    val typesValidity = types.map(_.checkValidity())
    val loadedDomains = this.domains(domainNames, tableNames, reload)
    val loadedDomainsValidity = loadedDomains.map(_.checkValidity(this))
    val domainsValidity =
      domainStructureValidity ++ loadedDomainsValidity
    val domainsVarsValidity = checkDomainsVars()
    val jobsVarsValidity = checkJobsVars() // job vars may be defined at runtime.
    val loadedJobs = this.jobs(reload)
    val jobsValidity = loadedJobs.map(_.checkValidity(this))
    val (targetOrchestrator, dagsValidity) = checkDagsValidity()
    val allErrors = typesValidity ++ domainsValidity ++ jobsValidity ++ dagsValidity

    val errs = allErrors.flatMap {
      case Left(values) => values
      case Right(_)     => Nil
    }
    errs ++ domainsVarsValidity ++ jobsVarsValidity
  }

  def orchestratorName(): Option[String] = {
    val (targetOrchestrator, _) = checkDagsValidity()
    targetOrchestrator
  }

  def checkDagsValidity(): (Option[String], List[Either[List[ValidationMessage], Boolean]]) = {
    val dagConfigs = this.loadDagGenerationConfigs(instantiateVars = false)
    val domainDags =
      this
        .domains()
        .flatMap(dom =>
          dom.tables
            .flatMap { table =>
              val dagRef = table.metadata.flatMap(_.dagRef)
              dagRef.map((dom.name, table.name, _))
            }
        )
    val taskDags = this.tasks().flatMap { task =>
      task.dagRef.map((task.domain, task.name, _))

    }
    val appDags = settings.appConfig.dagRef
      .map { dagRef =>
        val load =
          dagRef.load match {
            case Some(dag) =>
              Some("application", "load", dag)
            case None =>
              None
          }
        val transform =
          dagRef.transform match {
            case Some(dag) =>
              Some("application", "transform", dag)
            case None =>
              None
          }
        List(load, transform).flatten
      }
      .getOrElse(Nil)

    val modelDagConfigNames = (domainDags ++ taskDags ++ appDags).map {
      case (dom, tbl, dagConfigPath) =>
        val index = dagConfigPath.lastIndexOf("/")
        val dagConfigName =
          if (index >= 0)
            dagConfigPath.substring(index + 1)
          else
            dagConfigPath
        if (dagConfigName.endsWith(".sl.yml"))
          (dom, tbl, dagConfigName.dropRight(".sl.yml".length))
        else
          (dom, tbl, dagConfigName)
    }.toSet

    var targetOrchestrator: Option[String] = None
    val errors =
      modelDagConfigNames.toList.flatMap { case (dom, tbl, dagConfigName) =>
        if (!dagConfigs.contains(dagConfigName))
          Some(
            ValidationMessage(
              severity = Error,
              target = dagConfigName,
              message = s"DAG config $dagConfigName in model ${dom}.${tbl} not found"
            )
          )
        else {
          val dagConfig = dagConfigs(dagConfigName)
          val filenamePartStartIndex = dagConfig.template.lastIndexOf("/")
          val dagConfigTemplate =
            if (filenamePartStartIndex >= 0)
              dagConfig.template.substring(filenamePartStartIndex + 1)
            else
              dagConfig.template

          val firstSeparatorIndex = dagConfigTemplate.indexOf("__")
          if (firstSeparatorIndex < 0) {
            Some(
              ValidationMessage(
                severity = Error,
                target = dagConfigName,
                message =
                  s"DAG config $dagConfigName in ${dom}.${tbl} is not valid. template name ${dagConfig.template} should contain __"
              )
            )
          } else {
            val usedOrchestrator = dagConfigTemplate.substring(0, firstSeparatorIndex)
            // usedOrchestrator should be the same for all DAGs
            targetOrchestrator match {
              case None =>
                targetOrchestrator = Some(usedOrchestrator)
                None
              case Some(orch) =>
                if (orch != usedOrchestrator)
                  Some(
                    ValidationMessage(
                      severity = Error,
                      target = dagConfigName,
                      message =
                        s"DAG config $dagConfigName in ${dom}.${tbl} is not valid. template name ${dagConfig.template} should start with ${orch}__ but $usedOrchestrator found. Only one orchestrator can be targeted."
                    )
                  )
                else
                  None
            }

          }
        }
      }

    val result =
      if (errors.isEmpty)
        Right(true)
      else
        Left(errors)
    (targetOrchestrator, List(result))
  }

  private def checkDomainsVars(): List[ValidationMessage] = {
    storage
      .list(
        DatasetArea.load,
        extension = ".sl.yml",
        recursive = true,
        exclude = Some(Pattern.compile("_.*"))
      )
      .map(_.path)
      .flatMap(checkVarsAreDefined)
  }

  def checkJobsVars(): List[ValidationMessage] = {
    val paths =
      storage.list(DatasetArea.transform, ".sl.yml", recursive = true).map(_.path)
    val ymlWarnings = paths.flatMap(checkVarsAreDefined)
    val sqlPaths =
      (storage.list(DatasetArea.transform, ".sql.j2", recursive = true) ++ storage.list(
        DatasetArea.transform,
        ".sql",
        recursive = true
      ) ++ storage.list(
        DatasetArea.transform,
        ".py",
        recursive = true
      )).map(_.path)
    val sqlWarnings = sqlPaths.flatMap { path =>
      val filename = path.getName()
      val prefix = if (filename.endsWith(".sql.j2")) {
        filename.dropRight(7)
      } else if (filename.endsWith(".sql")) {
        filename.dropRight(4)
      } else { // .py
        filename.dropRight(3)
      }
      val sqlPath = taskCommandPath(path.getParent(), prefix)
      sqlPath.map(checkVarsAreDefined).getOrElse(Nil)
    }

    ymlWarnings ++ sqlWarnings
  }

  def checkValidity(
    config: ValidateConfig = ValidateConfig()
  ): Try[(List[ValidationMessage], Int, Int)] = Try {
    val envErrorsAndWarnings = EnvDesc.checkValidity(storage, settings)
    val settingsErrorsAndWarnings = settings.appConfig.checkValidity(storage, settings)
    val typesDomainsJobsErrorsAndWarnings =
      checkTypeDomainsJobsValidity(reload = config.reload)(storage)
    val deserErrors = deserializedDomains(DatasetArea.load)
      .filter { case (path, res) =>
        res.isFailure
      } map { case (path, err) =>
      ValidationMessage(
        severity = Error,
        target = path.toString,
        message = s"${path.toString} could not be deserialized"
      )
    }

    val allErrorsAndWarnings =
      envErrorsAndWarnings ++ settingsErrorsAndWarnings ++ typesDomainsJobsErrorsAndWarnings ++ deserErrors ++ this._domainErrors ++ this._jobErrors
    val (warnings, errors) = allErrorsAndWarnings.partition(_.severity == Warning)
    val errorCount = errors.length
    val warningCount = warnings.length

    (allErrorsAndWarnings, errorCount, warningCount)
  }

  def getDdlMapping(attributes: List[TableAttribute]): Map[String, Map[String, String]] = {
    attributes.flatMap { attr =>
      val ddlMapping = types().find(_.name == attr.`type`).map(_.ddlMapping)
      ddlMapping match {
        case Some(Some(mapping)) =>
          Some(attr.name -> mapping) // we found the primitive type and it has a ddlMapping
        case None       => None // we did not find the primitive type (should never happen)
        case Some(None) => None // we found the primitive type but it has no ddlMapping
      }
    }.toMap
  }

  def getAttributesWithDDLType(schema: SchemaInfo, dbName: String): List[(String, String)] = {
    schema.attributes.flatMap { attr =>
      val ddlMapping = types().find(_.name == attr.`type`).map(_.ddlMapping)
      ddlMapping match {
        case Some(Some(mapping)) =>
          Some(
            attr.name -> mapping.getOrElse(
              dbName,
              throw new Exception(
                s"${attr.name}: ${attr.`type`} DDL mapping not found for $dbName"
              )
            )
          ) // we found the primitive type and it has a ddlMapping
        case None if attr.`type` != "struct" =>
          // we did not find the primitive type (should never happen)
          throw new Exception(s"${attr.name}: ${attr.`type`} DDL mapping not found")
        case Some(None) if attr.`type` == "struct" =>
          // we found the primitive type but it has no ddlMapping
          None
        case Some(None) | None =>
          // we found the primitive type but it has no ddlMapping
          throw new Exception(s"${attr.name}: ${attr.`type`} DDL mapping not found (None)")
      }
    }
  }

  def loadTypes(filename: String): List[Type] = {
    val typesCometPath = new Path(DatasetArea.types, filename + ".sl.yml")
    if (storage.exists(typesCometPath)) {
      YamlSerde.deserializeYamlTypes(storage.read(typesCometPath), typesCometPath.toString)
    } else
      List.empty[Type]
  }

  def types(reload: Boolean = false): List[Type] =
    if (reload || _types == null) loadTypes() else _types

  // Do not load if not required (useful for the API)
  var _types: List[Type] = _

  /** All defined types. Load all default types defined in the file default.sl.yml Types are located
    * in the only file "types.sl.yml" Types redefined in the file "types.sl.yml" supersede the ones
    * in "default.sl.yml"
    */
  @throws[Exception]
  private def loadTypes(): List[Type] = {
    val resourceTypes = Resource.asString("types/default.sl.yml") match {
      case Some(value) =>
        YamlSerde.deserializeYamlTypes(value, "resource:types/default.sl.yml")
      case None =>
        Nil
    }
    val defaultTypes = loadTypes("default") :+ Type("struct", ".*", PrimitiveType.struct)
    val types = loadTypes("types")

    val redefinedResourceTypeNames = resourceTypes.map(_.name).intersect(defaultTypes.map(_.name))
    val defaultAndRedefinedTypes =
      resourceTypes.filter(resourceType =>
        !redefinedResourceTypeNames.contains(resourceType.name)
      ) ++ defaultTypes

    val redefinedTypeNames = defaultAndRedefinedTypes.map(_.name).intersect(types.map(_.name))

    this._types = defaultAndRedefinedTypes.filter(defaultType =>
      !redefinedTypeNames.contains(defaultType.name)
    ) ++ types

    this._types
  }

  def expectationMacros: String = {
    val j2Files = storage.list(DatasetArea.expectations, ".j2", recursive = true).map(_.path)
    val macros = j2Files
      .map { path =>
        val content = storage.read(path)
        "\n" + content + "\n"
      }
    macros.mkString("\n")
  }

  def macros: String = {
    val j2Files = storage.list(DatasetArea.macros, ".j2", recursive = true).map(_.path)
    val macros = j2Files
      .map { path =>
        val content = storage.read(path)
        "\n" + content + "\n"
      }
    macros.mkString("\n")
  }

  def allMacros: String = expectationMacros + "\n" + macros + "\n"

  private def viewName(sqlFile: Path) =
    if (sqlFile.getName().endsWith(".sql.j2"))
      sqlFile.getName().dropRight(".sql.j2".length)
    else
      sqlFile.getName().dropRight(".sql".length)

  private def loadSqlJ2File(sqlFile: Path): (String, String) = {
    val sqlExpr = storage.read(sqlFile)
    val sqlName = viewName(sqlFile)
    sqlName -> sqlExpr
  }

  private def loadSqlJ2File(path: Path, viewName: String): Option[String] = {
    val viewFile =
      listSqlj2Files(path).find(_.path.getName().startsWith(s"$viewName.sql")).map(_.path)
    viewFile.map { viewFile =>
      val (viewName, viewContent) = loadSqlJ2File(viewFile)
      viewContent
    }
  }

  private def listSqlj2Files(path: Path): List[FileInfo] = {
    val j2Files = storage.list(path, extension = ".sql.j2", recursive = true)
    val sqlFiles = storage.list(path, extension = ".sql", recursive = true)
    val allFiles = j2Files ++ sqlFiles
    allFiles
  }

  lazy val slDateVars: Map[String, String] = {
    val today = LocalDateTime.now
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val yearFormatter = DateTimeFormatter.ofPattern("yyyy")
    val monthFormatter = DateTimeFormatter.ofPattern("MM")
    val dayFormatter = DateTimeFormatter.ofPattern("dd")
    val hourFormatter = DateTimeFormatter.ofPattern("HH")
    val minuteFormatter = DateTimeFormatter.ofPattern("mm")
    val secondFormatter = DateTimeFormatter.ofPattern("ss")
    val milliFormatter = DateTimeFormatter.ofPattern("SSS")
    val epochMillis = today.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli
    Map(
      "sl_date"         -> today.format(dateFormatter),
      "sl_datetime"     -> today.format(dateTimeFormatter),
      "sl_year"         -> today.format(yearFormatter),
      "sl_month"        -> today.format(monthFormatter),
      "sl_day"          -> today.format(dayFormatter),
      "sl_hour"         -> today.format(hourFormatter),
      "sl_minute"       -> today.format(minuteFormatter),
      "sl_second"       -> today.format(secondFormatter),
      "sl_milli"        -> today.format(milliFormatter),
      "sl_epoch_second" -> (epochMillis / 1000).toString,
      "sl_epoch_milli"  -> epochMillis.toString
    )
  }

  def dataBranch(): Option[String] =
    this.activeEnvVars().get("sl_data_branch") match {
      case Some(branch) if branch.nonEmpty && branch != "None" => Some(branch)
      case _                                                   => None
    }

  /*
  area {
  incoming = "incoming"
  incoming = ${?SL_AREA_INCOMING}
  incoming = ${?SL_INCOMING}
  stage = "stage"
  stage = ${?SL_AREA_STAGE}
  unresolved = "unresolved"
  unresolved = ${?SL_AREA_UNRESOLVED}
  archive = "archive"
  archive = ${?SL_AREA_ARCHIVE}
  ingesting = "ingesting"
  ingesting = ${?SL_AREA_INGESTING}
  replay = "replay"
  replay = ${?SL_AREA_REPLAY}
  hiveDatabase = "${domain}_${area}"
  hiveDatabase = ${?SL_AREA_HIVE_DATABASE}
}

   */

  def activeEnvVars(
    reload: Boolean = false,
    env: Option[String] = None,
    root: Option[String] = None
  ): Map[String, String] = {

    val currentEnv =
      env.orElse {
        settings.appConfig.env match {
          case e if e.nonEmpty && e != "None" => Some(e)
          case _                              => None
        }
      }
    if (reload || _activeEnvVars == null) loadActiveEnvVars(currentEnv, root)
    val withRootValue =
      root match {
        case Some(value) => _activeEnvVars + ("SL_ROOT" -> value)
        case None        => this._activeEnvVars
      }
    val pathEnvVars = Map(
      "SL_AREA_INCOMING"      -> settings.appConfig.area.incoming,
      "SL_INCOMING"           -> settings.appConfig.area.incoming,
      "SL_AREA_STAGE"         -> settings.appConfig.area.stage,
      "SL_AREA_UNRESOLVED"    -> settings.appConfig.area.unresolved,
      "SL_AREA_ARCHIVE"       -> settings.appConfig.area.archive,
      "SL_AREA_INGESTING"     -> settings.appConfig.area.ingesting,
      "SL_AREA_HIVE_DATABASE" -> settings.appConfig.area.hiveDatabase,
      "SL_DATASETS"           -> settings.appConfig.datasets,
      "SL_METADATA"           -> settings.appConfig.metadata,
      "SL_DAGS"               -> settings.appConfig.dags,
      "SL_TESTS"              -> settings.appConfig.tests,
      "SL_TYPES"              -> settings.appConfig.types,
      "SL_MACROS"             -> settings.appConfig.macros
    )
    val result = withRootValue ++ pathEnvVars
    result
  }

  def refs(reload: Boolean = false): RefDesc = {
    if (reload || _refs == null) loadRefs()
    this._refs
  }

  // Do not load if not required (useful for the API)
  private var _activeEnvVars: Map[String, String] = _
  private var _refs: RefDesc = _

  @throws[Exception]
  private def loadActiveEnvVars(
    env: Option[String] = None,
    root: Option[String]
  ): Map[String, String] = {
    val slVarsAsProperties =
      System
        .getProperties()
        .keySet()
        .iterator()
        .asScala
        .filter(_.toString.startsWith("SL_"))
        .map { key =>
          key.toString -> System.getProperties().get(key).toString
        }
        .toMap

    val externalProps =
      root match {
        case None => sys.env ++ slVarsAsProperties
        case Some(root) => // root is forced by the user
          sys.env ++ slVarsAsProperties ++ Map("SL_ROOT" -> root)
      }

    // We first load all variables defined in the common environment file.
    // variables defined here are default values.
    val globalsCometPath = DatasetArea.env()
    val globalEnv = EnvDesc.loadEnv(globalsCometPath)(storage)
    // System Env variables may be used as values for variables defined in the env files.
    val globalEnvVars =
      globalEnv
        .map(_.env)
        .getOrElse(Map.empty)
        .view
        .mapValues(
          _.richFormat(externalProps, slDateVars)
        )
        .toMap
    // will replace with sys.env

    val activeEnvName =
      env // passed as argument (by the API)
        .orElse(Option(System.getProperties.get("env"))) // passed as a system property
        .orElse(Option(System.getenv().get("SL_ENV"))) // passed as an environment variable
        .orElse(globalEnvVars.get("SL_ENV")) // defined in the default env file
        .getOrElse(settings.appConfig.env) // defined in the application.sl.yml
        .toString
    // The env var SL_ENV should be set to the profile under wich starlake is run.
    // If no profile is defined, only default values are used.

    val localEnvVars =
      if (activeEnvName.nonEmpty && activeEnvName != "None") {
        val envsCometPath = DatasetArea.env(activeEnvName)

        // We subsittute values defined in the current profile with variables defined
        // in the default env file
        val allVars = externalProps ++ globalEnvVars ++ slDateVars

        val localEnvDesc = EnvDesc
          .loadEnv(envsCometPath)(storage)
          .map(_.env)
          .getOrElse(Map.empty)

        val independentKeyValues = localEnvDesc.map { case (k, v) =>
          k -> v.richFormat(localEnvDesc.removed(k), Map.empty)
        }
        independentKeyValues.view
          .mapValues(
            _.richFormat(sys.env, allVars)
          )
          .toMap

      } else
        Map.empty[String, String]

    // Please note below how profile specific vars override default profile vars.
    val activeEnvVars = externalProps ++ slDateVars ++ globalEnvVars ++ localEnvVars ++ cliEnv
    val undefinedVars = EnvDesc.allEnvVars(settings.storageHandler(), settings)
    val emptyVars = undefinedVars.flatMap { key =>
      if (activeEnvVars.contains(key)) None
      else {
        logger.warn(s"Variable $key defined to empty string in current execution environment")
        Some(key -> "")
      }
    }
    this._activeEnvVars = activeEnvVars ++ emptyVars.toMap
    this._activeEnvVars
  }

  @throws[Exception]
  private def loadRefs(): RefDesc = {
    val refsPath = DatasetArea.refs()
    val refs = if (storage.exists(refsPath)) {
      val rawContent = storage.read(refsPath)
      val content = Utils.parseJinja(rawContent, activeEnvVars())
      YamlSerde.deserializeYamlRefs(content, refsPath.toString)
    } else
      RefDesc(latestSchemaVersion, settings.appConfig.refs)
    this._refs = refs
    this._refs
  }

  /** Fnd type by name
    *
    * @param tpe
    *   : Type name
    * @return
    *   Unique type referenced by this name.
    */
  def getType(tpe: String): Option[Type] = types().find(_.name == tpe)
  def deserializedDomains(
    domainPath: Path,
    domainNames: List[String] = Nil,
    raw: Boolean = false
  ): List[(Path, Try[DomainInfo])] = {
    val directories = storage.listDirectories(path = domainPath)
    val requestedDomainDirectories =
      if (domainNames.isEmpty)
        directories
      else {
        directories.filter(p => domainNames.contains(p.getName()))
      }
    val domainsOnly = requestedDomainDirectories
      .map { directory =>
        val configPath = new Path(directory, "_config.sl.yml")
        if (storage.exists(configPath)) {
          val configContent = storage.read(configPath)
          logger.info(
            "Loading domain config from " + configPath.toString + " ... in raw mode = " + raw
          )
          logger.info("Content:\n" + configContent)
          val domainOnly = YamlSerde
            .deserializeYamlLoadConfig(
              if (raw) configContent
              else Utils.parseJinja(configContent, activeEnvVars()),
              configPath.toString,
              isForExtract = false
            )
          val domainWithName = domainOnly
            .map { domainOnly =>
              val domainName = if (domainOnly.name.isEmpty) directory.getName else domainOnly.name
              if (domainName != domainOnly.name)
                logger.warn(
                  s"Domain name ${domainOnly.name} in ${configPath.toString} is different from the directory name ${directory.getName}"
                )
              val namedDomain =
                if (domainOnly.name.isEmpty) {
                  logger.info(
                    s"Setting domain name to ${domainName} in ${configPath.toString}"
                  )
                  val domainUpdated =
                    YamlSerde
                      .deserializeYamlLoadConfig(
                        configContent,
                        configPath.toString,
                        isForExtract = false
                      )
                      .getOrElse(
                        throw new StarlakeConfigException(
                          "Failed to deserialize domain: " + configPath
                        )
                      )
                      .copy(name = domainName)
                  YamlSerde.serializeToPath(configPath, domainUpdated)(storage)
                  domainUpdated
                } else
                  domainOnly
              namedDomain
            }
          (configPath, domainWithName)

        } else {
          (
            configPath,
            Success(
              DomainInfo(name = directory.getName, comment = Some(s"${directory.getName} domain"))
            )
          )
        }
      }
    val domains = domainsOnly.map { case (configPath, domainOnly) =>
      // grants list can be set a comma separated list in YAML , and transformed to a list while parsing
      val domainCompleted = domainOnly.map { domain =>
        {
          if (raw) {
            domain
          } else {
            val tables = domain.tables.map { table => table.normalize() }
            val metadata = domain.metadata.getOrElse(Metadata())
            // ideally the emptyNull field should set during object construction but the settings
            // object is not available in the Metadata object
            val enrichedMetadata = metadata
              .copy(emptyIsNull = metadata.emptyIsNull.orElse(Some(settings.appConfig.emptyIsNull)))

            // set domain name
            val domainName =
              if (domain.name == "") configPath.getParent().getName() else domain.name
            val domainResult =
              domain.copy(name = domainName, tables = tables, metadata = Some(enrichedMetadata))
            domainResult
          }
        }
      }
      (configPath, domainCompleted)
    }
    domains
  }

  def tableNames(): List[String] = {
    domains().flatMap { d =>
      d.tables.map { t =>
        s"${d.finalName}.${t.finalName}"
      }
    }
  }

  def taskTableNames(): List[String] = {
    jobs().flatMap { j =>
      j.tasks.map { t =>
        s"${j.getName()}.${t.getTableName()}"
      }
    }
  }

  def findTableNames(domainName: Option[String]): List[String] = {
    val tablesFromDomain =
      domainName match {
        case Some(domainName) => {
          domains()
            .find(_.finalName.toLowerCase() == domainName.toLowerCase())
            .map { d => d.tables.map(d.finalName + "." + _.finalName) }
            .getOrElse(Nil)
        }
        case None =>
          domains().flatMap(d => d.tables.map(d.finalName + "." + _.finalName))
      }

    val tablesFromDomainOrTasks =
      if (tablesFromDomain.isEmpty) {
        domainName match {
          case Some(domainName) =>
            tasks().filter(_.domain.toLowerCase() == domainName.toLowerCase()).map(_.name)
          case None =>
            tasks().map(_.name)
        }
      } else
        tablesFromDomain

    tablesFromDomainOrTasks
  }

  def getTablesWithColumnNames(
    tableNames: List[String],
    accessToken: Option[String]
  ): List[(String, TableWithNameAndType)] = {
    val objects = objectNames()
    tableNames.flatMap { tableFullName =>
      val tableComponents = tableFullName.split('.')
      val (domainName, tableName) = tableComponents match {
        case Array(dbName, domainName, tableName) => (domainName, tableName)
        case Array(domainName, tableName)         => (domainName, tableName)
        case Array(tableName)                     => (null, tableName)
        case _ => throw new Exception(s"Invalid table name $tableFullName")
      }
      val domainNameAndTable =
        domainName match {
          case null =>
            objects
              .flatMap(_.tables)
              .find {
                _.name.toLowerCase() == tableName.toLowerCase()
              }
              .map {
                ("", _)
              }
          case _ =>
            objects
              .find { d =>
                d.name.toLowerCase() == domainName.toLowerCase()
              }
              .flatMap { d =>
                d.tables.find(_.name.toLowerCase() == tableName.toLowerCase())
              }
              .map { t =>
                (domainName, t)
              }
              .orElse {
                val connectionInfo =
                  settings.appConfig.getConnection(settings.appConfig.connectionRef)
                // getting metadata from snowflake is definitely too slow
                if (connectionInfo.isSnowflake()) {
                  JdbcDbUtils
                    .extractColumnsUsingInformationSchema(
                      connectionInfo,
                      domainName,
                      tableName
                    )
                    .toOption
                    .map { colsAndTypes =>
                      val t = TableWithNameAndType(
                        tableName,
                        colsAndTypes
                          .map(it =>
                            (
                              it._1,
                              PrimitiveType
                                .fromSQLType(
                                  connectionInfo.getJdbcEngineName().toString,
                                  it._2,
                                  this
                                )
                                .toString,
                              None
                            )
                          )
                      )
                      (domainName, t)
                    }
                } else {

                  val attrs =
                    new ExtractSchema(this)
                      .extractTable(domainName, tableName, None, accessToken)
                      .toOption
                      .filter(_.tables.nonEmpty)
                      .map { it =>
                        logger.info(s"Extracted schema for $domainName.$tableName from DB source")
                        val attrs =
                          it.tables.head.attributes.map(attr => (attr.name, attr.`type`, None))
                        attrs
                      }
                      .getOrElse {
                        logger.warn(s"Table $domainName.$tableNames not found in external schemas")
                        Nil
                      }

                  Some((domainName, TableWithNameAndType(tableName, attrs)))
                }
              }
        }
      domainNameAndTable
    }
  }

  def tableOnly(tableName: String): Try[TableDesc] =
    Try {
      val components = tableName.split('.')
      val domainName = components(0)
      val tablePartName = components(1)

      var folder = DomainInfo.path(domainName)
      loadTableRef(tablePartName + ".sl.yml", raw = false, folder)
    }

  def domains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    reload: Boolean = false,
    raw: Boolean = false
  ): List[DomainInfo] = {
    _domains match {
      case Some(domains) if domains.nonEmpty =>
        if (reload || raw) { // raw is used only for special use cases so we force it to reload
          val (_, domains) = initDomains(domainNames, tableNames, raw)
          domains
        } else
          domains
      case _ =>
        val (_, domains) = initDomains(domainNames, tableNames, raw)
        domains
    }
  }

  private[handlers] var (_domainErrors, _domains)
    : (List[ValidationMessage], Option[List[DomainInfo]]) =
    (Nil, None)

  private var _externals = List.empty[DomainInfo]
  private var _externalLastUpdate = LocalDateTime.MIN

  def externals(): List[DomainInfo] = {
    if (_externals.isEmpty) loadExternals()
    _externals
  }

  def loadExternals(): List[DomainInfo] = {
    _externals = if (storage.exists(DatasetArea.external)) {
      val loadedDomains =
        loadDomains(DatasetArea.external, Nil, Nil, raw = false, _externalLastUpdate) match {
          case (list, Nil) => list.collect { case Success(domain) => domain }
          case (list, errors) =>
            errors.foreach {
              case Failure(err) =>
                logger.warn(
                  s"There is one or more invalid Yaml files in your domains folder:${Utils.exceptionAsString(err)}"
                )
              case Success(_) => // ignore
            }
            list.collect { case Success(domain) => domain }
        }
      loadedDomains.foreach { domain =>
        domain.tables.foreach { table =>
          _externals.find(_.name == domain.name) match {
            case Some(existingDomain) =>
              val otherTables = existingDomain.tables.filter(_.name != table.name)
              _externals = _externals.map { d =>
                if (d.name == domain.name) {
                  d.copy(tables = otherTables :+ table)
                } else {
                  d
                }
              }
            case None =>
              _externals = _externals :+ domain
          }
        }
      }
      _externalLastUpdate = LocalDateTime.now()
      loadedDomains
    } else
      Nil
    _externals
  }

  def listDagNames(): List[String] =
    DagHandler.listDagNames(storage)

  def checkDagNameValidity(dagRef: String): Either[List[ValidationMessage], Boolean] =
    DagHandler.checkDagNameValidity(dagRef, storage)

  def deserializedDagGenerationConfigs(dagPath: Path): Map[String, DagInfo] =
    DagHandler.deserializedDagGenerationConfigs(dagPath, storage)

  /** Global dag generation config can only be defined in "dags" folder in the metadata folder.
    * Override of dag generation config can be done inside domain config file at domain or table
    * level.
    */
  def loadDagGenerationConfigs(instantiateVars: Boolean): Map[String, DagInfo] =
    DagHandler.loadDagGenerationConfigs(storage, instantiateVars)

  /** All defined domains Domains are defined under the "domains" folder in the metadata folder
    */
  @throws[Exception]
  private def initDomains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean = false
  ): (List[ValidationMessage], List[DomainInfo]) = {
    val area = DatasetArea.load
    val (validDomainsFile, invalidDomainsFiles) =
      loadDomains(area, domainNames, tableNames, raw)

    val domains = validDomainsFile
      .collect { case Success(domain) => domain }
      .map(domain => this.fromXSD(domain))

    val (nonEmptyDomains, emptyDomains) = domains.partition(_.tables.nonEmpty)
    emptyDomains.foreach { domain =>
      logger.warn(s"Domain ${domain.name} discarded because it's empty")
    }
    val nameErrors = Utils.duplicates(
      "Domain name",
      nonEmptyDomains.map(_.name),
      s"%s is defined %d times. A domain can only be defined once."
    ) match {
      case Right(_) => Nil
      case Left(errors) =>
        errors
    }
    val directoryErrors = Utils.duplicates(
      "Domain directory",
      nonEmptyDomains.flatMap(_.resolveDirectoryOpt()),
      s"%s is defined %d times. A directory can only appear once in a domain definition file."
    ) match {
      case Right(_) =>
        if (!raw) {
          this._domains = Some(nonEmptyDomains)
          DatasetArea.initDomains(storage, nonEmptyDomains.map(_.name))

        }
        Nil
      case Left(errors) =>
        errors
    }

    invalidDomainsFiles.foreach {
      case Failure(err) =>
        logger.error(
          s"There is one or more invalid Yaml files in your domains folder:${err.getMessage}"
        )
      case Success(_) => // ignore
    }
    this._domainErrors = nameErrors ++ directoryErrors
    this._domainErrors.foreach(err => logger.error(err.toString()))
    (this._domainErrors, nonEmptyDomains)
  }

  private def loadDomains(
    area: Path,
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil,
    raw: Boolean,
    lastUpdated: LocalDateTime = LocalDateTime.MIN
  ): (List[Try[DomainInfo]], List[Try[DomainInfo]]) = {
    val (validDomainsFile, invalidDomainsFiles) =
      deserializedDomains(area, domainNames, raw)
        .map {
          case (path, Success(domain)) =>
            logger.info(s"Loading domain from $path")
            val folder = path.getParent()
            val schemaRefs = loadTableRefs(tableNames, raw, folder, lastUpdated)
            val tables = Option(domain.tables).getOrElse(Nil) ::: schemaRefs
            val finalTables =
              if (raw) tables
              else tables.map(t => t.copy(metadata = Some(t.mergedMetadata(domain.metadata))))
            logger.info(s"Successfully loaded Domain  in $path")
            Success(domain.copy(tables = finalTables))
          case (path, Failure(e)) =>
            logger.error(s"Failed to load domain in $path")
            Utils.logException(logger, e)
            Failure(e)
        }
        .partition(_.isSuccess)
    (validDomainsFile, invalidDomainsFiles)
  }

  def loadTableRefs(
    tableNames: List[String] = Nil,
    raw: Boolean,
    folder: Path,
    lastUpdated: LocalDateTime
  ): List[SchemaInfo] = {
    val tableRefNames =
      storage
        .list(folder, extension = ".sl.yml", since = lastUpdated, recursive = true)
        .map(_.path.getName())
        .filter(!_.startsWith("_config."))

    val requestedTables =
      if (tableNames.isEmpty)
        tableRefNames
      else {
        tableRefNames.filter(tableNames.map(_ + ".sl.yml").contains(_))
      }

    val schemaRefs = requestedTables
      .map { tableRefName => loadTableRef(tableRefName, raw, folder) }
      .map(_.table)
    schemaRefs
  }

  def loadTableRef(
    tableRefName: String,
    raw: Boolean,
    folder: Path
  ): TableDesc = {
    val schemaPath = new Path(folder, tableRefName)
    logger.debug(s"Loading schema from $schemaPath")
    val tableDesc =
      YamlSerde
        .deserializeYamlTables(
          if (raw)
            storage.read(schemaPath)
          else
            Utils
              .parseJinja(storage.read(schemaPath), activeEnvVars()),
          schemaPath.toString
        )
        .head
    val namedTableDesc =
      if (tableDesc.table.name.isEmpty)
        tableDesc.copy(table = tableDesc.table.copy(name = tableRefName))
      else
        tableDesc
    if (namedTableDesc.table.name != tableRefName.dropRight(".sl.yml".length))
      logger.warn(
        s"Table name ${namedTableDesc.table.name} in ${schemaPath.toString} is different from the file name ${tableRefName}"
      )
    namedTableDesc
  }

  private def checkVarsAreDefined(path: Path): Set[ValidationMessage] = {
    val content = storage.read(path)
    checkVarsAreDefined(path, content)
  }

  private def checkVarsAreDefined(path: Path, content: String): Set[ValidationMessage] = {
    val vars = content.extractVars()
    val envVars = activeEnvVars().keySet
    val undefinedVars = vars.diff(envVars)
    undefinedVars.map(undefVar =>
      ValidationMessage(
        Warning,
        "Variable",
        s"""${path.getName} contains undefined var: ${undefVar}"""
      )
    )
  }

  def loadJobTasksFromFile(jobPath: Path, taskNames: List[String] = Nil): Try[AutoJobInfo] =
    for {
      jobDesc   <- loadJobDesc(jobPath)
      taskDescs <- loadAutoTasksInto(jobDesc, jobPath.getParent, taskNames)
    } yield taskDescs

  private def loadAutoTasksInto(
    jobDesc: AutoJobInfo,
    jobFolder: Path,
    taskNames: List[String] = Nil
  ): Try[AutoJobInfo] = {
    Try {
      // Load task refs and inject them in the job
      val autoTasksRefs = loadAutoTaskFiles(jobDesc, jobFolder, taskNames)

      val jobDescWithTaskRefs: AutoJobInfo =
        jobDesc.copy(tasks = Option(jobDesc.tasks).getOrElse(Nil) ::: autoTasksRefs)

      // set task name / domain / table and load sql/py file if any
      val tasks = jobDescWithTaskRefs.tasks.map { taskDesc =>
        buildAutoTaskInfo(jobDescWithTaskRefs, jobFolder, taskDesc)
      }
      val jobName = if (jobDesc.name.isEmpty) jobFolder.getName() else jobDesc.name
      // We do not check if task has a sql file associated with it, as it can be a python task

      val mergedTasks = jobDesc.default match {
        case Some(defaultTask) =>
          tasks.map { task =>
            defaultTask.merge(task)
          }
        case None =>
          tasks
      }
      // strip comments from SQL
      val mergedTasksWithStrippedComments = mergedTasks.map { task =>
        task.copy(
          presql = task.presql.map(sql => SQLUtils.stripComments(sql)),
          sql = task.sql.map(sql => SQLUtils.stripComments(sql)),
          postsql = task.postsql.map(sql => SQLUtils.stripComments(sql))
        )
      }
      AutoJobInfo(
        name = jobName,
        tasks = mergedTasksWithStrippedComments,
        default = jobDesc.default
      )
    } match {
      case Success(value) => Success(value)
      case Failure(exception) =>
        Failure(new Exception(s"Invalid Job file: $jobFolder(${exception.getMessage})", exception))
    }
  }

  private def loadJobDesc(jobPath: Path): Try[AutoJobInfo] =
    Try {
      logger.info("Loading job " + jobPath)
      val jobDesc =
        if (storage.exists(jobPath)) {
          val fileContent = storage.read(jobPath)
          val rootContent = Utils.parseJinja(fileContent, activeEnvVars())
          YamlSerde.deserializeYamlTransform(rootContent, jobPath.toString) match {
            case Failure(exception) =>
              throw exception
            case Success(autoJobDesc) =>
              if (autoJobDesc.getName().isEmpty)
                autoJobDesc.copy(name = jobPath.getParent.getName)
              else
                autoJobDesc
          }
        } else {
          // no _config.sl.yml file. Dir name is the job name
          AutoJobInfo(name = jobPath.getParent.getName, tasks = Nil)
        }

      if (jobDesc.name != jobPath.getParent.getName)
        logger.warn(
          s"Job name ${jobDesc.name} in ${jobPath.toString} is different from the folder name ${jobPath.getParent.getName}"
        )

      jobDesc
    }

  def buildAutoTaskInfo(
    jobDesc: AutoJobInfo,
    jobFolder: Path,
    taskDesc: AutoTaskInfo
  ): AutoTaskInfo = {
    val (taskShortName, tableName) = (taskDesc.name, taskDesc.table) match {
      case ("", "") =>
        throw new Exception(
          s"Task name or table must be defined for $taskDesc in ${jobFolder}"
        )
      case ("", table) =>
        (table, table)
      case (name, "") =>
        (name, name)
      case (name, table) =>
        (name, table)
    }
    // Domain name may be set explicitly or else we use the folder name
    val domainName = if (taskDesc.domain.isEmpty) jobFolder.getName else taskDesc.domain

    val filenamePrefix =
      if (taskDesc._filenamePrefix.isEmpty) taskShortName else taskDesc._filenamePrefix

    // task name is set explicitly and we always prefix it with the domain name except if it is already prefixed.
    val taskWithName = taskDesc.copy(
      domain = domainName,
      table = tableName,
      name = taskShortName,
      _filenamePrefix = filenamePrefix
    )

    val taskCommandFile: Option[Path] =
      taskCommandPath(jobFolder, taskWithName._filenamePrefix)

    val taskWithCommand = taskCommandFile
      .map { taskFile =>
        if (taskFile.toString.endsWith(".py"))
          taskWithName.copy(
            python = Some(taskFile)
          )
        else {
          val sqlContentRaw = storage.read(taskFile)
          val sqlContent = Utils.parseJinja(sqlContentRaw, activeEnvVars())
          val sqlTask = SqlTaskExtractor(sqlContent)
          val preSql = sqlTask.presql ++ taskWithName.presql
          val postSql = taskWithName.postsql ++ sqlTask.postsql
          taskWithName.copy(
            presql = preSql,
            sql = Option(sqlTask.sql),
            postsql = postSql
          )
        }
      }
      .getOrElse(taskWithName)

    // grants list can be set to a comma separated list in YAML , and transformed to a list while parsing
    val taskFinal =
      taskWithCommand.copy(
        rls = taskWithName.rls.map(rls => {
          val grants = rls.grants.flatMap(_.replaceAll("\"", "").split(','))
          rls.copy(grants = grants)
        }),
        acl = taskWithName.acl.map(acl => {
          val grants = acl.grants.flatMap(_.replaceAll("\"", "").split(','))
          acl.copy(grants = grants)
        })
      )

    // We do not check if task has a sql file associated with it, as it can be a python task

    val mergedTask = jobDesc.default match {
      case Some(defaultTask) =>
        defaultTask.merge(taskFinal)
      case None =>
        taskFinal
    }

    // apply refs
    val outputRef =
      mergedTask.database match {
        case Some(db) =>
          refs().getOutputRef(db, mergedTask.domain, mergedTask.table)
        case None =>
          refs().getOutputRef(mergedTask.domain, mergedTask.table)
      }
    val withRefTask =
      outputRef match {
        case Some(ref) =>
          mergedTask.copy(
            database = if (ref.database.isEmpty) None else Some(ref.database),
            domain = ref.domain,
            table = ref.table
          )
        case None =>
          mergedTask
      }

    // strip comments from SQL
    val taskWithStrippedComments =
      withRefTask.copy(
        presql = withRefTask.presql.map(sql => SQLUtils.stripComments(sql)),
        sql = withRefTask.sql.map(sql => SQLUtils.stripComments(sql)),
        postsql = withRefTask.postsql.map(sql => SQLUtils.stripComments(sql))
      )

    taskWithStrippedComments
  }

  private def loadAutoTaskFiles(
    jobDesc: AutoJobInfo,
    folder: Path,
    taskNames: List[String] = Nil
  ): List[AutoTaskInfo] = {
    // List[(prefix, filename, extension)]
    val taskNamesPrefix =
      taskNames.map(taskName => taskName + ".") // we search for any .sl.yml or .sql or .py or .***
    val allFiles =
      storage
        .list(folder, recursive = true)
        .map(_.path.getName())
        .filter(name =>
          !name.startsWith("_config.") &&
          (taskNames.isEmpty || taskNamesPrefix
            .exists(t => name.toLowerCase().startsWith(t.toLowerCase())))
        )
        .flatMap { filename =>
          // improve the code below to handle more than one extension
          List("sl.yml", "sql", "sql.j2", "py")
            .find(ext => filename.endsWith(s".$ext"))
            .map { ext =>
              val taskFileNameWithoutExt = filename.substring(0, filename.length - s".$ext".length)
              (taskFileNameWithoutExt, filename, ext)
            }
        }
    val ymlFiles = allFiles.filter { case (taskName, filename, ext) => ext == "sl.yml" }
    val sqlPyFiles = allFiles.filter { case (taskName, filename, ext) =>
      (ext == "sql" || ext == "sql.j2" || ext == "py") &&
      !ymlFiles.exists(_._1.equalsIgnoreCase(taskName)) &&
      !jobDesc.tasks.exists(_.name.equalsIgnoreCase(taskName))
    }
    val autoTasksRefNames: List[(String, String, String)] = ymlFiles ++ sqlPyFiles
    val autoTasksRefs = autoTasksRefNames.flatMap {
      case (taskFileNameWithoutExt, taskFilename, extension) =>
        val taskFullName = jobDesc.name + "." + taskFileNameWithoutExt
        extension match {
          case "sl.yml" =>
            Try {
              val taskPath = new Path(folder, taskFilename)
              val content = storage.read(taskPath)
              val taskDesc = YamlSerde
                .deserializeYamlTask(
                  Utils
                    .parseJinja(content, activeEnvVars()),
                  taskPath.toString
                )
                .copy(name = taskFileNameWithoutExt)

              val finalDomain = if (taskDesc.domain.isEmpty) jobDesc.name else taskDesc.domain
              val finalTable =
                if (taskDesc.table.isEmpty) taskFileNameWithoutExt else taskDesc.table
              taskDesc.copy(
                _filenamePrefix = taskFileNameWithoutExt,
                domain = finalDomain,
                table = finalTable
              )
            } match {
              case Failure(e) =>
                e.printStackTrace()
                logger.error(e.getMessage, e)
                None
              case Success(value) => Some(value)
            }
          case _ =>
            Some(
              AutoTaskInfo(
                name = taskFileNameWithoutExt,
                sql = None,
                database = None,
                domain = "",
                table = "",
                _filenamePrefix = taskFileNameWithoutExt,
                taskTimeoutMs = None
              )
            )
        }
    }
    autoTasksRefs
  }

  /** @param basePath
    * @param filePrefix
    * @param extensions
    * @return
    *   (path, prefix, extension)
    */
  def getTaskPathDetails(
    basePath: Path,
    filePrefix: String,
    extensions: List[String]
  ): Option[(Path, String, String)] = {
    extensions.flatMap { extension =>
      val path = new Path(basePath, s"$filePrefix.$extension")
      if (storage.exists(path)) {
        val prefix = path.getName.substring(0, path.getName.length - s".$extension".length)
        Some(path, prefix, extension)
      } else
        None
    }.headOption
  }

  /** @param basePath
    * @param sqlFilePrefix
    * @return
    *   (path, prefix, extension)
    */
  private def taskCommandPath(
    basePath: Path,
    sqlFilePrefix: String
  ): Option[Path] = {
    getTaskPathDetails(basePath, sqlFilePrefix, List("sql", "sql.j2", "py")).map {
      case (path, prefix, extension) => path
    }
  }

  def jobs(reload: Boolean = false): List[AutoJobInfo] = {
    if (_jobs.isEmpty || reload) {
      val (errors, validJobs) = loadJobs()
      this._jobs = validJobs
      this._jobErrors = errors
    }
    _jobs
  }

  def iamPolicyTags(): Option[IamPolicyTags] = {
    val path = DatasetArea.iamPolicyTags()
    if (storage.exists(path))
      Some(YamlSerde.deserializeIamPolicyTags(storage.read(path)))
    else {
      Utils.printOut(s"Warning: No IAM policy tags found in $path")
      None
    }
  }

  def tasks(reload: Boolean = false): List[AutoTaskInfo] = {
    if (reload) {
      val (errors, validJobs) = loadJobs()
      this._jobs = validJobs
      this._jobErrors = errors
    }
    jobs().flatMap(_.tasks)
  }

  def taskByFullName(taskFullName: String): Try[AutoTaskInfo] = Try {
    val allTasks = tasks()
    allTasks
      .find(t => t.fullName().equalsIgnoreCase(taskFullName))
      .getOrElse(throw new StarlakeNotFoundException(s"Task $taskFullName not found"))
  }
  def taskByTableName(domain: String, table: String): Option[AutoTaskInfo] = {
    val allTasks = tasks()
    allTasks.find(t => t.domain.equalsIgnoreCase(domain) && t.table.equalsIgnoreCase(table))
  }

  def taskByTableName(tableFullName: String): Option[AutoTaskInfo] = {
    val components = tableFullName.split('.')
    val domain = components(components.length - 2)
    val table = components(components.length - 1)
    val allTasks = tasks()
    allTasks.find(t => t.domain.equalsIgnoreCase(domain) && t.table.equalsIgnoreCase(table))
  }

  def taskOnly(
    taskFullName: String,
    reload: Boolean = false
  ): Try[AutoTaskInfo] = {
    val refs = loadRefs()
    if (refs.refs.isEmpty) {
      val components = taskFullName.split('.')
      require(
        components.length == 2,
        s"Task name $taskFullName should be composed of domain and task name separated by a dot"
      )
      val domainName = components(0)
      val taskPartName = components(1)
      val loadedTask =
        if (reload) {
          val theJob = _jobs.find(_.getName().equalsIgnoreCase(domainName))
          theJob match {
            case None =>
            case Some(job) =>
              val tasks = job.tasks.filterNot(_.fullName().equalsIgnoreCase(taskFullName))
              val newJob = job.copy(tasks = tasks)
              _jobs = _jobs.filterNot(_.getName().equalsIgnoreCase(domainName)) :+ newJob
          }
          None
        } else {
          _jobs.flatMap(_.tasks).find(_.fullName().equalsIgnoreCase(taskFullName))
        }
      loadedTask match {
        case Some(task) =>
          Success(task)
        case None =>
          val taskDesc =
            for {
              directory <- settings
                .storageHandler()
                .listDirectories(DatasetArea.transform)
                .find(_.getName().equalsIgnoreCase(domainName))
                .map(Success(_)) // convert Option to Try
                .getOrElse(Failure(new NoSuchElementException(s"Domain not found $domainName")))
              configPath = new Path(directory, "_config.sl.yml")
              jobDesc <- loadJobTasksFromFile(configPath, List(taskPartName))
              taskDesc = jobDesc.tasks
                .find(_.fullName().equalsIgnoreCase(taskFullName))
                .getOrElse(
                  throw new Exception(s"Task $taskFullName not found in $directory")
                )
            } yield {
              val mergedTask = jobDesc.default match {
                case Some(defaultTask) =>
                  defaultTask.merge(taskDesc)
                case None =>
                  taskDesc
              }
              mergedTask
            }

          taskDesc.map { t =>
            val theJob = _jobs.find(_.getName().equalsIgnoreCase(domainName))
            theJob match {
              case None =>
              case Some(job) =>
                val tasks = job.tasks :+ t
                val newJob = job.copy(tasks = tasks)
                _jobs = _jobs.filterNot(_.getName().equalsIgnoreCase(domainName)) :+ newJob
            }

          }

          taskDesc.orElse(
            taskByFullName(taskFullName)
          ) // because taskOnly can only handle task named after folder and file names
      }
    } else {
      taskByFullName(taskFullName)
    }
  }

  private var (_jobErrors, _jobs): (List[ValidationMessage], List[AutoJobInfo]) = (Nil, Nil)

  /** All Jobs are defined under the "transform" folder in the metadata folder
    */
  @throws[Exception]
  private def loadJobs(): (List[ValidationMessage], List[AutoJobInfo]) = {
    if (storage.exists(DatasetArea.transform)) {
      val directories = storage.listDirectories(DatasetArea.transform)
      val (validJobsFile, invalidJobsFile) = directories
        .map { directory =>
          val configPath = new Path(directory, "_config.sl.yml")
          val result = loadJobTasksFromFile(configPath)
          result match {
            case Success(_) =>
              logger.info(s"Successfully loaded Job $configPath")
            case Failure(e) =>
              logger.error(s"Failed to load Job $configPath")
              e.printStackTrace()
          }
          result
        }
        .partition(_.isSuccess)

      val validJobs = validJobsFile
        .collect { case Success(job) => job }

      val namePatternErrors = validJobs.filter(_.name.nonEmpty).map(_.name).flatMap { name =>
        if (!forceJobPrefixRegex.pattern.matcher(name).matches())
          Some(
            ValidationMessage(
              Error,
              "Transform",
              s"Transform with name $name should respect the pattern ${forceJobPrefixRegex.regex}"
            )
          )
        else
          None
      }

      val taskNamePatternErrors =
        validJobs.flatMap(_.tasks.filter(_.name.nonEmpty).map(_.fullName())).flatMap { name =>
          val components = name.split('.')
          if (components.length != 2) {
            Some(
              ValidationMessage(
                Error,
                "Task",
                s"Tasks with name $name should be prefixed with the domain name"
              )
            )
          } else if (!forceTaskPrefixRegex.pattern.matcher(components(1)).matches())
            Some(
              ValidationMessage(
                Error,
                "Task",
                s"Tasks with name ${components(1)} in domain ${components(0)} should respect the pattern ${forceTaskPrefixRegex.regex}"
              )
            )
          else
            None
        }
      (namePatternErrors ++ taskNamePatternErrors, validJobs)
    } else {
      (Nil, Nil)
    }
  }

  /** Find domain by name
    *
    * @param name
    *   : Domain name
    * @return
    *   Unique Domain referenced by this name.
    */
  def getDomain(name: String, raw: Boolean = false): Option[DomainInfo] =
    domains(domainNames = List(name), raw = raw).find(_.name == name)

  /** Return all schemas for a domain
    *
    * @param domain
    *   : Domain name
    * @return
    *   List of schemas for a domain, empty list if no schema or domain is found
    */
  def tables(domain: String): List[SchemaInfo] = getDomain(domain).map(_.tables).getOrElse(Nil)

  /** Get schema by name for a domain
    *
    * @param domainName
    *   : Domain name
    * @param schemaName
    *   : Sceham name
    * @return
    *   Unique Schema with this name for a domain
    */
  def table(domainName: String, schemaName: String): Option[SchemaInfo] =
    for {
      domain <- getDomain(domainName)
      schema <- domain.tables.find(_.name == schemaName)
    } yield schema

  def tableByFinalName(domainName: String, schemaName: String): Option[SchemaInfo] =
    for {
      domain <- getDomain(domainName)
      schema <- domain.tables.find(_.finalName == schemaName)
    } yield schema

  def table(domainNameAndSchemaName: String): Option[SchemaInfo] = {
    val components = domainNameAndSchemaName.split('.')
    require(
      components.length >= 2,
      s"Table name $domainNameAndSchemaName should be composed of domain and table name separated by a dot"
    )
    val domainName = components(components.length - 2)
    val schemaName = components(components.length - 1)
    for {
      domain <- getDomain(domainName)
      schema <- domain.tables.find(_.name == schemaName)
    } yield schema
  }

  def fromXSD(domain: DomainInfo): DomainInfo = {
    val domainMetadata = domain.metadata
    val tables = domain.tables.map { table =>
      fromXSD(table, domainMetadata)
    }
    domain.copy(tables = tables)
  }

  def fromXSD(ymlSchema: SchemaInfo, domainMetadata: Option[Metadata]): SchemaInfo = {
    val metadata = ymlSchema.mergedMetadata(domainMetadata)
    metadata.getXsdPath() match {
      case None =>
        ymlSchema
      case Some(xsd) =>
        val xsdContent = storage.read(new Path(xsd))
        val sparkType = XSDToSchema.read(xsdContent)
        val topElement = sparkType.fields.map(field => TableAttribute(field))
        val (nonScripted, scripted) = topElement.head.attributes.partition(_.script.isEmpty)
        val xsdAttributes = nonScripted ++ scripted
        // val xsdAttributes = sordtedAttrs.head.attributes
        val merged = TableAttribute.mergeAll(
          xsdAttributes,
          ymlSchema.attributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = true,
            keepSourceDiffAttributesStrategy =
              DropAll, // Keep existing behavior but wonder if we should use KeepAllDiff
            attributePropertiesMergeStrategy = SourceFirst
          )
        )(this)
        ymlSchema.copy(attributes = merged)
    }
  }

  def getFullTableName(domain: DomainInfo, schema: SchemaInfo)(implicit
    settings: Settings
  ): String = {
    val databaseName = domain.database.orElse(settings.appConfig.getDefaultDatabase())
    databaseName match {
      case Some(db) =>
        s"$db.${domain.finalName}.${schema.finalName}"
      case None =>
        s"${domain.finalName}.${schema.finalName}"
    }
  }

  def getDatabase(domain: DomainInfo)(implicit settings: Settings): Option[String] =
    domain.database.orElse(settings.appConfig.getDefaultDatabase())
  // SL_DATABASE
  // default database

  def objects(): List[DomainInfo] = domains() ++ externals() ++ List(auditTables)

  def findTableInObjects(domain: String, table: String): Option[SchemaInfo] =
    objects()
      .find(_.name.equalsIgnoreCase(domain))
      .flatMap(_.tables.find(_.name.equalsIgnoreCase(table)))

  def objectJdbcDefinitions(): JdbcMetaData = {
    val jdbcMetadata = new JdbcMetaData("", "")
    objectNames().foreach(_.asSchemaDefinition(jdbcMetadata))
    jdbcMetadata
  }

  def objectsMap(): CaseInsensitiveMap[TableWithNameAndType] = {
    val map =
      objectNames().flatMap { domain =>
        domain.tables.map { table =>
          s"${domain.name}.${table.name}" -> table
        }
      }.toMap
    CaseInsensitiveMap(map)
  }

  def dependenciesAsSchemaString(query: String): List[String] = {
    val tables =
      Try(SQLUtils.extractTableNames(query)) // if syntax is correct this works fine
        .getOrElse(
          SQLUtils.extractTableNamesUsingRegEx(query)
        ) // if syntax is incorrect, we try to extract using regex

    val domainAndTables = tables.flatMap { table =>
      val components = table.split("\\.")
      if (components.length >= 2)
        Some((components.dropRight(1).last, components.last)) // domainName, tableName
      else
        None
    }

    val schemaHandler = settings.schemaHandler()
    val createTables =
      domainAndTables.flatMap { case (domainName, tableName) =>
        val createTable = schemaHandler.findTableInObjects(domainName, tableName).map { table =>
          val columns = table.attributes
            .map { col =>
              val colType =
                schemaHandler
                  .getType(col.`type`)
                  .map(_.primitiveType.toString)
                  .getOrElse(col.`type`)
              val commentStr = col.comment.map(c => s" COMMENT '$c'").getOrElse("")
              s"${col.name} $colType$commentStr"
            }
            .mkString(", ")
          s"CREATE TABLE $tableName ($columns)"
        }
        createTable
      }
    createTables
  }

  def dependenciesAsSchemaString(): List[String] = {
    val schemaHandler = settings.schemaHandler()
    val domainAndTables = objectsMap()
    val createTables =
      domainAndTables.flatMap { case (domainName, table) =>
        val createTable = schemaHandler.findTableInObjects(domainName, table.name).map { table =>
          val columns = table.attributes
            .map { col =>
              val colType =
                schemaHandler
                  .getType(col.`type`)
                  .map(_.primitiveType.toString)
                  .getOrElse(col.`type`)
              val commentStr = col.comment.map(c => s" COMMENT '$c'").getOrElse("")
              s"${col.name} $colType$commentStr"
            }
            .mkString(", ")
          s"CREATE TABLE ${table.name} ($columns)"
        }
        createTable
      }
    createTables.toList
  }

  /** List of all domain.table including tasks and externals
    *
    * @return
    *   List of all objects in the metadata
    */
  def objectNames(): List[DomainWithNameOnly] = {
    val domains = objects()
    val tableNames =
      domains.map { domain =>
        DomainWithNameOnly(
          domain.finalName,
          domain.tables
            .map { table =>
              TableWithNameAndType(
                table.finalName,
                table.attributes.map { attr =>
                  (attr.getFinalName(), attr.`type`, attr.comment)
                }.sorted
              )
            }
            .distinctBy(_.name.toLowerCase())
            .sortBy(_.name)
        )
      }
    val jobs = this.jobs()
    val taskNames =
      jobs
        .map { job =>
          DomainWithNameOnly(
            job.name,
            job.tasks
              .distinctBy(_.table.toLowerCase())
              .sortBy(_.table)
              .map { t =>
                val attrs = t.attributes.map { attr =>
                  (attr.getFinalName(), attr.`type`, attr.comment)
                }
                TableWithNameAndType(t.table, attrs)
              }
          )
        }
    val externalNames = this.externals().map { domain =>
      DomainWithNameOnly(
        domain.finalName,
        domain.tables
          .map { table =>
            TableWithNameAndType(
              table.finalName,
              table.attributes.map { attr =>
                (attr.getFinalName(), attr.`type`, attr.comment)
              }.sorted
            )
          }
          .sortBy(_.name)
      )
    }
    val all = merge(externalNames, tableNames, taskNames)
    val result = all.sortBy(_.name)
    result
  }

  def merge(tables: List[DomainWithNameOnly]*): List[DomainWithNameOnly] = {
    def toMap(list: List[DomainWithNameOnly]): Map[String, DomainWithNameOnly] = {
      list.flatMap { domain =>
        domain.tables.map { table =>
          s"${domain.name.toLowerCase()}.${table.name.toLowerCase()}" -> domain
        }
      }.toMap
    }
    var result = Map.empty[String, DomainWithNameOnly]
    tables.foreach { list =>
      val incoming = toMap(list)
      incoming.foreach { case (key, value) =>
        if (!result.keys.exists(k => k == key))
          result = result + (key -> value)
      }
    }
    result.values.toList
  }

  def saveToExternals(domains: List[DomainInfo]) = {
    saveTo(domains, DatasetArea.external)
    loadExternals()
  }

  def saveTo(domains: List[DomainInfo], outputPath: Path) = {
    domains.foreach { domain =>
      domain.writeDomainAsYaml(outputPath)(settings.storageHandler())
    }
  }

  lazy val auditTables: DomainInfo =
    DomainInfo(
      settings.appConfig.audit.getDomain(),
      tables = List(
        AuditLog.starlakeSchema,
        ExpectationReport.starlakeSchema,
        RejectedRecord.starlakeSchema
      )
    )

  def tableDeleted(domain: String, table: String): Unit = {
    domains().find(_.name.toLowerCase() == domain.toLowerCase()) match {
      case None =>
        logger.warn(s"Domain $domain not found")
      case Some(domain) =>
        val tablesToKeep = domain.tables.filterNot(_.name.toLowerCase() == table.toLowerCase())
        val updatedDomain = domain.copy(tables = tablesToKeep)
        _domains = Some(_domains.get.filterNot(_.name == domain.name) :+ updatedDomain)
    }
  }

  def taskDeleted(domain: String, task: String): Unit = {
    jobs().find(_.name.toLowerCase() == domain.toLowerCase()) match {
      case None =>
        logger.warn(s"Job $domain not found")
      case Some(job) =>
        val tasksToKeep = job.tasks.filterNot(_.name.toLowerCase() == task.toLowerCase())
        val updatedJob = job.copy(tasks = tasksToKeep)
        _jobs = _jobs.filterNot(_.name == job.name) :+ updatedJob
    }
  }

  def taskUpdated(
    domainName: String,
    taskName: String
  ): Try[Unit] = {
    taskDeleted(domainName, taskName)
    taskAdded(domainName, taskName)
  }

  def tableUpdated(
    domainName: String,
    tableName: String
  ): Try[Unit] = {
    tableDeleted(domainName, tableName)
    tableAdded(domainName, tableName)
  }

  def tableAdded(domain: String, table: String): Try[Unit] = {
    domains().find(_.name.toLowerCase() == domain.toLowerCase()) match {
      case None =>
        _domains = Some(_domains.getOrElse(Nil) :+ DomainInfo(domain))
      case Some(_) =>
      // do nothing
    }
    tableOnly(s"${domain}.${table}") match {
      case Success(tableDesc) =>
        _domains.getOrElse(Nil).find(_.name.toLowerCase() == domain.toLowerCase()) match {
          case None =>
            throw new Exception(s"Should not happen: Domain $domain not found")
          case Some(domain) =>
            val updatedTables =
              domain.tables.filterNot(_.name.equalsIgnoreCase(tableDesc.table.name))
            val updatedDomain = domain.copy(tables = updatedTables :+ tableDesc.table)
            _domains = Some(_domains.get.filterNot(_.name == domain.name) :+ updatedDomain)
            Success(())
        }
      case Failure(e) =>
        Failure(e)
    }
  }

  def taskAdded(domainName: String, taskName: String): Try[Unit] = {
    // Add the domain if it does not exist
    jobs().find(_.name.toLowerCase() == domainName.toLowerCase()) match {
      case None =>
        _jobs = _jobs :+ AutoJobInfo(domainName)
      case Some(_) =>
      // do nothing
    }

    // Load the task and add it to the job, replace if it already exists
    taskOnly(s"${domainName}.${taskName}", reload = true) match {
      case Success(taskDesc) =>
        _jobs.find(_.name.toLowerCase() == domainName.toLowerCase()) match {
          case None =>
            throw new Exception(s"Should not happen: Job $domainName not found")
          case Some(job) =>
            val updatedTasks = job.tasks.filterNot(_.name.equalsIgnoreCase(taskDesc.name))
            val updatedJob = job.copy(tasks = updatedTasks :+ taskDesc)
            _jobs = _jobs.filterNot(_.name == job.name) :+ updatedJob
            Success(())
        }
      case Failure(e) =>
        Failure(e)
    }
  }

  def substituteRefTaskMainSQL(
    sql: String,
    connection: ConnectionInfo,
    allVars: Map[String, String] = Map.empty
  ): String =
    SqlTransformations.substituteRefTaskMainSQL(
      sql,
      connection,
      refs(),
      domains(),
      tasks(),
      allVars
    )

  def transpileAndSubstituteSelectStatement(
    sql: String,
    connection: ConnectionInfo,
    allVars: Map[String, String] = Map.empty,
    test: Boolean
  ): String =
    SqlTransformations.transpileAndSubstituteSelectStatement(
      sql,
      connection,
      refs(),
      domains(),
      tasks(),
      allVars,
      test
    )

  def deleteTablePK(
    domainName: String,
    tableName: String
  ): Try[Unit] =
    setTablePK(domainName, tableName, Nil)

  def setTablePK(
    domainName: String,
    tableName: String,
    pk: List[String]
  ): Try[Unit] = Try {
    val path = SchemaInfo.path(domainName, tableName)
    val content = storage.read(path)
    val tableDesc = YamlSerde.deserializeYamlTables(content, path.toString).head
    val table = tableDesc.table
    val tableWithPK = table.copy(primaryKey = pk)
    val updatedTableDesc = tableDesc.copy(table = tableWithPK)
    YamlSerde.serializeToPath(path, updatedTableDesc)(settings.storageHandler())
    tableUpdated(domainName, tableName)
  }

  def setTableFK(
    domainName: String,
    tableName: String,
    columnName: String,
    targetDomainName: String,
    targetTableName: String,
    targetColumnName: String
  ): Try[Unit] =
    setTableFK(
      domainName,
      tableName,
      columnName,
      Some(s"${targetDomainName}.${targetTableName}.${targetColumnName}")
    )

  def deleteTableFK(
    domainName: String,
    tableName: String,
    columnName: String
  ): Try[Unit] =
    setTableFK(domainName, tableName, columnName, None)

  def setTableFK(
    domainName: String,
    tableName: String,
    columnName: String,
    fk: Option[String]
  ): Try[Unit] = Try {
    val path = SchemaInfo.path(domainName, tableName)
    val content = storage.read(path)
    val tableDesc = YamlSerde.deserializeYamlTables(content, path.toString).head
    val table = tableDesc.table
    val tableWithFK = table.attributes.map { col =>
      if (col.name == columnName)
        col.copy(foreignKey = fk)
      else
        col
    }
    val updatedTable = table.copy(attributes = tableWithFK)
    val updatedTableDesc = tableDesc.copy(table = updatedTable)
    YamlSerde.serializeToPath(path, updatedTableDesc)(settings.storageHandler())
    tableUpdated(domainName, tableName)
  }

  def deleteTaskPK(
    domainName: String,
    tableName: String
  ): Try[Unit] =
    setTaskPK(domainName, tableName, Nil)

  def setTaskPK(
    domainName: String,
    tableName: String,
    pk: List[String]
  ): Try[Unit] = Try {
    val path = new Path(DatasetArea.transform, domainName + "/" + tableName + ".sl.yml")
    val content = storage.read(path)
    val taskInfo = YamlSerde.deserializeYamlTask(content, path.toString)

    val taskInfoWithPK = taskInfo.copy(primaryKey = pk)
    YamlSerde.serializeToPath(path, taskInfoWithPK)(settings.storageHandler())
    taskUpdated(domainName, tableName)
  }

  def streams(): CaseInsensitiveMap[String] =
    SchedulingQueries.streams(domains(), jobs())

  private def orderSchedules(orderBy: Option[String], schedules: List[ObjectSchedule]) =
    SchedulingQueries.orderSchedules(orderBy, schedules)

  def taskSchedules(orderBy: Option[String]): List[ObjectSchedule] =
    SchedulingQueries.taskSchedules(jobs(), orderBy)

  def tableSchedules(orderBy: Option[String]): List[ObjectSchedule] =
    SchedulingQueries.tableSchedules(domains(), orderBy)

  def allSchedules(orderBy: Option[String]): List[ObjectSchedule] =
    SchedulingQueries.allSchedules(jobs(), domains(), orderBy)

  def updateTableSchedule(obj: ObjectSchedule)(implicit settings: Settings) = {
    val domain = getDomain(obj.domain).getOrElse(
      throw new Exception(s"Domain ${obj.domain} not found")
    )
    val table = domain.tables
      .find(_.name.toLowerCase() == obj.table.toLowerCase())
      .getOrElse(
        throw new Exception(s"Table ${obj.table} not found in domain ${obj.domain}")
      )
    val metadata = table.metadata.getOrElse(
      Metadata()
    )
    val updatedMetadata = metadata.copy(schedule = obj.cron)
    val updatedTable = table.copy(metadata = Some(updatedMetadata))

    YamlSerde.serializeToPath(
      SchemaInfo.path(domain.name, table.name),
      updatedTable
    )(settings.storageHandler())

    val updatedDomain = domain.copy(tables = domain.tables.map {
      case t if t.name.toLowerCase() == obj.table.toLowerCase() => updatedTable
      case t                                                    => t
    })
    _domains = _domains.map(ds =>
      ds.filterNot(_.name.toLowerCase() == domain.name.toLowerCase()) :+ updatedDomain
    )

  }
  def updateTaskSchedule(obj: ObjectSchedule)(implicit settings: Settings) = {
    val job = jobs()
      .find(_.name.toLowerCase() == obj.domain.toLowerCase())
      .getOrElse(
        throw new Exception(s"Job ${obj.domain} not found")
      )
    val task = job.tasks
      .find(_.getTableName().toLowerCase() == obj.table.toLowerCase())
      .getOrElse(
        throw new Exception(s"Task ${obj.table} not found in job ${obj.domain}")
      )
    val updatedTask = task.copy(schedule = obj.cron)

    YamlSerde.serializeToPath(
      new Path(DatasetArea.transform, s"${job.name}/${obj.table}.sl.yml"),
      updatedTask
    )(settings.storageHandler())

    val updatedJob = job.copy(
      tasks = job.tasks.map {
        case t if t.getTableName().toLowerCase() == obj.table.toLowerCase() => updatedTask
        case t                                                              => t
      }
    )
    _jobs = _jobs.filterNot(_.name.toLowerCase() == job.name.toLowerCase()) :+ updatedJob
  }

  def external(domain: String, table: String): Option[SchemaInfo] = {
    externals().find(_.name.equalsIgnoreCase(domain)).flatMap { domainInfo =>
      domainInfo.tables.find(_.name.equalsIgnoreCase(table))
    }
    None
  }
  def attributesAsDiff(
    domain: String,
    table: String,
    accessToken: Option[String]
  ): List[Attribute] = {
    taskByTableName(domain, table).filter(it => it.attributes.nonEmpty) match {
      case Some(taskInfo) =>
        logger.info(s"Found task for $domain.$table from transform")
        taskInfo.attributes.map(_.toDiffAttribute())
      case None =>
        // If the task does not exist, we return undefined for all columns
        this.tableByFinalName(domain, table).filter(it => it.attributes.nonEmpty) match {
          case Some(schemaInfo) =>
            logger.info(s"Found schema for $domain.$table from load")
            schemaInfo.attributes.map(_.toDiffAttribute())
          case None =>
            external(domain, table).filter(it => it.attributes.nonEmpty) match {
              case Some(externalSchema) =>
                logger.info(s"Found external schema for $domain.$table from external")
                externalSchema.attributes.map(_.toDiffAttribute())
              case None =>
                val connectionInfo =
                  settings.appConfig.getConnection(settings.appConfig.connectionRef)
                if (
                  connectionInfo.isSnowflake()
                ) // getting metadata from snowflake is definitely too slow
                  JdbcDbUtils.extractColumnsUsingInformationSchema(
                    connectionInfo,
                    domain,
                    table
                  ) match {
                    case Success(cols) =>
                      logger.info(s"Extracted schema for $domain.$table from Snowflake source")
                      cols
                        .map { case (colName, colType) =>
                          TableAttribute(
                            name = colName,
                            `type` = PrimitiveType
                              .fromSQLType(
                                connectionInfo.getJdbcEngineName().toString,
                                colType,
                                this
                              )
                              .toString
                          )

                        }
                        .map(_.toDiffAttribute())
                    case Failure(exception) =>
                      logger.warn(
                        s"Table $domain.$table not found in Snowflake source",
                        exception
                      )
                      Nil
                  }
                else {
                  new ExtractSchema(this)
                    .extractTable(domain, table, None, accessToken)
                    .toOption
                    .filter(_.tables.nonEmpty)
                    .map { it =>
                      logger.info(s"Extracted schema for $domain.$table from DB source")
                      it.tables.head.attributes.map(_.toDiffAttribute())
                    }
                    .getOrElse {
                      logger.warn(s"Table $domain.$table not found in external schemas")
                      Nil
                    }
                }
            }
        }
    }
  }

  def syncApplySqlWithYaml(
    taskFullName: String,
    list: List[(TableAttribute, AttributeStatus)],
    optSql: Option[String]
  ): Unit = {
    settings.schemaHandler().taskByFullName(taskFullName) match {
      case Success(task) =>
        syncApplySqlWithYaml(task, list, optSql)
      case Failure(exception) =>
        logger.error(s"Failed to get task $taskFullName", exception)
        throw exception
    }
  }

  def syncApplySqlWithYaml(
    task: AutoTaskInfo,
    list: List[(TableAttribute, AttributeStatus)],
    optSql: Option[String]
  ): AutoTaskInfo = {
    logger.debug(s"Diff SQL attributes with YAML for task ${task.getName()}: $list")
    if (list.nonEmpty) {
      val updatedTask =
        task
          .updateAttributes(list.filterNot(_._2 == AttributeStatus.REMOVED).map(_._1))
          .copy(sql = None) // do not serialize sql. It is in its own file
      val taskPath = new Path(DatasetArea.transform, s"${task.domain}/${task.name}.sl.yml")

      YamlSerde.serializeToPath(taskPath, updatedTask)(
        settings.storageHandler()
      )
      val sqlPath = new Path(DatasetArea.transform, s"${task.domain}/${task.name}.sql")
      optSql.orElse(task.sql).foreach { sql =>
        storage.write(sql, sqlPath)
      }
      taskUpdated(task.domain, task.name)
      this.taskOnly(task.fullName()).getOrElse(throw new IllegalStateException("Should not happen"))
    } else {
      val sqlPath = new Path(DatasetArea.transform, s"${task.domain}/${task.name}.sql")
      optSql.foreach { sql =>
        storage.write(sql, sqlPath)
      }
      taskUpdated(task.domain, task.name)
      this.taskOnly(task.fullName()).getOrElse(throw new IllegalStateException("Should not happen"))
      task
    }
  }

  private def updateTypesIfNotDefined(attr: TableAttribute): TableAttribute = {
    if (attr.`type`.isEmpty) {
      attr.copy(`type` = "string")
    } else if (attr.`type`.contains('(')) {
      val components = attr.`type`.split('(')
      val typeName = components(0)
      val zone = components(1).trim.dropRight(1).trim
      attr.copy(`type` = typeName)
      /*
      this
        .types()
        .find(t => t.name == typeName)
        .map { typeInfo =>
          attr.copy(`type` = typeInfo.name)
        }
        .getOrElse {
          attr.copy(`type` = typeName)
        }*/
    } else
      attr
  }
  def syncPreviewSqlWithDb(
    taskFullName: String,
    query: Option[String],
    accessToken: Option[String]
  ): List[(TableAttribute, AttributeStatus)] = {
    settings.schemaHandler().taskByFullName(taskFullName) match {
      case Success(taskInfo) =>
        val list: List[(TableAttribute, AttributeStatus)] =
          taskInfo.diffSqlAttributesWithSQL(query, accessToken).map { it =>
            (updateTypesIfNotDefined(it._1), it._2)
          }

        logger.debug(
          s"Diff SQL attributes with YAML for task $taskFullName: ${list.length} attributes"
        )
        list.foreach { case (attribute, status) =>
          logger.info(s"\tAttribute: ${attribute.name}, Status: $status")
        }
        list
      case Failure(exception) =>
        logger.error("Failed to get task", exception)
        throw exception
    }
  }

  def findTableFromFilename(domainName: String, filename: String): Option[SchemaInfo] = {
    domains(List(domainName)).flatMap { domain =>
      domain.tables.find { table =>
        table.pattern.matcher(filename).matches()
      }
    }
  }.headOption

  // todo
  def upsertAttribute(
    domainName: String,
    tableName: String,
    columnName: String,
    columnType: String,
    columnDesc: Option[String]
  ) = ???

  def acls(): List[(String, List[AccessControlEntry])] =
    AccessControlQueries.acls(this.domains(), this.tasks())

  def rls(): List[(String, List[RowLevelSecurity])] =
    AccessControlQueries.rls(this.domains(), this.tasks())

  def cls(): List[IamPolicyTag] =
    AccessControlQueries.cls(this.iamPolicyTags())

  def accessPolicies(): CaseInsensitiveMap[String] =
    AccessControlQueries.accessPolicies(this.domains(), this.tasks())
}
