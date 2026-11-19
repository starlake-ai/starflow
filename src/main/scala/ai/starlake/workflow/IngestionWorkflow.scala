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

package ai.starlake.workflow

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractUtils, ParUtils}
import ai.starlake.job.ingest.*
import ai.starlake.job.load.LoadStrategy
import ai.starlake.schema.AdaptiveWriteStrategy
import ai.starlake.schema.handlers.{FileInfo, SchemaHandler, StorageHandler}
import ai.starlake.schema.model.*
import ai.starlake.utils.*
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.file.{FileSystems, ProviderNotFoundException}
import java.time.Instant
import java.util.Collections
import scala.annotation.nowarn
import scala.util.{Failure, Success, Try}

/** The whole worklfow works as follow :
  *   - loadLanding : Zipped files are uncompressed or raw files extracted from the local
  *     filesystem.
  * -loadPending : files recognized with filename patterns are stored in the ingesting area and
  * submitted for ingestion files with unrecognized filename patterns are stored in the unresolved
  * area
  *   - ingest : files are finally ingested and saved as parquet/orc/... files and hive tables
  *
  * @param storageHandler
  *   : Minimum set of features required for the underlying filesystem
  * @param schemaHandler
  *   : Schema interface
  */
class IngestionWorkflow(
  protected val storageHandler: StorageHandler,
  protected val schemaHandler: SchemaHandler
)(implicit protected val settings: Settings)
    extends LazyLogging
    with TransformWorkflow
    with TestWorkflow
    with SinkWorkflow
    with MetricsSecurityWorkflow
    with InferWorkflow {
  private var _domains: Option[List[DomainInfo]] = None

  private def domains(
    domainNames: List[String] = Nil,
    tableNames: List[String] = Nil
  ): List[DomainInfo] = {
    _domains match {
      case None =>
        if (domainNames.nonEmpty)
          logger.info(s"Loading domains : ${domainNames.mkString(",")}")
        else
          logger.info("Loading all domains")
        if (tableNames.nonEmpty)
          logger.info(s"Loading tables : ${tableNames.mkString(",")}")
        else
          logger.info("Loading all tables")
        val domains = schemaHandler.domains(domainNames, tableNames)
        _domains = Some(domains)
        domains
      case Some(domains) =>
        logger.info("Domains already loaded")
        domains
    }
  }

  /** Find all the domains that have a filename pattern that matches the given filename
    *
    * @param filename
    *   : filename to match
    * @return
    *   list of domain name and table name
    */
  def findAllFilenameMatchers(
    filename: String,
    domain: Option[String],
    table: Option[String]
  ): List[(String, String)] = {
    (domain, table) match {
      case (Some(domain), Some(table)) =>
        val path = SchemaInfo.path(domain, table)
        val exists = settings.storageHandler().exists(path)
        if (exists) {
          val tableContent = settings
            .storageHandler()
            .read(path)
          val rootNode = YamlSerde.deserializeYamlTables(tableContent, path.toString).head
          if (rootNode.table.pattern.matcher(filename).matches()) {
            List((domain, table))
          } else {
            Nil
          }
        } else {
          Nil
        }
      case (Some(domain), None) =>
        val domains = schemaHandler.domains(List(domain), reload = true)
        domains.flatMap { domain =>
          domain.tables.flatMap { table =>
            val pattern = table.pattern
            if (pattern.matcher(filename).matches()) {
              Some((domain.name, table.name))
            } else {
              None
            }
          }
        }
      case (_, _) =>
        val domains = schemaHandler.domains(reload = true)
        domains.flatMap { domain =>
          domain.tables.flatMap { table =>
            val pattern = table.pattern
            if (pattern.matcher(filename).matches()) {
              Some((domain.name, table.name))
            } else {
              None
            }
          }
        }
    }
  }

  /** Move the files from the landing area to the pending area. files are loaded one domain at a
    * time each domain has its own directory and is specified in the "directory" key of Domain YML
    * file compressed files are uncompressed if a corresponding ack file exist. Compressed files are
    * recognized by their extension which should be one of .tgz, .zip, .gz. raw file should also
    * have a corresponding ack file before moving the files to the pending area, the ack files are
    * deleted To import files without ack specify an empty "ack" key (aka ack:"") in the domain YML
    * file. "ack" is the default ack extension searched for but you may specify a different one in
    * the domain YML file.
    */

  def stage(config: StageConfig): Try[Unit] = Try {
    val filteredDomains = config.domains match {
      case Nil => domains(Nil, Nil) // Load all domains & tables
      case _   => domains(config.domains.toList, config.tables.toList)
    }
    logger.info(
      s"Loading files from Landing Zone for domains : ${filteredDomains.map(_.name).mkString(",")}"
    )
    filteredDomains.foreach { domain =>
      val storageHandler = settings.storageHandler()
      val filesToLoad = listStageFiles(domain, dryMode = false).flatMap { case (_, files) => files }
      val destFolder = DatasetArea.stage(domain.name)
      val inputDir = new Path(domain.resolveDirectory())
      Utils.printOut(s"Moving ${filesToLoad.size} file(s) from $inputDir to $destFolder")
      filesToLoad.foreach { file =>
        logger.info(s"Importing $file")
        val destFile = new Path(destFolder, file.path.getName)
        Utils.printOut(s"Moving ${file.path.getName}")
        storageHandler.moveFromLocal(file.path, destFile)
      }

    }
  }

  def listStageFiles(domain: DomainInfo, dryMode: Boolean): Map[String, List[FileInfo]] = {
    val inputDir = new Path(domain.resolveDirectory())
    val storageHandler = settings.storageHandler()
    if (storageHandler.exists(inputDir)) {
      logger.info(s"Scanning $inputDir")

      val domainFolderFilesInfo = storageHandler
        .list(inputDir, recursive = false)
        .filterNot(
          _.path.getName.startsWith(".")
        ) // ignore files starting with '.' aka .DS_Store

      val zippedFileExtensions = List(".gz", ".tgz", ".zip")

      def getFileWithoutExt(file: FileInfo): Path = {
        val fileName = file.path.getName
        fileName.lastIndexOf('.') match {
          case -1 => file.path
          case i  => new Path(file.path.getParent, fileName.substring(0, i))
        }
      }

      val zippedFiles = domainFolderFilesInfo
        .filter(fileInfo =>
          zippedFileExtensions.contains(
            fileInfo.path.getName.split('.').lastOption.getOrElse("")
          )
        )
        .map(zippedFileInfo => {
          val zippedFileWithoutExt = getFileWithoutExt(zippedFileInfo)
          val zippedOutputDirectory =
            new Path(zippedFileWithoutExt.getParent, zippedFileWithoutExt.getName)
          storageHandler.mkdirs(zippedOutputDirectory)
          // We use Java nio to handle archive files

          // Java nio FileSystems are only registered based on the system the JVM is based on
          // To handle external FileSystems (ex: GCS from a linux OS), we need to manually install them
          // As long as the user provide this app classpath with the appropriate FileSystem implementations,
          // we can virtually handle anything
          def asBetterFile(path: Path): File = {
            Try {
              StorageHandler.localFile(path) // try once
            } match {
              case Success(file) => file
              // There is no FileSystem registered that can handle this URI
              case Failure(_: ProviderNotFoundException) =>
                FileSystems
                  .newFileSystem(
                    path.toUri,
                    Collections.emptyMap[String, Any](),
                    getClass.getClassLoader
                  )
                // Try to install one from the classpath
                File(path.toUri) // retry
              case Failure(exception) => throw exception
            }
          }

          // File is a proxy to java.nio.Path / working with Uri
          val tmpDir = asBetterFile(zippedOutputDirectory)
          val zippedFile = asBetterFile(zippedFileInfo.path)
          zippedFile.extension() match {
            case Some(".tgz") => Unpacker.unpack(zippedFile, tmpDir)
            case Some(".gz")  => zippedFile.unGzipTo(tmpDir / zippedOutputDirectory.getName)
            case Some(".zip") => zippedFile.unzipTo(tmpDir)
            case _            => logger.error(s"Unsupported archive type for $zippedFile")
          }
          if (!dryMode) {
            storageHandler.delete(zippedFileInfo.path)
          }
          zippedFileInfo -> zippedOutputDirectory
        })

      val results = {
        for (table <- domain.tables) yield {
          val anyLevelAck = table.metadata
            .flatMap(_.ack)
            .orElse(domain.metadata.flatMap(_.ack))
            .orElse(settings.appConfig.ack)
            .getOrElse("")

          val tableFiles =
            domainFolderFilesInfo.filter(file => table.pattern.matcher(file.path.getName).matches())

          if (anyLevelAck.nonEmpty) {
            // for a given table, the files included within each zipped file that respect the table pattern
            // should be added to the table files if and only if the corresponding zipped ack file exists
            val zippedTableFiles =
              zippedFiles
                .filter(zippedFile => {
                  val ackPath = getFileWithoutExt(zippedFile._1).suffix(s".$anyLevelAck")
                  logger.info(s"Checking zipped ack file $ackPath")
                  val ret = storageHandler.exists(ackPath)
                  if (ret && !dryMode) {
                    storageHandler.delete(ackPath)
                  }
                  ret
                })
                .flatMap(zippedFile => {
                  storageHandler
                    .list(zippedFile._2, recursive = false)
                    .filter(fileInfo => table.pattern.matcher(fileInfo.path.getName).matches())
                })
            table.name -> (zippedTableFiles ++ tableFiles.filter(fileInfo => {
              val ackPath = getFileWithoutExt(fileInfo).suffix(s".$anyLevelAck")
              logger.info(s"Checking ack raw file $ackPath")
              val ret = storageHandler.exists(ackPath)
              if (ret && !dryMode) {
                storageHandler.delete(ackPath)
              }
              ret
            }))
          } else {
            val tableZippedFiles = zippedFiles
              .map(_._2)
              .flatMap(zippedOutputDirectory =>
                storageHandler
                  .list(zippedOutputDirectory, recursive = false)
                  .filter(fileInfo => table.pattern.matcher(fileInfo.path.getName).matches())
              )
            table.name -> (tableZippedFiles ++ tableFiles)
          }
        }
      }

      zippedFiles.map(_._2).foreach(storageHandler.delete)

      results.toMap
    } else {
      logger.error(s"Input path : $inputDir not found, ${domain.name} Domain is ignored")
      (for (table <- domain.tables) yield {
        table.name -> List.empty
      }).toMap
    }
  }

  /*private def listExtensionsMatchesInFolder(
    domainFolderContent: List[FileInfo],
    tablesPattern: List[Pattern]
  ): List[String] = {
    val tableExtensions: List[String] = tablesPattern.flatMap { pattern =>
      domainFolderContent.flatMap { fileInfo =>
        val fileName = fileInfo.path.getName
        if (pattern.matcher(fileName).matches()) {
          val res: Option[String] = fileName.lastIndexOf('.') match {
            case -1 => None
            case i  => Some(fileName.substring(i + 1))
          }
          res
        } else {
          None
        }
      }
    }.distinct
    logger.info(s"Found extensions : $tableExtensions")
    tableExtensions.map(ext => if (ext.startsWith(".") || ext.isEmpty) ext else s".$ext")
  }*/

  /** Split files into resolved and unresolved datasets. A file is unresolved if a corresponding
    * schema is not found. Schema matching is based on the dataset filename pattern
    *
    * @param config
    *   : includes Load pending dataset of these domain only excludes : Do not load datasets of
    *   these domains if both lists are empty, all domains are included
    */
  @nowarn
  def load(config: LoadConfig): Try[SparkJobResult] = {
    val loadResults =
      if (config.test || config.inPlace) {
        if (config.test) logger.info("Test mode enabled")
        if (config.inPlace) logger.info("In-place mode enabled, files will not be moved")
        val domain = schemaHandler.getDomain(config.domains.head)
        val jobResult =
          domain match {
            case Some(dom) =>
              val schema = dom.tables.find(_.name == config.tables.head)
              schema match {
                case Some(sch) =>
                  val withPrimaryKeySchema = sch.copy(primaryKey = config.primaryKey)
                  if (config.primaryKey.nonEmpty) {
                    YamlSerde.serializeToPath(
                      withPrimaryKeySchema.pathIn(dom.name),
                      withPrimaryKeySchema
                    )(
                      settings.storageHandler()
                    )
                  }
                  val paths = config.files.getOrElse(Nil).map(new Path(_)).sortBy(_.getName)
                  val fileInfos = paths.map(p =>
                    FileInfo(p, 0, Instant.now())
                  ) // dummy file infos for calling AdaptiveWriteStrategy
                  val updatedSchema =
                    AdaptiveWriteStrategy
                      .adaptThenGroup(withPrimaryKeySchema, fileInfos)
                      .head
                      ._1 // get the schema only
                  ingest(
                    dom,
                    updatedSchema,
                    paths,
                    config.options,
                    config.accessToken,
                    config.test || config.inPlace,
                    config.scheduledDate
                  )
                case None =>
                  throw new Exception(s"Schema ${config.tables.head} not found")
              }
            case None =>
              throw new Exception(s"Domain ${config.domains.head} not found")

          }
        List(jobResult)
      } else {
        val result =
          Try {
            val includedDomains = domainsToWatch(config)
            includedDomains.flatMap { domain =>
              logger.info(s"Watch Domain: ${domain.name}")
              // The tables whose files load no more in this run, since one of them failed before
              // writing its rows
              val heldTables = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
              val stagePath = DatasetArea.stage(domain.name)
              config.files.getOrElse(Nil).foreach { path =>
                logger.info(s"copying file $path in domain ${domain.name}")
                val incomingPath = new Path(path)
                if (incomingPath.getParent.toUri.getPath != stagePath.toUri.getPath) {
                  logger.info(s"Copying $incomingPath to $stagePath")
                  storageHandler.mkdirs(stagePath)
                  storageHandler.copy(incomingPath, new Path(stagePath, incomingPath.getName))
                }
              }
              val (resolved, unresolved) = pending(domain.name, config.tables.toList)
              unresolved.foreach { case (_, fileInfo) =>
                val targetPath =
                  new Path(DatasetArea.unresolved(domain.name), fileInfo.path.getName)
                logger.info(s"Unresolved file : ${fileInfo.path.getName}")
                storageHandler.move(fileInfo.path, targetPath)
              }

              val filteredResolved =
                if (settings.appConfig.privacyOnly) {
                  val (withPrivacy, _) =
                    resolved.partition { case (schema, _) =>
                      schema.exists(
                        _.attributes.map(_.resolvePrivacy()).exists(!TransformInput.None.equals(_))
                      )
                    }
                  withPrivacy
                } else {
                  resolved
                }

              // We group files with the same schema to ingest them together in a single step.
              val groupedResolved = filteredResolved.map {
                case (Some(schema), fileInfo) => (schema, fileInfo)
                case (None, _)                => throw new Exception("Should never happen")
              } groupBy { case (schema, _) => schema } mapValues (it =>
                it.map { case (_, fileInfo) => fileInfo }
              )

              case class JobContext(
                domain: DomainInfo,
                schema: SchemaInfo,
                paths: List[Path],
                options: Map[String, String],
                accessToken: Option[String]
              )
              groupedResolved.toList
                .flatMap { case (schema, pendingPaths) =>
                  AdaptiveWriteStrategy.adaptThenGroup(schema, pendingPaths)
                }
                .map { case (schema, pendingPaths) =>
                  logger.info(s"""Ingest resolved file : ${pendingPaths
                      .map(_.path.getName)
                      .mkString(",")} as table ${domain.name}.${schema.name}""")

                  // We group by groupedMax to avoid rateLimit exceeded when the number of grouped files is too big for some cloud storage rate limitations.
                  val groupedPendingPathsIterator =
                    pendingPaths.grouped(settings.appConfig.groupedMax).toList
                  val startTime = System.currentTimeMillis()
                  val groupedPendingSize = groupedPendingPathsIterator.size
                  groupedPendingPathsIterator.flatMap { pendingPaths =>
                    // Move files concurrently using Future
                    val ingestingPaths =
                      ParUtils.runInParallel(pendingPaths, Some(settings.appConfig.maxParCopy)) {
                        pendingPath =>
                          val ingestingPath =
                            new Path(
                              DatasetArea.ingesting(domain.name),
                              pendingPath.path.getName
                            )
                          if (!storageHandler.move(pendingPath.path, ingestingPath)) {
                            logger.error(s"Could not move $pendingPath to $ingestingPath")
                          }
                          ingestingPath
                      }

                    // Wait for all moves to complete
                    val jobs: Iterable[JobContext] = if (settings.appConfig.grouped) {
                      JobContext(
                        domain,
                        schema,
                        ingestingPaths.toList,
                        config.options,
                        config.accessToken
                      ) :: Nil
                    } else {
                      ingestingPaths.map { path =>
                        JobContext(
                          domain,
                          schema,
                          path :: Nil,
                          config.options,
                          config.accessToken
                        )
                      }
                    }
                    val moveDuration = System.currentTimeMillis() - startTime
                    // Utils.printOut("Grouped pending paths number = " + groupedPendingSize)
                    // Utils.printOut("Moved files number = " + pendingPaths.size)
                    // Utils.printOut("duration " + ExtractUtils.toHumanElapsedTime(moveDuration))
                    val res =
                      ParUtils.runInParallel(
                        jobs,
                        Some(settings.appConfig.sparkScheduling.maxJobs)
                      ) { jobContext =>
                        val table = jobContext.schema.name
                        if (heldTables.contains(table))
                          holdBack(jobContext.domain, jobContext.schema, jobContext.paths)
                        else {
                          val (result, failedBeforeWrite) = ingestFiles(
                            jobContext.domain,
                            jobContext.schema,
                            jobContext.paths,
                            jobContext.options,
                            jobContext.accessToken,
                            config.test,
                            config.scheduledDate
                          )
                          // The order of the files does not change a table they are appended to
                          if (failedBeforeWrite && !appends(jobContext.domain, jobContext.schema))
                            heldTables.add(table)
                          result
                        }
                      }
                    res
                  }
                }
            }.flatten
          }
        result match {
          case Success(jobs) =>
            jobs
          case Failure(exception) =>
            logger.error("Error during ingestion", exception)
            List(Failure(exception))
        }
      }
    val (successLoads, failureLoads) = loadResults.partition(_.isSuccess)
    val exceptionsAsString = failureLoads.map { f =>
      Utils.exceptionAsString(f.failed.get)
    }

    val successAsJobResult =
      successLoads
        .flatMap {
          case Success(result: SparkJobResult) =>
            Some(result.counters.getOrElse(IngestionCounters(0, 0, 0, Nil, "")))
          case Success(_) => None
        }

    val counters =
      successAsJobResult.fold(IngestionCounters(0, 0, 0, Nil, "")) { (acc, c) =>
        IngestionCounters(
          acc.inputCount + c.inputCount,
          acc.acceptedCount + c.acceptedCount,
          acc.rejectedCount + c.rejectedCount,
          acc.paths ++ c.paths,
          acc.jobid + "," + c.jobid
        )
      }
    if (exceptionsAsString.nonEmpty) {
      logger.error("Some tables failed to be loaded")
      logger.error(exceptionsAsString.mkString("\n"))
      Failure(new Exception(exceptionsAsString.mkString("\n")))
    } else {
      logger.info("All tables loaded successfully")
      Success(SparkJobResult(None, Some(counters)))
    }
  }

  protected def domainsToWatch(config: LoadConfig): List[DomainInfo] = {
    val includedDomains = config.domains match {
      case Nil =>
        domains()
      case _ =>
        domains(config.domains.toList, config.tables.toList)
    }
    logger.info(s"Domains that will be watched: ${includedDomains.map(_.name).mkString(",")}")
    includedDomains
  }

  /** @param domainName
    *   : Domaine name
    * @return
    *   resolved && unresolved schemas / path
    */
  private def pending(
    domainName: String,
    schemasName: List[String]
  ): (Iterable[(Option[SchemaInfo], FileInfo)], Iterable[(Option[SchemaInfo], FileInfo)]) = {
    val pendingArea = DatasetArea.stage(domainName)
    logger.info(s"List files in $pendingArea")
    val loadStrategy =
      Utils
        .loadInstance[LoadStrategy](settings.appConfig.loadStrategyClass)

    val files =
      loadStrategy
        .list(settings.storageHandler(), pendingArea, recursive = false)

    if (files.nonEmpty)
      logger.info(s"Found ${files.mkString(",")}")
    else
      logger.info(s"No Files Found.")
    val domain = schemaHandler.getDomain(domainName)

    val filteredFiles = (dom: DomainInfo) =>
      if (schemasName.nonEmpty) {
        logger.info(
          s"We will only watch files that match the schemas name:" +
          s" $schemasName for the Domain: ${dom.name}"
        )
        files.filter(f => predicate(dom, schemasName, f.path))
      } else {
        logger.info(
          s"We will watch all the files for the Domain:" +
          s" ${dom.name}"
        )
        files
      }

    val schemas = for {
      dom <- domain.toList
      (schema, path) <- filteredFiles(dom).map { file =>
        (dom.findSchema(file.path.getName), file)
      }
    } yield {
      logger.info(
        s"Found Schema ${schema.map(_.name).getOrElse("None")} for file $path"
      )
      (schema, path)
    }

    if (schemas.isEmpty) {
      logger.info(s"No Files found to ingest.")
    }
    schemas.partition { case (schema, _) => schema.isDefined }
  }

  private def predicate(domain: DomainInfo, schemasName: List[String], file: Path): Boolean = {
    schemasName.exists { schemaName =>
      val schema = domain.tables.find(_.name.equals(schemaName))
      schema.exists(_.pattern.matcher(file.getName).matches())
    }
  }

  /** Ingest the file (called by the cron manager at ingestion time for a specific dataset
    */
  def load(config: IngestConfig): Try[JobResult] = {
    if (config.domain.isEmpty || config.schema.isEmpty) {
      val domainToWatch = if (config.domain.nonEmpty) List(config.domain) else Nil
      val schemasToWatch = if (config.schema.nonEmpty) List(config.schema) else Nil
      load(
        LoadConfig(
          domains = domainToWatch,
          tables = schemasToWatch,
          options = config.options,
          accessToken = config.accessToken,
          test = false,
          files = None,
          scheduledDate = config.scheduledDate
        )
      )
    } else {
      val lockPath =
        new Path(settings.appConfig.lock.path, s"${config.domain}_${config.schema}.lock")
      val locker = new FileLock(lockPath, storageHandler)
      val waitTimeMillis = settings.appConfig.lock.timeout

      locker.doExclusively(waitTimeMillis) {
        val domainName = config.domain
        val schemaName = config.schema
        val ingestingPaths = config.paths
        val result =
          for {
            domain <- domains(domainName :: Nil, schemaName :: Nil).find(_.name == domainName)
            schema <- domain.tables.find(_.name == schemaName)
          } yield ingest(
            domain,
            schema,
            ingestingPaths,
            config.options,
            config.accessToken,
            test = false,
            scheduledDate = config.scheduledDate
          )
        result match {
          case None =>
            Success(SparkJobResult(None, None))
          case Some(Success(jobResult)) =>
            Success(jobResult)
          case Some(Failure(exception)) =>
            Failure(exception)
        }
      }
    }
  }

  def ingest(
    domain: DomainInfo,
    schema: SchemaInfo,
    ingestingPaths: List[Path],
    options: Map[String, String],
    accessToken: Option[String],
    test: Boolean,
    scheduledDate: Option[String]
  ): Try[JobResult] =
    ingestFiles(domain, schema, ingestingPaths, options, accessToken, test, scheduledDate)._1

  /** @return
    *   the result of the load, and whether it failed before writing any row to its table, its files
    *   being then back in the stage area
    */
  @nowarn
  private def ingestFiles(
    domain: DomainInfo,
    schema: SchemaInfo,
    ingestingPaths: List[Path],
    options: Map[String, String],
    accessToken: Option[String],
    test: Boolean,
    scheduledDate: Option[String]
  ): (Try[JobResult], Boolean) = {
    val metadata = schema.mergedMetadata(domain.metadata)
    Utils.printOut(
      s"""Loading
         |Format: ${metadata.resolveFormat()}
         |File(s): ${ingestingPaths.mkString(",")}
         |Table: ${domain.finalName}.${schema.finalName}""".stripMargin
    )
    logger.debug(
      s"Ingesting domain: ${domain.name} with schema: ${schema.name}"
    )

    val ingestionJob = Try {
      val optionsAndEnvVars = schemaHandler.activeEnvVars() ++ options
      val context = IngestionContext(
        domain = domain,
        schema = schema,
        types = schemaHandler.types(),
        path = ingestingPaths,
        storageHandler = storageHandler,
        schemaHandler = schemaHandler,
        options = optionsAndEnvVars,
        accessToken = accessToken,
        test = test,
        scheduledDate = scheduledDate
      )
      IngestionJobFactory.createJob(context)
    }
    ingestionJob.map(_.run()) match {
      case Success(Success(jobResult)) =>
        if (test) {
          logger.info(s"Test mode enabled, no file will be deleted")
        } else if (settings.appConfig.archive) {
          val now = System.currentTimeMillis()
          ParUtils.runInParallel(ingestingPaths, Some(settings.appConfig.maxParCopy)) {
            ingestingPath =>
              val archivePath =
                new Path(DatasetArea.archive(domain.name), ingestingPath.getName)
              logger.info(s"Backing up file $ingestingPath to $archivePath")
              val _ = storageHandler.move(ingestingPath, archivePath)
          }
          Utils.printOut("Archive duration takes " + ExtractUtils.toHumanElapsedTimeFrom(now))
        } else {
          logger.info(s"Deleting file $ingestingPaths")
          ParUtils.runInParallel(ingestingPaths, Some(settings.appConfig.maxParCopy)) {
            ingestingPath =>
              storageHandler.delete(ingestingPath)
          }
        }
        (Success(jobResult), false)
      case result =>
        val exception = result.flatten.failed.get
        Utils.logException(logger, exception)
        val failedBeforeWrite = !test && !ingestionJob.toOption.exists(_.rowsWritten)
        if (test)
          logger.info(s"Test mode enabled, no file will be moved")
        else if (failedBeforeWrite)
          giveBackToStage(domain, ingestingPaths)
        else
          logger.warn(
            s"The failed load of ${domain.finalName}.${schema.finalName} may have written rows: ${ingestingPaths
                .mkString(", ")} stay where they are, since loading them again could add these rows again"
          )
        (Failure(exception), failedBeforeWrite)
    }
  }

  /** @return
    *   true when every file of the table is appended to it, whatever its name: an adaptive strategy
    *   (`types`) may overwrite the table with some of them
    */
  private def appends(domain: DomainInfo, schema: SchemaInfo): Boolean = {
    val strategy = schema.mergedMetadata(domain.metadata).getStrategyOptions()
    strategy.types.filter(_.nonEmpty) match {
      case Some(types) => types.keys.forall(_ == WriteStrategyType.APPEND.value)
      case None        => strategy.getEffectiveType() == WriteStrategyType.APPEND
    }
  }

  /** Gives back the files of a table without loading them, when an earlier file of the table failed
    * before writing its rows: loading them first would apply the files out of their order.
    */
  private def holdBack(
    domain: DomainInfo,
    schema: SchemaInfo,
    paths: List[Path]
  ): Try[JobResult] = {
    val message =
      s"${paths.map(_.getName).mkString(", ")} not loaded into ${domain.finalName}.${schema.finalName}: an earlier file of the table failed before writing its rows, and loading these first would change the order of the files"
    logger.warn(message)
    giveBackToStage(domain, paths)
    Failure(new Exception(message) {
      // The failure of the earlier file holds the stack trace that matters
      override def fillInStackTrace(): Throwable = this
    })
  }

  /** Gives the files of a load that failed before writing its rows back to the stage area of their
    * domain, where the next load looks for them. Only the files of the ingesting area go back: a
    * file loaded from its own path stays there, and a file whose name was delivered to stage again
    * since stays in ingesting, since moving it would delete the new one.
    */
  private def giveBackToStage(domain: DomainInfo, ingestingPaths: List[Path]): Unit = {
    val ingestingArea = DatasetArea.ingesting(domain.name).toUri.getPath
    val stageArea = DatasetArea.stage(domain.name)
    val (ingestingFiles, ownFiles) =
      ingestingPaths.partition(path =>
        Option(path.getParent).exists(_.toUri.getPath == ingestingArea)
      )
    ownFiles.foreach(path => logger.info(s"$path stays where it is: it was loaded from its path"))
    ParUtils.runInParallel(ingestingFiles, Some(settings.appConfig.maxParCopy)) { ingestingPath =>
      val stagePath = new Path(stageArea, ingestingPath.getName)
      Try {
        if (!storageHandler.exists(ingestingPath))
          logger.warn(s"Could not give $ingestingPath back to $stagePath: it is gone")
        else if (storageHandler.exists(stagePath))
          logger.warn(
            s"$ingestingPath stays in the ingesting area: $stagePath was delivered again since"
          )
        else if (storageHandler.move(ingestingPath, stagePath))
          logger.info(s"Gave $ingestingPath back to $stagePath, for the next load")
        else
          logger.error(s"Could not give $ingestingPath back to $stagePath")
      }.failed.foreach { exception =>
        logger.error(s"Could not give $ingestingPath back to $stagePath", exception)
      }
    }
  }

}
