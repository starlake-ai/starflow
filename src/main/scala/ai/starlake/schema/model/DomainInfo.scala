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

package ai.starlake.schema.model

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.lineage.{AclDependenciesConfig, AutoTaskDependencies}
import ai.starlake.schema.handlers.{SchemaHandler, StorageHandler}
import ai.starlake.utils.Utils
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.hadoop.fs.Path
import ai.starlake.schema.model.Severity._
import ai.starlake.utils.YamlSerde.serializeToPath

import scala.annotation.nowarn
import scala.collection.mutable
import scala.util.Try

/** A domain is a set of tables. A domain is defined by a name and a list of tables and load
  * metadat.
  * @param load:
  *   Domain to load
  */
case class DomainDesc(version: Int, load: DomainInfo)

/** Let's say you are willing to import customers and orders from your Sales system. Sales is
  * therefore the domain and customers & orders are your datasets. In a DBMS, A Domain would be
  * implemented by a DBMS schema and a dataset by a DBMS table. In BigQuery, The domain name would
  * be the Big Query dataset name and the dataset would be implemented by a Big Query table.
  *
  * Domains are defined in the _config.sl.yml file located in a directory beneath the domain root
  * directory. The directory name is the domain name. The _config.sl.yml file contains the domain
  * definition.
  *
  * @param name:
  *   Domain name. Make sure you use a name that may be used as a folder name on the target storage.
  *   - When using HDFS or Cloud Storage, files once loaded are stored in a sub-directory named
  *     after the domain name.
  *   - When used with BigQuery, files are ingested and sorted in tables under a dataset named after
  *     the domain name. This attribute is optional.
  * @param metadata:
  *   Default Schema metadata. This metadata is applied to the schemas defined in this domain.
  *   Metadata properties may be redefined at the schema level. See Metadata Entity for more
  *   details.
  * @param tables:
  *   List of schemas for each dataset in this domain A domain usually contains multiple schemas.
  *   Each schema defining how the contents of the input file should be parsed. See Schema for more
  *   details.
  * @param comment:
  *   Domain Description (free text). This description will end up in the database schema
  *   description.
  * @param tags:
  *   Domain tags. Tags are used to categorize domains. These tags will end up in the database
  *   schema tags if supported.
  *   - When using BigQuery, tags are stored in the dataset labels.
  * @param rename:
  *   Domain rename. This attribute is used to rename the domain when ingesting data. This is useful
  *   when you want to rename a domain in the target database. For instance, you may want to rename
  *   a domain from "sales" to "sales_2020" when ingesting data from 2020. This attribute is
  *   optional.
  * @param database:
  *   Database name. This attribute is used to specify the database name when ingesting data. This
  *   is useful when you want to ingest data in a different database than the default one or the one
  *   specified in the settings. This attribute is optional.
  */
@JsonIgnoreProperties(
  Array("tables")
)
@nowarn case class DomainInfo(
  name: String,
  metadata: Option[Metadata] = None,
  tables: List[SchemaInfo] = Nil, // used internally only
  comment: Option[String] = None,
  tags: Set[String] = Set.empty,
  rename: Option[String] = None,
  database: Option[String] = None
) extends Named {

  def this() = this("") // Should never be called. Here for Jackson deserialization only

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  lazy val finalName: String = rename.getOrElse(name)

  /** Get schema from filename Schema are matched against filenames using filename patterns. The
    * schema pattern that matches the filename is returned
    *
    * @param filename
    *   : dataset filename
    * @return
    */
  def findSchema(filename: String): Option[SchemaInfo] = {
    tables.find(_.pattern.matcher(filename).matches())
  }

  /** Load Elasticsearch template file if it exist
    *
    * @param schema
    *   : Schema name to map to an elasticsearch index
    * @return
    *   ES template with optionally the __PROPERTIES__ string that will be replaced by the schema
    *   attributes dynamically computed mappings
    */
  def esMapping(
    schema: SchemaInfo
  )(implicit settings: Settings): Option[String] = {
    val template = new Path(new Path(DatasetArea.mapping, this.name), schema.name + ".json")
    if (settings.storageHandler().exists(template))
      Some(settings.storageHandler().read(template))
    else
      None
  }

  /** Resolve method are here to handle backward compatibility
    * @return
    */
  @nowarn def resolveDirectory(): String = {
    resolveDirectoryOpt() match {
      case Some(directory) => directory
      case None =>
        throw new Exception(
          """directory attribute is mandatory for the "stage" command. Make sure this attribute is defined in the domain metadata (_config.sl.yml file)"""
        )
    }
  }

  /** @return
    *   directory if any
    */
  def resolveDirectoryOpt(): Option[String] = {
    val maybeDirectory = for {
      metadata  <- metadata
      directory <- metadata.directory
    } yield directory
    maybeDirectory
  }

  @nowarn def resolveAck(): Option[String] = {
    val maybeAck = for {
      metadata <- metadata
      ack      <- metadata.ack
    } yield ack

    maybeAck
  }

  /** Ack file should be present for each file to ingest.
    *
    * @return
    *   the ack attribute or ".ack" by default
    */
  @JsonIgnore
  def getAck(): String = resolveAck().map(ack => if (ack.nonEmpty) "." + ack else ack).getOrElse("")

  /** Is this Domain valid ? A domain is valid if :
    *   - The domain name is a valid attribute
    *   - all the schemas defined in this domain are valid
    *   - No schema is defined twice
    *   - Partitions columns are valid columns
    *   - The input directory is a valid path
    *
    * @return
    */
  def checkValidity(
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {

    val messageList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty

    // Check Domain name validity
    val forceDomainPrefixRegex = settings.appConfig.forceDomainPattern.r
    // TODO: name doesn't need to respect the pattern because it may be renamed. Restriction is based on target database restriction.
    // We may check depending on the sink type but we may sink differently for each table.
    // It would be better to assume a starlake pattern to describe a dataset and the container of the dataset such as the bigquery syntax project:dataset
    // and then apply this syntax to all databases even if natively they don't accept that.
    // Therefore, it means that we need to adapt on writing to the database, the target name.
    // The same applies to table name.
    if (name != null && !forceDomainPrefixRegex.pattern.matcher(name).matches())
      messageList += ValidationMessage(
        Error,
        s"Domain $name",
        s"name: Domain with name $name should respect the pattern ${forceDomainPrefixRegex.regex}"
      )

    val forceTablePrefixRegex = settings.appConfig.forceTablePattern.r

    // Check Schemas validity
    tables.foreach { schema =>
      for (errors <- schema.checkValidity(this.name, schemaHandler).left) {
        messageList ++= errors
      }
    }

    // Check Metadata validity
    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(this.name, None).left) {
        messageList ++= errors
      }
    }

    val duplicatesErrorMessage =
      "Schema %s defined %d times. A schema can only be defined once."
    for (
      errors <- Utils.duplicates("Table name", tables.map(_.name), duplicatesErrorMessage).left
    ) {
      messageList ++= errors
    }

    // TODO Check partition columns
    if (messageList.nonEmpty)
      Left(messageList.toList)
    else
      Right(true)
  }

  def asItem(
    fkTables: Set[String]
  ): List[(AutoTaskDependencies.Item, List[AutoTaskDependencies.Relation])] = {
    tables
      .sortBy(_.name)
      .flatMap { schema =>
        schema.asItem(name, fkTables)
      }
  }

  def asDot(includeAllAttrs: Boolean, fkTables: Set[String]): String = {
    tables
      .sortBy(_.name)
      .map { schema =>
        schema.asDot(name, includeAllAttrs, fkTables)
      }
      .mkString("\n")
  }

  def foreignTableNames(tableNames: Seq[String]): List[String] = {
    // get tables included in tableNames
    val tableSchemas = findTables(tableNames)
    // get all tables referenced in foreign keys
    tableSchemas
      .flatMap(_.foreignTablesForDot(this.finalName))
  }

  def findTables(tableNames: Seq[String]): List[SchemaInfo] = {
    val filteredTables = tableNames.flatMap { tableName =>
      this.tables.filter { table =>
        tableNames
          .exists(_.toLowerCase() == (this.finalName + "." + table.finalName).toLowerCase())
      }
    }
    filteredTables.toList
  }

  def aclTables(config: AclDependenciesConfig): List[SchemaInfo] = {
    val filteredTables = if (config.tables.nonEmpty) {
      tables.filter { table =>
        config.tables.exists(
          _.toLowerCase() == (this.finalName + "." + table.finalName).toLowerCase()
        )
      }
    } else {
      tables
    }

    filteredTables
      .filter { table =>
        table.acl.nonEmpty && (config.all || table.containGrantees(config.grantees).nonEmpty)
      }
  }

  /** Get all the tables with RLS defined and the RLS grants match the grantees defined in the
    * config or if config.all is true then return all the tables with RLS defined
    * @param config
    * @return
    */
  def rlsTables(config: AclDependenciesConfig): Map[String, List[RowLevelSecurity]] = {
    val filteredTables = if (config.tables.nonEmpty) {
      tables.filter { table =>
        config.tables.exists(
          _.toLowerCase() == (this.finalName + "." + table.finalName).toLowerCase()
        )
      }
    } else {
      tables
    }

    filteredTables
      .filter { table =>
        table.rls.nonEmpty && (config.all ||
        table.rls
          .flatMap(_.grants)
          .intersect(config.grantees)
          .nonEmpty)
      }
      .map(t => (t.finalName, t.rls))
      .toMap
  }

  def policies(): List[RowLevelSecurity] = {
    tables
      .flatMap(_.acl)
      .map(ace => RowLevelSecurity(name = ace.role, grants = ace.grants)) ++
    tables.flatMap(
      _.rls
    )
  }

  def writeDomainAsYaml(loadBasePath: Path)(implicit storage: StorageHandler): Unit = {

    val folder = new Path(loadBasePath, this.name)
    storage.mkdirs(folder)
    this.tables foreach { schema =>
      serializeToPath(new Path(folder, s"${schema.name}.sl.yml"), schema)
    }
    val domainDataOnly = this.copy(tables = Nil)
    serializeToPath(new Path(folder, "_config.sl.yml"), domainDataOnly)
  }
  def path()(implicit settings: Settings): Path =
    new Path(DatasetArea.load, this.name)

}

object DomainInfo {
  def checkFilenamesValidity()(implicit
    storage: StorageHandler,
    settings: Settings
  ): List[Either[List[ValidationMessage], Boolean]] = {
    val domainRootFiles = storage.list(DatasetArea.load, recursive = false, exclude = None)
    val domainRootDirectories = storage.listDirectories(DatasetArea.load)
    val diff = domainRootFiles
      .diff(domainRootDirectories)
    val extraFileWarnings = if (diff.nonEmpty) {
      List(
        Left(
          List(
            ValidationMessage(
              Warning,
              "Domain",
              s"Domain root directory should only contain directories. Found ${diff.mkString(",")}"
            )
          )
        )
      )
    } else {
      List(Right(true))
    }
    val allWarnings = domainRootDirectories.flatMap { domainDirectory =>
      val domainName = domainDirectory.getName()
      val expectedDomainYmlName = "_config.sl.yml"
      val expectedDomainYmlPath = new Path(domainDirectory, expectedDomainYmlName)
      val domainYmlExists = storage.exists(expectedDomainYmlPath)
      val domainYmlWarnings = if (domainYmlExists) {
        Nil
      } else {
        List(
          Left(
            List(
              ValidationMessage(
                Warning,
                "Domain",
                s"Domain directory for $domainName does not have a _config.sl.yml file"
              )
            )
          )
        )
      }
      List(extraFileWarnings, domainYmlWarnings).flatten
    }
    allWarnings
  }

  def compare(existing: DomainInfo, incoming: DomainInfo): Try[DomainDiff] = {
    Try {
      val (addedTables, deletedTables, existingCommonTables) =
        AnyRefDiff.partitionNamed(existing.tables, incoming.tables)

      val commonTables: List[(SchemaInfo, SchemaInfo)] = existingCommonTables.map { table =>
        (
          table,
          incoming.tables
            .find(_.name.toLowerCase() == table.name.toLowerCase())
            .getOrElse(throw new IllegalStateException("Should not happen"))
        )
      }

      val updatedTablesDiff: List[TableDiff] = commonTables.flatMap {
        case (existingTable, incomingTable) =>
          SchemaInfo.compare(existingTable, incomingTable).toOption
      }
      val metadataDiff: ListDiff[Named] =
        AnyRefDiff.diffOptionAnyRef("metadata", existing.metadata, incoming.metadata)
      val commentDiff = AnyRefDiff.diffOptionString("comment", existing.comment, incoming.comment)
      val tagsDiffs = AnyRefDiff.diffSetString("tags", existing.tags, incoming.tags)
      val renameDiff = AnyRefDiff.diffOptionString("rename", existing.rename, incoming.rename)
      val databaseDiff =
        AnyRefDiff.diffOptionString("database", existing.database, incoming.database)
      DomainDiff(
        name = existing.name,
        tables = TablesDiff(
          added = addedTables.map(_.name),
          deleted = deletedTables.map(_.name),
          updated = updatedTablesDiff
        ),
        metadataDiff.asOption(),
        commentDiff.asOption(),
        tagsDiffs.asOption(),
        renameDiff.asOption(),
        databaseDiff.asOption()
      )
    }
  }

  def ddlExtract(datawarehouse: String, ddlType: String)(implicit
    settings: Settings
  ): (Path, String) = {
    val rootPath = new Path(new Path(DatasetArea.extract, "ddl"), datawarehouse)
    val j2 = new Path(rootPath, s"$ddlType.j2")
    val ssp = new Path(rootPath, s"$ddlType.ssp")
    val mustache = new Path(rootPath, s"$ddlType.mustache")
    val template =
      if (settings.storageHandler().exists(j2))
        j2
      else if (settings.storageHandler().exists(ssp))
        ssp
      else if (settings.storageHandler().exists(mustache))
        mustache
      else
        throw new Exception(
          s"No $j2, $ssp or $mustache found for datawarehouse $datawarehouse"
        )
    template -> settings.storageHandler().read(template)
  }

  def path(domainName: String)(implicit settings: Settings): Path =
    new Path(DatasetArea.load, domainName)

}
