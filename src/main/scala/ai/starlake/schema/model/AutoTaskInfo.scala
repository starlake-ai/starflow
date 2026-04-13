package ai.starlake.schema.model

import ai.starlake.config.Settings.ConnectionInfo
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.extract.{ExtractSchema, ExtractSchemaConfig}
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Severity.Error
import ai.starlake.sql.{SQLTypeMappings, SQLUtils}
import ai.starlake.transpiler.JSQLSchemaDiff
import ai.starlake.transpiler.diff.{Attribute as DiffAttribute, DBSchema}
import ai.starlake.transpiler.schema.CaseInsensitiveLinkedHashMap
import ai.starlake.utils.{SparkUtils, StarlakeNotFoundException, YamlSerde}
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

case class TaskDesc(version: Int, task: AutoTaskInfo)

/** Task executed in the context of a job. Each task is executed in its own session.
  *
  * @param sql
  *   Main SQL request to exexute (do not forget to prefix table names with the database name to
  *   avoid conflicts)
  * @param domain
  *   Output domain in output Area (Will be the Database name in Hive or Dataset in BigQuery)
  * @param table
  *   Dataset Name in output Area (Will be the Table name in Hive & BigQuery)
  * @param write
  *   Append to or overwrite existing data
  * @param partition
  *   List of columns used for partitioning the outtput.
  * @param presql
  *   List of SQL requests to executed before the main SQL request is run
  * @param postsql
  *   List of SQL requests to executed after the main SQL request is run
  * @param sink
  *   Where to sink the data
  * @param rls
  *   Row level security policy to apply too the output data.
  */
@JsonIgnoreProperties(
  Array("_filenamePrefix", "_auditTableName", "_dbComment", "write")
)
case class AutoTaskInfo(
  name: String, // Name of the task. Made of jobName + '.' + tableName
  sql: Option[String],
  database: Option[String],
  domain: String,
  table: String,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  sink: Option[AllSinks] = None,
  rls: List[RowLevelSecurity] = Nil,
  expectations: List[ExpectationItem] = Nil,
  acl: List[AccessControlEntry] = Nil,
  comment: Option[String] = None,
  freshness: Option[Freshness] = None,
  attributes: List[TableAttribute] = Nil,
  python: Option[Path] = None,
  tags: Set[String] = Set.empty,
  writeStrategy: Option[WriteStrategy] = None,
  schedule: Option[String] = None,
  dagRef: Option[String] = None,
  _filenamePrefix: String = "", // for internal use. prefix of sql / py file
  parseSQL: Option[Boolean] = None,
  _auditTableName: Option[String] = None,
  taskTimeoutMs: Option[Long] = None,
  _dbComment: Option[String] = None,
  connectionRef: Option[String] = None,
  streams: List[String] = Nil,
  primaryKey: List[String] = Nil,
  syncStrategy: Option[TableSync] = None,
  assetTriggeringStrategy: Option[String] = None
) extends Named {

  @JsonIgnore
  def isAuditTable(): Boolean = _auditTableName.isDefined

  @JsonIgnore
  def getSyncStrategyValue(): TableSync = syncStrategy.getOrElse(TableSync.ADD)

  @JsonIgnore
  def getTableName(): String = this.table

  @JsonIgnore
  def getStrategy()(implicit settings: Settings): WriteStrategy = {
    val st1 = writeStrategy.getOrElse(WriteStrategy(Some(WriteStrategyType.APPEND)))
    val startTs = st1.startTs.getOrElse(settings.appConfig.scd2StartTimestamp)
    val endTs = st1.endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    st1.copy(startTs = Some(startTs), endTs = Some(endTs))
  }

  @JsonIgnore
  def getWriteMode(): WriteMode =
    writeStrategy.map(_.toWriteMode()).getOrElse(WriteMode.APPEND)

  def fullName() = s"${this.domain}.${this.name}"

  def merge(child: AutoTaskInfo): AutoTaskInfo = {
    AutoTaskInfo(
      name = child.name,
      sql = child.sql,
      database = child.database.orElse(database),
      domain = if (child.domain.isEmpty) domain else child.domain,
      table = if (child.table.isEmpty) table else child.table,
      presql = presql ++ child.presql,
      postsql = postsql ++ child.postsql,
      sink = sink.orElse(child.sink).map(_.merge(child.sink.getOrElse(AllSinks()))),
      rls = rls ++ child.rls,
      expectations = expectations ++ child.expectations,
      acl = acl ++ child.acl,
      comment = child.comment,
      freshness = freshness.orElse(child.freshness),
      attributes = child.attributes,
      python = child.python,
      tags = tags ++ child.tags,
      writeStrategy = child.writeStrategy.orElse(writeStrategy),
      schedule = child.schedule.orElse(schedule),
      dagRef = child.dagRef.orElse(dagRef),
      _filenamePrefix = child._filenamePrefix,
      parseSQL = child.parseSQL.orElse(parseSQL),
      taskTimeoutMs = child.taskTimeoutMs.orElse(taskTimeoutMs),
      connectionRef = child.connectionRef.orElse(connectionRef),
      assetTriggeringStrategy = child.assetTriggeringStrategy.orElse(assetTriggeringStrategy)
    )
  }
  /*

   */
  def checkValidity()(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val sinkErrors = sink.map(_.checkValidity(this.name, None)).getOrElse(Right(true))
    val freshnessErrors = freshness.map(_.checkValidity(this.name)).getOrElse(Right(true))
    val emptySchema = new SchemaInfo()
    val writeStrategyErrors = writeStrategy
      .map(
        _.checkValidity(
          this.domain,
          Some(emptySchema.copy(name = this.name, metadata = Some(Metadata(sink = this.sink))))
        )
      )
      .getOrElse(Right(true))

    val scheduleErrors = schedule
      .map { schedule =>
        if (schedule.contains(" ") || settings.appConfig.schedulePresets.contains(schedule)) {
          Right(true)
        } else {
          Left(
            List(
              ValidationMessage(
                Error,
                s"Task $name",
                s"schedule: $schedule is not a valid schedule. Valid schedules are ${settings.appConfig.schedulePresets
                    .mkString(", ")}"
              )
            )
          )
        }
      }
      .getOrElse(Right(true))

    // TODO dag errors

    val dagRefErrors =
      this.dagRef
        .map { dagRef =>
          settings.schemaHandler().checkDagNameValidity(dagRef)
        }
        .getOrElse(Right(true))

    val fullTableName = s"${this.domain}_${this.table}"

    val streamsErrors = streams.map { stream =>
      if (!stream.startsWith(fullTableName)) {
        Left(
          List(
            ValidationMessage(
              Error,
              s"Task $name",
              s"stream: $stream is not a valid stream. Stream should start with $fullTableName"
            )
          )
        )
      } else {
        Right(true)
      }
    }
    // merge all errors
    val allErrors =
      List(sinkErrors, freshnessErrors, writeStrategyErrors, scheduleErrors, dagRefErrors)
    val errorList = allErrors.collect { case Left(errors) => errors }.flatten
    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList)
  }

  def this() = this(
    name = "",
    sql = None,
    database = None,
    domain = "",
    table = "",
    python = None,
    writeStrategy = None,
    taskTimeoutMs = None,
    assetTriggeringStrategy = None
  ) // Should never be called. Here for Jackson deserialization only

  def getSql(): String = sql.getOrElse("")

  def getDatabase()(implicit settings: Settings): Option[String] = {
    database
      .orElse(settings.appConfig.getDefaultDatabase()) // database passed in env vars
  }

  def getRunEngine()(implicit settings: Settings): Engine = {
    getRunConnection().getEngine()
  }

  def getSinkConnection()(implicit settings: Settings): ConnectionInfo = {
    val connection = settings.appConfig
      .connection(getSinkConnectionRef())
      .getOrElse(throw new StarlakeNotFoundException(s"Connection not found: $connectionRef"))
    connection
  }
  def getSinkConnectionRef()(implicit settings: Settings): String = {
    val connectionRef =
      sink.flatMap { sink => sink.connectionRef }.getOrElse(this.getRunConnectionRef())
    connectionRef
  }

  def getRunConnectionRef()(implicit settings: Settings): String = {
    val transformConnectionRef =
      if (settings.appConfig.transformConnectionRef.isEmpty)
        settings.appConfig.connectionRef
      else
        settings.appConfig.transformConnectionRef
    this.connectionRef.getOrElse(transformConnectionRef)
  }

  def getRunConnection()(implicit settings: Settings): ConnectionInfo = {
    val connection = settings.appConfig
      .connection(getRunConnectionRef())
      .getOrElse(
        throw new StarlakeNotFoundException(
          s"Connection not found: ${settings.appConfig.connectionRef}"
        )
      )
    connection
  }

  def getSinkConnectionType()(implicit settings: Settings): ConnectionType = {
    getSinkConnection().`type`
  }

  def getRunConnectionType()(implicit settings: Settings): ConnectionType = {
    getRunConnection().`type`
  }

  def getSinkConfig()(implicit settings: Settings): Sink = {
    val sinkConnectionRef = this.getSinkConnectionRef()
    this.sink
      .map(_.getSink())
      .getOrElse(AllSinks().copy(connectionRef = Some(sinkConnectionRef)).getSink())
  }

  def getTranspiledSql(
    extraVars: Map[String, String] = Map.empty
  )(implicit settings: Settings): String = {
    val schemaHandler = settings.schemaHandler()
    val allVars = schemaHandler.activeEnvVars() ++ extraVars
    val inputSQL = getSql()
    val runConnection = this.getRunConnection()
    if (parseSQL.getOrElse(true))
      schemaHandler.transpileAndSubstituteSelectStatement(
        sql = inputSQL,
        connection = runConnection,
        allVars = allVars,
        test = false
      )
    else
      inputSQL
  }

  /** Extracts attributes from the SQL statement and compares them with the existing attributes in
    * the yaml
    * @param sqlStatementAttributes
    *   List of attributes extracted from an incoming SQL statement
    * @param settings
    * @return
    *   * List of tuples containing the attribute and its status. The status can be one of the
    *   AttributeStatus values:
    *   - ADDED: The attribute is new and not present in the existing attributes
    *   - MODIFIED: The attribute is present but its type or comment has changed
    *   - UNCHANGED: The attribute is present and its type and comment are unchanged
    */
  def diffSqlAttributesWithYaml(
    sqlStatementAttributes: List[TableAttribute]
  )(implicit settings: Settings): List[(TableAttribute, AttributeStatus)] = {

    var nochange: Boolean = true
    val addedAndModifiedAttributes = sqlStatementAttributes
      .map { sqlAttr =>
        this.attributes.find(_.name.equalsIgnoreCase(sqlAttr.name)) match {
          case Some(existingAttr) =>
            val (updatedType, status) =
              if (existingAttr.`type` == sqlAttr.`type`) {
                existingAttr -> AttributeStatus.UNCHANGED
              } else {
                nochange = false
                existingAttr.copy(
                  `type` = sqlAttr.`type`,
                  array = sqlAttr.array
                ) -> AttributeStatus.MODIFIED
              }
            val (updateTypeAndComment, status2) =
              if (sqlAttr.comment.nonEmpty) {
                nochange = false
                updatedType.copy(comment = sqlAttr.comment) -> AttributeStatus.MODIFIED
              } else {
                updatedType -> status
              }
            (updateTypeAndComment, status2)
          case None =>
            nochange = false
            sqlAttr -> AttributeStatus.ADDED
        }
      }
    val deletedAttributes = this.attributes
      .filterNot(attDesc => sqlStatementAttributes.exists(_.name.equalsIgnoreCase(attDesc.name)))
      .map(attDesc => attDesc -> AttributeStatus.REMOVED)
    if (deletedAttributes.nonEmpty)
      nochange = false

    // we do not return anything if nothing has changed
    if (nochange)
      Nil
    else
      addedAndModifiedAttributes ++ deletedAttributes
  }

  def diffSqlAttributesWithSQL(
    sqlStatement: Option[String] = None,
    accessToken: Option[String] = None
  )(implicit
    settings: Settings
  ): List[(TableAttribute, AttributeStatus)] = {
    if (parseSQL.getOrElse(true)) {
      val connection =
        settings.appConfig
          .connection(settings.appConfig.connectionRef)
          .getOrElse(
            throw new Exception(s"Connection not found ${settings.appConfig.connectionRef}")
          )
          .withAccessToken(accessToken)

      val sqlSchema =
        AutoTask.executeSelectSchema(
          domain = "__ignore__",
          table = "__ignore__",
          sql = sqlStatement.getOrElse(this.getTranspiledSql()),
          summarizeOnly = false,
          connection = connection,
          accessToken = accessToken,
          connectionName = Some(settings.appConfig.connectionRef),
          pageSize = 1,
          pageNumber = 1,
          test = false,
          parseSQL = true,
          scheduledDate = None
        )(settings, settings.storageHandler(), settings.schemaHandler())

      val engineName = connection.getJdbcEngineName()
      val sqlAttributes =
        sqlSchema match {
          case Success(schema) =>
            val sqlStatementAttributes = schema.map { case (name, typ) =>
              val sparkType = SQLTypeMappings.getSparkType(typ, engineName.toString).getOrElse("")
              TableAttribute(
                name = name,
                `type` = sparkType,
                array = None
              )
            }
            sqlStatementAttributes
          case Failure(exception) =>
            Nil
        }

      // Extract attributes from the SQL statement
      val sqlStatementAttributesWithStatus =
        this.diffSqlAttributesWithYaml(sqlAttributes)
      sqlStatementAttributesWithStatus
    } else {
      logger.info(
        s"Skipping diff Sql Attributes With Yaml for task ${this.name} as parseSQL is set to false"
      )
      Nil
    }
  }

  /** Update the task description with the new attributes. Existing attributes are updated with the
    * new type and comment. Attributes not present in the new list are dropped.
    *
    * @param incomingAttributes
    *   List of attributes to update the task description with.
    * @param settings
    * @return
    */
  def updateAttributes(
    incomingAttributes: List[TableAttribute]
  )(implicit settings: Settings): AutoTaskInfo = {
    // Filter out attributes that are not present in the incoming attributes
    val withoutDropped =
      this.attributes.filter { existingAttr =>
        incomingAttributes.exists(_.name.equalsIgnoreCase(existingAttr.name))
      }

    // Update existing attributes with the new type and comment, or keep the existing ones if not
    val addAndUpdatedAttributes = incomingAttributes.map { newAttr =>
      this.attributes.find(_.name.equalsIgnoreCase(newAttr.name)) match {
        case Some(existingAttr) =>
          existingAttr.copy(
            `type` = newAttr.`type`,
            array = newAttr.array,
            comment = newAttr.comment
          )
        case None => newAttr
      }
    }
    this.copy(attributes = addAndUpdatedAttributes)
  }

  def diffYamlAttributesWithDB(
    accessToken: Option[String]
  )(implicit settings: Settings): List[(TableAttribute, AttributeStatus)] = {
    val sql = this.getSql()
    val tableNames = SQLUtils.extractTableNames(sql)
    val schemaHandler = settings.schemaHandler()
    val config = ExtractSchemaConfig(
      tables = tableNames,
      external = true,
      outputDir = None,
      connectionRef = Some(this.getRunConnectionRef()),
      accessToken = accessToken
    )
    val extractor = new ExtractSchema(schemaHandler)
    val dbDomain = extractor.extract(config).getOrElse(Nil).headOption
    val yamlAttributes = this.attributes
    val dbAttributes =
      dbDomain match {
        case Some(domain) =>
          domain.tables.headOption
            .map(_.attributes)
            .getOrElse(Nil)
        case None =>
          Nil
      }
    // list all attributes in the yaml that are not in the db
    val addedAttributes = yamlAttributes
      .filterNot(attr => dbAttributes.exists(_.name.equalsIgnoreCase(attr.name)))
      .map(attr => attr -> AttributeStatus.ADDED)
    // list all attributes in the db that are not in the yaml
    val deletedAttributes = dbAttributes
      .filterNot(attr => yamlAttributes.exists(_.name.equalsIgnoreCase(attr.name)))
      .map(dbAttr =>
        TableAttribute(
          name = dbAttr.name,
          `type` = dbAttr.`type`,
          comment = dbAttr.comment
        ) -> AttributeStatus.REMOVED
      )
    // list all attributes in the yaml that are in the db but with a different type or comment
    val modifiedAttributes = yamlAttributes
      .flatMap { yamlAttr =>
        dbAttributes.find(_.name.equalsIgnoreCase(yamlAttr.name)) match {
          case Some(dbAttr)
              if dbAttr.`type` != yamlAttr.`type` || dbAttr.comment != yamlAttr.comment =>
            Some(
              yamlAttr.copy(
                `type` = dbAttr.`type`,
                comment = dbAttr.comment
              ) -> AttributeStatus.MODIFIED
            )
          case _ => None
        }
      }
    // return all attributes with their status
    addedAttributes ++ deletedAttributes ++ modifiedAttributes
  }

  @JsonIgnore
  def getPath()(implicit settings: Settings): Path = {
    require(
      this.fullName().split('.').length == 2,
      s"Invalid full task name: ${this.fullName()}. Expected format: domainName.tableName"
    )
    new Path(DatasetArea.transform, this.getName().replace('.', '/') + ".sl.yml")
  }

  def sparkSchema(
    schemaHandler: SchemaHandler
  ): StructType = {
    val temporary = this.name.startsWith("zztmp_")
    SparkUtils.sparkSchemaWithCondition(
      schemaHandler = schemaHandler,
      attributes = attributes,
      p = _ => true,
      withFinalName = !temporary
    )
  }

  /** Is the task ready to be synced
    *   - has attributes
    *   - all attributes have a type
    *   - sync strategy is not NONE and by default it is ADD which means the task is ready to be
    *     synced
    * @return
    */
  def readyForSync(): Boolean = {
    this.attributes.nonEmpty &&
    this.attributes.forall(_.`type`.nonEmpty) &&
    this.getSyncStrategyValue() != TableSync.NONE
  }
}

object AutoTaskInfo {
  def compare(existing: AutoTaskInfo, incoming: AutoTaskInfo): ListDiff[Named] = {
    AnyRefDiff.diffAnyRef(existing.name, existing, incoming)
  }
  def main(args: Array[String]): Unit = {
    val sql = {
      val res = scala.io.Source.fromFile("/Users/hayssams/tmp/test.sql").getLines()
      val res2 = res.filter { it => !it.trim.startsWith("--") && !it.trim.isEmpty }.mkString("\n")
      val res3 = SQLUtils.stripComments(res2)
      println(res3)
      res3
    }
    val itables = SQLUtils.extractTableNames(sql)
    itables.foreach(println)

    val ymlFile = new java.io.File("/Users/hayssams/tmp/test.yml")
    val map =
      YamlSerde.mapper.readValue(ymlFile, classOf[Array[Map[String, Any]]])

    val diffSchemas: Array[DBSchema] =
      map.map { schema =>
        val name = schema("schemaName")
        val tables =
          schema("tables")
            .asInstanceOf[
              Map[String, List[Any]]
            ]
        val diffTables =
          new CaseInsensitiveLinkedHashMap[java.util.Collection[DiffAttribute]]()
        tables.foreach { table =>
          val tableName = table._1
          val attributes = table._2
            .asInstanceOf[List[Map[String, Any]]]
            .map { attr =>
              val attrName = attr("name").asInstanceOf[String]
              val attrType = attr("type").asInstanceOf[String]
              new DiffAttribute(attrName, attrType)
            }
            .asJava
          diffTables.put(tableName, attributes)
        }
        val dbSchema = new DBSchema("", name.asInstanceOf[String], diffTables)
        dbSchema
      }
    val differ = new JSQLSchemaDiff(diffSchemas.toList.asJava)
    val diff = differ
      .getDiff(sql.trim, "domain1.table1")
    println(diff)
  }
}
