package ai.starlake.job.sink.bigquery
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import ai.starlake.schema.model
import ai.starlake.schema.model.{SchemaInfo as _, TableInfo as _, *}
import ai.starlake.sql.SQLUtils
import ai.starlake.utils.conversion.BigQueryUtils.sparkToBq
import ai.starlake.utils.{GcpCredentials, StarlakeConfigException, Utils}
import better.files.File
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.retrying.PollException
import com.google.cloud.bigquery.{Schema as BQSchema, TableInfo as BQTableInfo, *}
import com.google.cloud.datacatalog.v1.*
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.{
  GoogleCredentials as GcsGoogleCredentials,
  ServiceAccountCredentials as GcsServiceAccountCredentials,
  UserCredentials as GcsUserCredentials
}
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.Credentials as GcsCredentials
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.{Storage, StorageOptions}
import com.google.cloud.{Identity, Policy, Role, ServiceOptions}
import com.google.iam.v1.{Binding, Policy as IAMPolicy, SetIamPolicyRequest}
import com.google.protobuf.FieldMask
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

import java.io.ByteArrayInputStream
import java.security.SecureRandom
import java.util
import java.util.concurrent.TimeoutException
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Base class for BigQuery jobs
  */
trait BigQueryJobBase extends LazyLogging {

  def settings: Settings
  def cliConfig: BigQueryLoadConfig
  logger.debug(s"cliConfig=$cliConfig")
  lazy val connectionName: scala.Option[String] = cliConfig.connectionRef
    .orElse(Some(settings.appConfig.connectionRef))

  lazy val connection: scala.Option[Settings.ConnectionInfo] =
    connectionName.flatMap(name => settings.appConfig.connections.get(name))

  lazy val connectionOptions: Map[String, String] = {
    val map = connection.map(_.options).getOrElse(Map.empty)
    if (cliConfig.forceIndirect)
      map + ("writeMethod" -> "indirect")
    else
      map
  }

  // Lazy otherwise tests fail since there is no GCP credentials in test mode

  private def policyClient()(implicit settings: Settings): PolicyTagManagerClient = {
    val credentials = GcpCredentials.credentials(connectionOptions)
    val policySettings =
      credentials match {
        case None =>
          PolicyTagManagerSettings.newBuilder().build()
        case Some(credentials) =>
          val credentialsProvider = FixedCredentialsProvider.create(credentials)
          PolicyTagManagerSettings.newBuilder().setCredentialsProvider(credentialsProvider).build()
      }
    PolicyTagManagerClient.create(policySettings)

  }
  def applyRLSAndCLS(forceApply: Boolean = false)(implicit settings: Settings): Try[Unit] = {
    val client = policyClient()
    val result = for {
      _ <- applyRLS(forceApply)
      _ <- applyCLS(forceApply, client)
    } yield ()
    client.shutdown()
    result
  }

  def bigquery(alwaysCreate: Boolean = false, accessToken: scala.Option[String])(implicit
    settings: Settings
  ): BigQuery = {
    val create = alwaysCreate || _bigquery.isEmpty
    if (create) {
      val bqService =
        BigQueryJobBase.bigquery(
          this.connectionName,
          accessToken,
          cliConfig.outputDatabase
        )
      if (!alwaysCreate)
        _bigquery = Some(bqService)
      bqService
    } else {
      _bigquery.getOrElse(throw new IllegalStateException("Should never happen"))
    }
  }

  private var _gcsStorage: scala.Option[Storage] = None

  def gcsStorage()(implicit settings: Settings): Storage = {
    _gcsStorage match {
      case None =>
        logger.info(s"Getting GCS credentials for connection $connectionName")
        val project =
          BigQueryJobBase.projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase)
        StorageOptions.newBuilder.setProjectId(project).build.getService
        val gcsOptionsBuilder = StorageOptions.newBuilder()
        val credentials = BigQueryJobBase.gcsCredentials(connectionOptions)
        val gcsOptions = gcsOptionsBuilder.setProjectId(
          BigQueryJobBase.projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase)
        )
        val gcsService = gcsOptions
          .setCredentials(credentials)
          .build()
          .getService()
        _gcsStorage = Some(gcsService)
        gcsService
      case Some(gcsService) =>
        gcsService
    }
  }

  private var _bigquery: scala.Option[BigQuery] = None

  private def applyRLS(forceApply: Boolean)(implicit settings: Settings): Try[Unit] = {
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply) {
        val outputTableId =
          cliConfig.outputTableId match {
            case None =>
              throw new RuntimeException(
                "TableId must be defined in order to apply access policies."
              )
            case Some(tableId) => tableId
          }
        applyACL(outputTableId, cliConfig.acl)
        BigQueryJobBase.buildRLSQueries(outputTableId, cliConfig.rls).foreach { rlsStatement =>
          logger.info(s"Applying row level security $rlsStatement")
          new BigQueryNativeJob(
            cliConfig,
            rlsStatement,
            jobTimeoutMs = Some(settings.appConfig.shortJobTimeoutMs)
          ).runBigQueryJob() match {
            case Failure(e) =>
              throw e
            case Success(BigQueryJobResult(_, _, Some(job)))
                if job.getStatus.getExecutionErrors != null =>
              throw new RuntimeException(
                job.getStatus.getExecutionErrors.asScala.reverse.mkString(",")
              )
            case Success(job) =>
              logger.info(s"Job with id $job on Statement $rlsStatement succeeded")
          }
        }
      }
    }
  }

  private def getPolicyTag(
    location: String,
    projectId: String,
    taxonomy: String,
    taxonomyRef: String,
    accessPolicy: String,
    policyTagClient: PolicyTagManagerClient
  ): String = {
    val policyTagsRequest =
      ListPolicyTagsRequest.newBuilder().setParent(taxonomyRef).build()
    val policyTags =
      policyTagClient.listPolicyTags(policyTagsRequest)
    val policyTagRef =
      policyTags
        .iterateAll()
        .asScala
        .filter(_.getDisplayName == accessPolicy)
        .map(_.getName)
        .headOption
        .getOrElse(
          throw new Exception(
            s"PolicyTag $accessPolicy not found in Taxonomy $taxonomy in project $projectId in location $location"
          )
        )
    policyTagRef
  }

  def applyIamPolicyTags(iamPolicyTags: IamPolicyTags)(implicit settings: Settings): Try[Unit] = {
    val client = policyClient()
    val result = Try {
      val (location, projectId, taxonomy, taxonomyRef) = getTaxonomy(client)

      val groupedPoliciyTags: Map[String, List[IamPolicyTag]] =
        iamPolicyTags.iamPolicyTags.groupBy(_.policyTag)

      groupedPoliciyTags.foreach { case (policyTag, iamPolicyTags) =>
        val bindings = iamPolicyTags.map { iamPolicyTag =>
          val binding = Binding.newBuilder()
          binding.setRole(
            iamPolicyTag.role.getOrElse(
              throw new IllegalStateException("Should never happen: Role not set")
            )
          )
          // binding.setCondition()
          binding.addAllMembers(iamPolicyTag.members.asJava)
          binding.build()
        }
        val iamPolicy = IAMPolicy.newBuilder().addAllBindings(bindings.asJava).build()
        val policyTagId =
          getPolicyTag(location, projectId, taxonomy, taxonomyRef, policyTag, client)
        val request =
          SetIamPolicyRequest
            .newBuilder()
            .setResource(policyTagId)
            .setPolicy(iamPolicy)
            .setUpdateMask(FieldMask.newBuilder().addPaths("bindings").build())
            .build()
        val createdPolicy = client.setIamPolicy(request)
        logger.info(createdPolicy.toString)
      }
    }
    client.shutdown()
    result
  }

  private def getTaxonomy(
    client: PolicyTagManagerClient
  )(implicit settings: Settings): (String, String, String, String) = {
    val taxonomyProjectId =
      if (settings.appConfig.accessPolicies.database == "invalid_project") {
        BigQueryJobBase.projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase)
      } else
        settings.appConfig.accessPolicies.database

    val location = settings.appConfig.accessPolicies.location
    val taxonomy = settings.appConfig.accessPolicies.taxonomy
    if (location == "invalid_location")
      throw new Exception("accessPolicies.location not set")
    if (taxonomyProjectId == "invalid_project")
      throw new Exception("accessPolicies.projectId not set")
    if (taxonomy == "invalid_taxonomy")
      throw new Exception("accessPolicies.taxonomy not set")

    val taxonomyListRequest =
      ListTaxonomiesRequest
        .newBuilder()
        .setParent(LocationName.of(taxonomyProjectId, location).toString)
        .setPageSize(1000)
        .build()
    logger.info(s"Getting Taxonomy $taxonomy in project $taxonomyProjectId in location $location")
    val taxonomyList = client.listTaxonomies(taxonomyListRequest)
    val taxonomyRef = taxonomyList
      .iterateAll()
      .asScala
      .filter(_.getDisplayName() == taxonomy)
      .map(_.getName)
      .headOption
      .getOrElse(
        throw new Exception(
          s"Taxonomy $taxonomy not found in project $taxonomyProjectId in location $location"
        )
      )
    logger.info(s"Taxonomy $taxonomy found in project $taxonomyProjectId in location $location")
    (location, taxonomyProjectId, taxonomy, taxonomyRef)
  }

  private def applyCLS(forceApply: Boolean, policyTagClient: PolicyTagManagerClient)(implicit
    settings: Settings
  ): Try[Unit] = {
    Try {
      if (forceApply || settings.appConfig.accessPolicies.apply) {
        val loadAttrs = cliConfig.starlakeSchema
          .map(_.attributes)
          .getOrElse(Nil)
          .map(attribute => (attribute.getFinalName(), attribute.accessPolicy))
        val transAttrs =
          cliConfig.attributesDesc.map(attrDesc => (attrDesc.name, attrDesc.accessPolicy))
        val attrsToSecure =
          if (loadAttrs.isEmpty)
            transAttrs
          else
            loadAttrs

        attrsToSecure match {
          case Nil =>
          case attrs =>
            val anyAccessPolicyToApply =
              attrs
                .map { case (attrName, attrAccessPolicy) => attrAccessPolicy }
                .count(_.isDefined) > 0
            if (anyAccessPolicyToApply) {
              val (location, projectId, taxonomy, taxonomyRef) =
                getTaxonomy(policyTagClient)
              val policyTagIds = mutable.Map.empty[String, String]
              val table = cliConfig.outputTableId match {
                case None =>
                  throw new RuntimeException("TableId must be defined in order to apply CLS")
                case Some(outputTableId) =>
                  bigquery(accessToken = cliConfig.accessToken).getTable(outputTableId)
              }
              val tableDefinition = table.getDefinition[StandardTableDefinition]
              val bqSchema = tableDefinition.getSchema()
              val bqFields = bqSchema.getFields.asScala.toList
              val attributesMap = attrs.toMap
              val updatedFields = bqFields.map { field =>
                attributesMap.get(field.getName.toLowerCase) match {
                  case None =>
                    // Maybe an ignored field
                    logger.info(
                      s"Ignore this field ${table}.${field.getName} during CLS application "
                    )
                    field
                  case Some(accessPolicy) =>
                    accessPolicy match {
                      case None =>
                        field
                      case Some(accessPolicy) =>
                        val policyTagId = policyTagIds.getOrElse(
                          accessPolicy, {
                            val policyTagRef =
                              getPolicyTag(
                                location,
                                projectId,
                                taxonomy,
                                taxonomyRef,
                                accessPolicy,
                                policyTagClient
                              )
                            policyTagIds.put(accessPolicy, policyTagRef)
                            policyTagRef
                          }
                        )

                        val fieldPolicyTags =
                          scala
                            .Option(field.getPolicyTags)
                            .map(_.getNames.asScala.toList)
                            .getOrElse(Nil)
                        if (fieldPolicyTags.length == 1 && fieldPolicyTags.head == policyTagId)
                          field
                        else {
                          Field
                            .newBuilder(field.getName, field.getType, field.getSubFields)
                            .setMode(field.getMode)
                            .setPolicyTags(
                              PolicyTags.newBuilder().setNames(List(policyTagId).asJava).build()
                            )
                            .build()
                        }
                    }
                }
              }
              table.toBuilder
                .setDefinition(StandardTableDefinition.of(BQSchema.of(updatedFields: _*)))
                .build()
                .update()
            }
        }
      }

    }
  }

  lazy val tableId: TableId = {
    cliConfig.outputTableId.getOrElse(throw new StarlakeConfigException("TableId must be defined"))
  }

  protected lazy val datasetId: DatasetId = BigQueryJobBase.getBqDatasetId(tableId)

  protected lazy val bqTable: String = BigQueryJobBase.getBqTableForSpark(tableId)

  protected lazy val bqNativeTable: String = BigQueryJobBase.getBqTableForNative(tableId)

  def tableExists(tableId: TableId)(implicit settings: Settings): Boolean = {
    try {
      val table = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
      table != null && table.exists
    } catch {
      case e: BigQueryException =>
        e.printStackTrace()
        false
    }
  }

  def tableExists(databaseName: scala.Option[String], datasetName: String, tableName: String)(
    implicit settings: Settings
  ): Boolean =
    tableExists(getTableId(databaseName, datasetName, tableName))

  def dropTable(tableId: TableId)(implicit
    settings: Settings
  ): Boolean = {
    val success = bigquery(accessToken = cliConfig.accessToken).delete(tableId)
    if (success)
      logger.info(s"Table $tableId deleted")
    else
      logger.info(s"Table $tableId not found")
    success
  }
  def dropTable(databaseName: scala.Option[String], datasetName: String, tableName: String)(implicit
    settings: Settings
  ): Boolean = {
    val tableId = getTableId(databaseName, datasetName, tableName)
    dropTable(tableId)
  }

  def getTableId(
    databaseName: scala.Option[String],
    datasetName: String,
    tableName: String
  ): TableId = {
    databaseName match {
      case Some(dbName) =>
        TableId.of(dbName, datasetName, tableName)
      case None => TableId.of(datasetName, tableName)
    }
  }

  def getBQSchema(tableId: TableId)(implicit settings: Settings): BQSchema = {
    val table = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
    require(table.exists)
    table.getDefinition[StandardTableDefinition].getSchema
  }

  def updateTableSchema(tableId: TableId, tableSchema: BQSchema)(implicit
    settings: Settings
  ): Try[StandardTableDefinition] = {
    Try {
      val table = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
      val newTableDefinition = StandardTableDefinition.newBuilder().setSchema(tableSchema).build()
      table.toBuilder.setDefinition(newTableDefinition).build().update()
      newTableDefinition
    }
  }

  /** Get or create a BigQuery dataset
    *
    * @param domainDescription
    *   the domain description
    * @return
    *   the dataset
    */
  def getOrCreateDataset(
    domainDescription: scala.Option[String],
    datasetName: scala.Option[String] = None
  )(implicit settings: Settings): Try[Dataset] = {
    val tryResult =
      BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
        val datasetId = datasetName match {
          case Some(name) =>
            DatasetId.of(
              BigQueryJobBase
                .projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase),
              name
            )
          case None => this.datasetId
        }
        val existingDataset =
          scala.Option(bigquery(accessToken = cliConfig.accessToken).getDataset(datasetId))
        val labels = Utils.extractTags(cliConfig.domainTags).toMap
        existingDataset match {
          case Some(ds) =>
            updateDatasetInfo(ds, domainDescription, labels)
          case None =>
            val datasetInfo = DatasetInfo
              .newBuilder(datasetId)
              .setLocation(
                connectionOptions.getOrElse(
                  "location",
                  throw new Exception(
                    s"location is required but not present in connection $connectionName"
                  )
                )
              )
              .setDescription(domainDescription.orNull)
              .setLabels(labels.asJava)
              .build
            bigquery(accessToken = cliConfig.accessToken).create(datasetInfo)
        }
      }
    tryResult match {
      case Failure(exception) =>
        logger.error(s"Dataset ${datasetId.getDataset} was not created / retrieved.")
        Utils.logException(logger, exception)
        tryResult
      case Success(_) => tryResult
    }
  }

  /** Get or create a BigQuery table
    *
    * @param domainDescription
    *   the domain description
    * @param tableInfo
    *   the table info
    * @param maybeSourceDF
    *   the source dataframe
    * @return
    *   the table and the source dataframe
    */
  def getOrCreateTable(
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame],
    outputTableId: Option[TableId] = None
  )(implicit settings: Settings): Try[(Table, StandardTableDefinition)] = {
    val targetTableId = outputTableId.getOrElse(tableId)
    val tryResult =
      BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
        val table =
          if (tableExists(targetTableId)) {
            val table = bigquery(accessToken = cliConfig.accessToken).getTable(targetTableId)
            updateTableDescription(table, tableInfo.maybeTableDescription.orNull)
          } else {
            val tableDefinition = newTableDefinition(tableInfo, dataFrame)
            val bqTableInfoBuilder = BQTableInfo
              .newBuilder(targetTableId, tableDefinition)
              .setDescription(tableInfo.maybeTableDescription.orNull)

            tableInfo.maybeDurationMs.foreach(d =>
              bqTableInfoBuilder.setExpirationTime(System.currentTimeMillis() + d)
            )

            val bqTableInfo = bqTableInfoBuilder.build
            logger.info(s"Creating table ${targetTableId.getDataset}.${targetTableId.getTable}")
            val result = bigquery(accessToken = cliConfig.accessToken).create(bqTableInfo)
            logger.info(
              s"Table ${targetTableId.getDataset}.${targetTableId.getTable} created successfully"
            )
            result
          }
        setTagsOnTable(table)
        (table, table.getDefinition[StandardTableDefinition])
      }
    tryResult match {
      case Failure(exception) =>
        logger.info(
          s"Table ${targetTableId.getDataset}.${targetTableId.getTable} was not created / retrieved."
        )
        Utils.logException(logger, exception)
        tryResult
      case Success(_) => tryResult
    }
  }

  protected def setTagsOnTable(table: Table): Unit = {
    cliConfig.starlakeSchema.foreach { schema =>
      BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
        val tags = Utils.extractTags(schema.tags)
        val tableTagPairs = tags map { case (k, v) => (k.toLowerCase, v.toLowerCase) }
        if (table.getLabels.asScala.toSet != tableTagPairs) {
          logger.info("Table's tag has changed")
          table.toBuilder.setLabels(tableTagPairs.toMap.asJava).build().update()
        } else {
          logger.info("Table's tag has not changed")
        }
      } match {
        case Failure(exception) => throw exception
        case Success(value)     => // do nothing
      }
    }
  }

  protected def updateTableDescription(bqTable: Table, description: String)(implicit
    settings: Settings
  ): Table = {
    BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
      if (scala.Option(bqTable.getDescription) != scala.Option(description)) {
        // Update dataset description only when description is explicitly set
        logger.info("Table's description has changed")
        bqTable.toBuilder.setDescription(description).build().update()
      } else {
        logger.info("Table's description has not changed")
        bqTable
      }
    } match {
      case Failure(exception) =>
        throw exception
      case Success(table) => table
    }
  }

  private def updateDatasetInfo(
    dataset: Dataset,
    description: scala.Option[String],
    labels: Map[String, String]
  ): Dataset = {
    val alterDescriptionIfChanged = (datasetBuilder: Dataset.Builder, changeState: Boolean) => {
      if (description.isDefined && scala.Option(dataset.getDescription) != description) {
        // Update dataset description only when description is explicitly set
        logger.info("Dataset's description has changed")
        datasetBuilder.setDescription(description.orNull) -> true
      } else
        datasetBuilder -> changeState
    }
    val alterLabelsIfChanged = (datasetBuilder: Dataset.Builder, changeState: Boolean) => {
      val existingLabels =
        scala.Option(dataset.getLabels).map(_.asScala).getOrElse(Map.empty[String, String])
      if (existingLabels.size != labels.size || existingLabels != labels) {
        logger.info("Dataset's labels has changed")
        datasetBuilder.setLabels(labels.asJava) -> true
      } else
        datasetBuilder -> changeState
    }
    val (builder, changed) =
      List(alterDescriptionIfChanged, alterLabelsIfChanged).foldLeft(dataset.toBuilder -> false) {
        case ((builder, state), alterFunc) => alterFunc(builder, state)
      }
    if (changed) {
      logger.info("Updating dataset")
      builder.build().update()
    } else {
      logger.info("No metadata change for dataset")
      dataset
    }
  }

  /** Parses the input sql parameter to find all the referenced tables and finds the description of
    * the columns of these tables based on the description found in the existing BigQuery tables and
    * AttributeDesc comments provided in the Job description. The result is returned as a
    * Map[String,String] with the column name as key, and its description as value.
    * @param sql
    * @return
    *   a Map with all the
    */
  def getFieldsDescriptionSource(
    sql: String
  )(implicit settings: Settings): Map[String, Option[String]] = {
    val tableIds = SQLUtils
      .extractTableNamesUsingRegEx(sql)
      .flatMap(table => {
        val infos = table.split("\\.").toList
        infos match {
          case datasetName :: tableName :: Nil =>
            Some(
              TableId.of(
                BigQueryJobBase
                  .projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase),
                datasetName,
                tableName
              )
            )
          case projectId :: datasetName :: tableName :: Nil =>
            Some(TableId.of(projectId, datasetName, tableName))
          case _ => None
        }
      })

    tableIds
      .flatMap(tableId =>
        Try(bigquery(accessToken = cliConfig.accessToken).getTable(tableId))
          .map { table =>
            logger.info(s"Get table source description field : ${table.getTableId.toString}")
            table
              .getDefinition[StandardTableDefinition]
              .getSchema
              .getFields
              .iterator()
              .asScala
              .toList
          }
          .getOrElse(Nil)
      )

      /** We expect that two tables with the same column name have exactly the same comment or else
        * we pick anyone by default
        */
      .map(field => field.getName -> Option(field.getDescription))
      .toMap ++
    cliConfig.attributesDesc
      .filter(_.comment.nonEmpty)
      .map(att => att.name -> att.comment)
      .toMap
  }

  /** Update columns description from source tables in sql or from yaml job with attributes
    * information
    *
    * @param dictField
    *   Map of columns and their descriptions
    * @return
    *   Table with columns description updated
    */
  def updateColumnsDescription(
    schema: BQSchema
  )(implicit settings: Settings): Table = {
    BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries) {
      val tableTarget = bigquery(accessToken = cliConfig.accessToken).getTable(tableId)
      val tableSchema = tableTarget.getDefinition.asInstanceOf[StandardTableDefinition].getSchema
      def buildSchema(
        originalFields: FieldList,
        incomingSchema: FieldList
      ): (List[Field], Boolean) = {
        originalFields
          .iterator()
          .asScala
          .toList
          .foldLeft(List[Field]() -> false) { case ((fields, changed), field) =>
            val incomingField = Try(incomingSchema.get(field.getName))
            val targetDescription = incomingField
              .map(_.getDescription)
              .getOrElse(field.getDescription)
            val fieldDescriptionHasChange =
              scala.Option(targetDescription) != scala.Option(field.getDescription)

            val subFieldsUpdatedDescription: scala.Option[(List[Field], Boolean)] =
              incomingField.map(f => Option(f.getSubFields)).getOrElse(None) match {
                case Some(subFields) =>
                  scala.Option(field.getSubFields).map(buildSchema(_, subFields))
                case None => None
              }
            val fieldBuilder = field.toBuilder
              .setDescription(targetDescription)
            subFieldsUpdatedDescription.foreach { case (subFields, _) =>
              fieldBuilder.setType(field.getType, FieldList.of(subFields.asJava))
            }
            (fields :+ fieldBuilder
              .build()) -> (changed || subFieldsUpdatedDescription
              .map { case (_, subfieldHasChanged) =>
                subfieldHasChanged || fieldDescriptionHasChange
              }
              .getOrElse(fieldDescriptionHasChange))
          }
      }
      val (fieldList, descriptionChanged) = buildSchema(tableSchema.getFields, schema.getFields)
      if (descriptionChanged) {
        logger.info(s"$bqTable's column description has changed")
        bigquery(accessToken = cliConfig.accessToken).update(
          tableTarget.toBuilder
            .setDefinition(StandardTableDefinition.of(BQSchema.of(fieldList.asJava)))
            .build()
        )
      } else {
        logger.info(s"$bqTable's column description has not changed")
        tableTarget
      }
    } match {
      case Failure(exception) =>
        throw exception
      case Success(table) => table
    }
  }

  /** To set access control on a table or view, we can use Identity and Access Management (IAM)
    * policy After you create a table or view, you can set its policy with a set-iam-policy call For
    * each call, we compare if the existing policy is equal to the defined one (in the Yaml file) If
    * it's the case, we do nothing, otherwise we update the Table policy
    * @param tableId
    * @param acl
    * @return
    */
  private def applyACL(
    tableId: TableId,
    acl: List[AccessControlEntry]
  )(implicit settings: Settings): Policy = {
    // val BIG_QUERY_VIEWER_ROLE = "roles/bigquery.dataViewer"
    val existingPolicy: Policy = bigquery(accessToken = cliConfig.accessToken).getIamPolicy(tableId)
    val existingPolicyBindings: util.Map[Role, util.Set[Identity]] = existingPolicy.getBindings

    val bindings = acl
      .groupBy(_.role)
      .map { case (role, listAcl) =>
        Role.of(role) -> listAcl.flatMap(_.grants).toSet.map(Identity.valueOf).asJava
      }
      .asJava

    if (!existingPolicyBindings.equals(bindings)) {
      logger.info(
        s"We are updating the IAM Policy on this Table: $tableId with new Policies"
      )
      val editedPolicy: Policy = existingPolicy.toBuilder
        .setBindings(
          bindings
        )
        .build()
      bigquery(accessToken = cliConfig.accessToken).setIamPolicy(tableId, editedPolicy)
      editedPolicy
    } else {
      logger.info(s"Iam Policy is the same as before on this Table: $tableId")
      existingPolicy
    }
  }

  private def newTableDefinition(
    tableInfo: model.TableInfo,
    dataFrame: scala.Option[DataFrame]
  )(implicit settings: Settings): TableDefinition = {
    val maybeTimePartitioning = tableInfo.maybePartition
      .map(partitionInfo =>
        timePartitioning(
          partitionInfo.field,
          partitionInfo.expirationDays,
          partitionInfo.requirePartitionFilter
        ).build()
      )
    val withPartitionDefinition = tableInfo.maybeSchema match {
      case Some(schema) =>
        val tableConstraints: TableConstraints = getTableConstraints()
        StandardTableDefinition
          .newBuilder()
          .setSchema(schema)
          .setTableConstraints(tableConstraints)
          .setTimePartitioning(maybeTimePartitioning.orNull)
      case None =>
        // We would have loved to let BQ do the whole job (StandardTableDefinition.newBuilder())
        // But however seems like it does not work when there is an output partition
        val tableDefinition =
          StandardTableDefinition
            .newBuilder()
            .setTimePartitioning(maybeTimePartitioning.orNull)
        dataFrame
          .map(dataFrame => tableDefinition.setSchema(sparkToBq(dataFrame)))
          .getOrElse(tableDefinition)
    }
    val withClusteringDefinition = tableInfo.maybeCluster match {
      case Some(clusterConfig) =>
        val clustering = Clustering.newBuilder().setFields(clusterConfig.fields.asJava).build()
        withPartitionDefinition.setClustering(clustering)
        withPartitionDefinition
      case None => withPartitionDefinition
    }
    withClusteringDefinition.build()
  }

  private def getTableConstraints()(implicit settings: Settings): TableConstraints = {
    val tableConstraints = cliConfig.starlakeSchema match {
      case Some(starlakeSchema) =>
        val pkTableConstraints = if (starlakeSchema.primaryKey.nonEmpty) {
          val primaryKey =
            PrimaryKey.newBuilder().setColumns(starlakeSchema.primaryKey.asJava).build()
          TableConstraints.newBuilder().setPrimaryKey(primaryKey);
        } else {
          TableConstraints.newBuilder();
        }
        val fkComponents = starlakeSchema.attributes.flatMap { attr =>
          starlakeSchema.fkComponents(attr, datasetId.getDataset)
        }
        val foreignKeys = fkComponents.flatMap { case (attr, domain, table, referencedColumn) =>
          if (datasetId.getDataset.equalsIgnoreCase(domain)) {
            val tableIdPk =
              TableId.of(
                cliConfig.outputDatabase.getOrElse(
                  BigQueryJobBase
                    .projectId(connectionOptions.get("projectId"), cliConfig.outputDatabase)
                ),
                domain,
                table
              )
            val referencedTable =
              bigquery(accessToken = cliConfig.accessToken).getTable(tableIdPk)
            if (referencedTable == null || !referencedTable.exists()) {
              logger.warn(
                s"Foreign key constraint on ${datasetId.getDataset}.${tableId.getTable}.${attr.getFinalName()} " +
                s"referencing $domain.$table.$referencedColumn not added because the referenced table does not exist"
              )
              None
            } else {
              logger.info(
                s"Adding foreign key constraint on ${datasetId.getDataset}.${tableId.getTable}.${attr
                    .getFinalName()} referencing $domain.$table.$referencedColumn"
              )
              val columnReference =
                ColumnReference.newBuilder
                  .setReferencingColumn(attr.getFinalName())
                  .setReferencedColumn(referencedColumn)
                  .build

              val fk = ForeignKey.newBuilder
                .setName(
                  s"FK_${datasetId.getDataset.toUpperCase()}_${tableId.getTable().toUpperCase()}_${attr.getFinalName().toUpperCase()}"
                )
                .setColumnReferences(List(columnReference).asJava)
                .setReferencedTable(tableIdPk)
                .build
              Some(fk)
            }
          } else {
            logger.warn(s"""Foreign key constraint
                 |${datasetId.getDataset}.${tableId.getTable}.${attr.getFinalName()}
                 |referencing
                 |$domain.$table.$referencedColumn
                 |not added because the referenced table is not in the same dataset""".stripMargin)
            None
          }
        }
        pkTableConstraints.setForeignKeys(foreignKeys.asJava)
      case None =>
        TableConstraints.newBuilder();
    }
    tableConstraints.build()
  }

  def timePartitioning(
    partitionField: String,
    days: scala.Option[Int] = None,
    requirePartitionFilter: Boolean
  ): TimePartitioning.Builder = {
    val partitionFilter = TimePartitioning
      .newBuilder(TimePartitioning.Type.DAY)
      .setRequirePartitionFilter(requirePartitionFilter)
    val partitioned =
      if (!Set("_PARTITIONTIME", "_PARTITIONDATE").contains(partitionField.toUpperCase))
        partitionFilter.setField(partitionField)
      else
        partitionFilter

    days match {
      case Some(d) =>
        partitioned.setExpirationMs(d * 24 * 3600 * 1000L)
      case _ =>
        partitioned
    }
  }

  def getTableDefinition(tableId: TableId)(implicit settings: Settings): StandardTableDefinition = {
    bigquery(accessToken = cliConfig.accessToken)
      .getTable(tableId)
      .getDefinition[StandardTableDefinition]
  }
}

object BigQueryJobBase extends LazyLogging {
  def dictToBQSchema(dictField: Map[String, Option[String]]): BQSchema = {
    // we don't know the type of field so we put string by default
    BQSchema.of(
      dictField
        .map { case (fieldName, description) =>
          Field
            .newBuilder(fieldName, StandardSQLTypeName.STRING)
            .setDescription(description.orNull)
            .build()
        }
        .toList
        .asJava
    )
  }

  private def getBqDatasetId(tableId: TableId): DatasetId = {
    val projectId = getProjectIdPrefix(scala.Option(tableId.getProject))
    scala.Option(projectId) match {
      case Some(_) => DatasetId.of(tableId.getProject, tableId.getDataset)
      case None    => DatasetId.of(tableId.getDataset)
    }
  }

  def getBqTableForSpark(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(scala.Option(tableId.getProject))
    projectId match {
      case None            => s"${tableId.getDataset}.${tableId.getTable}"
      case Some(projectId) => s"${projectId}:${tableId.getDataset}.${tableId.getTable}"
    }
  }

  def getBqTableForNative(tableId: TableId): String = {
    val projectId = getProjectIdPrefix(scala.Option(tableId.getProject))
    projectId match {
      case None            => s"${tableId.getDataset}.${tableId.getTable}"
      case Some(projectId) => s"`${projectId}.${tableId.getDataset}.${tableId.getTable}`"
    }
  }

  def extractProjectDatasetAndTable(
    databaseId: scala.Option[String],
    datasetId: String,
    tableId: String,
    prjId: Option[String]
  ): TableId = {
    databaseId.filter(_.nonEmpty) match {
      case Some(dbId) =>
        extractProjectDatasetAndTable(dbId + ":" + datasetId + "." + tableId, prjId)
      case None =>
        extractProjectDatasetAndTable(datasetId + "." + tableId, prjId)
    }
  }

  /** @param resourceId
    *   resourceId is a an id of one of the following pattern: <projectId>:<datasetId>.<tableId> or
    *   <datasetId>.<tableId>
    * @return
    */
  def extractProjectDatasetAndTable(resourceId: String, prjId: Option[String]): TableId = {
    def extractDatasetAndTable(str: String): (String, String) = {
      val sepIndex = str.indexOf('.')
      if (sepIndex > 0)
        (str.substring(0, sepIndex), str.substring(sepIndex + 1))
      else
        throw new Exception(s"Dataset cannot be null in BigQuery view name ($resourceId)")
    }

    val sepIndex = resourceId.indexOf(":")
    val (project, (dataset, table)) =
      if (sepIndex > 0)
        (
          Some(resourceId.substring(0, sepIndex)),
          extractDatasetAndTable(resourceId.substring(sepIndex + 1))
        )
      else // parquet is the default
        (None, extractDatasetAndTable(resourceId))

    project
      .map(project => TableId.of(project, dataset, table))
      .getOrElse(TableId.of(projectId(prjId, None), dataset, table))
  }

  def extractProjectDataset(value: String, prjId: Option[String]): DatasetId = {
    val sepIndex = value.indexOf(":")
    val (project, dataset) =
      if (sepIndex > 0)
        (Some(value.substring(0, sepIndex)), value.substring(sepIndex + 1))
      else // parquet is the default
        (None, value)

    project
      .map(project => DatasetId.of(project, dataset))
      .getOrElse(DatasetId.of(projectId(prjId, None), dataset))
  }

  private def getProjectIdPrefix(
    projectId: scala.Option[String]
  ): scala.Option[String] =
    projectId.filter(_.trim.nonEmpty)

  def projectId(
    connectionProjectId: Option[String],
    outputDatabase: scala.Option[String]
  ): String = {
    val projectId =
      outputDatabase
        .filter(_.nonEmpty)
        .orElse(connectionProjectId)
        .orElse(getPropertyOrEnv("SL_DATABASE"))
        .orElse(scala.Option(ServiceOptions.getDefaultProjectId))
        .getOrElse(throw new StarlakeConfigException("""GCP Project ID must be defined in one of the following ways:
                            |  - Set the environment variable GOOGLE_CLOUD_PROJECT
                            |  - Use the gcloud command `gcloud config set project YOUR_PROJECT_ID`
                            |  - Use the `database:YOUR_PROJECT_ID` attribute in your metadata/application.sl.yml
                            |""".stripMargin))
    logger.info(s"Using project $projectId")
    projectId
  }

  private def getPropertyOrEnv(envVar: String): scala.Option[String] =
    scala.Option(System.getProperty(envVar, System.getenv(envVar)))

  @nowarn
  def gcsCredentials(
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): GcsCredentials = {
    logger.info(s"Using ${connectionOptions("authType")} Credentials from GCS")
    connectionOptions("authType") match {
      case "APPLICATION_DEFAULT" =>
        val scopes = connectionOptions
          .getOrElse("authScopes", "https://www.googleapis.com/auth/cloud-platform")
          .split(',')
        val cred = GcsGoogleCredentials
          .getApplicationDefault()
          .createScoped(scopes: _*)
        cred.refresh()
        cred
      case "SERVICE_ACCOUNT_JSON_KEYFILE" =>
        val credentialsStream = getJsonKeyStream(connectionOptions)
        GcsServiceAccountCredentials.fromStream(credentialsStream)
      case "SERVICE_ACCOUNT_JSON_KEY_BASE64" =>
        val credentialsStream = BigQueryJobBase.getJsonKeyStreamFromBase64(connectionOptions)
        GcsServiceAccountCredentials.fromStream(credentialsStream)
      case "USER_CREDENTIALS" =>
        val clientId = connectionOptions("clientId")
        val clientSecret = connectionOptions("clientSecret")
        val refreshToken = connectionOptions("refreshToken")
        GcsUserCredentials
          .newBuilder()
          .setClientId(clientId)
          .setClientSecret(clientSecret)
          .setRefreshToken(refreshToken)
          .build()
      case "ACCESS_TOKEN" =>
        val accessToken = connectionOptions("gcpAccessToken")
        GcsGoogleCredentials.create(
          new com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.AccessToken(
            accessToken,
            null
          )
        )
    }
  }

  def getJsonKeyStream(
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): ByteArrayInputStream = {
    val gcpSAJsonKeyAsString: String = BigQueryJobBase.getJsonKeyContent(connectionOptions)
    val credentialsStream = new ByteArrayInputStream(
      gcpSAJsonKeyAsString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name)
    )
    credentialsStream
  }

  def getJsonKeyStreamFromBase64(connectionOptions: Map[String, String]): ByteArrayInputStream = {
    val gcpSAJsonKey = connectionOptions("jsonKeyBase64")
    val decoded = com.google.common.io.BaseEncoding.base64.decode(gcpSAJsonKey)
    val credentialsStream = new ByteArrayInputStream(decoded)
    credentialsStream
  }

  def getJsonKeyContent(
    connectionOptions: Map[String, String]
  )(implicit settings: Settings): String = {
    val gcpSAJsonKey = connectionOptions("jsonKeyfile")
    val path: File = getJsonKeyAbsoluteFile(gcpSAJsonKey)

    getJsonKeyContent(path)
  }

  def getJsonKeyAbsoluteFile(gcpSAJsonKey: String)(implicit settings: Settings): File = {
    val path =
      if (gcpSAJsonKey.contains("/") || gcpSAJsonKey.contains("\\"))
        File(gcpSAJsonKey)
      else
        File(StorageHandler.localFile(DatasetArea.keys), gcpSAJsonKey)
    path
  }

  def getJsonKeyContent(path: File): String = {
    val gcpSAJsonKeyAsString = if (path.exists()) {
      path.contentAsString
    } else {
      throw new Exception(s"Invalid GCP SA KEY Path: $path")
    }
    gcpSAJsonKeyAsString
  }

  def bigquery(
    connectionRef: scala.Option[String],
    accessToken: scala.Option[String],
    outputDatabase: scala.Option[String]
  )(implicit
    settings: Settings
  ): BigQuery = {
    val connectionName: scala.Option[String] = connectionRef
      .orElse(Some(settings.appConfig.connectionRef))

    val connection: scala.Option[Settings.ConnectionInfo] =
      connectionName.flatMap(name => settings.appConfig.connections.get(name))

    val connectionOptions: Map[String, String] =
      connection.map(_.options).getOrElse(Map.empty)

    logger.info(s"Getting BQ credentials for connection $connectionName")
    logger.debug(s"$connectionName")

    val bqOptionsBuilder = BigQueryOptions
      .newBuilder()
      .setLocation(
        connectionOptions.getOrElse(
          "location",
          throw new Exception(
            s"location is required but not present in connection $connectionName"
          )
        )
      )

    val credentials = GcpCredentials.credentials(connectionOptions, accessToken)
    val project = projectId(connectionOptions.get("projectId"), outputDatabase)
    val bqOptions = bqOptionsBuilder.setProjectId(project)
    val bqService =
      credentials match {
        case None =>
          bqOptions.build().getService()
        case Some(credentials) =>
          bqOptions.setCredentials(credentials).build().getService()
      }
    bqService
  }

  def executeUpdate(
    sql: String,
    connectionRef: String,
    accessToken: scala.Option[String]
  )(implicit
    settings: Settings
  ): Try[Boolean] = Try {
    val bq = bigquery(
      connectionRef = Some(connectionRef),
      accessToken = accessToken,
      outputDatabase = None
    )
    val _ = bq.query(QueryJobConfiguration.of(sql))
    true
  }

  def isRetryableException(ex: Throwable): Boolean = {
    if (ex == null) false
    else
      ex match {
        case be: GoogleJsonResponseException if be.getStatusCode >= 500 =>
          true
        case _: PollException =>
          true
        case be: BigQueryException
            if scala
              .Option(be.getError)
              .exists(e =>
                e.getReason() == "rateLimitExceeded" || e.getReason == "jobRateLimitExceeded"
              ) =>
          true
        case be: BigQueryException
            if scala.Option(be.getError).exists(_.getReason() == "duplicate") =>
          true
        case te: TimeoutException =>
          true
        case _ => isRetryableException(ex.getCause)
      }
  }

  /** Retry on retryable bigquery exception.
    */
  def recoverBigqueryException[T](maxAttempts: Int)(bigqueryProcess: => T): Try[T] = {
    def processWithRetry(retry: Int = 0, bigqueryProcess: => T): Try[T] = {
      def retryOneMoreTime(be: Throwable) = {
        val sleepTime = 5000 * (retry + 1) + SecureRandom.getInstanceStrong.nextInt(5000)
        logger.error(s"Retry in $sleepTime. ${be.getMessage}")
        Thread.sleep(sleepTime)
        processWithRetry(retry + 1, bigqueryProcess)
      }
      Try {
        bigqueryProcess
      }.recoverWith {
        case ex: Throwable if retry < maxAttempts && isRetryableException(ex) =>
          retryOneMoreTime(ex)
        case ex: Throwable if retry >= maxAttempts && isRetryableException(ex) =>
          logger.error(s"Failed to recover from exception after $maxAttempts attempts", ex)
          Failure(ex)
        case ex: Throwable =>
          logger.error(s"Could not recover from exception", ex)
          Failure(ex)
      }
    }
    processWithRetry(bigqueryProcess = bigqueryProcess)
  }

  def buildACLQueries(
    tableId: TableId,
    acl: List[AccessControlEntry]
  )(implicit settings: Settings): List[String] = {
    val fullTableName = getBqTableForNative(tableId)
    acl.flatMap { ace =>
      /*
        https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
        https://hevodata.com/learn/snowflake-grant-role-to-user/
       */
      ace.asSql(fullTableName, Engine.BQ)
    }
  }

  def buildRLSQueries(outputTableId: TableId, rls: List[RowLevelSecurity]): List[String] = {
    val outputTable = BigQueryJobBase.getBqTableForNative(outputTableId)
    buildRLSQueries(outputTable, rls)
  }
  def buildRLSQueries(outputTable: String, rls: List[RowLevelSecurity]): List[String] = {
    def revokeAllPrivileges(): String = {
      s"DROP ALL ROW ACCESS POLICIES ON $outputTable"
    }

    /** Grant privileges to the users and groups defined in the schema
      * @param rlsRetrieved
      * @return
      */
    def grantPrivileges(rlsRetrieved: RowLevelSecurity): String = {
      val grants = rlsRetrieved.grantees().map {
        case (UserType.SA, u) =>
          s"serviceAccount:$u"
        case (userOrGroupOrDomainType, userOrGroupOrDomainName) =>
          s"${userOrGroupOrDomainType.toString.toLowerCase}:$userOrGroupOrDomainName"
      }

      val name = rlsRetrieved.name
      val filter = rlsRetrieved.predicate
      s"""
             | CREATE ROW ACCESS POLICY
             |  $name
             | ON
             |  $outputTable
             | GRANT TO
             |  (${grants.mkString("\"", "\",\"", "\"")})
             | FILTER USING
             |  ($filter)
             |""".stripMargin
    }
    val rlsCreateStatements = rls.map { rlsRetrieved =>
      logger.info(s"Building security statement $rlsRetrieved")
      val rlsCreateStatement = grantPrivileges(rlsRetrieved)
      logger.info(s"An access policy will be created using $rlsCreateStatement")
      rlsCreateStatement
    }

    val rlsDeleteStatement = rls.map(_ => revokeAllPrivileges())

    rlsDeleteStatement ++ rlsCreateStatements
  }

}
