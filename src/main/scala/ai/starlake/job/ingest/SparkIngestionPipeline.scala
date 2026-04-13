package ai.starlake.job.ingest

import ai.starlake.config.{CometColumns, SparkSessionBuilder}
import ai.starlake.job.transform.TransformContext
import ai.starlake.job.validator.{CheckValidityResult, SimpleRejectedRecord}
import ai.starlake.schema.model.*
import ai.starlake.utils.*
import ai.starlake.utils.Formatter.*
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Timestamp
import java.time.Instant
import scala.util.{Failure, Success, Try}

/** Spark-specific DataFrame pipeline for ingestion jobs.
  *
  * Contains all methods that operate on Spark DataFrames: validation, transformation, renaming,
  * scripted/computed columns, filtering, sinking, and the ingestWithSpark entry point.
  */
trait SparkIngestionPipeline { self: IngestionJob =>

  /** Apply the schema to the dataset. Valid records are stored in the accepted path/table and
    * invalid records in the rejected path/table.
    */
  protected def ingest(dataset: DataFrame): (Dataset[SimpleRejectedRecord], Dataset[Row]) = {
    // Materialize the input dataset with localCheckpoint so the source file is read exactly once.
    // A lazy persist() is not enough: Spark's optimizer creates separate physical plans for
    // the rejected/accepted/count branches that bypass the cache and re-read from disk.
    // localCheckpoint() forces a single read and truncates lineage, preventing any re-scan.
    // If an executor fails, the truncated lineage means data cannot be recomputed in memory,
    // but the ingestion job will fail and can be safely retried since the source file persists.
    val cachedDataset = dataset.localCheckpoint()
    logger.info("Locally checkpointed")
    val validationResult = rowValidator.validate(
      session,
      mergedMetadata.resolveFormat(),
      mergedMetadata.resolveSeparator(),
      cachedDataset,
      schema.attributesWithoutScriptedFieldsWithInputFileName,
      types,
      schema.sourceSparkSchemaWithoutScriptedFieldsWithInputFileName(schemaHandler),
      settings.appConfig.privacy.options,
      settings.appConfig.cacheStorageLevel,
      settings.appConfig.sinkReplayToFile,
      mergedMetadata.emptyIsNull.getOrElse(settings.appConfig.emptyIsNull),
      settings.appConfig.rejectWithValue
    )(schemaHandler)

    saveRejected(
      validationResult.errors,
      validationResult.rejected.drop(CometColumns.cometInputFileNameColumn)
    )(
      settings,
      storageHandler,
      schemaHandler
    ).flatMap { _ =>
      saveAccepted(validationResult) // prefer to let Spark compute the final schema
    } match {
      case Failure(exception) =>
        cachedDataset.unpersist()
        throw exception
      case Success(_) =>
        cachedDataset.unpersist()
        (validationResult.errors, validationResult.accepted)
    }
  }

  protected def reorderTypes(orderedAttributes: List[TableAttribute]): (List[Type], StructType) = {
    val typeMap: Map[String, Type] = types.map(tpe => tpe.name -> tpe).toMap
    val (tpes, sparkFields) = orderedAttributes.map { attribute =>
      val tpe = typeMap(attribute.`type`)
      (tpe, tpe.sparkType(attribute.name, !attribute.resolveRequired(), attribute.comment))
    }.unzip
    (tpes, StructType(sparkFields))
  }

  protected def intersectHeaders(
    datasetHeaders: List[String],
    schemaHeaders: List[String]
  ): (List[String], List[String]) = {
    datasetHeaders.partition(schemaHeaders.contains)
  }

  def reorderAttributes(dataFrame: DataFrame): List[TableAttribute] = {
    val finalSchema = schema.attributesWithoutScriptedFields :+ TableAttribute(
      name = CometColumns.cometInputFileNameColumn
    )
    val attributesMap =
      finalSchema.map(attr => (attr.name, attr)).toMap
    dataFrame.columns.map(colName => attributesMap(colName)).toList
  }

  // ---- Spark ingestion entry point ----

  def ingestWithSpark(): Try[List[IngestionCounters]] = {
    if (!SparkSessionBuilder.isSparkConnectActive) {
      session.sparkContext.setLocalProperty(
        "spark.scheduler.pool",
        settings.appConfig.sparkScheduling.poolName
      )
    }
    val jobResult = {
      val start = Timestamp.from(Instant.now())
      val dataset = loadDataSet()
      dataset match {
        case Success(dataset) =>
          Try {
            val (rejectedDS, acceptedDS) = ingest(dataset)
            if (settings.appConfig.audit.detailedLoadAudit && path.size > 1) {
              collectDetailedCounters(rejectedDS, acceptedDS)
            } else {
              val totalAcceptedCount = acceptedDS.count()
              val totalRejectedCount = rejectedDS.count()
              List(
                IngestionCounters(
                  inputCount = totalAcceptedCount + totalRejectedCount,
                  acceptedCount = totalAcceptedCount,
                  rejectedCount = totalRejectedCount,
                  paths = path.map(_.toString),
                  jobid = applicationId()
                )
              )
            }
          }
        case Failure(exception) =>
          logLoadFailureInAudit(start, exception)
      }
    }
    // After each ingestion job we explicitly clear the spark cache
    if (!SparkSessionBuilder.isSparkConnectActive) {
      session.catalog.clearCache()
    }
    jobResult
  }

  private def collectDetailedCounters(
    rejectedDS: Dataset[SimpleRejectedRecord],
    acceptedDS: Dataset[Row]
  ): List[IngestionCounters] = {
    // Use Row-based collection to avoid Encoder issues with Spark Connect / Databricks Connect
    val statsDF = rejectedDS
      .groupBy("path")
      .count()
      .withColumnRenamed("count", "rejectedCount")
      .join(
        acceptedDS
          .groupBy(col(CometColumns.cometInputFileNameColumn).as("path"))
          .count()
          .withColumnRenamed("count", "acceptedCount"),
        "path",
        "full_outer"
      )
      .select(
        col("path"),
        coalesce(col("rejectedCount"), lit(0L)).as("rejectedCount"),
        coalesce(col("acceptedCount"), lit(0L)).as("acceptedCount")
      )
    statsDF
      .collect()
      .map { row =>
        val rejectedCount = row.getAs[Long]("rejectedCount")
        val acceptedCount = row.getAs[Long]("acceptedCount")
        IngestionCounters(
          inputCount = rejectedCount + acceptedCount,
          acceptedCount = acceptedCount,
          rejectedCount = rejectedCount,
          paths = List(row.getAs[String]("path")),
          jobid = applicationId()
        )
      }
      .toList
  }

  // ---- DataFrame transformation pipeline ----

  protected def saveAccepted(
    validationResult: CheckValidityResult
  ): Try[Long] = {
    if (!settings.appConfig.rejectAllOnError || validationResult.rejected.isEmpty) {
      logger.whenDebugEnabled {
        logger.debug(s"accepted SIZE ${validationResult.accepted.count()}")
        logger.debug(validationResult.accepted.showString(1000))
      }

      val finalAcceptedDF = computeFinalDF(validationResult.accepted)
      try {
        sinkAccepted(finalAcceptedDF)
          .map { rejectedRecordCount =>
            runMetrics(finalAcceptedDF)
            rejectedRecordCount
          }
      } finally {
        finalAcceptedDF.unpersist()
      }
    } else {
      Success(0)
    }
  }

  private def computeFinalDF(accepted: DataFrame): DataFrame = {
    val acceptedRenamedFields = dfWithAttributesRenamed(accepted)

    val acceptedDfWithScriptFields: DataFrame = computeScriptedAttributes(
      acceptedRenamedFields
    )

    val acceptedDfWithScriptAndTransformedFields: DataFrame = computeTransformedAttributes(
      acceptedDfWithScriptFields
    )

    val acceptedDfFiltered = filterData(acceptedDfWithScriptAndTransformedFields)

    val acceptedDfWithoutIgnoredFields: DataFrame = removeIgnoredAttributes(
      acceptedDfFiltered
    )

    val acceptedDF = acceptedDfWithoutIgnoredFields.drop(CometColumns.cometInputFileNameColumn)
    computeFinalSchema(acceptedDF).persist(settings.appConfig.cacheStorageLevel)
  }

  private def filterData(df: DataFrame): Dataset[Row] = {
    if (mergedMetadata.resolveFormat() == Format.POSITION)
      df // filtering is done at read time for position format
    else
      schema.filter
        .map { filterExpr =>
          logger.info(s"Applying data filter: $filterExpr")
          df.filter(filterExpr)
        }
        .getOrElse(df)
  }

  private def computeFinalSchema(df: DataFrame): DataFrame = {
    if (schema.attributes.exists(_.script.isDefined)) {
      logger.whenDebugEnabled {
        logger.debug("Accepted Dataframe schema right after adding computed columns")
        logger.debug(df.schemaString())
      }
      // adding computed columns can change the order of columns, we must force the order defined in the schema
      val cols = schema.finalAttributeNames().map(col)
      val orderedWithScriptFieldsDF = df.select(cols: _*)
      logger.whenDebugEnabled {
        logger.debug("Accepted Dataframe schema after applying the defined schema")
        logger.debug(orderedWithScriptFieldsDF.schemaString())
      }
      orderedWithScriptFieldsDF
    } else {
      df
    }
  }

  private def removeIgnoredAttributes(df: DataFrame): DataFrame = {
    val ignoredAttributes = schema.attributes.filter(_.resolveIgnore()).map(_.getFinalName())
    df.drop(ignoredAttributes: _*)
  }

  private def computeTransformedAttributes(df: DataFrame): DataFrame = {
    val sqlAttributes =
      schema.attributes.filter(_.resolvePrivacy().sql).filter(_.transform.isDefined)
    sqlAttributes.foldLeft(df) { case (acc, attr) =>
      acc.withColumn(
        attr.getFinalName(),
        expr(
          attr.transform
            .getOrElse(throw new IllegalStateException("Should never happen"))
            .richFormat(schemaHandler.activeEnvVars(), options)
        )
          .cast(attr.primitiveSparkType(schemaHandler))
          .as(attr.getFinalName(), metadata = buildColumnMetadata(attr))
      )
    }
  }

  private def computeScriptedAttributes(acceptedDF: DataFrame): DataFrame = {
    def enrichStructField(attr: TableAttribute, structField: StructField) = {
      structField.copy(
        name = attr.getFinalName(),
        nullable = if (attr.script.isDefined) true else !attr.resolveRequired()
      )
    }
    val scripts = schema.attributes
      .filter(_.script.isDefined)
      .map(attr =>
        (
          attr.getFinalName(),
          attr.sparkType(schemaHandler, enrichStructField),
          attr.resolveScript(),
          buildColumnMetadata(attr)
        )
      )
    // If any script references _metadata, reconstruct it as a real column from
    // sl_input_file_name and source file info. _metadata is a Spark virtual column
    // that is lost after localCheckpoint or column projections.
    val needsMetadata = scripts.exists { case (_, _, script, _) =>
      script.contains("_metadata")
    }
    val dfWithMetadata =
      if (needsMetadata && acceptedDF.columns.contains(CometColumns.cometInputFileNameColumn)) {
        val inputFileCol = col(CometColumns.cometInputFileNameColumn)
        acceptedDF.withColumn(
          "_metadata",
          struct(
            element_at(split(inputFileCol, "/"), -1).as("file_name"),
            inputFileCol.as("file_path"),
            lit(path.headOption.map(p => storageHandler.stat(p).fileSizeInBytes).getOrElse(0L))
              .as("file_size"),
            lit(
              path.headOption
                .map(p => Timestamp.from(storageHandler.stat(p).modificationInstant))
                .orNull
            ).as("file_modification_time")
          )
        )
      } else {
        acceptedDF
      }
    scripts
      .foldLeft(dfWithMetadata) { case (df, (name, sparkType, script, metadata)) =>
        df.withColumn(
          name,
          expr(script.richFormat(schemaHandler.activeEnvVars(), options))
            .cast(sparkType)
            .as(name, metadata = metadata)
        )
      }
  }

  private def sinkAccepted(mergedDF: DataFrame): Try[Long] = {
    val result: Try[Try[Long]] = Try {
      val taskDesc = AutoTaskInfo(
        name = schema.finalName,
        presql = schema.presql,
        postsql = schema.postsql,
        sql = None,
        database = schemaHandler.getDatabase(domain),
        domain = domain.finalName,
        table = schema.finalName,
        sink = mergedMetadata.sink,
        acl = schema.acl,
        rls = schema.rls,
        comment = schema.comment,
        tags = schema.tags,
        writeStrategy = Some(strategy),
        connectionRef = Option(mergedMetadata.getSinkConnectionRef())
      )
      val context = TransformContext(
        appId = Option(applicationId()),
        taskDesc = taskDesc,
        commandParameters = Map.empty,
        interactive = None,
        truncate = false,
        test = test,
        logExecution = false,
        accessToken = accessToken,
        resultPageSize = 200,
        resultPageNumber = 1,
        dryRun = false,
        scheduledDate = scheduledDate,
        syncSchema = false
      )(settings, storageHandler, schemaHandler)

      val autoTask =
        taskDesc.getSinkConfig() match {
          case fsSink: FsSink if fsSink.isExport() && !strategy.isMerge() =>
            TransformContext.createSparkExportTask(context)
          case _ =>
            TransformContext.createSparkTask(context, Some(schema))
        }
      if (autoTask.sink(mergedDF, Some(this.schema))) {
        Success(0L)
      } else {
        Failure(new Exception("Failed to sink"))
      }
    }
    result.flatten
  }

  // ---- Attribute renaming ----

  private def buildColumnMetadata(attr: TableAttribute): org.apache.spark.sql.types.Metadata = {
    val builder = new org.apache.spark.sql.types.MetadataBuilder()
    if (attr.`type` == "variant") {
      builder.putString("sqlType", "JSON")
    }
    attr.comment.foreach(builder.putString("description", _))
    builder.build()
  }

  def renameAttributesWithReport(
    dfSchema: List[TableAttribute],
    slSchema: List[TableAttribute]
  )(inputDF: DataFrame): (DataFrame, List[String]) = {

    val renamed = scala.collection.mutable.ListBuffer[String]()

    def normalizePath(path: String, isArray: Boolean): String =
      if (isArray) {
        if (path.endsWith("[]")) path else path + "[]"
      } else path

    def collectRename(
      dfAttr: TableAttribute,
      slAttr: TableAttribute,
      normalizedPath: String,
      isArray: Boolean,
      inArrayElement: Boolean
    ): Unit = {
      slAttr.rename.foreach { newName =>
        if (!inArrayElement && newName != dfAttr.name) {
          val newName = normalizePath(slAttr.rename.get, isArray)
          renamed += s"$normalizedPath -> ${newName}"
        }
      }
    }

    def renameField(
      dfAttr: TableAttribute,
      slAttr: TableAttribute,
      path: String,
      inArrayElement: Boolean
    ): Column => Column = {

      val isArray = dfAttr.resolveArray() && slAttr.resolveArray()
      val normalizedPath = normalizePath(path, isArray)

      collectRename(dfAttr, slAttr, normalizedPath, isArray, inArrayElement)

      if (isArray) { (arrayCol: Column) =>
        transform(
          arrayCol,
          elemCol =>
            renameField(
              dfAttr.copy(array = Some(false)),
              slAttr.copy(array = Some(false)),
              normalizedPath,
              inArrayElement = true
            )(elemCol)
        ).as(
          slAttr.rename.getOrElse(slAttr.name),
          metadata = buildColumnMetadata(slAttr)
        )

      } else if (dfAttr.attributes.nonEmpty && slAttr.attributes.nonEmpty) { (structCol: Column) =>
        struct(dfAttr.attributes.map { nestedDfAttr =>
          slAttr.attributes
            .find(_.name == nestedDfAttr.name)
            .map { nestedSlAttr =>
              val nestedPath = s"$normalizedPath.${nestedDfAttr.name}"
              renameField(nestedDfAttr, nestedSlAttr, nestedPath, inArrayElement = false)(
                structCol(nestedDfAttr.name)
              )
                .as(
                  nestedSlAttr.rename.getOrElse(nestedSlAttr.name),
                  metadata = buildColumnMetadata(slAttr)
                )
            }
            .getOrElse(structCol(nestedDfAttr.name))
        }: _*).as(
          slAttr.rename.getOrElse(slAttr.name),
          metadata = buildColumnMetadata(slAttr)
        )

      } else { (col: Column) =>
        col.as(
          slAttr.rename.getOrElse(slAttr.name),
          metadata = buildColumnMetadata(slAttr)
        )
      }
    }

    val projections: List[Column] = dfSchema.map { dfField =>
      slSchema
        .find(_.name == dfField.name)
        .map { slField =>
          renameField(dfField, slField, dfField.name, inArrayElement = false)(col(dfField.name))
            .as(
              slField.rename.getOrElse(slField.name),
              metadata = buildColumnMetadata(slField)
            )
        }
        .getOrElse(col(dfField.name))
    }

    (inputDF.select(projections: _*), renamed.toList)
  }

  def dfWithAttributesRenamed(acceptedDF: DataFrame): DataFrame = {
    val (resultDF, renamedAttributes) =
      renameAttributesWithReport(Attributes.from(acceptedDF.schema), schema.attributes)(acceptedDF)
    logger.whenInfoEnabled {
      renamedAttributes.foreach { case rename =>
        logger.info(s"renaming column $rename")
      }
    }
    resultDF
  }
}
