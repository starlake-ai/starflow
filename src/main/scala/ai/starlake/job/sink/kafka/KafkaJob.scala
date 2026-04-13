package ai.starlake.job.sink.kafka

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.sink.DataFrameTransform
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.Formatter._
import ai.starlake.utils.kafka.KafkaClient
import ai.starlake.utils.{JobResult, SparkJob, SparkJobResult, StarlakeConfigException, Utils}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.streaming.Trigger

import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaJob(
  kafkaJobConfig: KafkaJobConfig,
  schemaHandler: SchemaHandler
)(implicit val settings: Settings)
    extends SparkJob {
  import settings.storageHandler
  DatasetArea.initMetadata(storageHandler())

  private val topicConfig: Option[Settings.KafkaTopicConfig] =
    kafkaJobConfig.topicConfigName.map(settings.appConfig.kafka.topics)

  private val writeTopicConfig: Option[Settings.KafkaTopicConfig] =
    kafkaJobConfig.writeTopicConfigName.map(settings.appConfig.kafka.topics)

  private val finalWritePath: Option[String] = formatPath(kafkaJobConfig.writePath)

  private val finalLoadPath: Option[String] = formatPath(kafkaJobConfig.path)

  private def formatPath(path: Option[String]): Option[String] = path
    .map(
      _.richFormat(
        schemaHandler.activeEnvVars(),
        Map(
          "config" -> kafkaJobConfig.topicConfigName.getOrElse(""),
          "topic"  -> topicConfig.map(_.topicName).getOrElse("")
        )
      )
    )

  val schemaRegistryUrl: Option[String] =
    settings.appConfig.kafka.serverOptions.get("schema.registry.url")

  val schemaRegistryClient: Option[CachedSchemaRegistryClient] =
    schemaRegistryUrl.map(schemaRegistryUrl =>
      new CachedSchemaRegistryClient(
        schemaRegistryUrl,
        128,
        settings.appConfig.kafka.serverOptions.asJava
      )
    )

  def lookupTopicSchema(topic: String, isKey: Boolean = false): Option[String] = {
    schemaRegistryClient.map(
      _.getLatestSchemaMetadata(topic + (if (isKey) "-key" else "-value")).getSchema
    )
  }

  def avroSchemaToSparkSchema(avroSchema: String): SchemaConverters.SchemaType = {
    val parser = new org.apache.avro.Schema.Parser
    SchemaConverters.toSqlType(parser.parse(avroSchema))
  }

  private val writeOptions = kafkaJobConfig.writeOptions.get("config") match {
    case Some(configValue) if kafkaJobConfig.writeFormat == "kafka" =>
      settings.appConfig.kafka
        .topics(configValue)
        .allAccessOptions() ++ (kafkaJobConfig.writeOptions - "config")
    case Some(configValue) =>
      loadOptionsFromConfig(configValue) ++ (kafkaJobConfig.writeOptions - "config")
    case None =>
      kafkaJobConfig.writeOptions
  }

  private val options = kafkaJobConfig.options.get("config") match {
    case Some(configValue) if kafkaJobConfig.format == "kafka" =>
      settings.appConfig.kafka
        .topics(configValue)
        .allAccessOptions() ++ (kafkaJobConfig.options - "config")
    case Some(configValue) =>
      loadOptionsFromConfig(configValue) ++ (kafkaJobConfig.options - "config")
    case None =>
      kafkaJobConfig.options
  }

  private def loadOptionsFromConfig(configValue: String): Map[String, String] = {
    settings.extraConf
      .getConfig(configValue)
      .entrySet()
      .asScala
      .toVector
      .map(x => (x.getKey, x.getValue.unwrapped().toString))
      .toMap
  }

  private def validateBigQueryConfig(): Unit = {
    if (kafkaJobConfig.writeFormat == "bigquery") {
      require(
        writeOptions.contains("table"),
        "BigQuery sink requires 'table' in write-options (e.g., project:dataset.table)"
      )
      require(
        writeOptions.contains("temporaryGcsBucket"),
        "BigQuery sink requires 'temporaryGcsBucket' in write-options"
      )
    }
    if (kafkaJobConfig.format == "bigquery") {
      require(
        kafkaJobConfig.path.isDefined || options.contains("query"),
        "BigQuery source requires either --path (table reference) or --options query=... (SQL query)"
      )
      require(
        !kafkaJobConfig.streaming,
        "BigQuery cannot be used as a streaming source. Use batch mode."
      )
      if (options.contains("query")) {
        require(
          options.contains("temporaryGcsBucket"),
          "BigQuery query source requires 'temporaryGcsBucket' in options for query materialization"
        )
      }
    }
  }

  def pipeline(): Try[SparkJobResult] = {
    Try {
      validateBigQueryConfig()
      topicConfig match {
        case Some(topicConfig) =>
          if (kafkaJobConfig.streaming) {
            val df = KafkaClient.consumeTopicStreaming(
              session,
              topicConfig
            )
            val transformedDF = transform(df)
            writeStreaming(transformedDF)
            SparkJobResult(None, None)
          } else {
            Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
              val (df, offsets) = kafkaClient
                .consumeTopicBatch(
                  kafkaJobConfig.topicConfigName.getOrElse(""),
                  session,
                  topicConfig
                )
              val transformedDF = transform(df)

              val savedDF: DataFrame = batchSave(transformedDF)

              kafkaClient.topicSaveOffsets(
                kafkaJobConfig.topicConfigName.getOrElse(""),
                topicConfig.allAccessOptions(),
                offsets
              )
              SparkJobResult(Some(savedDF), None)
            }
          }
        case None =>
          if (kafkaJobConfig.streaming) {
            require(kafkaJobConfig.format != "kafka")
            val df = session.readStream.format(kafkaJobConfig.format).options(options).load()
            val transformedDF = transform(df)
            writeStreaming(transformedDF)
            SparkJobResult(None, None)
          } else {
            val df = if (kafkaJobConfig.format == "bigquery" && options.contains("query")) {
              session.read
                .format("bigquery")
                .options(options)
                .load()
            } else {
              require(kafkaJobConfig.path.isDefined, "Load path is required")
              // Pass options to all formats for consistency with the streaming path (line 168)
              val reader = session.read
                .format(kafkaJobConfig.format)
                .options(options)
              if (kafkaJobConfig.format == "bigquery") {
                reader.load(
                  finalLoadPath
                    .getOrElse(
                      throw new StarlakeConfigException("Load path should be set in config")
                    )
                )
              } else {
                reader.load(
                  finalLoadPath
                    .getOrElse(
                      throw new StarlakeConfigException("Load path should be set in config")
                    )
                    .split(',')
                    .toIndexedSeq: _*
                )
              }
            }
            val transformedDF: DataFrame = transform(df)
            (kafkaJobConfig.writeFormat, writeTopicConfig) match {
              case ("kafka", Some(writeTopicConfig)) =>
                Utils.withResources(new KafkaClient(settings.appConfig.kafka)) { kafkaClient =>
                  kafkaClient.sinkToTopic(
                    writeTopicConfig,
                    transformedDF
                  )
                }
              case _ =>
                batchSave(transformedDF)
            }
            SparkJobResult(Some(transformedDF), None)
          }
      }
    }
  }

  private def batchSave(df: DataFrame): DataFrame = {
    val finalDF =
      kafkaJobConfig.coalesce match {
        case None    => df
        case Some(x) => df.repartition(x)
      }

    logger.info(s"Saving to $kafkaJobConfig")
    val kafkaOptions =
      if (kafkaJobConfig.writeFormat == "kafka")
        writeTopicConfig.map(_.allAccessOptions()).getOrElse(Map.empty)
      else
        Map.empty[String, String]

    val dfWriter = finalDF.write
      .mode(kafkaJobConfig.writeMode)
      .format(kafkaJobConfig.writeFormat)
      .options(kafkaOptions ++ writeOptions)

    finalWritePath match {
      case None =>
        dfWriter.save()
      case Some(path) =>
        dfWriter.save(path)
    }

    logger.info(s"Kafka saved messages to offload -> ${finalWritePath}")

    if (kafkaJobConfig.writeFormat != "bigquery") {
      (kafkaJobConfig.coalesce, finalWritePath) match {
        case (Some(1), Some(path)) =>
          val targetPath = new Path(path)
          val singleFile = settings
            .storageHandler()
            .list(
              targetPath,
              recursive = false
            )
            .map(_.path)
            .filter(_.getName.startsWith("part-"))
            .head
          val tmpPath = new Path(targetPath.toString + ".tmp")
          if (settings.storageHandler().move(singleFile, tmpPath)) {
            settings.storageHandler().delete(targetPath)
            settings.storageHandler().move(tmpPath, targetPath)
          }
        case (None, _) =>
        case (_, _) =>
          throw new Exception("Only coalesce(1) supported. Anything else is ignored")
      }
    }
    df
  }

  private def transform(df: DataFrame) = {
    val transformedDF = DataFrameTransform.transform(transformInstance, df, session)
    transformedDF
  }

  private def writeStreaming(df: DataFrame) = {

    // BigQuery streaming uses foreachBatch to route through the batch write API,
    // avoiding the BigQueryStreamingSink which is incompatible with Spark 3.5+
    // (NoSuchMethodError on RowEncoder.apply)
    if (kafkaJobConfig.writeFormat == "bigquery") {
      writeStreamingViaBatchSink(df)
    } else {
      writeStreamingDirect(df)
    }
  }

  private def writeStreamingViaBatchSink(df: DataFrame) = {
    val trigger = resolveStreamingTrigger()

    val writer = df.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .mode(kafkaJobConfig.writeMode)
          .format("bigquery")
          .options(writeOptions)
          .save()
      }

    val triggerWriter = trigger match {
      case Some(t) => writer.trigger(t)
      case None    => writer
    }

    val finalWriter = kafkaJobConfig.streamingWritePartitionBy match {
      case Nil  => triggerWriter
      case list => triggerWriter.partitionBy(list: _*)
    }

    val checkpointLocation = writeOptions.getOrElse(
      "checkpointLocation",
      finalWritePath.getOrElse(
        throw new Exception(
          "BigQuery streaming requires 'checkpointLocation' in write-options or --write-path"
        )
      )
    )

    finalWriter
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()
  }

  private def writeStreamingDirect(df: DataFrame) = {

    val writer = {
      (kafkaJobConfig.writeFormat, writeTopicConfig) match {
        case ("kafka", Some(writeTopicConfig)) =>
          df.writeStream
            .outputMode(kafkaJobConfig.writeMode)
            .options(writeTopicConfig.allAccessOptions())
            .option("topic", writeTopicConfig.topicName)
            .format(kafkaJobConfig.writeFormat)
            .options(writeOptions)
        case (_, None) =>
          // ("kafka", None) is accepted because we can set the topic & brokers config in the writeOptions
          df.writeStream
            .outputMode(kafkaJobConfig.writeMode)
            .format(kafkaJobConfig.writeFormat)
            .options(writeOptions)
        case (_, Some(_)) =>
          throw new Exception(
            "Cannot load to destination not kafka  with topic config name"
          )
      }
    }

    val trigger = resolveStreamingTrigger()

    val triggerWriter = trigger match {
      case Some(trigger) => writer.trigger(trigger)
      case None          => writer
    }

    val partitionedWriter = kafkaJobConfig.streamingWritePartitionBy match {
      case Nil =>
        triggerWriter
      case list =>
        triggerWriter.partitionBy(list: _*)
    }
    val streamingQuery =
      (finalWritePath, kafkaJobConfig.streamingWriteToTable) match {
        case (_, true) =>
          throw new Exception("streamingWriteToTable Not Supported")
        case (Some(path), false) =>
          partitionedWriter
            .start(path)
        case (None, false) =>
          partitionedWriter.start()
      }

    streamingQuery
      .awaitTermination()
  }

  private def resolveStreamingTrigger(): Option[Trigger] = {
    kafkaJobConfig.streamingTrigger.map(_.toLowerCase).map {
      case "once"           => Trigger.AvailableNow()
      case "processingtime" => Trigger.ProcessingTime(kafkaJobConfig.streamingTriggerOption)
      case "continuous"     => Trigger.Continuous(kafkaJobConfig.streamingTriggerOption)
      case "availablenow"   => Trigger.AvailableNow()
    }
  }

  private val transformInstance: Option[DataFrameTransform] = {
    kafkaJobConfig.transform
      .map(Utils.loadInstance[DataFrameTransform])
  }

  override def run(): Try[JobResult] = {
    val customDeserializers = settings.appConfig.kafka.customDeserializers.getOrElse(Map.empty)
    customDeserializers.foreach { case (customDeserializerName, customDeserializerFunction) =>
      val topicName = topicConfig
        .map(_.topicName)
        .getOrElse(
          writeTopicConfig
            .map(_.topicName)
            .getOrElse(
              throw new StarlakeConfigException(
                "Cannot register de/serializers if topic not defined"
              )
            )
        )
      CustomDeserializer.configure(
        customDeserializerName,
        customDeserializerFunction,
        settings.appConfig.kafka.serverOptions
      )

      session.udf.register(
        customDeserializerName,
        (bytes: Array[Byte]) =>
          CustomDeserializer.deserialize(customDeserializerName, topicName, bytes)
      )
    }
    pipeline()
  }
  override def name: String = s"${kafkaJobConfig.topicConfigName}"
}

object CustomDeserializer {
  val deserializers: scala.collection.mutable.Map[String, Deserializer[Any]] =
    scala.collection.mutable.Map.empty

  def configure(
    customDeserializerName: String,
    customDeserializerFunction: String,
    configs: Map[String, _]
  ): Unit = {
    val userDefinedDeserializer = Class
      .forName(customDeserializerFunction)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[Deserializer[Any]]
    userDefinedDeserializer.configure(configs.asJava, false)
    deserializers.put(customDeserializerName, userDefinedDeserializer)
  }

  def deserialize(
    userDefinedDeserializerName: String,
    topic: String,
    bytes: Array[Byte]
  ): String = deserializers(userDefinedDeserializerName).deserialize(topic, bytes).toString
}
