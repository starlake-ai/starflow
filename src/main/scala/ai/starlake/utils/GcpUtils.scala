package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import better.files.File
// gcs-connector's shaded jar vendors com.google.cloud.logging.* unrelocated (it wins classpath
// precedence over the real google-cloud-logging artifact), but relocates its own MonitoredResource
// dependency to this repackaged path; the two must match or LogEntry.Builder#setResource won't compile.
import com.google.cloud.MonitoredResource
import com.google.cloud.logging.Payload.JsonPayload
import com.google.cloud.logging.{LogEntry, LoggingException, LoggingOptions}
import com.google.common.base.VerifyException
import com.typesafe.scalalogging.LazyLogging

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Locale
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

object GcpUtils extends LazyLogging {
  private val WELL_KNOWN_CREDENTIALS_FILE = "application_default_credentials.json"

  private val CLOUDSDK_CONFIG_DIRECTORY = "gcloud"

  def getApplicationDefaultFile(): Option[File] = {
    val credentialsPath = sys.env.get("GOOGLE_APPLICATION_CREDENTIALS")
    credentialsPath.map(File(_)).orElse {
      val applicationDefaultFile = getWellKnownCredentialsFile()
      applicationDefaultFile
    }
  }

  protected def getWellKnownCredentialsFile(): Option[File] = {
    val os = System.getProperty("os.name", "").toLowerCase(Locale.US)
    val cloudConfigPath = if (os.indexOf("windows") >= 0) {
      val appDataPath = File(System.getenv("APPDATA"))
      File(appDataPath, CLOUDSDK_CONFIG_DIRECTORY)
    } else {
      val configPath = File(System.getProperty("user.home", ""), ".config")
      File(configPath, CLOUDSDK_CONFIG_DIRECTORY)
    }
    val credentialFilePath = File(cloudConfigPath, WELL_KNOWN_CREDENTIALS_FILE)
    if (credentialFilePath.exists) {
      Some(credentialFilePath)
    } else {
      None
    }
  }

  def adapt_map_to_gcp_log(log: Map[String, Any]): Map[String, Any] = {
    // JsonPayload.of doesn't handle all types. Types handle by this method are defined in com.google.cloud.Structs.objectToValue.

    def adapt_values(value: Any): Any = {
      if (value != null) {
        value match {
          case v: Map[_, _] =>
            // force key to be string and value to any supported type
            v.iterator.map { case (k, v) =>
              (if (k != null) k.toString else k.asInstanceOf[String]) -> adapt_values(v)
            }.toMap
          case v: Iterable[_]                     => v.map(adapt_values)
          case _: String | _: Boolean | _: Number => value
          case _ if value.getClass.isEnum         => value
          // special handling starts here
          case v: java.sql.Timestamp => v.getTime
          case _ =>
            value.toString // by default, serialize to string instead of throwing an exception
        }
      } else {
        value
      }
    }
    adapt_values(log) match {
      case result: Map[String @unchecked, Any @unchecked] => result
      case result =>
        throw new RuntimeException(
          s"Expected type Map[String, Any], got ${result.getClass.getSimpleName}"
        )
    }
  }

  private def estimateSize(entry: LogEntry): Int = {
    entry.toString.getBytes(StandardCharsets.UTF_8).length
  }

  private def chunkLogs(
    entries: Seq[LogEntry],
    maxChunkSize: Int = 9 * 1024 * 1024 // 9 MB
  ): Seq[Seq[LogEntry]] = {
    val result = scala.collection.mutable.ArrayBuffer[Seq[LogEntry]]()
    var currentChunk = scala.collection.mutable.ArrayBuffer[LogEntry]()
    var currentSize = 0

    for (entry <- entries) {
      val size = estimateSize(entry)
      if (currentSize + size > maxChunkSize && currentChunk.nonEmpty) {
        result += currentChunk.toSeq
        currentChunk = scala.collection.mutable.ArrayBuffer(entry)
        currentSize = size
      } else {
        currentChunk += entry
        currentSize += size
      }
    }

    if (currentChunk.nonEmpty) result += currentChunk.toSeq
    result.toSeq
  }

  def sinkToGcpCloudLogging(logs: Seq[Map[String, Any]], typ: String, logName: String)(implicit
    settings: Settings
  ): Unit = {
    val connProjectId = settings.appConfig.audit.getSink().getConnection().options.get("projectId")
    val logging = LoggingOptions.getDefaultInstance
      .toBuilder()
      .setRetrySettings(
        LoggingOptions.getDefaultInstance.getRetrySettings.toBuilder
          .setInitialRpcTimeoutDuration(Duration.ZERO) // Increase initial timeout
          .setTotalTimeoutDuration(Duration.ZERO)
          .setMaxRetryDelayDuration(Duration.ofSeconds(10))
          .setMaxAttempts(Int.MaxValue)
          .build()
      )
      .setProjectId(BigQueryJobBase.projectId(connProjectId, settings.appConfig.audit.database))
      .build()
      .getService
    try {
      val entries = logs.map { log =>
        LogEntry
          .newBuilder(JsonPayload.of(adapt_map_to_gcp_log(log).asJava))
          .setSeverity(com.google.cloud.logging.Severity.INFO)
          .addLabel("type", typ)
          .addLabel("app", "starlake")
          .setLogName(logName)
          .setResource(MonitoredResource.newBuilder("global").build)
          .build
      }
      // Writes the log entry asynchronously
      val chunks = chunkLogs(entries)

      chunks.foreach { chunk =>
        logging.write(chunk.asJava)
        // Optional - flush any pending log entries just before Logging is closed
        BigQueryJobBase.recoverBigqueryException(settings.appConfig.onExceptionRetries)(
          logging.flush()
        ) match {
          case Failure(exception: VerifyException)
              if Utils.hasCauseInStack[LoggingException](exception) =>
            logger.error("Failed to log entries " + chunk, exception)
          case Failure(exception: LoggingException) =>
            logger.error("Failed to log entries " + chunk, exception)
          case Failure(exception) => throw exception
          case Success(_)         => //
        }
      }
    } finally if (logging != null) logging.close()
  }
}
