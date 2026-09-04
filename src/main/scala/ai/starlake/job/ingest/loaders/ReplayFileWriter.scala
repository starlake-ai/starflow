package ai.starlake.job.ingest.loaders

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import java.sql.Timestamp
import java.text.SimpleDateFormat

/** Writes rejected input lines to a replay file that can be dropped back into the landing
  * area and ingested again. Engine neutral on purpose: it takes raw lines rather than a
  * DataFrame, so native loaders can use it without pulling in Spark.
  */
object ReplayFileWriter extends LazyLogging {

  /** @return
    *   the path written, or None when there is nothing to replay
    */
  def write(
    domainName: String,
    tableName: String,
    rejectedLines: List[RejectedLine],
    header: Option[String],
    encoding: String,
    timestamp: Timestamp
  )(implicit settings: Settings, storageHandler: StorageHandler): Option[Path] = {
    if (rejectedLines.isEmpty) {
      None
    } else {
      val replayArea = DatasetArea.replay(domainName)
      storageHandler.mkdirs(replayArea)
      val formattedDate = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)
      val targetPath = new Path(replayArea, s"$domainName.$tableName.$formattedDate.replay")
      val content = (header.toList ++ rejectedLines.map(_.rawLine)).mkString("", "\n", "\n")
      storageHandler.write(content, targetPath)(Charset.forName(encoding))
      logger.info(s"Wrote ${rejectedLines.size} rejected line(s) to $targetPath")
      Some(targetPath)
    }
  }
}
