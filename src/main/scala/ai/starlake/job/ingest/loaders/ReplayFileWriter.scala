package ai.starlake.job.ingest.loaders

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.nio.charset.Charset
import java.sql.Timestamp
import java.text.SimpleDateFormat

/** Writes rejected input lines to a replay file that can be dropped back into the landing area and
  * ingested again. Engine neutral on purpose: it takes raw lines rather than a DataFrame, so native
  * loaders can use it without pulling in Spark.
  */
object ReplayFileWriter extends LazyLogging {

  /** Anything a file name cannot safely carry becomes a dash. The job id is built from the domain,
    * the table and the input file name, so it can carry path separators or whatever else the
    * landing file was called.
    */
  private def sanitizeForFileName(jobid: String): String =
    jobid.replaceAll("[^A-Za-z0-9._-]", "-")

  /** Writes the rejected lines to `{domain}.{table}.{yyyyMMddHHmmss}.{jobid}.replay`.
    *
    * The job id is part of the name because the timestamp only resolves to the second: two loads of
    * the same table inside one wall clock second, which autoload produces by triggering two
    * ingestion jobs back to back, would otherwise write the same name and the second would silently
    * overwrite the first one's rejected lines. Each attempt has its own job id, so the name stays
    * deterministic per attempt while distinguishing attempts.
    *
    * The Spark loader (`IngestionJob.saveRejected`) still writes the shorter
    * `{domain}.{table}.{yyyyMMddHHmmss}.replay`, so the two loaders name their replay files
    * differently. Both land in the same domain replay area and both are ingestable again, only the
    * name diverges.
    *
    * @return
    *   the path written, or None when there is nothing to replay
    */
  def write(
    domainName: String,
    tableName: String,
    rejectedLines: List[RejectedLine],
    header: Option[String],
    encoding: String,
    timestamp: Timestamp,
    jobid: String
  )(implicit settings: Settings, storageHandler: StorageHandler): Option[Path] = {
    if (rejectedLines.isEmpty) {
      None
    } else {
      val replayArea = DatasetArea.replay(domainName)
      storageHandler.mkdirs(replayArea)
      val formattedDate = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)
      val targetPath =
        new Path(
          replayArea,
          s"$domainName.$tableName.$formattedDate.${sanitizeForFileName(jobid)}.replay"
        )
      val content = (header.toList ++ rejectedLines.map(_.rawLine)).mkString("", "\n", "\n")
      storageHandler.write(content, targetPath)(Charset.forName(encoding))
      logger.info(s"Wrote ${rejectedLines.size} rejected line(s) to $targetPath")
      Some(targetPath)
    }
  }
}
