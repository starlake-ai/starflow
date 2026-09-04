package ai.starlake.job.ingest.loaders

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import java.io.{BufferedOutputStream, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Files
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Using

/** Writes rejected input lines to a replay file that can be dropped back into the landing area and
  * ingested again. Engine neutral on purpose: it takes a capture rather than a DataFrame, so native
  * loaders can use it without pulling in Spark.
  *
  * The rejected lines are streamed from the capture's spill files to the target, never assembled
  * into one String: a load that rejects a multi gigabyte file has to produce its replay file
  * without the heap growing with it.
  */
object ReplayFileWriter extends LazyLogging {

  /** How many characters of the discriminator the file name keeps. The whole name also carries the
    * domain, the table and a timestamp, and it has to stay under the 255 bytes a file name gets on
    * most filesystems: the replay file is written before the target rows are inserted, so a name
    * too long to create would fail a load that used to succeed.
    */
  private val maxDiscriminatorLength = 80

  /** What tells two replay files of the same table, written in the same second, apart.
    *
    * The job id alone is not enough: `JobBase.appName` returns the SL_JOB_ID environment variable
    * verbatim when it is set, so every job of an orchestrated run shares one application id. The
    * landing file name is what actually varies between two loads of the same table, which is
    * exactly what autoload produces, so it is folded in.
    *
    * Anything a file name cannot safely carry becomes a dash, and only the tail is kept: the
    * discriminating parts, the job id's timestamp when it has one and the input file name, are at
    * the end, while the head repeats the domain and the table the name already carries.
    */
  private def nameDiscriminator(jobid: String, inputFileName: Option[String]): String = {
    val joined = jobid + inputFileName.map("-" + _).getOrElse("")
    joined.replaceAll("[^A-Za-z0-9._-]", "-").takeRight(maxDiscriminatorLength)
  }

  /** Writes the rejected lines to `{domain}.{table}.{yyyyMMddHHmmss}.{jobid}-{input file
    * name}.replay`.
    *
    * The discriminator is part of the name because the timestamp only resolves to the second: two
    * loads of the same table inside one wall clock second, which autoload produces by triggering
    * two ingestion jobs back to back, would otherwise write the same name and the second would
    * silently overwrite the first one's rejected lines. See `nameDiscriminator` for why the job id
    * alone does not separate them.
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
    rejected: RejectCapture,
    header: Option[String],
    encoding: String,
    timestamp: Timestamp,
    jobid: String,
    inputFileName: Option[String]
  )(implicit settings: Settings, storageHandler: StorageHandler): Option[Path] = {
    if (rejected.isEmpty) {
      None
    } else {
      // resolved before anything is created: an unsupported encoding has to fail before the
      // target file exists, or the load leaves an empty replay file behind and leaks the stream
      val charset = Charset.forName(encoding)
      val replayArea = DatasetArea.replay(domainName)
      storageHandler.mkdirs(replayArea)
      val formattedDate = new SimpleDateFormat("yyyyMMddHHmmss").format(timestamp)
      val targetPath =
        new Path(
          replayArea,
          s"$domainName.$tableName.$formattedDate.${nameDiscriminator(jobid, inputFileName)}.replay"
        )
      // Every spill file already holds its raw lines terminated by \n, in the order the input
      // paths were read, which is exactly the replay file's content after the header. So the
      // files are transcoded straight through, in fixed size chunks, rather than read as lines:
      // a raw line carrying a lone carriage return would come back out as a line feed otherwise.
      Using.resource(
        new OutputStreamWriter(
          new BufferedOutputStream(storageHandler.output(targetPath)),
          charset
        )
      ) { writer =>
        header.foreach { headerLine =>
          writer.write(headerLine)
          writer.write('\n')
        }
        rejected.spillFiles.foreach { spillFile =>
          Using.resource(Files.newBufferedReader(spillFile, StandardCharsets.UTF_8)) { reader =>
            reader.transferTo(writer)
          }
        }
      }
      logger.info(s"Wrote ${rejected.count} rejected line(s) to $targetPath")
      Some(targetPath)
    }
  }
}
