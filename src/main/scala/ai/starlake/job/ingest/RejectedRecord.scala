package ai.starlake.job.ingest

import ai.starlake.schema.model.{SchemaInfo, TableAttribute}
import com.google.cloud.bigquery.QueryParameterValue

import java.sql.Timestamp
import java.util.regex.Pattern

case class RejectedRecord(
  jobid: String,
  timestamp: Timestamp,
  domain: String,
  schema: String,
  error: String,
  path: String
) {
  def asMap(): Map[String, Any] = {
    Map(
      "jobid"     -> jobid,
      "timestamp" -> timestamp,
      "domain"    -> domain,
      "schema"    -> schema,
      "error"     -> error,
      "path"      -> path
    )
  }
}

object RejectedRecord {

  /** The longest error or path kept, as for the audit log messages: some engines store at most 8192
    * characters
    */
  private val maxTextLength = 8192 - 192

  /** @return
    *   the text, cut to the longest text kept, without splitting a character
    */
  def limited(text: String): String =
    if (text.length <= maxTextLength) text
    else {
      val end =
        if (Character.isHighSurrogate(text.charAt(maxTextLength - 1))) maxTextLength - 1
        else maxTextLength
      text.take(end) + s"...(truncated, original length=${text.length})"
    }

  /** @return
    *   the error of a rejected row as the Spark validators write it: its errors, one per line, then
    *   its file, and its value as JSON when there is one
    */
  def errorText(errors: List[String], file: String, value: Option[String] = None): String =
    limited(
      s"ERR -> \n${errors.mkString("\n")}\n\nFILE -> $file" + value
        .map(v => s"\n\nROW -> $v")
        .getOrElse("")
    )

  /** @return
    *   the records in groups that a statement holds: BigQuery accepts at most 10000 parameters and
    *   requests of 10 MB
    */
  def batches(records: List[RejectedRecord], maxSize: Int = 4000000): List[List[RejectedRecord]] =
    records
      .foldLeft(List.empty[(List[RejectedRecord], Int)]) { case (groups, record) =>
        val size = record.error.length + record.path.length + 256
        groups match {
          case (group, groupSize) :: others if group.size < 1000 && groupSize + size <= maxSize =>
            (record :: group, groupSize + size) :: others
          case _ => (List(record), size) :: groups
        }
      }
      .map(_._1.reverse)
      .reverse

  /** @return
    *   the statement inserting the records into the BigQuery table, and its parameters: the values
    *   need no SQL escaping
    */
  def bigQueryInsert(
    table: String,
    records: List[RejectedRecord]
  ): (String, Map[String, QueryParameterValue]) = {
    val indexed = records.zipWithIndex
    val rows = indexed.map { case (_, i) =>
      s"(@jobid_$i, @timestamp_$i, @domain_$i, @schema_$i, @error_$i, @path_$i)"
    }
    val parameters = indexed.flatMap { case (record, i) =>
      val micros = record.timestamp.getTime / 1000 * 1000000 + record.timestamp.getNanos / 1000
      List(
        s"jobid_$i"     -> QueryParameterValue.string(record.jobid),
        s"timestamp_$i" -> QueryParameterValue.timestamp(micros),
        s"domain_$i"    -> QueryParameterValue.string(record.domain),
        s"schema_$i"    -> QueryParameterValue.string(record.schema),
        s"error_$i"     -> QueryParameterValue.string(record.error),
        s"path_$i"      -> QueryParameterValue.string(record.path)
      )
    }.toMap
    (
      s"INSERT INTO $table (JOBID, TIMESTAMP, DOMAIN, SCHEMA, ERROR, PATH) VALUES ${rows.mkString(", ")}",
      parameters
    )
  }
  val starlakeSchema = SchemaInfo(
    name = "rejected",
    pattern = Pattern.compile("ignore"),
    attributes = List(
      TableAttribute("jobid", "string"),
      TableAttribute("timestamp", "timestamp"),
      TableAttribute("domain", "string"),
      TableAttribute("schema", "string"),
      TableAttribute("error", "string"),
      TableAttribute("path", "string")
    ),
    None,
    None
  )
}
