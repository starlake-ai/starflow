package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings, SparkEnv}
import Rows._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

import java.util.Locale
import scala.util.{Failure, Success, Try}

object SparkStatsParsing {
  private val BytesRe = """(\d+)\s+bytes""".r.unanchored
  private val RowsRe = """(\d+)\s+rows""".r.unanchored
  private val DetailMarker = "# detailed table information"

  /** Spark keeps the declared CHAR/VARCHAR spelling in the field metadata under this key. */
  private val CharVarcharKey = "__CHAR_VARCHAR_TYPE_STRING"

  /** "1234 bytes, 10 rows" -> (bytes, rows) */
  def statistics(s: String): (Option[Long], Option[Long]) = {
    val bytes = s match { case BytesRe(b) => Some(b.toLong); case _ => None }
    val rows = s match { case RowsRe(r) => Some(r.toLong); case _ => None }
    (bytes, rows)
  }

  def describeExtended(rows: List[(String, String)]): Map[String, String] =
    rows.foldLeft(Map.empty[String, String]) { case (acc, (k, v)) =>
      val key = k.trim.toLowerCase(Locale.ROOT)
      if (key.isEmpty || key.startsWith("#") || acc.contains(key)) acc else acc + (key -> v.trim)
    }

  /** The DESCRIBE EXTENDED rows after "# Detailed Table Information", so that a column named `type`
    * or `provider` cannot shadow the table details. All rows when the marker is absent.
    */
  def detailSection(rows: List[(String, String)]): List[(String, String)] = {
    val detail = rows.dropWhile(_._1.trim.toLowerCase(Locale.ROOT) != DetailMarker)
    if (detail.isEmpty) rows else detail
  }

  def tableType(raw: Option[String]): String = raw.map(_.toUpperCase(Locale.ROOT)) match {
    case Some("VIEW")     => "VIEW"
    case Some("EXTERNAL") => "EXTERNAL"
    case _                => "TABLE"
  }

  def fromDeltaDetail(row: Row): (Option[Long], Option[String]) =
    (long(row, "sizeinbytes"), instant(row, "lastmodified"))

  def fromIcebergSnapshot(row: Row): (Option[Long], Option[Long], Option[String]) =
    (long(row, "total_records"), long(row, "total_files_size"), instant(row, "committed_at"))

  /** Position or equality deletes make the data file record counts an overcount. */
  def icebergHasDeletes(row: Row): Boolean =
    List("total_position_deletes", "total_equality_deletes", "delete_files")
      .exists(k => long(row, k).exists(_ > 0))

  def quote(t: PhysicalTable): String =
    (t.database.toList :+ t.schema :+ t.table).map(quotePart).mkString(".")

  def quotePart(p: String): String = "`" + p.replace("`", "``") + "`"

  def deltaDetailSql(q: String): String = s"DESCRIBE DETAIL $q"

  /** The summary of the snapshot the main branch points to: metadata only. The latest commit is not
    * the current state after a rollback, and a commit on another branch does not change main. No
    * row means the table has no snapshot yet.
    */
  def icebergSnapshotSql(q: String): String =
    "SELECT s.summary['total-records'] AS total_records, " +
    "s.summary['total-files-size'] AS total_files_size, " +
    "s.summary['total-position-deletes'] AS total_position_deletes, " +
    "s.summary['total-equality-deletes'] AS total_equality_deletes, s.committed_at " +
    s"FROM $q.refs r JOIN $q.snapshots s ON s.snapshot_id = r.snapshot_id " +
    "WHERE r.name = 'main' AND r.type = 'BRANCH'"

  /** Sums over the files metadata table (manifests, never the data files): content 0 is data. */
  def icebergFilesSql(q: String): String =
    "SELECT sum(CASE WHEN content = 0 THEN record_count END) AS total_records, " +
    "sum(file_size_in_bytes) AS total_files_size, " +
    "sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END) AS delete_files " +
    s"FROM $q.files"

  /** The full type spelling: varchar(10) rather than the string Spark reads it as. */
  def columnType(f: StructField): String =
    if (f.metadata.contains(CharVarcharKey)) f.metadata.getString(CharVarcharKey)
    else f.dataType.catalogString
}

object SparkStatsCollector extends LazyLogging {
  import SparkStatsParsing._

  val defaultSession: Settings => SparkSession = settings =>
    SparkEnv.get("starlake-site-stats", identity, settings).session(settings)

  /** Adds the format specific stats to a table known to exist. Delta: DESCRIBE DETAIL (no row
    * count). Iceberg: summary of the snapshot main points to (0 rows when there is none), or the
    * files metadata table when the summary has no totals. Anything else: catalog statistics
    * (ANALYZE TABLE), reported as estimates. A failing stats query keeps the table and its columns
    * and adds `<label> stats failed: <message>`.
    */
  private[stats] def enrich(
    base: RawTableStats,
    details: Map[String, String],
    q: String,
    label: String,
    query: String => List[Row]
  ): (RawTableStats, List[String]) = {
    def statsQuery(sql: String): (Option[Row], List[String]) = {
      val (rows, failures) = StatsCollector.statsOnly(label)(query(sql))
      (rows.headOption, failures)
    }
    base.format match {
      case Some("delta") =>
        val (row, failures) = statsQuery(deltaDetailSql(q))
        val stats = row.fold(base) { r =>
          val (bytes, lastModified) = fromDeltaDetail(r)
          base.copy(bytes = bytes, lastModified = lastModified)
        }
        (stats, failures)
      case Some("iceberg") =>
        val (snapshot, failures) = statsQuery(icebergSnapshotSql(q))
        snapshot match {
          case None if failures.isEmpty =>
            // no snapshot on main: the table was created and never written
            (base.copy(rowCount = Some(0L), rowCountEstimated = false, bytes = Some(0L)), Nil)
          case None => (base, failures)
          case Some(s) =>
            val (rows, bytes, lastModified) = fromIcebergSnapshot(s)
            val fromSnapshot = base.copy(
              rowCount = rows,
              rowCountEstimated = rows.isDefined && icebergHasDeletes(s),
              bytes = bytes,
              lastModified = lastModified
            )
            if (rows.isDefined && bytes.isDefined) (fromSnapshot, failures)
            else {
              val (files, filesFailures) = statsQuery(icebergFilesSql(q))
              val stats = files.fold(fromSnapshot) { f =>
                val fileRows = long(f, "total_records")
                fromSnapshot.copy(
                  rowCount = fileRows,
                  rowCountEstimated = fileRows.isDefined && icebergHasDeletes(f),
                  bytes = long(f, "total_files_size")
                )
              }
              (stats, failures ++ filesFailures)
            }
        }
      case _ =>
        val (bytes, rows) = details.get("statistics").map(statistics).getOrElse((None, None))
        (base.copy(rowCount = rows, rowCountEstimated = rows.isDefined, bytes = bytes), Nil)
    }
  }
}

/** Spark (FS) connections: Delta, Iceberg and catalog (Hive, Parquet, ...) tables. Metadata only:
  * DESCRIBE, the table schema, DESCRIBE DETAIL and the Iceberg metadata tables, never a data scan.
  * The session is created only when there are tables to collect.
  */
class SparkStatsCollector(
  sessionProvider: Settings => SparkSession = SparkStatsCollector.defaultSession
) extends StatsCollector
    with LazyLogging {
  import SparkStatsParsing._

  val engine = "spark"

  private def lowerRow(r: org.apache.spark.sql.Row): Row =
    r.getValuesMap[Any](r.schema.fieldNames.toIndexedSeq).map { case (k, v) =>
      k.toLowerCase(Locale.ROOT) -> v
    }

  private def query(spark: SparkSession)(sql: String): List[Row] =
    spark.sql(sql).collect().map(lowerRow).toList

  /** One table: DESCRIBE EXTENDED and the schema failing fail the table, a format stats query
    * failing only drops the stats.
    */
  private def one(spark: SparkSession, t: PhysicalTable, label: String) = {
    val q = quote(t)
    if (!spark.catalog.tableExists(q)) (RawTableStats.Missing, Nil)
    else {
      val described = spark
        .sql(s"DESCRIBE TABLE EXTENDED $q")
        .collect()
        .map(r => (Option(r.getString(0)).getOrElse(""), Option(r.getString(1)).getOrElse("")))
        .toList
      val details = describeExtended(detailSection(described))
      val columns = spark
        .table(q)
        .schema
        .fields
        .map(f => ColumnStat(f.name, columnType(f), f.nullable))
        .toList
      val base = RawTableStats(
        exists = true,
        tableType = Some(tableType(details.get("type"))),
        format = details.get("provider").map(_.toLowerCase(Locale.ROOT)),
        rowCount = None,
        rowCountEstimated = false,
        bytes = None,
        lastModified = None,
        columns = columns
      )
      SparkStatsCollector.enrich(base, details, q, label, query(spark))
    }
  }

  def collect(connectionRef: String, connection: ConnectionInfo, tables: List[PhysicalTable])(
    implicit settings: Settings
  ): CollectResult =
    if (tables.isEmpty) CollectResult(Map.empty, Nil)
    else {
      val spark = sessionProvider(settings) // created only when there is something to collect
      StatsCollector.isolate(StatsCollector.bySchema(tables)) { g =>
        val namespace = (g.database.toList :+ g.schema).map(quotePart).mkString(".")
        if (!spark.catalog.databaseExists(namespace))
          GroupResult(g.tables.map(_ -> RawTableStats.Missing).toMap)
        else {
          // one corrupt table must not hide the other tables of its schema
          val results = g.tables.map { t =>
            val label = s"${g.label}.${t.table}"
            Try(one(spark, t, label)) match {
              case Success((stats, failures)) => (Some(t -> stats), failures)
              case Failure(e) =>
                logger.warn(s"Spark stats for $label failed: ${StatsCollector.message(e)}", e)
                (None, List(s"$label failed: ${StatsCollector.message(e)}"))
            }
          }
          GroupResult(results.flatMap(_._1).toMap, results.flatMap(_._2))
        }
      }
    }
}
