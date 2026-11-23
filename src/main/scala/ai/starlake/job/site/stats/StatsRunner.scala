package ai.starlake.job.site.stats

import ai.starlake.config.{ConnectionInfo, Settings}
import ai.starlake.job.site.SiteNode
import ai.starlake.schema.model.ConnectionType
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.TreeMap
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Collects warehouse stats for every connection referenced by the nodes, at most `parallelism`
  * connections at a time, each bounded by `timeout`. A failing, unsupported or slow connection gets
  * a status and an error message, never an exception: the other connections are not affected.
  * Physical tables are collected once and fanned out to every node id that points at them.
  */
class StatsRunner(
  connections: String => Option[ConnectionInfo],
  collectorFor: ConnectionInfo => Option[StatsCollector],
  timeout: FiniteDuration,
  parallelism: Int = 4
)(implicit settings: Settings)
    extends LazyLogging {

  private type Result = (ConnectionStats, Map[PhysicalTable, RawTableStats])

  private def daemonThreads(prefix: String): ThreadFactory = {
    val n = new AtomicInteger(0)
    (r: Runnable) => {
      val t = new Thread(r, s"$prefix-${n.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
  }

  private val timeoutLabel: String =
    java.math.BigDecimal.valueOf(timeout.toMillis, 3).stripTrailingZeros.toPlainString + "s"

  def run(nodes: TreeMap[String, SiteNode]): SiteStats = {
    val groups = PhysicalTables.group(nodes.values)
    // `pool` bounds the number of connections collected at once; each collection runs on a
    // `workers` thread so that its pool thread can stop waiting for it after the timeout.
    val pool = Executors.newFixedThreadPool(parallelism, daemonThreads("sl-stats"))
    val workers = Executors.newCachedThreadPool(daemonThreads("sl-stats-worker"))
    try {
      val futures = groups.toList.sortBy(_._1).map { case (ref, tables) =>
        ref -> pool.submit(new Callable[Result] {
          def call(): Result = runOne(ref, tables.map(_._1), workers)
        })
      }
      val results = futures.map { case (ref, f) =>
        // runOne bounds its own wait and never throws a non fatal exception
        val result =
          try f.get()
          catch {
            case e: ExecutionException => failed(ref, "", Option(e.getCause).getOrElse(e), 0L)
          }
        ref -> result
      }
      val connectionStats = TreeMap(results.map { case (ref, (cs, _)) => ref -> cs }: _*)
      val tableStats = for {
        (ref, (cs, raw)) <- results
        (pt, ids)        <- groups(ref)
        r                <- raw.get(pt).toList
        id               <- ids
      } yield id -> StatsRunner.toTableStats(ref, cs.engine, nodes(id), r)
      SiteStats(1, Instant.now().toString, connectionStats, TreeMap(tableStats: _*))
    } finally {
      pool.shutdownNow()
      workers.shutdownNow()
    }
  }

  private def failed(ref: String, engine: String, e: Throwable, durationMs: Long): Result = {
    logger.warn(s"Stats for connection $ref failed", e)
    (ConnectionStats(engine, "error", Some(StatsCollector.message(e)), durationMs), Map.empty)
  }

  private def runOne(ref: String, tables: List[PhysicalTable], workers: ExecutorService): Result = {
    val start = System.currentTimeMillis()
    def elapsed = System.currentTimeMillis() - start
    Try(connections(ref)) match {
      case Failure(e) => failed(ref, "", e, elapsed)
      case Success(None) =>
        logger.warn(s"Stats for connection $ref skipped: connection not found")
        (ConnectionStats("", "error", Some(s"Connection not found: $ref"), 0L), Map.empty)
      case Success(Some(conn)) =>
        val engine = Try(conn.getJdbcEngineName().toString).getOrElse(conn.`type`.toString)
        Try(collectorFor(conn)) match {
          case Failure(e) => failed(ref, engine, e, elapsed)
          case Success(None) =>
            val msg = s"No stats collector for connection type ${conn.`type`}"
            logger.info(s"Stats for connection $ref skipped: $msg")
            (ConnectionStats(engine, "unsupported", Some(msg), 0L), Map.empty)
          case Success(Some(collector)) => collect(ref, conn, collector, tables, workers, elapsed)
        }
    }
  }

  private def collect(
    ref: String,
    conn: ConnectionInfo,
    collector: StatsCollector,
    tables: List[PhysicalTable],
    workers: ExecutorService,
    elapsed: => Long
  ): Result = {
    val f = workers.submit(new Callable[CollectResult] {
      def call(): CollectResult = collector.collect(ref, conn, tables)
    })
    try {
      val raw = f.get(timeout.toMillis, TimeUnit.MILLISECONDS)
      val result = raw.copy(failures = raw.failures.map(Sanitize.message))
      if (raw.failures.nonEmpty)
        logger.warn(s"Stats for connection $ref partially failed: ${raw.failures.mkString("; ")}")
      val cs = ConnectionStatus.from(collector.engine, result, elapsed, collector.connectionStatus)
      (cs, result.tables)
    } catch {
      case _: TimeoutException =>
        f.cancel(true)
        val msg = s"timed out after $timeoutLabel"
        logger.warn(s"Stats for connection $ref $msg, abandoned")
        (ConnectionStats(collector.engine, "error", Some(msg), elapsed), Map.empty)
      case e: ExecutionException =>
        failed(ref, collector.engine, Option(e.getCause).getOrElse(e), elapsed)
      case e: InterruptedException =>
        f.cancel(true)
        throw e
      case NonFatal(e) =>
        f.cancel(true)
        failed(ref, collector.engine, e, elapsed)
    }
  }
}

object StatsRunner {

  /** BigQuery, Spark (FS), or JDBC with a `url` (dialect collector, else the generic one). Any
    * other connection (Kafka, Elasticsearch, Snowflake with only `sfUrl`, ...) is unsupported.
    */
  def collectorFor(conn: ConnectionInfo): Option[StatsCollector] = conn.`type` match {
    case ConnectionType.BQ => Some(new BigQueryStatsCollector())
    case ConnectionType.FS => Some(new SparkStatsCollector())
    case ConnectionType.JDBC if conn.options.contains("url") =>
      val engine = conn.getJdbcEngineName().toString
      Some(
        JdbcDialectStats
          .forEngine(engine)
          .map(new JdbcStatsCollector(_))
          .getOrElse(new GenericJdbcStatsCollector(engine))
      )
    case _ => None
  }

  def apply(timeout: FiniteDuration)(implicit settings: Settings): StatsRunner =
    new StatsRunner(name => settings.appConfig.connection(name), collectorFor, timeout)

  /** The column diff runs against the declared columns only when the table exists. */
  private[stats] def toTableStats(
    ref: String,
    engine: String,
    node: SiteNode,
    r: RawTableStats
  ): TableStats =
    TableStats(
      connectionRef = ref,
      exists = r.exists,
      tableType = r.tableType,
      format = r.format,
      rowCount = r.rowCount,
      rowCountEstimated = r.rowCountEstimated,
      bytes = r.bytes,
      lastModified = r.lastModified,
      columns = r.columns,
      columnDiff = if (r.exists) ColumnDiffer.diff(node.columns, r.columns, engine) else Nil
    )
}
