package ai.starlake.job.site.stats

import java.sql.{Connection, ResultSet}
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset, ZonedDateTime}
import java.util.Locale
import scala.util.{Try, Using}

object Rows {
  type Row = Map[String, Any]

  private def get(r: Row, k: String): Option[Any] =
    r.get(k.toLowerCase(Locale.ROOT)).flatMap(Option(_))

  def str(r: Row, k: String): Option[String] = get(r, k).map(_.toString)

  def long(r: Row, k: String): Option[Long] = get(r, k).flatMap {
    case n: java.lang.Number => Some(n.longValue())
    case other               => Try(BigDecimal(other.toString.trim).toLong).toOption
  }

  def instant(r: Row, k: String): Option[String] = get(r, k).map {
    case t: java.sql.Timestamp => t.toInstant.toString
    case o: OffsetDateTime     => o.toInstant.toString
    case z: ZonedDateTime      => z.toInstant.toString
    case i: Instant            => i.toString
    case l: LocalDateTime      => l.toInstant(ZoneOffset.UTC).toString
    case other                 => other.toString
  }
}

object JdbcRows {
  import Rows.Row

  /** Runs one query bounded by `timeoutSeconds`. A driver that rejects query timeouts still runs
    * it: the per-connection timeout of the runner bounds the collection anyway.
    */
  def query(conn: Connection, sql: String, timeoutSeconds: Int): List[Row] =
    Using.resource(conn.createStatement()) { st =>
      Try(st.setQueryTimeout(timeoutSeconds))
      readAndClose(st.executeQuery(sql))
    }

  def readAndClose(rs: ResultSet): List[Row] =
    Using.resource(rs) { rs =>
      val md = rs.getMetaData
      val labels = (1 to md.getColumnCount).map(i => md.getColumnLabel(i).toLowerCase(Locale.ROOT))
      val out = List.newBuilder[Row]
      while (rs.next())
        out += labels.zipWithIndex.map { case (l, i) => l -> rs.getObject(i + 1) }.toMap
      out.result()
    }
}
