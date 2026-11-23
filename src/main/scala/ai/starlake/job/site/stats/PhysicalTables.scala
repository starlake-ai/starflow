package ai.starlake.job.site.stats

import ai.starlake.config.ConnectionInfo
import ai.starlake.job.site.SiteNode
import ai.starlake.schema.model.ConnectionType

import scala.util.Try

/** Where a node's table physically lives: the connection it is written to and its table there. */
case class PhysicalLocation(connectionRef: String, table: PhysicalTable)

object PhysicalTables {

  /** The connection the node's table is written to: the sink connection, else the node's own one.
    * For a transform this is AutoTaskInfo.getSinkConnectionRef (`connectionRef` is the run
    * connection); for a load, `connectionRef` is already Metadata.getSinkConnectionRef.
    */
  def connectionRefOf(n: SiteNode): Option[String] =
    n.sink.flatMap(_.connectionRef).orElse(n.connectionRef)

  /** The database core writes the node's table to on this connection:
    *   - Spark (FS): none, SparkAutoTask writes `domain.table`;
    *   - BigQuery: the node's database, else the default database (SL_DATABASE), else the
    *     connection projectId (AutoTaskInfo.getDatabase, SchemaHandler.getDatabase,
    *     BigQueryAutoTask target table id);
    *   - JDBC loads: the domain database, else the default database (SchemaHandler.getDatabase,
    *     used by the native loaders and the Spark ingestion pipeline);
    *   - JDBC transforms: the task database only (AutoTask.fullDomainName).
    */
  def databaseOf(
    n: SiteNode,
    connection: Option[ConnectionInfo],
    defaultDatabase: Option[String]
  ): Option[String] =
    connection.map(_.`type`) match {
      case Some(ConnectionType.FS) => None
      case Some(ConnectionType.BQ) =>
        n.database
          .orElse(defaultDatabase)
          .orElse(connection.flatMap(_.options.get("projectId")).filter(_.nonEmpty))
      case _ if n.kind == "load" => n.database.orElse(defaultDatabase)
      case _                     => n.database
    }

  /** Where the node lives, or None for external nodes and nodes without a connection. A connection
    * that cannot be read is treated as an unknown type: a load takes its declared database, else
    * the default one, a transform its declared database. The runner reports that connection's
    * error.
    */
  def locate(
    n: SiteNode,
    connections: String => Option[ConnectionInfo],
    defaultDatabase: Option[String]
  ): Option[PhysicalLocation] =
    if (n.kind == "external") None
    else
      connectionRefOf(n).map { ref =>
        val connection = Try(connections(ref)).toOption.flatten
        PhysicalLocation(
          ref,
          PhysicalTable(databaseOf(n, connection, defaultDatabase), n.domain, n.table)
        )
      }

  /** connectionRef -> physical tables (sorted by key), each with the sorted ids of the nodes that
    * point at it. External nodes and nodes without a connection are skipped.
    */
  def group(
    nodes: Iterable[SiteNode],
    connections: String => Option[ConnectionInfo] = _ => None,
    defaultDatabase: Option[String] = None
  ): Map[String, List[(PhysicalTable, List[String])]] =
    nodes.toList
      .flatMap(n =>
        locate(n, connections, defaultDatabase).map(l => (l.connectionRef, l.table, n.id))
      )
      .groupBy(_._1)
      .map { case (ref, entries) =>
        ref -> entries
          .groupBy(_._2.key)
          .values
          .toList
          .map(es => es.head._2 -> es.map(_._3).distinct.sorted)
          .sortBy(_._1.key)
      }
}
