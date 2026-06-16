/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.config.Settings.{ConnectionInfo, JdbcEngine}
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonTypeName}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

/** Recognized file type format. This will select the correct parser
  *
  * @param value
  *   : NONE, FS, JDBC, BQ, ES One of the possible supported sinks
  */

@JsonSerialize(`using` = classOf[ToStringSerializer])
@JsonDeserialize(`using` = classOf[ConnectionTypeDeserializer])
sealed case class ConnectionType(value: String) {
  override def toString: String = value
}

object ConnectionType {

  def fromString(value: String): ConnectionType = {
    value.toUpperCase match {
      case "SNOWFLAKE_LOG" => ConnectionType.SNOWFLAKE_LOG
      case "GCPLOG"        => ConnectionType.GCPLOG
      case "LOCAL" | "FS" | "FILESYSTEM" | "HIVE" | "DATABRICKS" | "SPARK" => ConnectionType.FS
      case "JDBC"                                                          => ConnectionType.JDBC
      case "BIGQUERY" | "BQ"                                               => ConnectionType.BQ
      case "ES" | "ELASTICSEARCH"                                          => ConnectionType.ES
      case "KAFKA"                                                         => ConnectionType.KAFKA
      case "REST" | "HTTP" | "API"                                         => ConnectionType.REST
      case _ => throw new Exception(s"Unsupported ConnectionType $value")
    }
  }

  object FS extends ConnectionType("FS") // Means databricks or spark
  object BQ extends ConnectionType("BQ")
  object ES extends ConnectionType("ES")
  object KAFKA extends ConnectionType("KAFKA")
  object JDBC extends ConnectionType("JDBC")

  object GCPLOG extends ConnectionType("GCPLOG")

  object SNOWFLAKE_LOG extends ConnectionType("SNOWFLAKE_LOG")

  object REST extends ConnectionType("REST")

  val sinks: Set[ConnectionType] = Set(FS, BQ, ES, KAFKA, JDBC, GCPLOG)
}

class ConnectionTypeDeserializer extends JsonDeserializer[ConnectionType] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): ConnectionType = {
    val value = jp.readValueAs[String](classOf[String])
    ConnectionType.fromString(value)
  }
}

/** Once ingested, files may be sinked to BigQuery, Elasticsearch or any JDBC compliant Database.
  * @param `type`:
  *   Enum
  *   - JDBC : dataset will be sinked to a JDBC Database. See JdbcSink below
  *   - ES : dataset is indexed into Elasticsearch. See EsSink below
  *   - BQ : Dataset is sinked to BigQuery. See BigQuerySink below
  *   - None: Don't sink. This is the default.
  */

sealed abstract class Sink {
  val connectionRef: Option[String]
  def toAllSinks(): AllSinks

  def asMap(jdbcEngine: JdbcEngine)(implicit settings: Settings): Map[String, Object] =
    toAllSinks().asMap(jdbcEngine)

  def getConnectionType()(implicit
    settings: Settings
  ): ConnectionType = {
    getConnection().`type`
  }

  def getConnectionRef()(implicit settings: Settings): String =
    connectionRef.getOrElse(settings.appConfig.connectionRef)

  def getConnection()(implicit
    settings: Settings
  ): ConnectionInfo = {
    val ref = connectionRef.getOrElse(settings.appConfig.connectionRef)
    settings.appConfig.connections(ref)
  }

  @JsonIgnore
  def connectionRefOptions(defaultConnectionName: String)(implicit
    settings: Settings
  ): Map[String, String] = {
    val ref = connectionRef.getOrElse(defaultConnectionName)
    settings.appConfig.connections
      .get(ref)
      .map(_.options)
      .getOrElse(Map.empty)
  }
}

object Sink {
  def xlsfromConnectionType(sinkTypeStr: String): Sink = {
    val sinkType = ConnectionType.fromString(sinkTypeStr)
    sinkType match {
      case ConnectionType.FS => FsSink(connectionRef = Some(sinkTypeStr))
      case ConnectionType.BQ => BigQuerySink(connectionRef = Some(sinkTypeStr))
      case ConnectionType.ES => EsSink(connectionRef = Some(sinkTypeStr))
      case _ => throw new Exception(s"Unsupported creation of SinkType from $sinkType")
    }
  }
}

final case class AllSinks(
  // All sinks
  connectionRef: Option[String] = None,
  // depending on the connection.type coming from the connection ref, some of the options below may be required

  // BigQuery
  sharding: Option[List[String]] = None,
  // partition: Option[List[String]] = None,  // only one column allowed
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None,
  requirePartitionFilter: Option[Boolean] = None,
  materializedView: Option[Materialization] = None,
  enableRefresh: Option[Boolean] = None, // only if materializedView is MATERIALIZED_VIEW
  refreshIntervalMs: Option[Long] = None, // only if enable refresh is true

  // ES
  id: Option[String] = None,
  // partition: Option[List[String]] = None,
  // options: Option[Map[String, String]] = None,

  // FS
  format: Option[String] = None,
  extension: Option[String] = None,
  // clustering: Option[Seq[String]] = None,
  partition: Option[Seq[String]] = None,
  coalesce: Option[Boolean] = None,
  options: Option[Map[String, String]] = None
  // JDBC
  // partition: Option[List[String]] = None, // Only one column allowed

) {

  def this() = this(None)

  def asMap(jdbcEngine: JdbcEngine)(implicit settings: Settings): Map[String, Object] = {
    val map = scala.collection.mutable.Map.empty[String, Object]
    connectionRef.foreach(map += "sinkConnectionRef" -> _)
    sharding.foreach(map += "sinkShardSuffix" -> _.asJava)
    clustering.foreach(map += "sinkClustering" -> _.asJava)
    days.foreach(it => map += "sinkDays" -> it.toString)
    requirePartitionFilter.foreach(map += "sinkRequirePartitionFilter" -> _.toString)
    materializedView.foreach(map += "sinkMaterializedView" -> _.toString)
    enableRefresh.foreach(map += "sinkEnableRefresh" -> _.toString)
    refreshIntervalMs.foreach(map += "sinkRefreshIntervalMs" -> _.toString)
    id.foreach(map += "sinkId" -> _)
    map += "sinkFormat" -> format.getOrElse("parquet") // TODO : default format
    extension.foreach(map += "sinkExtension" -> _)
    partition.foreach(map += "sinkPartition" -> _.asJava)
    coalesce.foreach(it => map += "sinkCoalesce" -> it.toString)
    options.foreach(map += "sinkOptions" -> _.asJava)

    map += "sinkTableOptionsClause"    -> this.getTableOptionsClause(jdbcEngine)
    map += "sinkTablePartitionClause"  -> this.getPartitionByClauseSQL(jdbcEngine).getOrElse("")
    map += "sinkTableClusteringClause" -> this.getClusterByClauseSQL(jdbcEngine)

    map.toMap
  }

  def getFormat()(implicit settings: Settings): String = {
    format.getOrElse(settings.appConfig.defaultWriteFormat)
  }

  @JsonIgnore
  def getConnection()(implicit
    settings: Settings
  ): ConnectionInfo = {
    val ref = connectionRef.getOrElse(settings.appConfig.connectionRef)
    settings.appConfig.connections(ref)
  }

  @JsonIgnore
  def getPartitionByClauseSQL(jdbcEngine: JdbcEngine)(implicit settings: Settings): Option[String] =
    jdbcEngine.partitionBy match {
      case Some(partitionBy) =>
        val connection = this.getConnection()
        // duckdb without ducklake does not support partitioned tables
        if (!connection.isDuckDb() || connection.isDucklake())
          partition.map(_.mkString(s"$partitionBy (", ",", ")"))
        else
          None
      case None => None
    }

  @JsonIgnore
  def getClusterByClauseSQL(jdbcEngine: JdbcEngine): String =
    jdbcEngine.clusterBy match {
      case Some(clusterBy) =>
        clustering.map(_.mkString(s"$clusterBy (", ",", ")")) getOrElse ""
      case None => ""
    }

  @JsonIgnore
  def getTableOptionsClause(jdbcEngine: JdbcEngine): String = {
    val opts = options.getOrElse(Map.empty)
    if (opts.isEmpty) {
      ""
    } else {
      opts.map { case (k, v) => s"'$k'='$v'" }.mkString("OPTIONS(", ",", ")")
    }
  }

  def checkValidity(
    tableName: String,
    table: Option[SchemaInfo]
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    var errors = List.empty[ValidationMessage]

    connectionRef match {
      case None =>
      case Some(ref) =>
        if (!settings.appConfig.connections.contains(ref)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            s"connectionRef in $tableName",
            s"ConnectionRef '$ref' not found in the application.sl.yml file"
          )
        } else {
          val connection = settings.appConfig.connections(ref)
          if (connection.`type` != ConnectionType.BQ) {
            if (this.sharding.nonEmpty) {
              errors = errors :+ ValidationMessage(
                Severity.Error,
                s"sharding in $tableName",
                s"sharding is only supported for BigQuery sinks"
              )
            }
          } else if (connection.`type` == ConnectionType.FS) {
            val defaultConnection = settings.appConfig.connections(settings.appConfig.connectionRef)
            if (defaultConnection.sparkFormat.isEmpty) {
              errors = errors :+ ValidationMessage(
                Severity.Warning,
                s"connectionRef in $tableName",
                s"Even though  the default connection ${settings.appConfig.connectionRef} does not have a sparkFormat defined, spark will be used to execute the request"
              )
            }
          }
        }
    }
    table.foreach { table =>
      this.sharding.getOrElse(Nil).foreach { column =>
        if (!table.attributes.exists(_.getFinalName() == column)) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            s"sharding in $tableName",
            s"Column $column not found in the table"
          )
        }
      }
      // TODO check data type
      /*
        if (
        table
        .attributes(column)
        .primitiveType() != PrimitiveType.string

        ) {
          errors = errors :+ ValidationMessage(
            Severity.Error,
            s"sharding in $tableName",
            s"Column $column should be of type string"
          )
        }
       */

      this.partition.getOrElse(Nil).foreach {
        case column if table.attributes.exists(_.getFinalName() == column) =>
        case column =>
          errors = errors :+ ValidationMessage(
            Severity.Error,
            s"partition in $tableName",
            s"Column $column not found in the table"
          )
      }
    }

    if (errors.isEmpty) {
      Right(true)
    } else {
      Left(errors)
    }
  }

  def merge(child: AllSinks): AllSinks = {
    AllSinks(
      connectionRef = child.connectionRef.orElse(this.connectionRef),
      clustering = child.clustering.orElse(this.clustering),
      days = child.days.orElse(this.days),
      requirePartitionFilter = child.requirePartitionFilter.orElse(this.requirePartitionFilter),
      materializedView = child.materializedView.orElse(this.materializedView),
      enableRefresh = child.enableRefresh.orElse(this.enableRefresh),
      refreshIntervalMs = child.refreshIntervalMs.orElse(this.refreshIntervalMs),
      id = child.id.orElse(this.id),
      format = child.format.orElse(this.format),
      extension = child.extension.orElse(this.extension),
      sharding = child.sharding.orElse(this.sharding),
      partition = child.partition.orElse(this.partition),
      coalesce = child.coalesce.orElse(this.coalesce),
      options = child.options.orElse(this.options)
    )
  }
  def toAllSinks(): AllSinks = this

  @JsonIgnore
  def getConnectionRef()(implicit settings: Settings): String =
    this.connectionRef.getOrElse(settings.appConfig.connectionRef)

  def getSink()(implicit settings: Settings): Sink = {
    val ref = getConnectionRef()
    if (ref.isEmpty) {
      throw new Exception("No connectionRef found")
    }
    val connection = settings.appConfig.connections
      .getOrElse(ref, throw new Exception(s"Could not find connection $ref"))
    val options = this.options.getOrElse(connection.options)
    val allSinksWithOptions = this.copy(options = Some(options), connectionRef = Some(ref))
    val connResult =
      connection.`type` match {
        case ConnectionType.SNOWFLAKE_LOG => SnowflakeLogSink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.GCPLOG        => GcpLogSink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.FS            => FsSink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.JDBC          => JdbcSink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.BQ            => BigQuerySink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.ES            => EsSink.fromAllSinks(allSinksWithOptions)
        case ConnectionType.KAFKA         => KafkaSink.fromAllSinks(allSinksWithOptions)
        case _ => throw new Exception(s"Unsupported SinkType sink type ${connection.`type`}")

      }
    connResult
  }
}

/** When the sink *type* field is set to BQ, the options below should be provided.
  * @param location
  *   : Database location (EU, US, ...)
  * @param clustering:
  *   List of ordered columns to use for table clustering
  * @param days:
  *   Number of days before this table is set as expired and deleted. Never by default.
  * @param requirePartitionFilter:
  *   Should be require a partition filter on every request ? No by default.
  */
final case class BigQuerySink(
  connectionRef: Option[String] = None,
  sharding: Option[List[String]] = None,
  partition: Option[Seq[String]] = None,
  clustering: Option[Seq[String]] = None,
  days: Option[Int] = None,
  requirePartitionFilter: Option[Boolean] = None,
  materialization: Option[Materialization] = None,
  enableRefresh: Option[Boolean] = None,
  refreshIntervalMs: Option[Long] = None
) extends Sink {
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef,
      sharding,
      clustering,
      days,
      requirePartitionFilter,
      materialization,
      enableRefresh,
      refreshIntervalMs,
      partition = partition
    )
  }

  @JsonIgnore
  def getPartitionColumn(): Option[String] = this.partition.flatMap(_.headOption)
}

object BigQuerySink {
  def fromAllSinks(allSinks: AllSinks): BigQuerySink = {
    BigQuerySink(
      connectionRef = allSinks.connectionRef,
      sharding = allSinks.sharding,
      partition = allSinks.partition,
      clustering = allSinks.clustering,
      days = allSinks.days,
      requirePartitionFilter = allSinks.requirePartitionFilter,
      materialization = allSinks.materializedView,
      enableRefresh = allSinks.enableRefresh,
      refreshIntervalMs = allSinks.refreshIntervalMs
    )
  }
}

/** When the sink *type* field is set to ES, the options below should be provided. Elasticsearch
  * options are specified in the application.conf file.
  * @param id:
  *   Attribute to use as id of the document. Generated by Elasticseach if not specified.
  * @param timestamp:
  *   Timestamp field format as expected by Elasticsearch ("{beginTs|yyyy.MM.dd}" for example).
  */
case class EsSink(
  connectionRef: Option[String] = None,
  id: Option[String] = None,
  timestamp: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      partition = timestamp.map(List(_)),
      id = id,
      options = options
    )
  }
}

object EsSink {
  def fromAllSinks(allSinks: AllSinks): EsSink = {
    EsSink(
      allSinks.connectionRef,
      allSinks.id,
      allSinks.partition.flatMap(_.headOption),
      allSinks.options
    )
  }
}
// We had to set format and extension outside options because of the bug below
// https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjo9qr3v4PxAhWNohQKHfh1CqoQFjAAegQIAhAD&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F218&usg=AOvVaw02niMBgrqd-BWw7-e1YQfc
case class FsSink(
  connectionRef: Option[String] = None,
  format: Option[String] = None,
  extension: Option[String] = None,
  clustering: Option[Seq[String]] = None,
  partition: Option[Seq[String]] = None,
  coalesce: Option[Boolean] = None,
  options: Option[Map[String, String]] = None,
  path: Option[String] = None
) extends Sink {

  private lazy val xlsOptions: Map[String, String] = options
    .getOrElse(Map.empty)
    .filter { case (k, _) => k.startsWith("xls:") }
    .map { case (k, v) => k.split(":").last -> v }

  lazy val sheetName: Option[String] = xlsOptions.get("sheetName")

  lazy val startCell: Option[String] = xlsOptions.get("startCell")

  lazy val template: Option[String] = xlsOptions.get("template")

  private lazy val csvOptions: Map[String, String] = options
    .getOrElse(Map.empty)
    .filter { case (k, _) => k.startsWith("csv:") }
    .map { case (k, v) => k.split(":").last -> v }

  lazy val withHeader: Option[Boolean] =
    csvOptions
      .get("withHeader")
      .orElse(this.getOptions().get("withHeader"))
      .map(_.toLowerCase == "true")

  lazy val delimiter: Option[String] =
    csvOptions
      .get("delimiter")
      .orElse(csvOptions.get("separator"))
      .orElse(this.getOptions().get("delimiter"))
      .orElse(this.getOptions().get("separator"))

  val finalPath: Option[String] = path.orElse(xlsOptions.get("path")).orElse(csvOptions.get("path"))

  def getStorageFormat()(implicit settings: Settings): String = {
    if (isExport())
      "csv"
    else
      format.getOrElse(settings.appConfig.defaultWriteFormat)
  }

  def sparkStorageOptions(): Map[String, String] = {
    getOptions() +
    ("delimiter"  -> delimiter.getOrElse("µ")) +
    ("withHeader" -> "false")
  }

  def isExport(): Boolean = {
    val format = this.format.getOrElse("")
    val exportFormats = Set("csv", "xls")
    exportFormats.contains(format)
  }

  // Get the partition clause for Spark SQL only
  def sparkPartitionByClauseSQL(): String =
    partition.map(_.mkString("PARTITIONED BY (", ",", ")")) getOrElse ""

  // Get the cluster clause for Spark SQL only
  def sparkClusterByClauseSQL(): String =
    clustering.map(_.mkString("CLUSTERED BY (", ",", ")")) getOrElse ""

  // Get the options clause for Spark SQL only
  def sparkTableOptionsClause(): String = {
    val opts = getOptions()
    if (opts.isEmpty) {
      ""
    } else {
      opts.map { case (k, v) => s"'$k'='$v'" }.mkString("OPTIONS(", ",", ")")
    }
  }

  /** Get the options for the sink that are not specific to the format (e.g. csv:, xls:)
    * @return
    */
  def getOptions(): Map[String, String] =
    options.getOrElse(Map.empty).filterNot { case (k, _) => k.contains(":") }

  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      format = format,
      extension = extension,
      clustering = clustering,
      partition = partition,
      coalesce = coalesce,
      options = options
    )
  }
}

case class GcpLogSink(
  connectionRef: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      options = options
    )
  }
}
case class SnowflakeLogSink(
  connectionRef: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      options = options
    )
  }
}

object SnowflakeLogSink {
  def fromAllSinks(allSinks: AllSinks): SnowflakeLogSink = {
    SnowflakeLogSink(
      allSinks.connectionRef,
      allSinks.options
    )
  }
}

case class KafkaSink(
  connectionRef: Option[String] = None,
  options: Option[Map[String, String]] = None
) extends Sink {
  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      options = options
    )
  }
}

object KafkaSink {
  def fromAllSinks(allSinks: AllSinks): KafkaSink = {
    KafkaSink(
      allSinks.connectionRef,
      allSinks.options
    )
  }
}

object GcpLogSink {
  def fromAllSinks(allSinks: AllSinks): GcpLogSink = {
    GcpLogSink(
      allSinks.connectionRef,
      allSinks.options
    )
  }
}

object FsSink {
  def fromAllSinks(allSinks: AllSinks): FsSink = {
    FsSink(
      connectionRef = allSinks.connectionRef,
      format = allSinks.format,
      extension = allSinks.extension,
      clustering = allSinks.clustering,
      partition = allSinks.partition,
      coalesce = allSinks.coalesce,
      options = allSinks.options
    )
  }
}

/** When the sink *type* field is set to JDBC, the options below should be provided.
  * @param connectionRef:
  *   Connection String
  * @param partitions:
  *   Number of Spark partitions
  * @param batchsize:
  *   Batch size of each JDBC bulk insert
  */
@JsonTypeName("JDBC")
case class JdbcSink(connectionRef: Option[String] = None, partition: Option[Seq[String]] = None)
    extends Sink {
  def toAllSinks(): AllSinks = {
    AllSinks(
      connectionRef = connectionRef,
      partition = partition
    )
  }
}

object JdbcSink {
  def fromAllSinks(allSinks: AllSinks): JdbcSink = {
    JdbcSink(
      allSinks.connectionRef,
      allSinks.partition
    )
  }
}
