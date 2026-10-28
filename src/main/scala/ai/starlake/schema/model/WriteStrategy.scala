package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.model.Severity._
import ai.starlake.utils.Formatter.RichFormatter
import com.fasterxml.jackson.annotation.JsonIgnore

import java.util.regex.Pattern
import scala.collection.mutable

case class WriteStrategy(
  `type`: Option[WriteStrategyType] = None,
  types: Option[Map[String, String]] = None,
  key: List[String] = Nil,
  timestamp: Option[String] = None,
  queryFilter: Option[String] = None,
  on: Option[MergeOn] = None, // target or both (on source and target
  startTs: Option[String] = None,
  endTs: Option[String] = None
) {

  override def toString: String = {
    val keyStr = key.mkString(",")
    val typeStr = `type`.map(_.toString).getOrElse("")
    val timestampStr = timestamp.getOrElse("")
    val queryFilterStr = queryFilter.getOrElse("")
    val onStr = on.map(_.toString).getOrElse("")
    val startTsStr = startTs.getOrElse("")
    val endTsStr = endTs.getOrElse("")
    s"WriteStrategy(type=$typeStr, key=$keyStr, timestamp=$timestampStr, queryFilter=$queryFilterStr, on=$onStr, startTs=$startTsStr, endTs=$endTsStr)"
  }

  def checkValidity(
    domainName: String,
    table: Option[SchemaInfo]
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val tableName = table.map(_.name).getOrElse("")
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    table match {
      case Some(table) =>
        key.foreach { key =>
          if (
            table.attributes.nonEmpty && !table.attributes.exists(
              _.getFinalName().equalsIgnoreCase(key)
            )
          )
            errorList += ValidationMessage(
              Error,
              "WriteStrategy",
              s"key: '$key' does not exist in table: $tableName"
            )
        }
        timestamp.foreach { timestamp =>
          if (
            table.attributes.nonEmpty && !table.attributes.exists { attr =>
              val primitiveType = attr.primitiveType(settings.schemaHandler())
              primitiveType match {
                case Some(PrimitiveType.timestamp) | Some(PrimitiveType.date) |
                    Some(PrimitiveType.datetime) =>
                  attr.getFinalName().equalsIgnoreCase(timestamp)
                case _ => false
              }
            }
          )
            errorList += ValidationMessage(
              Error,
              "WriteStrategy",
              s"timestamp: '$timestamp' does not exist in table: $tableName"
            )
        }
      case None =>
    }

    val definedTs = Set(startTs, endTs).flatten
    if (definedTs.size == 1)
      errorList += ValidationMessage(
        Error,
        "WriteStrategy",
        s"startTs and endTs must be defined together in table: $tableName"
      )

    this.getEffectiveType() match {
      case WriteStrategyType.UPSERT_BY_KEY =>
        if (key.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"key must be defined for upsert strategy in table: $tableName"
          )
      case WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP =>
        if (key.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"key must be defined for upsert strategy in table: $tableName"
          )
        if (timestamp.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"timestamp must be defined for upsert strategy in table: $tableName"
          )
      case WriteStrategyType.SCD2 =>
        if (key.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"key must be defined for scd2 strategy in table: $tableName"
          )
        if (timestamp.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"timestamp must be defined for scd2 strategy in table: $tableName"
          )
      case WriteStrategyType.OVERWRITE_BY_PARTITION =>
        table.foreach { table =>
          val partition = table.metadata.flatMap(_.sink.flatMap(_.partition)).getOrElse(Nil)
          if (partition.isEmpty)
            errorList += ValidationMessage(
              Error,
              "WriteStrategy",
              s"partition must be defined in the sink section for overwrite by partition strategy in table: $tableName"
            )
        }
      case WriteStrategyType.DELETE_THEN_INSERT =>
        if (key.isEmpty)
          errorList += ValidationMessage(
            Error,
            "WriteStrategy",
            s"key must be defined for delete then insert strategy in table: $tableName"
          )
      case WriteStrategyType.OVERWRITE =>
      case WriteStrategyType.APPEND    =>
      case _                           =>
    }
    if (errorList.isEmpty)
      Right(true)
    else
      Left(errorList.toList)
  }

  @JsonIgnore
  def getEffectiveType(): WriteStrategyType =
    `type`.getOrElse(WriteStrategyType.APPEND)

  @JsonIgnore
  def isMerge(): Boolean =
    !Set(WriteStrategyType.APPEND, WriteStrategyType.OVERWRITE).contains(
      `type`.getOrElse(WriteStrategyType.APPEND)
    )

  def validate(): Unit = {}

  def toWriteMode(): WriteMode = `type`.getOrElse(WriteStrategyType.APPEND).toWriteMode()

  def requireKey(): Boolean = `type`.getOrElse(WriteStrategyType.APPEND).requireKey()

  def requireTimestamp(): Boolean = `type`.getOrElse(WriteStrategyType.APPEND).requireTimestamp()

  def keyCsv(quote: String) = this.key.map(key => s"$quote$key$quote").mkString(",")

  def keyJoinCondition(quote: String, incoming: String, existing: String): String =
    this.key
      .map(key => s"$incoming.$quote$key$quote = $existing.$quote$key$quote")
      .mkString(" AND ")

  @JsonIgnore
  private val lastPat =
    Pattern.compile(".*(in)\\s+last\\(\\s*(\\d+)\\s*(\\)).*", Pattern.DOTALL)

  @JsonIgnore
  private val lastMatcher = lastPat.matcher(queryFilter.getOrElse(""))

  @JsonIgnore
  private val queryFilterContainsLast: Boolean =
    queryFilter.exists { queryFilter =>
      lastMatcher.matches()
    }
  @JsonIgnore
  private val queryFilterContainsLatest: Boolean = queryFilter.exists(_.contains("latest"))

  @JsonIgnore
  private val canOptimizeQueryFilter: Boolean = queryFilterContainsLast || queryFilterContainsLatest

  @JsonIgnore
  private val nbPartitionQueryFilter: Int =
    if (queryFilterContainsLast) lastMatcher.group(2).toInt else -1

  @JsonIgnore
  val lastStartQueryFilter: Int = if (queryFilterContainsLast) lastMatcher.start(1) else -1

  @JsonIgnore
  val lastEndQueryFilter: Int = if (queryFilterContainsLast) lastMatcher.end(3) else -1

  private def formatQuery(
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  )(implicit
    settings: Settings
  ): Option[String] =
    queryFilter.map(_.richFormat(activeEnv, options))

  def buidlBQQuery(
    partitions: List[String],
    options: Map[String, String]
  )(implicit
    settings: Settings
  ): Option[String] = {
    val filteredPartitions = partitions.filter(!_.startsWith("__"))
    (queryFilterContainsLast, queryFilterContainsLatest) match {
      case (true, false)  => buildBQQueryForLast(filteredPartitions, options)
      case (false, true)  => buildBQQueryForLastest(filteredPartitions, options)
      case (false, false) => formatQuery(options)
      case (true, true) =>
        val last = buildBQQueryForLast(filteredPartitions, options)
        this.copy(queryFilter = last).buildBQQueryForLastest(filteredPartitions, options)
    }
  }

  private def buildBQQueryForLastest(
    partitions: List[String],
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  )(implicit
    settings: Settings
  ): Option[String] = {
    val latestPartition = partitions.max
    val queryArgs = formatQuery(activeEnv, options).getOrElse("")
    Some(queryArgs.replace("latest", s"PARSE_DATE('%Y%m%d','$latestPartition')"))
  }

  private def buildBQQueryForLast(
    partitions: List[String],
    activeEnv: Map[String, String] = Map.empty,
    options: Map[String, String] = Map.empty
  )(implicit
    settings: Settings
  ): Option[String] = {
    val sortedPartitions = partitions.sorted
    val (oldestPartition, newestPartition) = if (sortedPartitions.length < nbPartitionQueryFilter) {
      (
        sortedPartitions.headOption.getOrElse("19700101"),
        sortedPartitions.lastOption.getOrElse("19700101")
      )
    } else {
      (
        sortedPartitions(sortedPartitions.length - nbPartitionQueryFilter),
        sortedPartitions.last
      )

    }
    val lastStart = lastStartQueryFilter
    val lastEnd = lastEndQueryFilter
    val queryArgs = formatQuery(activeEnv, options)
    queryArgs.map { queryArgs =>
      queryArgs
        .substring(
          0,
          lastStart
        ) + s"between PARSE_DATE('%Y%m%d','$oldestPartition') and PARSE_DATE('%Y%m%d','$newestPartition')" + queryArgs
        .substring(lastEnd)
    }
  }

  def compare(other: WriteStrategy): ListDiff[Named] =
    AnyRefDiff.diffAnyRef("", this, other)
}

object WriteStrategy {
  val Overwrite = WriteStrategy(Some(WriteStrategyType.OVERWRITE))
  val Append = WriteStrategy(Some(WriteStrategyType.APPEND))
}
