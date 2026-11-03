package ai.starlake.schema.model

import ai.starlake.config.Settings
import ai.starlake.schema.model.Severity._
import ai.starlake.utils.Formatter.RichFormatter
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

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
  endTs: Option[String] = None,
  startTsType: Option[String] = None,
  endTsType: Option[String] = None
) {

  override def toString: String = {
    val keyStr = key.mkString(",")
    val typeStr = `type`.map(_.toString).getOrElse("")
    val timestampStr = timestamp.getOrElse("")
    val queryFilterStr = queryFilter.getOrElse("")
    val onStr = on.map(_.toString).getOrElse("")
    val startTsStr = startTs.getOrElse("")
    val endTsStr = endTs.getOrElse("")
    val startTsTypeStr = startTsType.getOrElse("")
    val endTsTypeStr = endTsType.getOrElse("")
    s"WriteStrategy(type=$typeStr, key=$keyStr, timestamp=$timestampStr, queryFilter=$queryFilterStr, on=$onStr, startTs=$startTsStr, endTs=$endTsStr, startTsType=$startTsTypeStr, endTsType=$endTsTypeStr)"
  }

  /** The names of the SCD2 validity columns: `startTs` and `endTs`, or the global
    * `scd2StartTimestamp` and `scd2EndTimestamp`
    */
  @JsonIgnore
  def validityColumns()(implicit settings: Settings): (String, String) =
    (
      startTs.getOrElse(settings.appConfig.scd2StartTimestamp),
      endTs.getOrElse(settings.appConfig.scd2EndTimestamp)
    )

  /** This strategy, whose SCD2 validity columns take the type of the attributes that declare them,
    * matched on their name in the table whatever its case. A declared type wins over `startTsType`
    * and `endTsType`, which `validate` checks against it, when it is a date or a timestamp type of
    * the project; another one is ignored, as `validate` warns (#1885).
    */
  def withDeclaredValidityTypes(
    attributes: List[TableAttribute]
  )(implicit settings: Settings): WriteStrategy = {
    lazy val types = settings.schemaHandler().types()
    def declared(column: String): Option[String] =
      attributes
        .find(_.getFinalName().equalsIgnoreCase(column))
        .flatMap(attribute => Option(attribute.`type`))
        .filter(name => types.exists(tpe => tpe.name == name && WriteStrategy.isValidityType(tpe)))
    val (start, end) = validityColumns()
    copy(
      startTsType = declared(start).orElse(startTsType),
      endTsType = declared(end).orElse(endTsType)
    )
  }

  /** The primitive types of the SCD2 validity columns, from `startTsType` and `endTsType`: `date`
    * or `timestamp`. A missing type is a timestamp, as is a type that is unknown or of another
    * primitive type, which `validate` reports (#1885).
    */
  @JsonIgnore
  def validityColumnTypes()(implicit settings: Settings): (PrimitiveType, PrimitiveType) = {
    lazy val types = settings.schemaHandler().types()
    def primitive(typeName: Option[String]): PrimitiveType =
      typeName
        .flatMap(name => types.find(_.name == name))
        .map(_.primitiveType)
        .filter(_ == PrimitiveType.date)
        .getOrElse(PrimitiveType.timestamp)
    (primitive(startTsType), primitive(endTsType))
  }

  /** @param taskAttributes
    *   the attributes of a transform task, which declare the types of its validity columns. Those
    *   of the table serve otherwise.
    */
  def checkValidity(
    domainName: String,
    table: Option[SchemaInfo],
    taskAttributes: List[TableAttribute] = Nil
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
        errorList ++= checkValidityColumnTypes(
          tableName,
          if (taskAttributes.nonEmpty) taskAttributes else table.map(_.attributes).getOrElse(Nil)
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

  /** The types of the SCD2 validity columns (#1885):
    *   - a `startTsType` or `endTsType` names a date or a timestamp type of the project, and does
    *     not contradict the declared type of the same column: errors;
    *   - a column declared with another type, or with a type unknown to the project: a warning,
    *     since the declaration is then ignored.
    */
  private def checkValidityColumnTypes(
    tableName: String,
    attributes: List[TableAttribute]
  )(implicit settings: Settings): List[ValidationMessage] = {
    val types = settings.schemaHandler().types()
    val (start, end) = validityColumns()
    List(("startTsType", start, startTsType), ("endTsType", end, endTsType)).flatMap {
      case (property, column, named) =>
        val namedType = named.map(name => name -> types.find(_.name == name))
        val declared = attributes
          .find(_.getFinalName().equalsIgnoreCase(column))
          .flatMap(attribute => Option(attribute.`type`))
          .map(name => name -> types.find(_.name == name))
        val namedErrors = namedType.collect {
          case (name, None) =>
            ValidationMessage(
              Error,
              "WriteStrategy",
              s"$property: '$name' is not a type of the project in table: $tableName"
            )
          case (name, Some(tpe)) if !WriteStrategy.isValidityType(tpe) =>
            ValidationMessage(
              Error,
              "WriteStrategy",
              s"$property: '$name' is not a date or a timestamp type in table: $tableName"
            )
        }
        val declaredWarnings = declared.collect {
          case (name, tpe) if !tpe.exists(WriteStrategy.isValidityType) =>
            ValidationMessage(
              Severity.Warning,
              "WriteStrategy",
              s"SCD2 validity column $column: its declared type '$name' is not a date or a timestamp type of the project, so it is ignored in table: $tableName"
            )
        }
        val contradiction = (namedType, declared) match {
          case (Some((name, Some(named))), Some((declaredName, Some(declared))))
              if WriteStrategy.isValidityType(named) && WriteStrategy.isValidityType(declared) &&
              named.primitiveType != declared.primitiveType =>
            Some(
              ValidationMessage(
                Error,
                "WriteStrategy",
                s"$property: '$name' contradicts the type '$declaredName' of the declared column $column in table: $tableName"
              )
            )
          case _ => None
        }
        namedErrors.toList ++ declaredWarnings ++ contradiction
    }
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

  /** The column type of an SCD2 validity column for an engine or a strategy builder: `DATE`, or
    * `TIMESTAMP`, written `DATETIME2` on SQL Server, where `TIMESTAMP` is a row version (#1885)
    */
  def validityColumnDdl(primitiveType: PrimitiveType, engine: String): String =
    if (primitiveType == PrimitiveType.date) "DATE"
    else if (engine.equalsIgnoreCase("sqlserver")) "DATETIME2"
    else "TIMESTAMP"

  /** Whether a type can type an SCD2 validity column: a date or a timestamp */
  def isValidityType(tpe: Type): Boolean =
    tpe.primitiveType == PrimitiveType.date || tpe.primitiveType == PrimitiveType.timestamp

  /** The Spark type of an SCD2 validity column */
  def validitySparkType(primitiveType: PrimitiveType): DataType =
    if (primitiveType == PrimitiveType.date) DateType else TimestampType
}
