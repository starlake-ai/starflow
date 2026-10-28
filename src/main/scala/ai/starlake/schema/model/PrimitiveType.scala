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

import ai.starlake.extract.JdbcDbUtils.SqlColumn
import ai.starlake.schema.handlers.SchemaHandler
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.*

import java.sql.{Date, Timestamp}
import java.text.{DecimalFormat, NumberFormat, SimpleDateFormat}
import java.time.*
import java.time.format.DateTimeFormatter.*
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, ChronoUnit, TemporalAccessor, TemporalQueries}
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern
import java.util.{Base64, Locale, TimeZone}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Spark supported primitive types. These are the only valid raw types. Dataframes columns are
  * converted to these types before the dataset is ingested
  *
  * @param value
  *   : string, long, double, boolean, byte, date, timestamp, decimal with (precision=30, scale=15)
  */
@JsonSerialize(`using` = classOf[ToStringSerializer])
@JsonDeserialize(`using` = classOf[PrimitiveTypeDeserializer])
sealed abstract case class PrimitiveType(value: String) {
  def fromString(str: String, pattern: String, zone: Option[String] = None): Any

  override def toString: String = value

  def sparkType(zone: Option[String]): DataType

}

class PrimitiveTypeDeserializer extends JsonDeserializer[PrimitiveType] {

  def simpleTypeFromString(value: String): PrimitiveType = {
    value match {
      case "string"    => PrimitiveType.string
      case "long"      => PrimitiveType.long
      case "int"       => PrimitiveType.int
      case "short"     => PrimitiveType.short
      case "double"    => PrimitiveType.double
      case "boolean"   => PrimitiveType.boolean
      case "byte"      => PrimitiveType.byte
      case "date"      => PrimitiveType.date
      case "timestamp" => PrimitiveType.timestamp
      case "decimal"   => PrimitiveType.decimal
      case "struct"    => PrimitiveType.struct
      case "variant"   => PrimitiveType.variant
      case "datetime"  => PrimitiveType.datetime
      case "time"      => PrimitiveType.time
      case "binary"    => PrimitiveType.binary
      case _ =>
        throw new Exception(
          s"Invalid primitive type: $value not in ${PrimitiveType.primitiveTypes}"
        )
    }
  }

  override def deserialize(jp: JsonParser, ctx: DeserializationContext): PrimitiveType = {
    val value = jp.readValueAs[String](classOf[String])
    simpleTypeFromString(value)
  }
}

object PrimitiveType {

  object string extends PrimitiveType("string") {
    def fromString(str: String, pattern: String, zone: Option[String]): Any = str

    def sparkType(zone: Option[String]): DataType = StringType
  }

  object variant extends PrimitiveType("variant") {
    def fromString(str: String, pattern: String, zone: Option[String]): Any = str

    def sparkType(zone: Option[String]): DataType = StringType // VarcharType(Int.MaxValue)
  }

  object long extends PrimitiveType("long") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toLong

    def sparkType(zone: Option[String]): DataType = LongType
  }

  object int extends PrimitiveType("int") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toInt

    def sparkType(zone: Option[String]): DataType = IntegerType
  }

  object short extends PrimitiveType("short") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.trim.toShort

    def sparkType(zone: Option[String]): DataType = ShortType
  }

  object double extends PrimitiveType("double") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, zone)
      }
    }

    private def typedConvert(str: String, zone: Option[String]) = {
      zone match {
        case Some(zone) =>
          val locale = zone.split('_')
          val currentLocale: Locale = new Locale(locale(0), locale(1))
          val numberFormatter =
            NumberFormat.getNumberInstance(currentLocale).asInstanceOf[DecimalFormat]
          if (str.head == '+')
            numberFormatter.setPositivePrefix("+")
          numberFormatter.parse(str.trim).doubleValue()
        case None => str.trim.toDouble
      }
    }

    def sparkType(zone: Option[String]): DataType = DoubleType

    def parseUDF(zone: Option[String]) = {
      udf((input: String) => {
        Option(input).flatMap(str => {
          Try(typedConvert(str, zone)).toOption
        })
      }).withName("as_double")
    }
  }

  object decimal extends PrimitiveType("decimal") {
    val defaultDecimalType = DataTypes.createDecimalType(38, 9)
    var decimals: mutable.Map[String, DecimalType] = mutable.Map.empty

    /** The largest precision of a Spark decimal */
    val maxSparkPrecision: Int = DecimalType.MAX_PRECISION

    /** @return
      *   the precision and scale of a decimal zone, "precision,scale", if it is one
      */
    def precisionAndScale(zone: String): Option[(Int, Int)] =
      zone.split(",").map(_.trim) match {
        case Array(precision, scale) =>
          for {
            precision <- precision.toIntOption
            scale     <- scale.toIntOption
          } yield (precision, scale)
        case _ => None
      }

    /** A decimal type whose zone is above the precision of Spark */
    def isWide(tpe: Type): Boolean =
      tpe.primitiveType == decimal && tpe.zone
        .flatMap(precisionAndScale)
        .exists(_._1 > maxSparkPrecision)

    /** @return
      *   why the precision and scale of a decimal zone are not valid: a scale between 0 and the
      *   precision, and, above the precision 38 of Spark, the bounds of a BigQuery BIGNUMERIC
      */
    def zoneError(zone: String): Option[String] =
      precisionAndScale(zone) match {
        case None =>
          Some(s"$zone is not a precision and a scale separated by a comma")
        // Spark reads the zone of a decimal without spaces only
        case Some(_) if zone.exists(_.isWhitespace) =>
          Some(s"$zone must be written without spaces")
        case Some((precision, scale)) if precision < 1 || scale < 0 || scale > precision =>
          Some(s"$zone must have a precision of at least 1 and a scale between 0 and the precision")
        case Some((precision, scale))
            if precision > maxSparkPrecision && (scale > 38 || precision > scale + 38) =>
          Some(
            s"$zone is above the precision $maxSparkPrecision of Spark, and out of the bounds of a BigQuery BIGNUMERIC: a scale of at most 38 and a precision of at most the scale + 38"
          )
        case _ => None
      }
    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else BigDecimal(str.trim)

    override def sparkType(zone: Option[String]): DataType = {
      zone match {
        case None =>
          defaultDecimalType
        case Some(zone) =>
          // A zone with spaces keeps the default decimal, as it always did: the Spark types of
          // existing tables do not change
          Try {
            val precisionScale = zone.split(",")
            val precision = precisionScale(0).toInt
            val scale = precisionScale(1).toInt
            val key = s"$precision/$scale"
            decimals.get(key) match {
              case None =>
                val newDecimalType = DataTypes.createDecimalType(precision, scale)
                decimals.put(key, newDecimalType)
                newDecimalType
              case Some(decimalType) => decimalType
            }
          } match {
            case Success(res) => res
            case Failure(_)   => defaultDecimalType
          }
      }
    }
  }

  object boolean extends PrimitiveType("boolean") {

    def matches(str: String, truePattern: Pattern, falsePattern: Pattern): Boolean = {
      truePattern
        .matcher(str)
        .matches() ||
      falsePattern
        .matcher(str)
        .matches()
    }

    private def getPatterns(pattern: String) = {
      if (pattern.indexOf("<-TF->") >= 0) {
        val tf = pattern.split("<-TF->")
        val (truePattern, falsePattern) = (
          Pattern
            .compile(tf(0), Pattern.DOTALL),
          Pattern
            .compile(tf(1), Pattern.DOTALL)
        )
        truePattern -> falsePattern
      } else {
        throw new Exception(
          s"Boolean pattern must have '<-TF->' in its pattern $pattern to validate input and choose between true or false"
        )
      }
    }

    private def resolveBoolean(
      truePattern: Pattern,
      falsePattern: Pattern,
      value: String,
      trim: Boolean
    ): Option[Boolean] = {
      val str = if (trim) {
        value.trim
      } else {
        value
      }
      if (truePattern.matcher(str.trim).matches())
        Some(true)
      else if (falsePattern.matcher(str.trim).matches())
        Some(false)
      else
        None
    }

    def fromString(str: String, pattern: String, zone: Option[String]) = {
      if (str == null || str.isEmpty)
        null
      else {
        val (truePattern, falsePattern) = getPatterns(pattern)
        resolveBoolean(truePattern, falsePattern, str, trim = true).getOrElse(
          throw new Exception(s"value $str does not match $pattern")
        )
      }
    }

    def sparkType(zone: Option[String]): DataType = BooleanType

    def parseUDF(pattern: String) = {
      val (truePattern, falsePattern) = getPatterns(pattern)
      udf((str: String) => {
        Option(str).flatMap { str =>
          resolveBoolean(truePattern, falsePattern, str, trim = false)
        }
      }).withName("as_boolean")
    }
  }

  object byte extends PrimitiveType("byte") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.head.toByte

    def sparkType(zone: Option[String]): DataType = ByteType

    // Could not find the equivalent spark sql expression
    def parseUDF =
      udf((str: String) => Option(str).flatMap(_.headOption).flatMap(c => Try(c.toByte).toOption))
        .withName("as_byte")
  }

  object struct extends PrimitiveType("struct") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else str.toByte

    def sparkType(zone: Option[String]): DataType = new StructType(Array.empty[StructField])
  }

  private def instantFromString(str: String, pattern: String, zoneId: ZoneId): Instant = {

    def simpleDateFormat(str: String, pattern: String, zoneId: ZoneId): Instant = {
      if (zoneId.getId != "UTC")
        throw new IllegalArgumentException(
          s"Explicit zoneId $zoneId not supported for pattern $pattern"
        )
      val df = new SimpleDateFormat(pattern)
      df.setTimeZone(TimeZone.getTimeZone("UTC"))
      val date = df.parse(str.trim)
      Instant.ofEpochMilli(date.getTime)
    }

    pattern match {
      case "epoch_second" =>
        Instant.ofEpochSecond(str.toLong)
      case "epoch_milli" =>
        Instant.ofEpochMilli(str.toLong)
      case _ =>
        val formatter = PrimitiveType.dateFormatters
          .getOrElse(pattern, DateTimeFormatter.ofPattern(pattern))
        Try {
          formatter.parse(str)
        } match {
          case Success(dateTime: TemporalAccessor) =>
            Try(Instant.from(dateTime)) match {
              case Success(instant) =>
                Try {
                  ZoneId.from(dateTime)
                } match {
                  case Success(zone) =>
                    val dateTimeRawOffset = TimeZone.getTimeZone(zone.normalized).getRawOffset
                    val zoneIdRawOffset = TimeZone.getTimeZone(zoneId.normalized).getRawOffset

                    if (zoneId.getId == "UTC" || dateTimeRawOffset == zoneIdRawOffset)
                      instant
                    else
                      throw new IllegalArgumentException(
                        s"Incompatible timezones found in (pattern zone = $zone, configuration zone = $zoneId)"
                      )
                  case Failure(_) =>
                    if (zoneId.getId == "UTC")
                      instant
                    else
                      throw new IllegalArgumentException(
                        s"Incompatible timezones found in (pattern zone = UTC, configuration zone = $zoneId)"
                      )
                }

              case Failure(_) =>
                // Try to parse it as a date without time and still make it a timestamp.
                Try {
                  val localDateTime = LocalDateTime.from(dateTime)
                  ZonedDateTime.of(localDateTime, zoneId).toInstant
                } match {
                  case Success(instant) => instant
                  case Failure(_) =>
                    Try(LocalDate.from(dateTime)) match {
                      case Success(date) =>
                        val zone = Try {
                          ZoneId.from(dateTime)
                        } match {
                          case Success(z)
                              if zoneId.getId == "UTC" | z.normalized.getId == zoneId.normalized.getId =>
                            z
                          case Success(z) =>
                            throw new IllegalArgumentException(
                              s"Incompatible timezones found in (pattern zone = $z, configuration zone = $zoneId)"
                            )
                          case Failure(_) => zoneId
                        }
                        date.atStartOfDay(zone).toInstant
                      case Failure(_) => simpleDateFormat(str, pattern, zoneId)
                    }
                }
            }
          case Failure(_) =>
            // Try to parse it as a date without time and still make it a timestamp.
            // Cloudera 5.X with Hive 1.1 workaround
            // Compatible with time without date
            simpleDateFormat(str, pattern, zoneId)
        }
    }
  }

  object date extends PrimitiveType("date") {

    override def fromString(str: String, pattern: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, pattern, zone)
      }
    }

    private def typedConvert(str: String, pattern: String, zone: Option[String]): Date = {
      val formatter = zone match {
        case None => DateTimeFormatter.ofPattern(pattern)
        case Some(zone) =>
          val locale = zone.split('_')
          val currentLocale: Locale = new Locale(locale(0), locale(1))
          new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern(pattern)
            .toFormatter
            .withLocale(currentLocale)
      }
      Try {
        val date = LocalDate.parse(str.trim, formatter)
        java.sql.Date.valueOf(date)
      }.getOrElse(
        java.sql.Date.valueOf(YearMonth.parse(str.trim, formatter).atDay(1))
      )
    }

    def sparkType(zone: Option[String]): DataType = DateType

    def parseUDF(pattern: String, zone: Option[String]) =
      udf((str: String) =>
        Option(str).flatMap { str =>
          Try(typedConvert(str, pattern, zone)).toOption
        }
      ).withName("as_date")
  }

  object timestamp extends PrimitiveType("timestamp") {

    def fromString(str: String, timeFormat: String, zone: Option[String]): Any = {
      if (str == null || str.isEmpty)
        null
      else {
        typedConvert(str, timeFormat, zone)
      }
    }

    private def typedConvert(str: String, timeFormat: String, zone: Option[String]): Timestamp = {
      val zoneId = zone match {
        case Some(z) => ZoneId.of(z)
        case None    => ZoneId.of("UTC")
      }

      val instant = instantFromString(str.trim, timeFormat, zoneId)
      val tsValue = Timestamp.from(instant)
      tsValue
    }

    def sparkType(zone: Option[String]): DataType = TimestampType

    def parseUDF(timeFormat: String, zone: Option[String]) =
      udf((str: String) =>
        Option(str).flatMap { str =>
          Try(typedConvert(str, timeFormat, zone)).toOption
        }
      ).withName("as_timestamp")
  }

  /** The formatters of the patterns that are not a predefined formatter name, by pattern.
    * DateTimeFormatter is not serializable: each JVM builds its own.
    */
  private val patternFormatters = new ConcurrentHashMap[String, DateTimeFormatter]()

  private def formatterOf(pattern: String, predefined: Map[String, DateTimeFormatter]) =
    predefined.getOrElse(
      pattern,
      patternFormatters.computeIfAbsent(pattern, pattern => DateTimeFormatter.ofPattern(pattern))
    )

  private val timeFields = List(
    ChronoField.HOUR_OF_DAY,
    ChronoField.CLOCK_HOUR_OF_AMPM,
    ChronoField.HOUR_OF_AMPM,
    ChronoField.MINUTE_OF_HOUR
  )

  /** A civil date and time, read and written without a time zone */
  private def hasZone(parsed: TemporalAccessor): Boolean =
    parsed.isSupported(ChronoField.OFFSET_SECONDS) || parsed.query(TemporalQueries.zoneId()) != null

  /** Date and time without time zone, such as the BigQuery DATETIME type. The pattern is a
    * DateTimeFormatter pattern or the name of a formatter without offset, see
    * [[datetimeFormatters]]. A date without time is at the start of the day. A value with an offset
    * or a time zone, or out of the years 1 to 9999 of the SQL date and time types, is rejected.
    */
  object datetime extends PrimitiveType("datetime") {

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else typedConvert(str, pattern)

    private def typedConvert(str: String, pattern: String): LocalDateTime = {
      val parsed = formatterOf(pattern, datetimeFormatters).parse(str.trim)
      if (hasZone(parsed))
        throw new IllegalArgumentException(s"$str has a time zone: a datetime has none")
      // A value without time is at the start of its day, a value with a time must give it
      val dateTime =
        if (timeFields.exists(parsed.isSupported)) LocalDateTime.from(parsed)
        else LocalDate.from(parsed).atStartOfDay()
      if (dateTime.getYear < 1 || dateTime.getYear > 9999)
        throw new IllegalArgumentException(s"$str is not between the years 1 and 9999")
      dateTime
    }

    /** Fails when the pattern cannot read the date and time it writes: it has an offset, a time
      * zone, or no date
      */
    def checkPattern(pattern: String): Unit = {
      val formatter = formatterOf(pattern, datetimeFormatters)
      val written = formatter.format(LocalDateTime.of(2001, 2, 3, 4, 5, 6))
      require(
        formatter.format(typedConvert(written, pattern)) == written,
        s"$pattern does not read the date and time it writes"
      )
    }

    def sparkType(zone: Option[String]): DataType = TimestampNTZType

    def parseUDF(pattern: String) =
      udf((str: String) => Option(str).flatMap(str => Try(typedConvert(str, pattern)).toOption))
        .withName("as_datetime")
  }

  /** Time of day without time zone, such as the BigQuery TIME type. Spark has no time type: the
    * value is the text HH:mm:ss, followed by the microseconds when they are not 0. The pattern is a
    * DateTimeFormatter pattern or the name of a formatter without offset, see [[timeFormatters]].
    * Digits beyond the microsecond are dropped. A value with an offset is rejected.
    */
  object time extends PrimitiveType("time") {
    private val withoutFraction = DateTimeFormatter.ofPattern("HH:mm:ss")
    private val withMicros = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else typedConvert(str, pattern)

    private def typedConvert(str: String, pattern: String): String = {
      val parsed = formatterOf(pattern, timeFormatters).parse(str.trim)
      if (hasZone(parsed))
        throw new IllegalArgumentException(s"$str has a time zone: a time has none")
      val time = LocalTime.from(parsed).truncatedTo(ChronoUnit.MICROS)
      (if (time.getNano == 0) withoutFraction else withMicros).format(time)
    }

    def sparkType(zone: Option[String]): DataType = StringType

    /** Fails when the pattern cannot read the time it writes: it has an offset, a date, or no hour
      * of the day
      */
    def checkPattern(pattern: String): Unit = {
      val _ =
        typedConvert(formatterOf(pattern, timeFormatters).format(LocalTime.of(4, 5, 6)), pattern)
    }

    def parseUDF(pattern: String) =
      udf((str: String) => Option(str).flatMap(str => Try(typedConvert(str, pattern)).toOption))
        .withName("as_time")
  }

  /** Sequence of bytes, such as the BigQuery BYTES type. The pattern is the text encoding of the
    * bytes: base64, as BigQuery exports them, or hex.
    */
  object binary extends PrimitiveType("binary") {
    val encodings: Set[String] = Set("base64", "hex")

    def fromString(str: String, pattern: String, zone: Option[String]): Any =
      if (str == null || str.isEmpty) null else typedConvert(str, pattern)

    private def typedConvert(str: String, pattern: String): Array[Byte] =
      pattern.toLowerCase(Locale.ROOT) match {
        // Line breaks, such as those of MIME or XML, are not part of the bytes
        case "base64" => Base64.getDecoder.decode(str.replaceAll("\\s", ""))
        case "hex" =>
          val hex = str.trim
          require(
            hex.length % 2 == 0 && hex.forall(c => Character.digit(c, 16) >= 0 && c < 128),
            s"$hex is not an even number of hexadecimal digits"
          )
          hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
        case _ =>
          throw new IllegalArgumentException(
            s"Unknown binary encoding $pattern, expected one of ${encodings.mkString(", ")}"
          )
      }

    def sparkType(zone: Option[String]): DataType = BinaryType

    def parseUDF(pattern: String) =
      udf((str: String) => Option(str).flatMap(str => Try(typedConvert(str, pattern)).toOption))
        .withName("as_binary")
  }

  val primitiveTypes: Set[PrimitiveType] =
    Set(
      string,
      long,
      int,
      double,
      short,
      decimal,
      boolean,
      byte,
      date,
      timestamp,
      struct,
      variant,
      datetime,
      time,
      binary
    )

  /** A date, a space or a T, and a time with optional seconds and fraction: the text of the SQL
    * date and time types, 2019-12-31 23:59:02.123456 for example, and of ISO_LOCAL_DATE_TIME
    */
  private lazy val sqlDateTime: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .append(ISO_LOCAL_DATE)
      .optionalStart()
      .appendLiteral(' ')
      .optionalEnd()
      .optionalStart()
      .appendLiteral('T')
      .optionalEnd()
      .append(ISO_LOCAL_TIME)
      .toFormatter(Locale.ROOT)
      .withResolverStyle(ResolverStyle.STRICT)
      .withChronology(IsoChronology.INSTANCE)

  /** The formatters of a datetime pattern: SQL_DATE_TIME, and those of [[dateFormatters]] that
    * never require an offset or a zone. The others accept an optional one, which the value must not
    * have.
    */
  lazy val datetimeFormatters: Map[String, DateTimeFormatter] = Map(
    "SQL_DATE_TIME"       -> sqlDateTime,
    "BASIC_ISO_DATE"      -> BASIC_ISO_DATE,
    "ISO_LOCAL_DATE"      -> ISO_LOCAL_DATE,
    "ISO_DATE"            -> ISO_DATE,
    "ISO_LOCAL_DATE_TIME" -> ISO_LOCAL_DATE_TIME,
    "ISO_DATE_TIME"       -> ISO_DATE_TIME,
    "ISO_ORDINAL_DATE"    -> ISO_ORDINAL_DATE,
    "ISO_WEEK_DATE"       -> ISO_WEEK_DATE
  )

  /** The formatters of a time pattern. ISO_TIME accepts an offset, which the value must not have.
    */
  lazy val timeFormatters: Map[String, DateTimeFormatter] = Map(
    "ISO_LOCAL_TIME" -> ISO_LOCAL_TIME,
    "ISO_TIME"       -> ISO_TIME
  )

  val dateFormatters: Map[String, DateTimeFormatter] = Map(
    // ISO_LOCAL_TIME, ISO_OFFSET_TIME and ISO_TIME patterns are specific to time handling, without a date
    // We choose not to handle them
    "BASIC_ISO_DATE"       -> BASIC_ISO_DATE,
    "ISO_LOCAL_DATE"       -> ISO_LOCAL_DATE,
    "ISO_OFFSET_DATE"      -> ISO_OFFSET_DATE,
    "ISO_DATE"             -> ISO_DATE,
    "ISO_LOCAL_DATE_TIME"  -> ISO_LOCAL_DATE_TIME,
    "ISO_OFFSET_DATE_TIME" -> ISO_OFFSET_DATE_TIME,
    "ISO_ZONED_DATE_TIME"  -> ISO_ZONED_DATE_TIME,
    "ISO_DATE_TIME"        -> ISO_DATE_TIME,
    "ISO_ORDINAL_DATE"     -> ISO_ORDINAL_DATE,
    "ISO_WEEK_DATE"        -> ISO_WEEK_DATE,
    "ISO_INSTANT"          -> ISO_INSTANT,
    "RFC_1123_DATE_TIME"   -> RFC_1123_DATE_TIME
  )

  // fail to compile here because we need to get the type from the ddlMapping
  def fromSQLType(db: String, sqlCol: SqlColumn, schemaHandler: SchemaHandler): PrimitiveType = {
    val typesInfo = TypesInfo(1, schemaHandler.typesForSqlTypeLookup())
    typesInfo.getApproximateTypeForSqlType(db, sqlCol.sqlTypeAsString()) match {
      case Some(typeInfo) => typeInfo.primitiveType
      case None =>
        val sqlTypePrefix = sqlCol.dataType.takeWhile(it => it.isLetter)
        sqlTypePrefix.toLowerCase match {
          case "varchar" | "char" | "text" | "string" | "uuid" => PrimitiveType.string
          case "int" | "integer"                               => PrimitiveType.int
          case "smallint"                                      => PrimitiveType.int
          case "bigint" | "number"                             => PrimitiveType.long
          case "float" | "double" | "real"                     => PrimitiveType.double
          case "decimal" | "numeric"                           => PrimitiveType.decimal
          case "boolean" | "bool"                              => PrimitiveType.boolean
          case "tinyint"                                       => PrimitiveType.int
          case "date"                                          => PrimitiveType.date
          case "timestamp" | "datetime"                        => PrimitiveType.timestamp
          case _                                               => PrimitiveType.string
        }
    }
  }

  def from(elementType: DataType): PrimitiveType = {
    elementType match {
      case _: BinaryType       => PrimitiveType.binary
      case _: TimestampNTZType => PrimitiveType.datetime
      case _: ByteType         => PrimitiveType.byte
      case _: ShortType        => PrimitiveType.short
      case _: IntegerType      => PrimitiveType.int
      case _: LongType         => PrimitiveType.long
      case _: BooleanType      => PrimitiveType.boolean
      case _: FloatType        => PrimitiveType.double
      case _: DoubleType       => PrimitiveType.double
      case _: DecimalType      => PrimitiveType.decimal
      case _: StringType       => PrimitiveType.string
      case _: TimestampType    => PrimitiveType.timestamp
      case _: DateType         => PrimitiveType.date
      case _: VarcharType      => PrimitiveType.variant
      case _ =>
        throw new IllegalArgumentException("Data type not expected: " + elementType.simpleString)
    }
  }

}
