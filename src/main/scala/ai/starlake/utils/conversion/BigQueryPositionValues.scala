package ai.starlake.utils.conversion

import ai.starlake.schema.model.{Position, PrimitiveType, Type}
import com.google.cloud.bigquery.StandardSQLTypeName

/** The values of the attributes of POSITION files, read by the second step of a native BigQuery
  * load from the slices of the lines that its first step loads as strings, in a column named
  * `value` (#1886).
  *
  * A value is NULL when its slice does not convert to the type of its column: the second step
  * rejects the row of a value that is NULL while its slice is not blank. The values of the types
  * with a pattern that BigQuery does not cast from a string, such as `yyyyMMdd` dates, are parsed
  * with their pattern; a table with a pattern that does not translate is loaded with Spark.
  */
object BigQueryPositionValues {

  /** The column of the first step that holds the lines of the files */
  val lineColumn = "value"

  /** @return
    *   the SQL of the slice of a line at this position
    */
  def slice(position: Position): String =
    s"SUBSTR($lineColumn, ${position.first + 1}, ${position.last - position.first + 1})"

  /** @return
    *   the SQL condition, never NULL, that a slice is empty or holds only whitespace, as the unused
    *   fields of a fixed-width line do
    */
  def blank(slice: String): String = s"IFNULL(TRIM($slice), '') = ''"

  private def isStringLike(ddlType: String): Boolean = {
    val upper = ddlType.trim.toUpperCase
    upper == "STRING" || upper.startsWith("VARCHAR") || upper.startsWith("CHAR") ||
    upper == "TEXT"
  }

  /** @param raw
    *   the SQL of a slice
    * @param ddlType
    *   the type of its column, if known
    * @param tpe
    *   the type of its attribute, if known
    * @return
    *   the SQL of the value of the slice in its column, NULL when it does not convert: a short or
    *   malformed line fails no INSERT. A string or bytes keep their spaces; the other values are
    *   trimmed first, since fixed-width fields are padded. A value without column type is the
    *   slice.
    */
  def value(raw: String, ddlType: Option[String], tpe: Option[Type]): String =
    ddlType match {
      case None => raw
      case Some(ddlType) =>
        val parsed = BigQueryType.parse(ddlType.trim)
        val textual = parsed.exists(t =>
          t.standardType == StandardSQLTypeName.STRING || t.standardType == StandardSQLTypeName.BYTES
        )
        val text = if (textual) raw else s"TRIM($raw)"
        patternValue(text, parsed, tpe).getOrElse {
          val upper = ddlType.trim.toUpperCase
          // BigQuery cannot cast a string to these types: it parses them
          if (upper.startsWith("GEOGRAPHY")) s"SAFE.ST_GEOGFROM($text)"
          else if (upper.startsWith("JSON")) s"SAFE.PARSE_JSON($text)"
          else if (upper.startsWith("RANGE"))
            parsed
              .flatMap(_.rangeElementType)
              .map(elementType =>
                BigQueryNativeSchema.rangeFromText(text, elementType.name(), safe = true)
              )
              .getOrElse(raw)
          // BigQuery refuses parameterized types in CAST, such as STRING(10) or NUMERIC(10, 2)
          else if (parsed.exists(t => t.hasParameters && t.rangeElementType.isEmpty))
            BigQueryNativeSchema.castWithinParameters(text, parsed.get)
          else if (!isStringLike(ddlType)) s"SAFE_CAST($text AS $ddlType)"
          else raw
        }
    }

  /** @param columnType
    *   the type of the column that the values are read into, by default the `ddlMapping.bigquery`
    *   of the type, or else the type that its primitive type is read into
    * @return
    *   why the native loader does not read the values of this type in POSITION files, if it does
    *   not: its pattern, or its zone, sets a format that BigQuery cannot apply in SQL
    */
  def unreadable(tpe: Type, columnType: Option[String] = None): Option[String] = {
    val parsed = columnType
      .orElse(tpe.ddlMapping.flatMap(_.get("bigquery")))
      .flatMap(c => BigQueryType.parse(c.trim))
      .orElse(patternColumnTypes.get(tpe.primitiveType).map(BigQueryType(_)))
    if (tpe.primitiveType == PrimitiveType.double && tpe.zone.exists(_.trim.nonEmpty))
      Some(
        s"the zone ${tpe.zone.get} of its type ${tpe.name}, which sets the format of its numbers"
      )
    else if (readsPattern(parsed, tpe) && patternValue(lineColumn, parsed, Some(tpe)).isEmpty)
      Some(s"the pattern ${tpe.pattern} of its type ${tpe.name}")
    else None
  }

  /** The column type that a primitive type with a pattern is read into */
  private val patternColumnTypes: Map[PrimitiveType, StandardSQLTypeName] = Map(
    PrimitiveType.date      -> StandardSQLTypeName.DATE,
    PrimitiveType.timestamp -> StandardSQLTypeName.TIMESTAMP,
    PrimitiveType.datetime  -> StandardSQLTypeName.DATETIME,
    PrimitiveType.time      -> StandardSQLTypeName.TIME,
    PrimitiveType.boolean   -> StandardSQLTypeName.BOOL
  )

  private val dateAndTimeTypes =
    Set(StandardSQLTypeName.DATE, StandardSQLTypeName.DATETIME, StandardSQLTypeName.TIMESTAMP)

  /** @return
    *   true if the values of the type are read with its pattern: its column has the type that its
    *   primitive type is read into, or a date and time type that BigQuery casts it to (a timestamp
    *   into a DATE column, for example). Otherwise, they are cast to the type of their column,
    *   which kept another type, such as STRING.
    */
  def readsPattern(columnType: Option[BigQueryType], tpe: Type): Boolean =
    columnType.exists(c =>
      !c.hasParameters && patternColumnTypes.get(tpe.primitiveType).exists { own =>
        own == c.standardType || dateAndTimeTypes(own) && dateAndTimeTypes(c.standardType)
      }
    )

  /** @return
    *   the SQL of the value of a trimmed slice read with the pattern of its type, then cast to the
    *   type of its column, None when the pattern does not translate or the type has none
    */
  private def patternValue(
    text: String,
    columnType: Option[BigQueryType],
    tpe: Option[Type]
  ): Option[String] =
    tpe.filter(readsPattern(columnType, _)).flatMap { tpe =>
      val value =
        if (tpe.primitiveType == PrimitiveType.boolean) booleanValue(text, tpe.pattern)
        else temporalValue(text, tpe)
      val column = columnType.get.standardType
      value.map(value =>
        if (patternColumnTypes.get(tpe.primitiveType).contains(column)) value
        else s"CAST($value AS ${column.name()})"
      )
    }

  /** The patterns whose values BigQuery casts from a string, kept as they were read before #1886:
    * the default patterns of the built-in types, and the ISO formats of the SQL types. The
    * timestamps with an offset are cast whatever the zone of their type.
    */
  private val castPatterns: Map[PrimitiveType, Set[String]] = Map(
    PrimitiveType.date -> Set("yyyy-MM-dd"),
    PrimitiveType.timestamp -> Set(
      "yyyy-MM-dd HH:mm:ss",
      "ISO_LOCAL_DATE_TIME",
      "ISO_OFFSET_DATE_TIME",
      "ISO_INSTANT"
    ),
    PrimitiveType.datetime -> Set("SQL_DATE_TIME", "ISO_LOCAL_DATE_TIME"),
    PrimitiveType.time     -> Set("ISO_LOCAL_TIME", "ISO_TIME", "HH:mm:ss")
  )

  private val offsetPatterns = Set("ISO_OFFSET_DATE_TIME", "ISO_INSTANT")

  /** The predefined formatters that translate to a BigQuery format, with their Java pattern */
  private val namedPatterns: Map[String, String] = Map(
    "BASIC_ISO_DATE" -> "yyyyMMdd",
    "ISO_LOCAL_DATE" -> "yyyy-MM-dd"
  )

  /** The zones that are UTC */
  private val utcZones = Set("UTC", "Z", "GMT", "UT", "Etc/UTC", "Etc/GMT")

  /** @return
    *   true if BigQuery reads this time zone, as Java does: a name of the time zone database or an
    *   offset
    */
  private def bigQueryZone(zone: String): Boolean =
    java.time.ZoneId.getAvailableZoneIds.contains(zone) ||
    zone.matches("[+-][0-9]{2}(:[0-9]{2})?")

  /** @return
    *   the SQL of a date, time or timestamp read with the pattern of its type from a trimmed slice,
    *   as the Spark loader reads it:
    *   - `epoch_second` and `epoch_milli` timestamps are numbers of seconds or milliseconds since
    *     the Unix epoch;
    *   - the patterns that BigQuery casts are cast;
    *   - the others are parsed with their BigQuery format (see [[format]]), once the slice has the
    *     digits and separators of the pattern, since the BigQuery formats read fields of any width.
    *
    * The timestamps without offset are in the zone of their type, UTC by default.
    */
  private def temporalValue(text: String, tpe: Type): Option[String] = {
    val zone =
      if (tpe.primitiveType == PrimitiveType.timestamp)
        tpe.zone.map(_.trim).filterNot(z => z.isEmpty || utcZones(z))
      else None
    val pattern = tpe.pattern
    // An epoch has no zone
    if (tpe.primitiveType == PrimitiveType.timestamp && pattern == "epoch_second")
      Some(s"SAFE.TIMESTAMP_SECONDS(SAFE_CAST($text AS INT64))")
    else if (tpe.primitiveType == PrimitiveType.timestamp && pattern == "epoch_milli")
      Some(s"SAFE.TIMESTAMP_MILLIS(SAFE_CAST($text AS INT64))")
    else if (zone.exists(z => !bigQueryZone(z))) None
    else if (
      castPatterns.get(tpe.primitiveType).exists(_.contains(pattern)) &&
      (zone.isEmpty || offsetPatterns.contains(pattern))
    )
      Some(s"SAFE_CAST($text AS ${patternColumnTypes(tpe.primitiveType).name()})")
    else {
      // The dates of Spark read Java patterns only, the others predefined formatters too
      val javaPattern =
        if (tpe.primitiveType == PrimitiveType.date) Some(pattern)
        else if (namedPatterns.contains(pattern)) namedPatterns.get(pattern)
        else if (predefinedFormatters.contains(pattern)) None
        else Some(pattern)
      for {
        javaPattern  <- javaPattern
        bigQueryForm <- format(javaPattern, tpe.primitiveType)
      } yield {
        val function = s"SAFE.PARSE_${patternColumnTypes(tpe.primitiveType).name()}"
        val zoneArgument = zone.map(z => s", '$z'").getOrElse("")
        s"IF(REGEXP_CONTAINS($text, '^${bigQueryForm.shape}$$'), $function('${bigQueryForm.format}', $text$zoneArgument), NULL)"
      }
    }
  }

  private lazy val predefinedFormatters: Set[String] =
    PrimitiveType.dateFormatters.keySet ++ PrimitiveType.datetimeFormatters.keySet ++
    PrimitiveType.timeFormatters.keySet

  /** A Java date and time pattern translated to BigQuery
    *
    * @param format
    *   its BigQuery format, for PARSE_DATE, PARSE_TIMESTAMP...
    * @param shape
    *   the RE2 regular expression of the text that it reads, with the width of its fields
    */
  case class DateFormat(format: String, shape: String)

  private sealed trait Token
  private case class Field(letter: Char, count: Int) extends Token
  private case class Literal(char: Char) extends Token

  /** @return
    *   the letters and literals of a Java pattern, None for its optional sections and reserved
    *   characters
    */
  private def tokens(pattern: String): Option[List[Token]] = {
    val result = List.newBuilder[Token]
    var index = 0
    var valid = true
    while (valid && index < pattern.length) {
      val char = pattern.charAt(index)
      if (char.isLetter && char < 128) {
        val end = pattern.indexWhere(_ != char, index)
        val next = if (end < 0) pattern.length else end
        result += Field(char, next - index)
        index = next
      } else if (pattern.startsWith("''", index)) {
        result += Literal('\'')
        index += 2
      } else if (char == '\'') {
        // A quoted literal, where two quotes are a quote
        var closed = false
        index += 1
        while (!closed && index < pattern.length) {
          if (pattern.charAt(index) == '\'') {
            if (index + 1 < pattern.length && pattern.charAt(index + 1) == '\'') {
              result += Literal('\'')
              index += 2
            } else {
              closed = true
              index += 1
            }
          } else {
            result += Literal(pattern.charAt(index))
            index += 1
          }
        }
        valid = closed
      } else if ("[]{}#".contains(char)) valid = false
      else {
        result += Literal(char)
        index += 1
      }
    }
    if (valid) Some(result.result()) else None
  }

  /** @return
    *   the BigQuery format of a Java pattern of numeric fields, for a primitive type, if it
    *   translates: `yyyy` or `uuuu`, `M` or `MM`, `d` or `dd`, `H` or `HH`, `m` or `mm`, `s` or
    *   `ss` followed by a point and up to 6 fractions of seconds `S`, and literals. A single letter
    *   field is followed by a literal, or ends the pattern. A date has a year and a month, and a
    *   datetime a day too; the hours, minutes and seconds come in this order; a date has no time,
    *   and a time no date. Text fields (`MMM`, `E`, `a`), two-digit years, zones and optional
    *   sections are not translated: their BigQuery formats read other values, or none.
    */
  def format(pattern: String, primitiveType: PrimitiveType): Option[DateFormat] =
    tokens(pattern).flatMap { tokens =>
      val format = new StringBuilder
      val shape = new StringBuilder
      val letters = scala.collection.mutable.ListBuffer.empty[Char]
      def digits(count: Int) = if (count == 1) "[0-9]{1,2}" else s"[0-9]{$count}"
      def translate(tokens: List[Token]): Boolean =
        tokens match {
          case Nil => true
          // Java reads a single letter field of 1 or 2 digits only before a literal: before another
          // number, it reads 1 digit, which BigQuery formats do not
          case Field(_, 1) :: (_: Field) :: _ =>
            false
          // TIMESTAMP, DATETIME and TIME hold microseconds
          case Field('s', count) :: Literal('.') :: Field('S', fraction) :: rest
              if count <= 2 && fraction <= 6 =>
            format ++= "%E*S"
            shape ++= s"${digits(count)}[.][0-9]{$fraction}"
            letters += 's' += 'S'
            translate(rest)
          case Field(letter, count) :: rest =>
            val element = (letter, count) match {
              case ('y' | 'u', 4) => Some("%Y" -> "[0-9]{4}")
              case ('M', 1 | 2)   => Some("%m" -> digits(count))
              case ('d', 1 | 2)   => Some("%d" -> digits(count))
              case ('H', 1 | 2)   => Some("%H" -> digits(count))
              case ('m', 1 | 2)   => Some("%M" -> digits(count))
              case ('s', 1 | 2)   => Some("%S" -> digits(count))
              case _              => None
            }
            element.exists { case (formatElement, shapeElement) =>
              format ++= formatElement
              shape ++= shapeElement
              letters += (if (letter == 'u') 'y' else letter)
              translate(rest)
            }
          // Neither quotes nor backslashes: the format and the shape are SQL string literals
          case Literal(char) :: _ if char == '\'' || char == '\\' || char == ']' || char == '^' =>
            false
          case Literal(char) :: rest =>
            format ++= (if (char == '%') "%%" else char.toString)
            shape ++= s"[$char]"
            translate(rest)
        }
      val translated = translate(tokens)
      val used = letters.toList
      def has(letter: Char) = used.contains(letter)
      val dateLetters = Set('y', 'M', 'd')
      val timeLetters = Set('H', 'm', 's', 'S')
      // No field twice, and no minutes without hours or seconds without minutes
      val wellFormed = used.distinct.size == used.size &&
        (!has('m') || has('H')) && (!has('s') || has('m'))
      val valid = translated && wellFormed && (primitiveType match {
        case PrimitiveType.date      => has('y') && has('M') && used.forall(dateLetters)
        case PrimitiveType.timestamp => has('y') && has('M')
        case PrimitiveType.datetime  => has('y') && has('M') && has('d')
        case PrimitiveType.time      => has('H') && used.forall(timeLetters)
        case _                       => false
      })
      if (valid) Some(DateFormat(format.result(), shape.result())) else None
    }

  /** @return
    *   the SQL of a boolean read from a trimmed slice with a pattern `true<-TF->false` of Java
    *   regular expressions, as the Spark loader reads it: true when the whole slice matches the
    *   first one, false when it matches the second one. None when a regular expression may not have
    *   the same meaning in RE2, the regular expressions of BigQuery: it has quotes, backslashes,
    *   lookarounds, possessive quantifiers, nested classes, repetitions beyond the 1000 of RE2 or
    *   flags other than `i`, `m` and `s`.
    */
  def booleanValue(text: String, pattern: String): Option[String] =
    pattern.split("<-TF->", -1) match {
      case Array(truePattern, falsePattern, _*) if re2(truePattern) && re2(falsePattern) =>
        // Spark compiles them with DOTALL
        def matches(regex: String) = s"REGEXP_CONTAINS($text, '(?s)^(?:$regex)$$')"
        Some(
          s"CASE WHEN ${matches(truePattern)} THEN TRUE WHEN ${matches(falsePattern)} THEN FALSE END"
        )
      case _ => None
    }

  private val unsafeRegex =
    "['\\\\]|&&|\\[[^\\]]*\\[|\\[:|[*+?}]\\+|\\{[0-9]{4,}|\\(\\?(?![ims-]*[):])".r

  private def re2(regex: String): Boolean = unsafeRegex.findFirstIn(regex).isEmpty
}
