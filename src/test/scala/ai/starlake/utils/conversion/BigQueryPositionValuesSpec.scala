package ai.starlake.utils.conversion

import ai.starlake.schema.model.{Position, PrimitiveType, Type}
import ai.starlake.utils.conversion.BigQueryPositionValues.DateFormat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The values of the attributes of POSITION files, read by the second step of a native BigQuery
  * load (#1886). Pure SQL: no BigQuery access.
  */
class BigQueryPositionValuesSpec extends AnyFlatSpec with Matchers {

  private def tpe(
    name: String,
    pattern: String,
    primitiveType: PrimitiveType,
    bigquery: String,
    zone: Option[String] = None
  ) = Type(name, pattern, primitiveType, zone, ddlMapping = Some(Map("bigquery" -> bigquery)))

  private val basicIsoDate =
    tpe("basic_iso_date", "BASIC_ISO_DATE", PrimitiveType.timestamp, "TIMESTAMP")
  private val timestamp =
    tpe("timestamp", "yyyy-MM-dd HH:mm:ss", PrimitiveType.timestamp, "TIMESTAMP")
  private val date = tpe("date", "yyyy-MM-dd", PrimitiveType.date, "DATE")
  private val boolean =
    tpe("boolean", "(?i)true|yes|[y1]|t<-TF->(?i)false|no|[n0]|f", PrimitiveType.boolean, "BOOL")

  private def valueOf(raw: String, ddlType: Option[String], tpe: Option[Type]): String =
    BigQueryPositionValues.value(raw, ddlType, tpe)

  private val slice = "SUBSTR(value, 5, 8)"
  private val text = s"TRIM($slice)"

  "slice" should "be the SQL of the characters of a position, from 0" in {
    BigQueryPositionValues.slice(Position(4, 11)) shouldBe slice
    BigQueryPositionValues.blank(slice) shouldBe s"IFNULL(TRIM($slice), '') = ''"
  }

  "format" should "translate the Java patterns of numeric fields to BigQuery formats" in {
    import BigQueryPositionValues.format
    format("yyyyMMdd", PrimitiveType.timestamp) shouldBe Some(
      DateFormat("%Y%m%d", "[0-9]{4}[0-9]{2}[0-9]{2}")
    )
    format("dd/MM/yyyy", PrimitiveType.date) shouldBe Some(
      DateFormat("%d/%m/%Y", "[0-9]{2}[/][0-9]{2}[/][0-9]{4}")
    )
    format("d/M/uuuu", PrimitiveType.date) shouldBe Some(
      DateFormat("%d/%m/%Y", "[0-9]{1,2}[/][0-9]{1,2}[/][0-9]{4}")
    )
    format("yyyy-MM-dd'T'HH:mm:ss.SSS", PrimitiveType.timestamp) shouldBe Some(
      DateFormat(
        "%Y-%m-%dT%H:%M:%E*S",
        "[0-9]{4}[-][0-9]{2}[-][0-9]{2}[T][0-9]{2}[:][0-9]{2}[:][0-9]{2}[.][0-9]{3}"
      )
    )
    format("yyyyMMddHHmmss", PrimitiveType.datetime).map(_.format) shouldBe Some("%Y%m%d%H%M%S")
    format("HHmmss", PrimitiveType.time).map(_.format) shouldBe Some("%H%M%S")
    // Quoted literals, a quote being two quotes, and the percent sign of BigQuery formats
    format("yyyy'%'MM", PrimitiveType.date) shouldBe Some(
      DateFormat("%Y%%%m", "[0-9]{4}[%][0-9]{2}")
    )
    // A date without day is the first day of its month, as with Spark, but not a datetime
    format("yyyyMM", PrimitiveType.date).map(_.format) shouldBe Some("%Y%m")
    format("yyyyMM", PrimitiveType.datetime) shouldBe None
    // A single letter field ends the pattern, or comes before a literal
    format("yyyyMMd", PrimitiveType.date).map(_.shape) shouldBe Some(
      "[0-9]{4}[0-9]{2}[0-9]{1,2}"
    )
    format("HH:mm:ss.SSSSSS", PrimitiveType.time).map(_.format) shouldBe Some("%H:%M:%E*S")
  }

  it should "not translate the patterns whose BigQuery format reads other values" in {
    import BigQueryPositionValues.format
    List(
      // Two-digit years, text fields, zones, optional sections, fractions without seconds
      "yy-MM-dd"                 -> PrimitiveType.date,
      "dd MMM yyyy"              -> PrimitiveType.date,
      "EEE yyyy-MM-dd"           -> PrimitiveType.date,
      "yyyy-MM-dd hh:mm a"       -> PrimitiveType.timestamp,
      "yyyy-MM-dd'T'HH:mm:ssXXX" -> PrimitiveType.timestamp,
      "yyyy-MM-dd[ HH:mm]"       -> PrimitiveType.timestamp,
      "yyyy-MM-dd HH:mm:ss,SSS"  -> PrimitiveType.timestamp,
      "yyyyDDD"                  -> PrimitiveType.date,
      "yyyy-MM-dd HH:mm"         -> PrimitiveType.date,
      "MMdd"                     -> PrimitiveType.date,
      "mm:ss"                    -> PrimitiveType.time,
      "yyyy-MM-dd HH:ss"         -> PrimitiveType.timestamp,
      "yyyy-MM-dd yyyy"          -> PrimitiveType.timestamp,
      "yyyy-MM-dd"               -> PrimitiveType.time,
      "HH:mm"                    -> PrimitiveType.boolean,
      // Literals that a SQL string or a regular expression class cannot hold, an unclosed quote
      "yyyyMMdd''"     -> PrimitiveType.date,
      "yyyy\\MM"       -> PrimitiveType.date,
      "yyyy]MM"        -> PrimitiveType.date,
      "yyyy^MM"        -> PrimitiveType.date,
      "yyyy-MM-dd'T"   -> PrimitiveType.date,
      "ISO_LOCAL_DATE" -> PrimitiveType.date,
      // Java reads 1 digit for a single letter before another number, BigQuery 2
      "Hmmss"   -> PrimitiveType.time,
      "yyyyMdd" -> PrimitiveType.date,
      // Beyond the microseconds of BigQuery
      "HH:mm:ss.SSSSSSSSS" -> PrimitiveType.time
    ).foreach { case (pattern, primitiveType) =>
      withClue(pattern)(format(pattern, primitiveType) shouldBe None)
    }
  }

  "value" should "parse the dates and timestamps with their pattern, once the slice has its shape" in {
    valueOf(slice, Some("TIMESTAMP"), Some(basicIsoDate)) shouldBe
    s"IF(REGEXP_CONTAINS($text, '^[0-9]{4}[0-9]{2}[0-9]{2}$$'), SAFE.PARSE_TIMESTAMP('%Y%m%d', $text), NULL)"
    valueOf(slice, Some("TIMESTAMP"), Some(basicIsoDate.copy(zone = Some("Europe/Paris")))) shouldBe
    s"IF(REGEXP_CONTAINS($text, '^[0-9]{4}[0-9]{2}[0-9]{2}$$'), SAFE.PARSE_TIMESTAMP('%Y%m%d', $text, 'Europe/Paris'), NULL)"
    valueOf(slice, Some("DATE"), Some(date.copy(pattern = "dd/MM/yyyy"))) shouldBe
    s"IF(REGEXP_CONTAINS($text, '^[0-9]{2}[/][0-9]{2}[/][0-9]{4}$$'), SAFE.PARSE_DATE('%d/%m/%Y', $text), NULL)"
    valueOf(
      slice,
      Some("DATETIME"),
      Some(tpe("d", "BASIC_ISO_DATE", PrimitiveType.datetime, "DATETIME"))
    ) should include("SAFE.PARSE_DATETIME('%Y%m%d', ")
    valueOf(slice, Some("TIME"), Some(tpe("t", "HHmmss", PrimitiveType.time, "TIME"))) should
    include("SAFE.PARSE_TIME('%H%M%S', ")
    // ISO_LOCAL_DATE is parsed as yyyy-MM-dd, at the start of the day in the zone of the type
    valueOf(
      slice,
      Some("TIMESTAMP"),
      Some(basicIsoDate.copy(pattern = "ISO_LOCAL_DATE", zone = Some("UTC")))
    ) should include("SAFE.PARSE_TIMESTAMP('%Y-%m-%d', TRIM(SUBSTR(value, 5, 8))), NULL)")
  }

  it should "cast the values of the patterns that BigQuery casts, and of the epoch timestamps" in {
    valueOf(slice, Some("TIMESTAMP"), Some(timestamp)) shouldBe s"SAFE_CAST($text AS TIMESTAMP)"
    valueOf(slice, Some("DATE"), Some(date)) shouldBe s"SAFE_CAST($text AS DATE)"
    valueOf(
      slice,
      Some("DATETIME"),
      Some(tpe("datetime", "SQL_DATE_TIME", PrimitiveType.datetime, "DATETIME"))
    ) shouldBe s"SAFE_CAST($text AS DATETIME)"
    valueOf(
      slice,
      Some("TIME"),
      Some(tpe("time", "ISO_LOCAL_TIME", PrimitiveType.time, "TIME"))
    ) shouldBe
    s"SAFE_CAST($text AS TIME)"
    // A timestamp without offset is in the zone of its type: its pattern is parsed
    valueOf(slice, Some("TIMESTAMP"), Some(timestamp.copy(zone = Some("Europe/Paris")))) shouldBe
    s"IF(REGEXP_CONTAINS($text, '^[0-9]{4}[-][0-9]{2}[-][0-9]{2}[ ][0-9]{2}[:][0-9]{2}[:][0-9]{2}$$'), SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', $text, 'Europe/Paris'), NULL)"
    valueOf(
      slice,
      Some("TIMESTAMP"),
      Some(timestamp.copy(pattern = "ISO_OFFSET_DATE_TIME", zone = Some("Europe/Paris")))
    ) shouldBe s"SAFE_CAST($text AS TIMESTAMP)"
    valueOf(slice, Some("TIMESTAMP"), Some(timestamp.copy(pattern = "epoch_second"))) shouldBe
    s"SAFE.TIMESTAMP_SECONDS(SAFE_CAST($text AS INT64))"
    valueOf(slice, Some("TIMESTAMP"), Some(timestamp.copy(pattern = "epoch_milli"))) shouldBe
    s"SAFE.TIMESTAMP_MILLIS(SAFE_CAST($text AS INT64))"
    // An epoch has no zone, and the zones that are UTC are none
    valueOf(
      slice,
      Some("TIMESTAMP"),
      Some(timestamp.copy(pattern = "epoch_second", zone = Some("Mars/Olympus")))
    ) shouldBe s"SAFE.TIMESTAMP_SECONDS(SAFE_CAST($text AS INT64))"
    List("UTC", "Z", "GMT").foreach { zone =>
      valueOf(slice, Some("TIMESTAMP"), Some(basicIsoDate.copy(zone = Some(zone)))) should endWith(
        s"SAFE.PARSE_TIMESTAMP('%Y%m%d', $text), NULL)"
      )
    }
    valueOf(slice, Some("TIMESTAMP"), Some(basicIsoDate.copy(zone = Some("+01:00")))) should
    endWith(s"SAFE.PARSE_TIMESTAMP('%Y%m%d', $text, '+01:00'), NULL)")
  }

  it should "read the booleans with their pattern" in {
    BigQueryPositionValues.value(slice, Some("BOOL"), Some(boolean)) shouldBe
    s"CASE WHEN REGEXP_CONTAINS($text, '(?s)^(?:(?i)true|yes|[y1]|t)$$') THEN TRUE WHEN REGEXP_CONTAINS($text, '(?s)^(?:(?i)false|no|[n0]|f)$$') THEN FALSE END"
    BigQueryPositionValues.booleanValue("x", "O<-TF->N") shouldBe Some(
      "CASE WHEN REGEXP_CONTAINS(x, '(?s)^(?:O)$') THEN TRUE WHEN REGEXP_CONTAINS(x, '(?s)^(?:N)$') THEN FALSE END"
    )
    List(
      "(?=y)y<-TF->n",
      "(?<!x)y<-TF->n",
      "(?>y)<-TF->n",
      "y++<-TF->n",
      "y{1}+<-TF->n",
      "\\d<-TF->n",
      "it's<-TF->n",
      "[a&&b]<-TF->n",
      "[[a]]<-TF->n",
      "(?x)y<-TF->n",
      "(?U)y<-TF->n",
      "[y[o]]<-TF->n",
      "y{1001}<-TF->n",
      "yes"
    ).foreach(pattern =>
      withClue(pattern)(BigQueryPositionValues.booleanValue("x", pattern) shouldBe None)
    )
    BigQueryPositionValues.booleanValue("x", "(?i:y|o)<-TF->(?s)n").isDefined shouldBe true
  }

  it should "cast or keep the other values as before, trimmed unless they are strings" in {
    val long = tpe("long", "-?\\d+", PrimitiveType.long, "INT64")
    valueOf(slice, Some("INT64"), Some(long)) shouldBe s"SAFE_CAST($text AS INT64)"
    valueOf(slice, Some("INT64"), None) shouldBe s"SAFE_CAST($text AS INT64)"
    valueOf(slice, Some("STRING"), None) shouldBe slice
    valueOf(slice, None, Some(long)) shouldBe slice
    valueOf(slice, Some("STRING(10)"), None) shouldBe
    s"IF(LENGTH($slice) <= 10, $slice, NULL)"
    valueOf(slice, Some("NUMERIC(10, 2)"), None) should startWith(
      s"IF(ABS(ROUND(SAFE_CAST($text AS NUMERIC), 2))"
    )
    valueOf(slice, Some("GEOGRAPHY"), None) shouldBe s"SAFE.ST_GEOGFROM($text)"
    // Bytes keep their spaces, as strings
    valueOf(slice, Some("BYTES"), None) shouldBe s"SAFE_CAST($slice AS BYTES)"
    valueOf(slice, Some("BYTES(16)"), None) should include(s"SAFE_CAST($slice AS BYTES)")
    valueOf(slice, Some("JSON"), None) shouldBe s"SAFE.PARSE_JSON($text)"
    // A value read with its pattern is cast to the date and time type that its column kept
    valueOf(slice, Some("DATE"), Some(basicIsoDate)) shouldBe
    s"CAST(${valueOf(slice, Some("TIMESTAMP"), Some(basicIsoDate))} AS DATE)"
    valueOf(slice, Some("DATETIME"), Some(date)) shouldBe
    s"CAST(SAFE_CAST($text AS DATE) AS DATETIME)"
    // Into a column of another type, the value is cast: the pattern does not apply
    valueOf(slice, Some("STRING"), Some(basicIsoDate)) shouldBe slice
    valueOf(slice, Some("STRING"), Some(boolean)) shouldBe slice
    valueOf(slice, Some("TIME"), Some(date)) shouldBe s"SAFE_CAST($text AS TIME)"
    // A pattern that does not translate keeps the cast, for an ignored attribute: the loaded ones
    // are read by Spark
    valueOf(
      slice,
      Some("TIMESTAMP"),
      Some(timestamp.copy(pattern = "ISO_ZONED_DATE_TIME"))
    ) shouldBe
    s"SAFE_CAST($text AS TIMESTAMP)"
  }

  "unreadable" should "tell the types whose format BigQuery cannot apply to the values of POSITION files" in {
    import BigQueryPositionValues.unreadable
    unreadable(basicIsoDate) shouldBe None
    unreadable(timestamp) shouldBe None
    unreadable(timestamp.copy(zone = Some("Europe/Paris"))) shouldBe None
    unreadable(date.copy(pattern = "dd/MM/yyyy")) shouldBe None
    unreadable(boolean) shouldBe None
    unreadable(timestamp.copy(pattern = "epoch_milli")) shouldBe None
    unreadable(tpe("long", "-?\\d+", PrimitiveType.long, "INT64")) shouldBe None
    unreadable(tpe("double", "x", PrimitiveType.double, "FLOAT64")) shouldBe None
    unreadable(timestamp.copy(pattern = "ISO_ZONED_DATE_TIME")) shouldBe Some(
      "the pattern ISO_ZONED_DATE_TIME of its type timestamp"
    )
    unreadable(
      timestamp.copy(pattern = "ISO_LOCAL_DATE_TIME", zone = Some("Europe/Paris"))
    ) shouldBe
    Some("the pattern ISO_LOCAL_DATE_TIME of its type timestamp")
    unreadable(timestamp.copy(zone = Some("Europe/Paris'"))).isDefined shouldBe true
    unreadable(timestamp.copy(zone = Some("Mars/Olympus"))).isDefined shouldBe true
    unreadable(date.copy(pattern = "dd MMM yyyy")).isDefined shouldBe true
    unreadable(boolean.copy(pattern = "(?=y)y<-TF->n")).isDefined shouldBe true
    unreadable(tpe("double", "x", PrimitiveType.double, "FLOAT64", Some("fr_FR"))) shouldBe Some(
      "the zone fr_FR of its type double, which sets the format of its numbers"
    )
    // Read into a STRING column, the values are the slices: the pattern does not apply
    val stringDate =
      date.copy(pattern = "dd MMM yyyy", ddlMapping = Some(Map("bigquery" -> "STRING")))
    unreadable(stringDate) shouldBe None
    // Unless the column kept a date and time type
    unreadable(stringDate, Some("TIMESTAMP")).isDefined shouldBe true
    unreadable(date.copy(ddlMapping = Some(Map("bigquery" -> "STRING"))), Some("DATETIME")) shouldBe
    None
    // A type without mapping is read into the column of its primitive type
    unreadable(date.copy(pattern = "dd MMM yyyy", ddlMapping = None)).isDefined shouldBe true
    unreadable(date.copy(ddlMapping = None)) shouldBe None
  }
}
