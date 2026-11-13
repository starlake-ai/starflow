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
import ai.starlake.schema.model.Format.DSV
import ai.starlake.schema.model.Severity._
import ai.starlake.schema.model.WriteMode.APPEND
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Specify Schema properties. These properties may be specified at the schema or domain level Any
  * property not specified at the schema level is taken from the one specified at the domain level
  * or else the default value is returned.
  *
  * @param mode
  *   : FILE mode by default. FILE and STREAM are the two accepted values. STREAM is used when load
  *   data from Kafka
  * @param format
  *   : DSV by default. Supported file formats are :
  *   - DSV : Delimiter-separated values file. Delimiter value iss specified in the "separator"
  *     field.
  *   - POSITION : FIXED format file where values are located at an exact position in each line.
  *   - JSON_FLAT : For optimisation purpose, we differentiate JSON with top level values from JSON
  *     with deep level fields. JSON_FLAT are JSON files with top level fields only.
  *   - JSON : Deep JSON file. Use only when your json documents contain subdocuments, otherwise
  *     prefer to use JSON_FLAT since it is much faster.
  *   - XML : XML files
  * @param encoding
  *   : UTF-8 if not specified.
  * @param multiline
  *   : are json objects on a single line or multiple line ? Single by default. false means single.
  *   false also means faster
  * @param array
  *   : Is the json stored as a single object array ? false by default. This means that by default
  *   we have one json document per line.
  * @param withHeader
  *   : does each file start with a header line? true by default, false for POSITION files
  * @param separator
  *   : the values delimiter, ';' by default value may be a multichar string starting from Spark3
  * @param quote
  *   : The String quote char, '"' by default
  * @param escape
  *   : escaping char '\' by default
  * @param writeStrategy
  *   : Write mode, APPEND by default
  * @param sink
  *   : should the dataset be indexed in elasticsearch after ingestion ?
  * @param directory:
  *   Folder on the local filesystem where incoming files are stored. Typically, this folder will be
  *   scanned periodically to move the datasets to the cluster for ingestion. Files located in this
  *   folder are moved to the pending folder for ingestion by the "stage" command.
  * @param ack:
  *   Ack extension used for each file. If specified, files are moved to the pending folder only
  *   once a file with the same name as the source file and with this extension is present. To move
  *   a file without requiring an ack file to be present, do not specify this attribute or set its
  *   string value "".
  * @param options:
  *   Any option we wish to pass to the loader. For example, for XML, we may want to specify the
  *   rowValidationXSDPath option to validate the XML files against an XSD schema.
  * @param loader:
  *   Loader to use to load the dataset. If not specified, the default loader for the format is
  *   used. Possible values are :
  *   - "spark" : Spark loader. This is the default loader for all formats.
  *   - "native" : Native loader. Using the datawarehouse native loader. Provides faster loads but
  *     less features.
  * @param dagRef:
  *   Reference to the DAG that should be triggered after ingestion of this dataset. If not defined,
  *   no DAG is triggered.
  * @param freshness:
  *   Freshness policy to apply to this dataset. If not defined, no freshness policy is applied.
  * @param nullValue:
  *   Value to use to replace null values. If not defined, the default value for the format is used.
  *   For DSV, the default value is "". For JSON, the default value is "null". For XML, the default
  *   value is "". For POSITION, the default value is "".
  * @param emptyIsNull:
  *   Should empty values be considered as null values ? true by default. If true, empty values are
  *   replaced by the nullValue. If false, empty values are kept as is.
  * @param fillWithDefaultValue:
  *   if true, then it getters return default value, otherwise return currently defined value only
  */
case class Metadata(
  format: Option[Format] = None,
  encoding: Option[String] = None,
  multiline: Option[Boolean] = None,
  array: Option[Boolean] = None,
  withHeader: Option[Boolean] = None,
  separator: Option[String] = None,
  quote: Option[String] = None,
  escape: Option[String] = None,
  sink: Option[AllSinks] = None,
  directory: Option[String] = None,
  ack: Option[String] = None,
  options: Option[Map[String, String]] = None,
  loader: Option[String] = None,
  emptyIsNull: Option[Boolean] = None,
  dagRef: Option[String] = None,
  freshness: Option[Freshness] = None,
  nullValue: Option[String] = None,
  fillWithDefaultValue: Boolean = true,
  schedule: Option[String] = None,
  writeStrategy: Option[WriteStrategy] = None
) {

  def this() = this(None) // Should never be called. Here for Jackson deserialization only

  def checkValidity(domainName: String, table: Option[SchemaInfo])(implicit
    settings: Settings
  ): Either[List[ValidationMessage], Boolean] = {
    val tableName = table.map(_.name).getOrElse("")
    val sinkErrors =
      sink.map(_.checkValidity(tableName, table)).getOrElse(Right(true))
    val freshnessErrors = freshness.map(_.checkValidity(tableName)).getOrElse(Right(true))
    val scheduleErrors = schedule
      .map { schedule =>
        if (
          schedule.contains(" ") ||
          settings.appConfig.schedulePresets.contains(schedule)
        ) {
          // inline cron expression or reference to a schedule preset
          Right(true)
        } else {
          Left(
            List(
              ValidationMessage(
                Error,
                s"Table metadata in $domainName.$tableName",
                s"schedule: $schedule is not a valid schedule. Valid schedules are ${settings.appConfig.schedulePresets
                    .mkString(", ")}"
              )
            )
          )
        }
      }
      .getOrElse(Right(true))

    val loaderErrors =
      Set("native", "spark").contains(loader.getOrElse("spark")) match {
        case false if loader.getOrElse("").isEmpty =>
          Right(true)
        case false if this.resolveFormat() != Format.DATAFRAME =>
          Left(
            List(
              ValidationMessage(
                Error,
                s"Table metadata $domainName.$tableName",
                s"loader: $loader is not a valid loader. Valid loaders are native, spark"
              )
            )
          )
        case _ => Right(true)
      }
    val writeStrategyErrors =
      writeStrategy.map(_.checkValidity(domainName, table)).getOrElse(Right(true))
    // merge all errors above
    val errors =
      List(sinkErrors, freshnessErrors, scheduleErrors, loaderErrors, writeStrategyErrors).collect {
        case Left(err) => err
      }.flatten

    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    this.dagRef.foreach { dagRef =>
      settings.schemaHandler().checkDagNameValidity(dagRef) match {
        case Left(err) => errorList ++= err
        case _         =>
      }
    }

    val allErrors = errors ++ errorList.toList
    if (allErrors.nonEmpty)
      Left(allErrors)
    else
      Right(true)

  }

  def getSink()(implicit settings: Settings): Sink = {
    sink.map(_.getSink()).getOrElse(AllSinks().getSink())
  }

  @JsonIgnore
  def getClustering(): Option[Seq[String]] = sink.flatMap(_.clustering)

  def asMap(): Map[String, Object] = {
    Map(
      "format"      -> resolveFormat().toString,
      "encoding"    -> resolveEncoding(),
      "multiline"   -> resolveMultiline().toString,
      "array"       -> resolveArray().toString,
      "withHeader"  -> resolveWithHeader().toString,
      "separator"   -> resolveSeparator(),
      "quote"       -> resolveQuote(),
      "escape"      -> resolveEscape(),
      "directory"   -> directory.getOrElse(""),
      "nullValue"   -> resolveNullValue(),
      "emptyIsNull" -> resolveEmptyIsNull().toString,
      "options"     -> getOptions().map { case (k, v) => (k.toLowerCase(), v) }.asJava
    )
  }

  override def toString: String =
    s"""
       |format:${resolveFormat()}
       |encoding:${resolveEncoding()}
       |multiline:${resolveMultiline()}
       |array:${resolveArray()}
       |withHeader:${resolveWithHeader()}
       |separator:${resolveSeparator()}
       |quote:${resolveQuote()}
       |escape:${resolveEscape()}
       |sink:${sink}
       |directory:${directory}
       |ack:${ack}
       |options:${getOptions()}
       |loader:${loader}
       |dag:${dagRef}
       |freshness:${freshness}
       |nullValue:${resolveNullValue()}
       |emptyIsNull:${emptyIsNull}
       |dag:$dagRef
       |fillWithDefaultValue:$fillWithDefaultValue""".stripMargin

  @JsonIgnore
  def getStrategyOptions(): WriteStrategy = {
    val writeMode: WriteMode = this.getWrite()
    writeStrategy.getOrElse(WriteStrategy(Some(WriteStrategyType.fromString(writeMode.value))))
  }

  def resolveFormat(): Format = getFinalValue(format, DSV)

  def resolveEncoding(): String = getFinalValue(encoding, "UTF-8")

  // scala Boolean value don't have implicit ev Null subtype and boxing java boolean to scala boolean turn null to False. That is why we keep the type as java one here
  def resolveMultiline(): java.lang.Boolean =
    getFinalValue(multiline.map(_.booleanValue()), false)

  def resolveArray(): java.lang.Boolean =
    getFinalValue(array.map(_.booleanValue()), false)

  /** @return
    *   true if each file starts with a header line: by default, except for POSITION files, whose
    *   lines are all records
    */
  def resolveWithHeader(): java.lang.Boolean =
    getFinalValue(withHeader.map(_.booleanValue()), !format.contains(Format.POSITION))

  def resolveSeparator(): String = getFinalValue(separator, ";")

  @JsonInclude(JsonInclude.Include.NON_NULL)
  def resolveQuote(): String = getFinalValue(quote, "\"")

  def resolveEscape(): String = getFinalValue(escape, "\\")

  @JsonIgnore
  def getWrite(): WriteMode = writeStrategy.map(_.toWriteMode()).getOrElse(APPEND)

  // scalastyle:off null
  def resolveNullValue(): String = nullValue.getOrElse(if (resolveEmptyIsNull()) "" else null)
  // scalastyle:on null

  @JsonIgnore
  def getPartitionAttributes()(implicit settings: Settings): Seq[String] = {
    this.getSink().toAllSinks().partition.getOrElse(Nil)
  }

  def resolveEmptyIsNull(): java.lang.Boolean = emptyIsNull.getOrElse(true).booleanValue()

  def getOptions(): Map[String, String] = options.getOrElse(Map.empty)

  @JsonIgnore
  def getXmlOptions(): Map[String, String] = this.getOptions()

  @JsonIgnore
  def getXsdPath(): Option[String] = {
    val xmlOptions = getXmlOptions()
    xmlOptions.get("rowValidationXSDPath").orElse(xmlOptions.get("xsdPath"))
  }

  @JsonIgnore
  def isFillWithDefaultValue(): Boolean = {
    fillWithDefaultValue
  }

  @JsonIgnore
  def getSinkConnectionRef()(implicit settings: Settings): String = {
    val loadConnectionRef =
      if (settings.appConfig.loadConnectionRef.isEmpty)
        settings.appConfig.connectionRef
      else
        settings.appConfig.loadConnectionRef
    getSink().connectionRef.getOrElse(loadConnectionRef)
  }

  @JsonIgnore
  def getSinkConnection()(implicit settings: Settings): Settings.ConnectionInfo =
    settings.appConfig.connections(this.getSinkConnectionRef())

  private def getFinalValue[T](param: Option[T], defaultValue: => T)(implicit ev: Null <:< T): T = {
    if (fillWithDefaultValue)
      param.getOrElse(defaultValue)
    else
      param.orNull
  }

  /** Merge a single attribute
    *
    * @param parent
    *   : Domain level metadata attribute
    * @param child
    *   : Schema level metadata attribute
    * @return
    *   attribute if merge, the domain attribute otherwise.
    */
  protected def merge[T](parent: Option[T], child: Option[T]): Option[T] =
    child.orElse(parent)

  protected def merge[T, U](parent: Map[T, U], child: Map[T, U]): Map[T, U] =
    parent ++ child
  protected def merge[T](parent: List[T], child: List[T]): List[T] =
    if (child.nonEmpty) child else parent

  protected def typeMerge[T](parentOpt: Option[T], childOpt: Option[T])(implicit
    tMerger: (T, T) => T
  ): Option[T] = {
    (childOpt, parentOpt) match {
      case (Some(child), Some(parent)) => Some(tMerger(parent, child))
      case (Some(_), _)                => childOpt
      case _                           => parentOpt
    }
  }

  /** Merge this metadata with its child. Any property defined at the child level overrides the one
    * defined at this level This allow a schema to override the domain metadata attribute Applied to
    * a Domain level metadata
    *
    * @param child
    *   : Schema level metadata
    * @return
    *   the metadata resulting of the merge of the schema and the domain metadata.
    */
  def merge(child: Metadata): Metadata = {
    val mergedSchedule = merge(this.schedule, child.schedule)

    Metadata(
      format = merge(this.format, child.format),
      encoding = merge(this.encoding, child.encoding),
      multiline = merge(this.multiline, child.multiline),
      array = merge(this.array, child.array),
      withHeader = merge(this.withHeader, child.withHeader),
      separator = merge(this.separator, child.separator),
      quote = merge(this.quote, child.quote),
      escape = merge(this.escape, child.escape),
      sink = merge(this.sink, child.sink),
      directory = merge(this.directory, child.directory),
      ack = merge(this.ack, child.ack),
      options = merge(this.options, child.options),
      loader = merge(this.loader, child.loader),
      dagRef = merge(this.dagRef, child.dagRef),
      freshness = merge(this.freshness, child.freshness),
      nullValue = merge(this.nullValue, child.nullValue),
      emptyIsNull = merge(this.emptyIsNull, child.emptyIsNull),
      schedule = mergedSchedule,
      writeStrategy = merge(this.writeStrategy, child.writeStrategy)
      // fillWithDefaultValue = merge(this.fillWithDefaultValue, child.fillWithDefaultValue)
    )
  }

  /** Keep metadata that are different only
    *
    * @param parent
    *   : Schema level metadata
    * @return
    *   the metadata that differs between parent and this element.
    */
  def `keepIfDifferent`(parent: Metadata): Metadata = {
    Metadata(
      format = if (parent.format != this.format) this.format else None,
      encoding = if (parent.encoding != this.encoding) this.encoding else None,
      multiline = if (parent.multiline != this.multiline) this.multiline else None,
      array = if (parent.array != this.array) this.array else None,
      withHeader = if (parent.withHeader != this.withHeader) this.withHeader else None,
      separator = if (parent.separator != this.separator) this.separator else None,
      quote = if (parent.quote != this.quote) this.quote else None,
      escape = if (parent.escape != this.escape) this.escape else None,
      sink = if (parent.sink != this.sink) this.sink else None,
      directory = if (parent.directory != this.directory) this.directory else None,
      ack = if (parent.ack != this.ack) this.ack else None,
      options = if (parent.options != this.options) this.options else None,
      loader = if (parent.loader != this.loader) this.loader else None,
      dagRef = if (parent.dagRef != this.dagRef) this.dagRef else None,
      freshness = if (parent.freshness != this.freshness) this.freshness else None,
      nullValue = if (parent.nullValue != this.nullValue) this.nullValue else None,
      emptyIsNull = if (parent.emptyIsNull != this.emptyIsNull) this.emptyIsNull else None,
      writeStrategy = if (parent.writeStrategy != this.writeStrategy) this.writeStrategy else None
      // fillWithDefaultValue = if (parent.fillWithDefaultValue != this.fillWithDefaultValue) this.fillWithDefaultValue else None
    )
  }

  /** @return
    *   Some of current instance if any attribute is filled otherwise return None
    */
  def asOption(): Option[Metadata] = {
    if (
      format.nonEmpty || encoding.nonEmpty || multiline.nonEmpty || array.nonEmpty ||
      withHeader.nonEmpty || separator.nonEmpty || quote.nonEmpty || escape.nonEmpty || writeStrategy.nonEmpty ||
      sink.nonEmpty || directory.nonEmpty ||
      ack.nonEmpty || options.nonEmpty || loader.nonEmpty || dagRef.nonEmpty ||
      freshness.nonEmpty || nullValue.nonEmpty || emptyIsNull.nonEmpty
    )
      Some(this)
    else
      None
  }

  def compare(other: Metadata): ListDiff[Named] =
    AnyRefDiff.diffAnyRef("", this, other)
}

object Metadata {

  /** Predefined partition columns.
    */
  val CometPartitionColumns =
    List("sl_date", "sl_year", "sl_month", "sl_day", "sl_hour", "sl_minute")

  /** Merge all metadata into one. End of list element have higher precedence.
    */
  def mergeAll(metadatas: List[Metadata]): Metadata = {
    metadatas.foldLeft(Metadata())(_.merge(_))
  }
}
