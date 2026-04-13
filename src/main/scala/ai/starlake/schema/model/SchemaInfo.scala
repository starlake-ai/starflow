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

import ai.starlake.config.Settings.JdbcEngine
import ai.starlake.config.{CometColumns, Settings}
import ai.starlake.lineage.AutoTaskDependencies.{Column, Item, Relation}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Format.{DSV, XML}
import ai.starlake.schema.model.Severity.*
import ai.starlake.utils.Formatter.*
import ai.starlake.utils.conversion.BigQueryUtils
import ai.starlake.utils.{SparkUtils, Utils}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.cloud.bigquery.Schema as BQSchema
import com.google.cloud.spark.bigquery.SparkBigQueryUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{Metadata as SparkMetadata, *}

import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Dataset Schema
  *
  * @param name
  *   : Schema name, must be unique among all the schemas belonging to the same domain. Will become
  *   the hive table name On Premise or BigQuery Table name on GCP.
  * @param pattern
  *   : filename pattern to which this schema must be applied. This instructs the framework to use
  *   this schema to parse any file with a filename that match this pattern.
  * @param attributes
  *   : Attributes parsing rules. See :ref:`attribute_concept`
  * @param metadata
  *   : Dataset metadata See :ref:`metadata_concept`
  * @param comment
  *   : free text
  * @param presql
  *   : Reserved for future use.
  * @param postsql
  *   : We use this attribute to execute sql queries before writing the final dataFrame after
  *   ingestion
  * @param tags
  *   : Set of string to attach to this Schema
  * @param rls
  *   : Experimental. Row level security to this to this schema. See :ref:`rowlevelsecurity_concept`
  * @param filter
  *   allow accepted data to be filtered out before sinking it and column renamed. Only apply to
  *   spark engine.
  */
case class SchemaInfo(
  name: String, // table name without the domain name prefix
  pattern: Pattern,
  attributes: List[TableAttribute],
  metadata: Option[Metadata] = None,
  comment: Option[String] = None,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  tags: Set[String] = Set.empty,
  rls: List[RowLevelSecurity] = Nil,
  expectations: List[ExpectationItem] = Nil,
  primaryKey: List[String] = Nil,
  acl: List[AccessControlEntry] = Nil,
  rename: Option[String] = None,
  sample: Option[String] = None,
  filter: Option[String] = None,
  patternSample: Option[String] = None,
  streams: List[String] = Nil
) extends Named {

  def asMap(): Map[String, Object] = {
    Map(
      "name"          -> name,
      "pattern"       -> pattern.toString,
      "attributes"    -> attributes.map(_.asMap()).asJava,
      "metadata"      -> metadata.getOrElse(Metadata()).asMap().asJava,
      "comment"       -> comment,
      "presql"        -> presql,
      "postsql"       -> postsql,
      "tags"          -> tags,
      "rls"           -> rls.map(_.asMap()).asJava,
      "expectations"  -> expectations.map(_.asMap()).asJava,
      "primaryKey"    -> primaryKey,
      "acl"           -> acl.map(_.asMap()).asJava,
      "rename"        -> rename,
      "filter"        -> filter,
      "patternSample" -> patternSample,
      "streams"       -> streams,
      "finalName"     -> finalName
    )
  }

  def this() = this(
    "",
    Pattern.compile("."),
    Nil,
    None,
    None
  ) // Should never be called. Here for Jackson deserialization only

  @JsonIgnore
  def getTypesMap(): Map[String, String] = {
    attributes.map(attr => attr.name -> attr.`type`).toMap
  }

  @JsonIgnore
  def isPrimaryKey(name: String): Boolean = {
    val isStrategyKey = metadata.exists(_.writeStrategy.exists(_.key.contains(name)))
    primaryKey.contains(name) || isStrategyKey
  }

  def containsRepeatedOrNestedFields(): Boolean = {
    attributes.exists(_.isNestedOrRepeatedField())
  }

  def ddlMapping(datawarehouse: String, schemaHandler: SchemaHandler): List[DDLField] = {
    attributes.map { attribute =>
      val isPrimaryKey = primaryKey.contains(attribute.name)
      attribute.ddlMapping(isPrimaryKey, datawarehouse, schemaHandler)
    }
  }

  @JsonIgnore
  def isFlat(): Boolean = !attributes.exists(attr => Set("variant", "struct").contains(attr.`type`))

  @JsonIgnore
  def isVariant(): Boolean =
    attributes.exists(attr => "variant" == attr.`type`)

  @JsonIgnore
  def isDeep(): Boolean =
    attributes.exists(attr => "struct" == attr.`type`)

  /** @return
    *   renamed column if defined, source name otherwise
    */
  @JsonIgnore
  lazy val finalName: String = rename.getOrElse(name)

  @JsonIgnore
  lazy val attributesWithoutScriptedFields: List[TableAttribute] =
    attributes.filter(_.script.isEmpty)

  @JsonIgnore
  lazy val attributesWithoutScriptedFieldsWithInputFileName: List[TableAttribute] =
    attributesWithoutScriptedFields :+ TableAttribute(
      name = CometColumns.cometInputFileNameColumn
    )

  def scriptAndTransformAttributes(): List[TableAttribute] = {
    attributes.filter { attribute =>
      !attribute.resolveIgnore() && (attribute.script.nonEmpty || attribute.transform.nonEmpty)
    }
  }

  def exceptIgnoreScriptAndTransformAttributes(): List[TableAttribute] = {
    attributes.filter { attribute =>
      !attribute.resolveIgnore() && attribute.script.isEmpty && attribute.transform.isEmpty
    }
  }

  def exceptIgnoreAttributes(): List[TableAttribute] = attributes.filter(!_.resolveIgnore())

  def ignoredAttributes(): List[TableAttribute] = {
    attributes.filter { attribute =>
      attribute.resolveIgnore()
    }
  }

  def hasTransformOrIgnoreOrScriptColumns(): Boolean = {
    attributes.count(attr =>
      attr.resolveIgnore() || attr.script.nonEmpty || attr.transform.nonEmpty
    ) > 0
  }

  /** This Schema as a Spark Catalyst Schema
    *
    * @return
    *   Spark Catalyst Schema
    */
  def sparkSchema(schemaHandler: SchemaHandler): StructType = {
    val temporary = this.name.startsWith("zztmp_")
    val fields = attributes.map { attr =>
      StructField(
        if (temporary) attr.name else attr.getFinalName(),
        attr.sparkType(schemaHandler),
        !attr.resolveRequired()
      )
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  private def sparkSchemaWithCondition(
    schemaHandler: SchemaHandler,
    p: TableAttribute => Boolean,
    withFinalName: Boolean
  ): StructType = {
    SparkUtils.sparkSchemaWithCondition(schemaHandler, attributes, p, withFinalName)
  }

  /** This Schema as a Spark Catalyst Schema, without scripted fields
    *
    * @return
    *   Spark Catalyst Schema
    */
  def sourceSparkSchemaWithoutScriptedFields(schemaHandler: SchemaHandler): StructType = {
    val fields = attributes.filter(_.script.isEmpty).map { attr =>
      StructField(attr.name, attr.sparkType(schemaHandler), !attr.resolveRequired())
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  def sourceSparkSchemaUntypedEpochWithoutScriptedFields(
    schemaHandler: SchemaHandler
  ): StructType = {
    val fields = attributesWithoutScriptedFields.map { attr =>
      val metadata =
        if (attr.`type` == "variant")
          org.apache.spark.sql.types.Metadata.fromJson("""{ "sqlType" : "JSON"}""")
        else org.apache.spark.sql.types.Metadata.empty
      val sparkType = attr
        .`type`(schemaHandler)
        .fold(attr.sparkType(schemaHandler)) { tpe =>
          (tpe.primitiveType, tpe.pattern) match {
            case (PrimitiveType.timestamp, "epoch_second") => LongType
            case (PrimitiveType.timestamp, "epoch_milli")  => LongType
            case (PrimitiveType.date, _)                   => StringType
            case (_, _)                                    => attr.sparkType(schemaHandler)
          }
        }

      StructField(
        name = attr.name,
        dataType = sparkType,
        nullable = !attr.resolveRequired(),
        metadata = metadata
      )
        .withComment(attr.comment.getOrElse(""))
    }
    StructType(fields)
  }

  def sourceSparkSchemaWithoutScriptedFieldsWithInputFileName(
    schemaHandler: SchemaHandler
  ): StructType = {
    sourceSparkSchemaWithoutScriptedFields(schemaHandler)
      .add(StructField(CometColumns.cometInputFileNameColumn, StringType))
  }

  def sparkSchemaWithoutIgnore(
    schemaHandler: SchemaHandler,
    withFinalName: Boolean
  ): StructType =
    sparkSchemaWithCondition(schemaHandler, attr => !attr.resolveIgnore(), withFinalName)

  def sparkSchemaWithIgnoreAndScript(
    schemaHandler: SchemaHandler,
    withFinalName: Boolean
  ): StructType =
    sparkSchemaWithCondition(schemaHandler, _ => true, withFinalName)

  def bigquerySchemaWithoutIgnore(
    schemaHandler: SchemaHandler,
    withFinalName: Boolean
  ): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaWithoutIgnore(schemaHandler, withFinalName))
  }

  def bigquerySchemaWithIgnoreAndScript(
    schemaHandler: SchemaHandler,
    withFinalName: Boolean
  ): BQSchema = {
    BigQueryUtils.bqSchema(sparkSchemaWithIgnoreAndScript(schemaHandler, withFinalName))
  }

  /** return the list of renamed attributes
    *
    * @return
    *   list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(attr => attr.name != attr.getFinalName()).map { attr =>
      (attr.name, attr.getFinalName())
    }
  }

  def finalAttributeNames(): List[String] =
    attributes.filterNot(_.resolveIgnore()).map(attr => attr.getFinalName())

  /** Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @return
    *   error list or true
    */
  def checkValidity(
    domainName: String,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    val forceTablePrefixRegex = settings.appConfig.forceTablePattern.r
    if (!forceTablePrefixRegex.pattern.matcher(name).matches())
      errorList += ValidationMessage(
        Error,
        s"Table $domainName.$name",
        s"name: Table with name $name should respect the pattern ${forceTablePrefixRegex.regex}"
      )

    this.primaryKey.foreach { key =>
      if (!attributes.exists(_.getFinalName() == key))
        errorList += ValidationMessage(
          Error,
          s"Table $domainName.$name",
          s"Primary key $key is not defined in the schema"
        )
    }

    metadata.foreach { metadata =>
      for (errors <- metadata.checkValidity(domainName, Some(this)).left) {
        errorList ++= errors
      }
    }

    attributes.foreach { attribute =>
      for (errors <- attribute.checkValidity(schemaHandler, domainName, this).left) {
        errorList ++= errors
      }
    }

    val format = this.metadata.map(_.resolveFormat()).getOrElse(DSV)
    def isXMLAttribute: TableAttribute => Boolean =
      (format == XML && _.getFinalName().startsWith("_"))
    val firstScriptedFiedlIndex = attributes.indexWhere(_.script.isDefined)
    val lastNonScriptedFiedlIndex =
      attributes.lastIndexWhere(x => x.script.isEmpty && !isXMLAttribute(x))

    if (firstScriptedFiedlIndex >= 0 && firstScriptedFiedlIndex < lastNonScriptedFiedlIndex) {
      errorList +=
        ValidationMessage(
          Error,
          "Table attributes",
          s"""Scripted fields can only appear at the end of the schema. Found scripted field at position $firstScriptedFiedlIndex and non scripted field at position $lastNonScriptedFiedlIndex""".stripMargin
        )
    }

    val duplicateErrorMessage =
      "%s is defined %d times. An attribute can only be defined once."
    for (
      errors <- Utils
        .duplicates("Table attribute name", attributes.map(_.name), duplicateErrorMessage)
        .left
    ) {
      errorList ++= errors
    }

    metadata.map(_.getStrategyOptions()).foreach { strategy =>
      if (strategy.requireKey() && strategy.key.isEmpty) {
        errorList +=
          ValidationMessage(
            Error,
            "Table/Metadata/Strategy attributes",
            s"""key cannot be empty""".stripMargin
          )
      }
      if (strategy.requireTimestamp() && strategy.timestamp.isEmpty) {
        errorList +=
          ValidationMessage(
            Error,
            "Table/Metadata/Strategy attributes",
            s"""timestamp cannot be empty""".stripMargin
          )
      }
    }

    val fullTableName = s"${domainName}_${name}"
    val streamsErrors = streams.map { stream =>
      if (!stream.startsWith(fullTableName)) {
        Left(
          List(
            ValidationMessage(
              Error,
              s"Table $domainName.$name",
              s"stream name error: $stream is not a valid stream name. Stream name should start with $fullTableName"
            )
          )
        )
      } else {
        Right(true)
      }
    }
    errorList ++= streamsErrors.collect { case Left(errors) => errors }.flatten

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def discreteAttrs(schemaHandler: SchemaHandler): List[TableAttribute] =
    attributes.filter(_.getMetricType(schemaHandler) == MetricType.DISCRETE)

  def continuousAttrs(schemaHandler: SchemaHandler): List[TableAttribute] =
    attributes.filter(_.getMetricType(schemaHandler) == MetricType.CONTINUOUS)

  def esMapping(
    template: Option[String],
    domainName: String,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): String = {
    val attrs = attributes.map(_.indexMapping(schemaHandler)).mkString(",")
    val properties =
      s"""
         |"properties": {
         |$attrs
         |}""".stripMargin

    val tse = Map(
      "properties" -> properties,
      "attributes" -> attrs,
      "domain"     -> domainName.toLowerCase,
      "table"      -> finalName.toLowerCase
    )

    template
      .getOrElse {
        """
         |{
         |  "index_patterns": ["${domain}.${table}", "${domain}.${table}-*"],
         |  "settings": {
         |    "number_of_shards": "1",
         |    "number_of_replicas": "0"
         |  },
         |  "mappings": {
         |      "_source": {
         |        "enabled": true
         |      },
         |
         |      "properties": {
         |        ${attributes}
         |      }
         |  }
         |}""".stripMargin
      }
      .richFormat(schemaHandler.activeEnvVars(), tse)
  }

  def mergedMetadata(domainMetadata: Option[Metadata]): Metadata = {
    domainMetadata
      .getOrElse(Metadata())
      .merge(this.metadata.getOrElse(Metadata()))

  }

  def containsArrayOfRecords(): Boolean = attributes.exists(_.containsArrayOfRecords())

  def containsVariant(): Boolean = attributes.exists(_.containsVariant())

  private def dotRow(
    attr: TableAttribute,
    isPK: Boolean,
    isFK: Boolean,
    includeAllAttrs: Boolean
  ): Option[String] = {
    val col = attr.default match {
      case None    => s"""${attr.getFinalName()}:${attr.`type`}"""
      case Some(x) => s"""${attr.getFinalName()}:${attr.`type`} = $x"""
    }
    (isPK, isFK, includeAllAttrs) match {
      case (true, true, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><B><I> $col </I></B></td></tr>""")
      case (true, false, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><B> $col </B></td></tr>""")
      case (false, true, _) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"><I> $col </I></td></tr>""")
      case (false, false, true) =>
        Some(s"""<tr><td port="${attr.getFinalName()}"> $col </td></tr>""")
      case (false, false, false) => None
    }
  }

  private def relationAsRelation(
    attr: TableAttribute,
    domain: String,
    tableNames: Set[String]
  ): Option[Relation] = {
    val tableLabel = s"${domain}.$name"
    attr.deepForeignKey() match {
      case None => None
      case Some(ref) =>
        val tab = ref.split('.')
        val (refDomain, refSchema, refAttr) = tab.length match {
          case 3 =>
            (tab(0), tab(1), if (tab(2).isEmpty) "0" else tab(2)) // ref to domain.table.column
          case 2 =>
            (domain, tab(0), if (tab(1).isEmpty) "0" else tab(1)) // ref to table.column
          case 1 => (domain, tab(0), "0") // ref to table
          case _ =>
            throw new Exception(
              s"Invalid number of parts in relation $ref in domain $domain and table $name"
            )
        }
        val fullRefName = refDomain + "." + refSchema
        if (tableNames.contains(fullRefName.toLowerCase()))
          Some(
            Relation(
              s"$tableLabel.${attr.getFinalName()}",
              s"${refDomain}.$refSchema.$refAttr",
              " " // No Label
            )
          )
        else
          None
    }
  }

  private def relationAsDot(
    attr: TableAttribute,
    domain: String,
    tableNames: Set[String]
  ): Option[String] = {
    val tableLabel = s"${domain}_$name"
    attr.deepForeignKey() match {
      case None => None
      case Some(ref) =>
        val tab = ref.split('.')
        val (refDomain, refSchema, refAttr) = tab.length match {
          case 3 =>
            (tab(0), tab(1), if (tab(2).isEmpty) "0" else tab(2)) // ref to domain.table.column
          case 2 =>
            (domain, tab(0), if (tab(1).isEmpty) "0" else tab(1)) // ref to table.column
          case 1 => (domain, tab(0), "0") // ref to table
          case _ =>
            throw new Exception(
              s"Invalid number of parts in relation $ref in domain $domain and table $name"
            )
        }
        val fullRefName = refDomain + "." + refSchema
        if (tableNames.contains(fullRefName.toLowerCase()))
          Some(s"$tableLabel:${attr.getFinalName()} -> ${refDomain}_$refSchema:$refAttr")
        else
          None
    }
  }

  def fkComponents(
    attr: TableAttribute,
    domain: String
  ): Option[(TableAttribute, String, String, String)] = { // (attr, refDomain, refSchema, refAttr)
    attr.foreignKey match {
      case None => None
      case Some(ref) =>
        val tab = ref.split('.')
        val (refDomain, refSchema, refAttr) = tab.length match {
          case 3 =>
            (
              tab(0),
              tab(1),
              if (tab(2).isEmpty) attr.getFinalName() else tab(2)
            ) // ref to domain.table.column
          case 2 =>
            (domain, tab(0), if (tab(1).isEmpty) this.finalName else tab(1)) // ref to table.column
          case 1 => (domain, tab(0), attr.getFinalName()) // ref to table
          case _ =>
            throw new Exception(
              s"Invalid number of parts in relation $ref in domain $domain and table $name"
            )
        }
        Some((attr, refDomain, refSchema, refAttr))
    }
  }

  def foreignTablesForDot(domainNamePrefix: String): List[String] = {
    val fkTables = attributes.flatMap(_.deepForeignKey()).map { fk =>
      val tab = fk.split('.')
      tab.length match {
        case 3 => tab(0) + "." + tab(1) // reference to domain.table.column
        case 2 => domainNamePrefix + "." + tab(0) // reference to table.column
        case 1 => domainNamePrefix + "." + tab(0) // reference to table
      }
    }
    if (fkTables.nonEmpty)
      fkTables :+ finalName
    else
      fkTables
  }

  def asItem(domainName: String, tableNames: Set[String]): Option[(Item, List[Relation])] = {
    val fullName = domainName + "." + this.finalName
    val includeTable = tableNames.contains(fullName.toLowerCase)
    if (includeTable) {
      val relations = attributes
        .flatMap { attr => relationAsRelation(attr, domainName, tableNames) }

      val columns =
        attributes.map { attr =>
          val isPK = isPrimaryKey(attr.getFinalName())
          val isFK = attr.deepForeignKey().isDefined
          Column(
            s"$fullName.${attr.getFinalName()}",
            attr.getFinalName(),
            attr.`type`,
            attr.comment,
            isPK,
            isFK
          )
        }
      val item = Item(
        fullName,
        this.finalName,
        "table",
        columns
      )
      Some((item, relations))
    } else {
      None
    }
  }

  def asDot(domainName: String, includeAllAttrs: Boolean, tableNames: Set[String]): String = {
    val fullName = domainName + "." + this.finalName
    val includeTable = tableNames.contains(fullName.toLowerCase)
    if (includeTable) {
      val tableLabel = s"${domainName}_$finalName"
      val header =
        s"""<tr>
           |<td port="0" bgcolor="#008B00"><B><FONT color="white"> $finalName </FONT></B></td>
           |</tr>\n""".stripMargin
      val relations = attributes
        .flatMap { attr => relationAsDot(attr, domainName, tableNames) }
        .mkString("\n")

      val rows =
        attributes.flatMap { attr =>
          val isPK = isPrimaryKey(attr.getFinalName())
          val isFK = attr.deepForeignKey().isDefined
          dotRow(attr, isPK, isFK, includeAllAttrs)
        } mkString "\n"

      s"""
         |$tableLabel [label=<
         |<table border="0" cellborder="1" cellspacing="0">
         |""".stripMargin +
      header +
      rows +
      """
          |</table>>];
          |
          |""".stripMargin +
      relations
    } else {
      ""
    }
  }

  def normalize(): SchemaInfo = {
    this.copy(
      rls = this.rls.map(rls => {
        val grants = rls.grants.flatMap(_.replaceAll("\"", "").split(','))
        rls.copy(grants = grants)
      }),
      acl = this.acl.map(acl => {
        val grants = acl.grants.flatMap(_.replaceAll("\"", "").split(','))
        acl.copy(grants = grants)
      }),
      metadata = metadata
        .map(m =>
          m.copy(writeStrategy = m.writeStrategy.map { s =>
            if (s.key.isEmpty) s.copy(key = this.primaryKey) else s
          })
        ),
      primaryKey = if (this.primaryKey.isEmpty) {
        metadata.flatMap(_.writeStrategy.map(_.key)).getOrElse(Nil)
      } else this.primaryKey
    )

  }

  /** @param table
    *   table to add field to
    * @param sourceTableFilter
    *   filter applied after transformation and before field removal
    * @return
    *   query
    */
  def buildSecondStepSqlSelectOnLoad(
    table: String,
    jdbcEngine: Option[JdbcEngine] = None
  ): String = {
    val attributeQuote = jdbcEngine.map(_.quote).getOrElse("")
    val (scriptAttributes, transformAttributes) =
      scriptAndTransformAttributes().partition(_.script.nonEmpty)

    val simpleAttributes = exceptIgnoreScriptAndTransformAttributes()

    val sqlScripts: List[String] = scriptAttributes.map { scriptField =>
      val script =
        scriptField.script.getOrElse(throw new IllegalStateException("Should never happen"))
      s"$script AS $attributeQuote${scriptField.getFinalName()}$attributeQuote"
    }

    val sqlScriptsFinalName: List[String] = scriptAttributes.map { scriptField =>
      s"$attributeQuote${scriptField.getFinalName()}$attributeQuote"
    }

    val sqlTransforms: List[String] = transformAttributes.map { transformField =>
      val transform =
        transformField.transform.getOrElse(throw new IllegalStateException("Should never happen"))
      s"$transform AS $attributeQuote${transformField.getFinalName()}$attributeQuote"
    }

    val sqlTransformsFinalName: List[String] = transformAttributes.map { transformField =>
      s"$attributeQuote${transformField.getFinalName()}$attributeQuote"
    }

    val sqlSimple = simpleAttributes.map { field =>
      s"$attributeQuote${field.getName()}$attributeQuote as $attributeQuote${field.getFinalName()}$attributeQuote"
    }

    val sqlFinalSimple = simpleAttributes.map { field =>
      s"$attributeQuote${field.getFinalName()}$attributeQuote"
    }

    val sqlIgnored = ignoredAttributes().map { field =>
      s"$attributeQuote${field.getName()}$attributeQuote"
    }

    val allFinalAttributes =
      (sqlFinalSimple ++ sqlScriptsFinalName ++ sqlTransformsFinalName).mkString(", ")
    val allAttributes = (sqlSimple ++ sqlScripts ++ sqlTransforms ++ sqlIgnored).mkString(", ")

    val sourceTableFilterSQL = this.filter match {
      case Some(filter) => s"WHERE $filter"
      case None         => ""

    }
    s"""
       |SELECT $allFinalAttributes
       |  FROM (
       |    SELECT $allAttributes
       |    FROM $table
       |  ) AS SL_INTERNAL_FROM_SELECT
       |  $sourceTableFilterSQL
       |""".stripMargin

  }

  /** @param fallbackSchema
    *   complete missing information with this schema
    * @param domainMetadata
    *   metadata to compare with. Useful to keep only things that are different.
    * @return
    *   merged schema
    */
  def mergeWith(
    fallbackSchema: SchemaInfo,
    domainMetadata: Option[Metadata] = None,
    attributeMergeStrategy: AttributeMergeStrategy
  )(implicit
    schemaHandler: SchemaHandler
  ): SchemaInfo = {
    this.copy(
      rename = this.rename.orElse(fallbackSchema.rename),
      comment = this.comment.orElse(fallbackSchema.comment),
      metadata = Metadata
        .mergeAll(Nil ++ domainMetadata ++ fallbackSchema.metadata ++ this.metadata)
        .`keepIfDifferent`(domainMetadata.getOrElse(Metadata()))
        .asOption(),
      presql = if (this.presql.isEmpty) fallbackSchema.presql else this.presql,
      postsql = if (this.postsql.isEmpty) fallbackSchema.postsql else this.postsql,
      tags = if (this.tags.isEmpty) fallbackSchema.tags else this.tags,
      rls = if (this.rls.isEmpty) fallbackSchema.rls else this.rls,
      expectations =
        if (this.expectations.isEmpty) fallbackSchema.expectations else this.expectations,
      acl = if (this.acl.isEmpty) fallbackSchema.acl else this.acl,
      sample = this.sample.orElse(fallbackSchema.sample),
      filter = this.filter.orElse(fallbackSchema.filter),
      attributes = TableAttribute.mergeAll(
        this.attributes,
        fallbackSchema.attributes,
        attributeMergeStrategy
      )
    )
  }

  def containGrantees(grantees: List[String]): List[String] = {
    val intersection = this.acl.flatMap(_.grants).intersect(grantees)
    intersection
  }

  def pathIn(domainName: String)(implicit settings: Settings): Path =
    SchemaInfo.path(domainName, this.name)
}

object SchemaInfo {

  val SL_INTERNAL_TABLE = "SL_INTERNAL_TABLE"

  def mapping(
    domainName: String,
    schemaName: String,
    obj: StructField,
    schemaHandler: SchemaHandler
  )(implicit settings: Settings): String = {
    fromSparkSchema(schemaName, obj).esMapping(
      None,
      domainName,
      schemaHandler
    )
  }
  def fromSparkSchema(
    schemaName: String,
    obj: StructField
  ): SchemaInfo = {
    def buildAttributeTree(obj: StructField): TableAttribute = {
      if (SparkBigQueryUtil.isJson(obj.metadata)) {
        TableAttribute(
          obj.name,
          PrimitiveType.variant.value,
          required = Some(!obj.nullable),
          comment = obj.getComment()
        )
      } else {
        obj.dataType match {
          case StringType | LongType | IntegerType | ShortType | DoubleType | BooleanType |
              ByteType | DateType | TimestampType =>
            TableAttribute(
              obj.name,
              obj.dataType.typeName,
              required = Some(!obj.nullable),
              comment = obj.getComment()
            )
          case _: DecimalType =>
            TableAttribute(
              obj.name,
              "decimal",
              required = Some(!obj.nullable),
              comment = obj.getComment()
            )
          case ArrayType(eltType, containsNull) =>
            buildAttributeTree(obj.copy(dataType = eltType)).copy(array = Some(true))
          case x: StructType =>
            TableAttribute(
              obj.name,
              "struct",
              required = Some(!obj.nullable),
              attributes = x.fields.map(buildAttributeTree).toList,
              comment = obj.getComment()
            )
          case _ => throw new Exception(s"Unsupported Date type ${obj.dataType} for object $obj ")
        }
      }
    }

    SchemaInfo(
      schemaName,
      Pattern.compile("ignore"),
      buildAttributeTree(obj).attributes,
      None,
      None
    )
  }

  def compare(existing: SchemaInfo, incoming: SchemaInfo): Try[TableDiff] = {
    Try {
      if (!existing.isFlat() || !incoming.isFlat())
        throw new Exception("Only flat schemas are supported")

      val patternDiff: ListDiff[String] =
        if (existing.pattern.toString != incoming.pattern.toString)
          ListDiff(
            "pattern",
            Nil,
            Nil,
            List((None, existing.pattern.toString, incoming.pattern.toString))
          )
        else {
          ListDiff(
            "pattern",
            Nil,
            Nil,
            Nil
          )
        }
      val attributesDiff =
        AnyRefDiff.diffListNamed("attributes", existing.attributes, incoming.attributes)

      val metadataDiff: ListDiff[Named] =
        AnyRefDiff.diffOptionAnyRef("metadata", existing.metadata, incoming.metadata)

      val commentDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("comment", existing.comment, incoming.comment)

      val presqlDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("presql", existing.presql.toSet, incoming.presql.toSet)

      val postsqlDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("postsql", existing.postsql.toSet, incoming.postsql.toSet)

      val tagsDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("tags", existing.tags, incoming.tags)

      val rlsDiff: ListDiff[Named] = AnyRefDiff.diffListNamed("rls", existing.rls, incoming.rls)

      val expectationsDiff: ListDiff[Named] =
        AnyRefDiff.diffAnyRef("expectations", existing.expectations, incoming.expectations)

      val primaryKeyDiff: ListDiff[String] =
        AnyRefDiff.diffSetString("primaryKey", existing.primaryKey.toSet, incoming.primaryKey.toSet)

      val aclDiff: ListDiff[Named] = AnyRefDiff.diffListNamed("acl", existing.acl, incoming.acl)

      val renameDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("rename", existing.rename, incoming.rename)

      val sampleDiff: ListDiff[String] =
        AnyRefDiff.diffOptionString("sample", existing.sample, incoming.sample)

      val filter: ListDiff[String] =
        AnyRefDiff.diffOptionString("filter", existing.filter, incoming.filter)

      val patternSample: ListDiff[String] =
        AnyRefDiff.diffOptionString("patternSample", existing.patternSample, incoming.patternSample)

      TableDiff(
        existing.name,
        attributesDiff.asOption(),
        patternDiff.asOption(),
        metadataDiff.asOption(),
        commentDiff.asOption(),
        presqlDiff.asOption(),
        postsqlDiff.asOption(),
        tagsDiff.asOption(),
        rlsDiff.asOption(),
        expectationsDiff.asOption(),
        primaryKeyDiff.asOption(),
        aclDiff.asOption(),
        renameDiff.asOption(),
        sampleDiff.asOption(),
        filter.asOption(),
        patternSample.asOption()
      )
    }
  }

  def path(domainName: String, schemaName: String)(implicit settings: Settings): Path =
    new Path(DomainInfo.path(domainName), schemaName + ".sl.yml")
}

case class TableDesc(version: Int, table: SchemaInfo)

object MakesCompîlerHappyCrossCompile {
  SparkMetadata.empty
}
