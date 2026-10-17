package ai.starlake.job.validator

import ai.starlake.job.validator.RowValidator.{SL_ERROR_COL, SL_INPUT_COL}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.Trim.{BOTH, LEFT, NONE, RIGHT}
import ai.starlake.schema.model.{PrimitiveType, TableAttribute, TransformInput, Type}
import ai.starlake.utils.TransformEngine
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, Metadata, MetadataBuilder, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
object RowValidator {
  val SL_INPUT_COL = "$SL_INPUT_COL"
  val SL_ERROR_COL = "$SL_ERROR_COL"
}

class RowValidator(
  attributes: List[TableAttribute],
  types: List[Type],
  allPrivacyLevels: Map[String, ((TransformEngine, List[String]), TransformInput)],
  emptyIsNull: Boolean,
  rejectWithValue: Boolean
) {

  protected val typesMap = types.map(tpe => tpe.name -> tpe).toMap

  // One UDF per privacy level: the same UDF instance on the same column is evaluated once by Spark
  private val privacyUDFs = scala.collection.mutable.Map.empty[String, UserDefinedFunction]

  /** @return
    *   the privacy engine of the attribute, applied to its input value as a string, or None when it
    *   has no privacy or a SQL one, which the SQL of the load applies. A failing engine returns
    *   null.
    */
  private def privacyEngine(attribute: TableAttribute): Option[Column => Column] = {
    val privacyLevel = attribute.resolvePrivacy()
    if (privacyLevel == TransformInput.None || privacyLevel.sql) {
      None
    } else {
      val ((privacyAlgo, privacyParams), _) = allPrivacyLevels.getOrElse(
        privacyLevel.value,
        throw new IllegalArgumentException(
          s"Unknown privacy level ${privacyLevel.value} of attribute ${attribute.name}. Known levels are ${allPrivacyLevels.keys.toList.sorted
              .mkString(", ")}"
        )
      )
      val privacyUDF = privacyUDFs.getOrElseUpdate(
        privacyLevel.value,
        privacyLevel.cryptUDF(privacyAlgo, privacyParams)
      )
      Some(column => privacyUDF(column.cast(StringType)))
    }
  }

  /** @return
    *   the privacy transformation of the attribute: the output of its privacy engine, where an
    *   empty output, such as HIDE, is null for a non-string attribute
    */
  private def privacyTransform(attribute: TableAttribute): Option[Column => Column] =
    privacyEngine(attribute).map { engine => column =>
      val output = engine(column)
      if (attribute.`type` == PrimitiveType.variant.value) {
        // A variant holds JSON: the output is stored as a JSON string, ["..."] without its brackets
        val jsonArray = to_json(array(output))
        when(output.isNull.or(output === ""), lit(null))
          .otherwise(jsonArray.substr(lit(2), length(jsonArray) - 2))
      } else if (isStringAttribute(attribute)) output
      else when(output === "", lit(null)).otherwise(output)
    }

  private def isStringAttribute(attribute: TableAttribute): Boolean =
    typesMap.get(attribute.`type`).forall { tpe =>
      tpe.primitiveType == PrimitiveType.string || tpe.primitiveType == PrimitiveType.variant
    }

  /** @return
    *   the value converted to the attribute type, null if it cannot be
    */
  private def fitToField(attribute: TableAttribute, column: Column)(implicit
    schemaHandler: SchemaHandler
  ): Column = {
    val attributeType = typesMap(attribute.`type`)
    val fittedAttribute: Column = attributeType.primitiveType match {
      case PrimitiveType.timestamp =>
        PrimitiveType.timestamp
          .parseUDF(attributeType.pattern, attributeType.zone)(column)
      case PrimitiveType.date =>
        PrimitiveType.date.parseUDF(attributeType.pattern, attributeType.zone)(column)
      case PrimitiveType.boolean =>
        PrimitiveType.boolean.parseUDF(attributeType.pattern)(column)
      case PrimitiveType.double =>
        PrimitiveType.double.parseUDF(attributeType.zone)(column)
      case _ =>
        column.cast(attribute.sparkType(schemaHandler))
      // we should never have struct because it is not a valid leaf
    }
    when(column.isNull, lit(null).cast(attribute.sparkType(schemaHandler)))
      .otherwise(fittedAttribute)
  }

  def validateWorkflow(inputDF: DataFrame, inputAttributes: List[TableAttribute])(implicit
    schemaHandler: SchemaHandler
  ): (Dataset[Row], Dataset[Row]) = {
    val assertedDF =
      inputDF.transform(prepareData(inputAttributes)).transform(validate(inputAttributes))
    val validDF = assertedDF
      .filter(array_size(col(SL_ERROR_COL)) === 0)
      .drop(SL_ERROR_COL, SL_INPUT_COL)
      .transform(fitToSchema(inputAttributes))
    val invalidDF = assertedDF
      .filter(array_size(col(SL_ERROR_COL)) =!= 0)
    invalidDF -> validDF
  }

  /** This function expects intersect schema to be compatible therefore, no check is done during
    * fitting. This function also fill the gap with input schema for missing columns.
    * @param inputDF
    * @return
    */
  def fitToSchema(inputAttributes: List[TableAttribute])(inputDF: DataFrame)(implicit
    schemaHandler: SchemaHandler
  ): DataFrame = {
    def buildSparkFieldMetadata(currentSchema: TableAttribute): Metadata = {
      val metadataBuilder = new MetadataBuilder()
      if (currentSchema.`type` == PrimitiveType.variant.value) {
        metadataBuilder.putString("sqlType", "JSON")
      }
      metadataBuilder.build()
    }

    def fitToSchema(
      currentSchema: TableAttribute,
      currentInputSchema: Option[TableAttribute]
    ): Column => Column = {
      currentInputSchema match {
        case Some(inputSchema) if currentSchema.resolveArray() =>
          // array case: don't check element type yet
          (arrayColumn: Column) => {
            transform(
              arrayColumn,
              elementColumn => {
                fitToSchema(
                  currentSchema.copy(array = Some(false)),
                  Some(inputSchema.copy(array = Some(false)))
                )(elementColumn)
              }
            )
          }
        case Some(_)
            if currentSchema.attributes.isEmpty && privacyTransform(currentSchema).isDefined =>
          // Leaf with privacy, variant included: the privacy is applied to the input value, then
          // its output is fitted to the attribute type
          val privacy = privacyTransform(currentSchema).get
          (column: Column) => fitToField(currentSchema, privacy(column))
        case Some(_) if currentSchema.`type` == PrimitiveType.variant.value =>
          (column: Column) => column
        case Some(inputSchema) if currentSchema.attributes.nonEmpty =>
          (structColumn: Column) => {
            struct(currentSchema.attributes.map { field =>
              fitToSchema(
                field,
                inputSchema.attributes.find(_.name == field.name)
              )(structColumn.getField(field.name))
                .as(field.name, metadata = buildSparkFieldMetadata(field))
            }: _*)
          }
        case Some(_) =>
          // This is the leaf case.
          (column: Column) => {
            when(
              (lit(
                currentSchema.primitiveSparkType(schemaHandler).typeName
              ) =!= StringType.typeName)
                .and(
                  typeof(column) === lit(
                    currentSchema.primitiveSparkType(schemaHandler).typeName
                  )
                ),
              column.cast(currentSchema.sparkType(schemaHandler))
            )
              .otherwise(
                fitToField(currentSchema, column)
              )
          }
        case None =>
          (_: Column) => {
            // default value is not subject to trim. This means that default value, if column is trimmed, should be trimmed.
            val default = lit(currentSchema.default.orNull)
            fitToField(
              currentSchema,
              privacyTransform(currentSchema).map(_(default)).getOrElse(default)
            )
          }
      }
    }
    val projectionsList: List[Column] = for {
      field <- attributes
    } yield {
      fitToSchema(
        field,
        inputAttributes.find(_.name == field.name)
      )(col(field.name)).as(field.name, metadata = buildSparkFieldMetadata(field))
    }
    inputDF.select(projectionsList: _*)
  }

  def validate(inputAttributes: List[TableAttribute])(inputDF: DataFrame)(implicit
    schemaHandler: SchemaHandler
  ): DataFrame = {

    def rejectValue(path: Column, message: String, currentColumn: Column): Column = {
      val rejectMessage = concat(lit("`"), path, lit("` "), lit(message))
      if (rejectWithValue) {
        concat(
          rejectMessage,
          lit(": "),
          coalesce(currentColumn.cast(StringType), lit("NULL"))
        )
      } else rejectMessage
    }

    def isRequired(attribute: TableAttribute, column: Column, path: Column): Column = {
      if (attribute.resolveRequired()) {
        when(column.isNull, rejectValue(path, " can't be null", path))
          .otherwise(lit(null))
      } else {
        lit(null)
      }
    }

    def matchPattern(attribute: TableAttribute, column: Column, path: Column): Column = {
      val attributeType = typesMap(attribute.`type`)
      val attributePatternIsValid: Column = attributeType.primitiveType match {
        case PrimitiveType.timestamp =>
          PrimitiveType.timestamp
            .parseUDF(attributeType.pattern, attributeType.zone)(column)
            .isNotNull
        case PrimitiveType.date =>
          PrimitiveType.date.parseUDF(attributeType.pattern, attributeType.zone)(column).isNotNull
        case PrimitiveType.boolean =>
          PrimitiveType.boolean.parseUDF(attributeType.pattern)(column).isNotNull
        case PrimitiveType.double =>
          PrimitiveType.double.parseUDF(attributeType.zone)(column).isNotNull
        case _ =>
          val finalPattern =
            if (attributeType.pattern.startsWith("(?")) attributeType.pattern
            else "(?s)" + attributeType.pattern
          regexp_like(column, lit(finalPattern))
            .and(
              length(column) === length(regexp_extract(column, finalPattern, 0))
            )
        // we should never have struct because it is not a valid leaf
      }
      when(column.isNull, lit(null))
        .when(
          not(attributePatternIsValid),
          rejectValue(
            path,
            s"(${attribute.`type`}) doesn't match pattern `" + attributeType.pattern + "`",
            column
          )
        )
        .otherwise(lit(null))
    }

    def checkCast(attribute: TableAttribute, column: Column, path: Column): Column = {
      // this is useful to check for overflow cases
      val attributeType = typesMap(attribute.`type`)
      val attributePatternIsCastable: Column = attributeType.primitiveType match {
        case PrimitiveType.timestamp | PrimitiveType.date | PrimitiveType.boolean |
            PrimitiveType.string =>
          lit(true)
        case PrimitiveType.byte =>
          PrimitiveType.byte.parseUDF(column).isNotNull
        case _ =>
          column
            .cast(attributeType.primitiveType.sparkType(attributeType.zone))
            .isNotNull
        // we should never have struct because it is not a valid leaf
      }
      when(column.isNull, lit(null))
        .when(
          not(attributePatternIsCastable),
          rejectValue(
            path,
            "cannot be converted to `" + attributeType.primitiveType.value + "` with sl type `" + attributeType.name + "`",
            column
          )
        )
        .otherwise(lit(null))
    }

    def checkPrivacyOutputType(attribute: TableAttribute, column: Column, path: Column): Column =
      (privacyEngine(attribute), privacyTransform(attribute)) match {
        case (Some(engine), Some(privacy)) =>
          val output = privacy(column)
          // A privacy engine that fails returns null
          when(
            column.isNotNull.and(engine(column).isNull),
            rejectValue(path, "privacy could not be applied", column)
          )
            .when(
              column.isNotNull.and(output.isNull).and(lit(attribute.resolveRequired())),
              rejectValue(path, "is required but its privacy output is null", column)
            )
            // The output is checked with the conversion that stores it
            .when(
              output.isNotNull.and(fitToField(attribute, output).isNull),
              rejectValue(path, "input and privacy output type are not compatible", column)
            )
            .otherwise(lit(null))
        case _ =>
          lit(null)
      }

    /** @return
      *   (data Column, column name) => column validation expression
      */
    def validate(
      currentSchema: TableAttribute,
      currentInputSchema: Option[TableAttribute]
    ): (Column, Column) => Column = {
      currentInputSchema match {
        case Some(inputSchema) =>
          if (
            currentSchema.`type` == PrimitiveType.variant.value && !currentSchema.resolveArray()
          ) { (dataColumn: Column, pathColumn: Column) =>
            array_compact(array(checkPrivacyOutputType(currentSchema, dataColumn, pathColumn)))
              .cast(ArrayType(StringType))
          } else if (inputSchema.resolveArray() != currentSchema.resolveArray()) {
            if (currentSchema.resolveArray()) { (dataColumn: Column, pathColumn: Column) =>
              array(rejectValue(pathColumn, "is not an array", dataColumn))
            } else { (column: Column, pathColumn: Column) =>
              array(
                rejectValue(
                  pathColumn,
                  "expected to be a " + currentSchema.`type` + " but found " + inputSchema.`type`,
                  column
                )
              )
            }
          } else if (
            inputSchema.attributes.nonEmpty != currentSchema.attributes.nonEmpty && currentSchema.`type` != PrimitiveType.variant.value
          ) {
            if (
              currentSchema.attributes.nonEmpty && inputSchema.`type` == PrimitiveType.string.value
            ) {
              // When field is completely null in the input, spark infer it as a string instead of struct. We ignore this case and let the fit to the schema fails if we were wrong.
              // if we want to be sure that this is not an error, we would have to check that all values are null before validating the type but this may be costly.
              (column: Column, pathColumn: Column) => array()
            } else { (column: Column, pathColumn: Column) =>
              array(
                rejectValue(
                  pathColumn,
                  "expected to be a " + currentSchema.`type` + " but found " + inputSchema.`type`,
                  column
                )
              )
            }
          }
          // at this point, there is no container mismatch
          else if (currentSchema.resolveArray()) {
            val fAttributes = validate(
              currentSchema.copy(array = Some(false)),
              Some(inputSchema.copy(array = Some(false)))
            )
            (arrayColumn: Column, pathColumn: Column) => {
              reduce(
                coalesce(arrayColumn, array()),
                struct(
                  array().cast(ArrayType(StringType)).as("errors"),
                  lit(0L).as("index")
                ),
                (errorInfos, elementColumn) => {
                  struct(
                    concat(
                      errorInfos("errors"),
                      array_compact(
                        fAttributes(
                          elementColumn,
                          concat(pathColumn, lit("["), errorInfos("index"), lit("]"))
                        )
                      )
                    ).as("errors"),
                    errorInfos("index") + 1
                  )
                }
              )("errors")
            }
          } else if (currentSchema.attributes.nonEmpty) {
            (structColumn: Column, pathColumn: Column) =>
              {
                array_compact(flatten(array(currentSchema.attributes.map { field =>
                  validate(
                    field,
                    inputSchema.attributes.find(_.name == field.name)
                  )(
                    structColumn.getField(field.name),
                    when(length(pathColumn) === 0, lit(field.name))
                      .otherwise(concat(pathColumn, lit("."), lit(field.name)))
                  )
                }: _*)))
              }
          } else { (dataColumn: Column, pathColumn: Column) =>
            {
              when(
                dataColumn.isNull,
                array_compact(array(isRequired(currentSchema, dataColumn, pathColumn)))
              )
                .when(
                  (lit(
                    currentSchema.primitiveSparkType(schemaHandler).typeName
                  ) =!= StringType.typeName)
                    .and(
                      typeof(dataColumn) === lit(
                        currentSchema.primitiveSparkType(schemaHandler).typeName
                      )
                    ),
                  // Typed input: only its privacy output needs a check
                  array_compact(
                    array(checkPrivacyOutputType(currentSchema, dataColumn, pathColumn))
                  )
                )
                .otherwise(
                  array_compact(
                    array(
                      coalesce(
                        matchPattern(currentSchema, dataColumn, pathColumn),
                        checkCast(currentSchema, dataColumn, pathColumn)
                      ),
                      checkPrivacyOutputType(currentSchema, dataColumn, pathColumn)
                    )
                  )
                )
            }
          }
        case None =>
          // can't apply validation on it but check if schema was expecting this field to be not null
          if (currentSchema.resolveRequired()) { (_: Column, pathColumn: Column) =>
            array(rejectValue(pathColumn, "is required", lit(null)))
          } else { (_: Column, _: Column) =>
            array()
          }
      }
    }
    val projectionsList: List[Column] = for {
      field <- attributes
    } yield {
      validate(
        field,
        inputAttributes.find(_.name == field.name)
      )(col(field.name), lit(field.name)).as(field.name)
    }
    inputDF.withColumn(SL_ERROR_COL, array_compact(flatten(array(projectionsList: _*))))
  }

  /** BEWARE: prepare data doesn't check input dataframe schema at the moment because it is supposed
    * to match expected schema. Therefore we can't prepare data where there is a type mismatch with
    * array or struct. Maybe we should add input dataframe schema since in most input, we already
    * read the input schema.
    *
    * This function apply some data prep before it can be validated
    *   - trim columns when column type is string
    *   - replace with default value when null or if string, when empty as well
    *     - add original input to $SL_INPUT_ROW
    * @param inputDF
    * @return
    */
  def prepareData(inputAttributes: List[TableAttribute])(inputDF: DataFrame)(implicit
    schemaHandler: SchemaHandler
  ): DataFrame = {
    def substituteToNull(column: Column): Column = {
      if (emptyIsNull) {
        when(typeof(column) =!= StringType.typeName, column)
          .otherwise(
            when(column === "", lit(null))
              .otherwise(column)
          )
      } else {
        column
      }
    }

    def replaceWithDefaultValue(attribute: TableAttribute, column: Column): Column = {
      attribute.default match {
        case Some(defaultValue) =>
          when(
            column.isNull,
            lit(defaultValue).cast(attribute.sparkType(schemaHandler))
          )
            .otherwise(column)
        case None => column
      }
    }

    /** applying trim transform the column type to string
      * @param attribute
      * @param column
      * @return
      *   trimmed column if input column type is string
      */
    def applyTrim(attribute: TableAttribute, column: Column): Column = {
      val trimmedColumn = attribute.trim match {
        case Some(NONE) | None => column
        case Some(LEFT)        => ltrim(column)
        case Some(RIGHT)       => rtrim(column)
        case Some(BOTH)        => trim(column)
        case strat => throw new RuntimeException(s"$strat is not supported as trim strategy yet")
      }
      when(typeof(column) === "string", trimmedColumn).otherwise(column)
    }

    def prepareData(
      currentSchema: TableAttribute,
      currentInputSchema: Option[TableAttribute],
      currentPath: String
    ): Option[Column => Column] = {
      currentInputSchema match {
        case Some(inputSchema)
            if currentSchema
              .resolveArray() && inputSchema.resolveArray() == currentSchema.resolveArray() =>
          // array case: don't check element type yet
          Some((arrayColumn: Column) => {
            prepareData(
              currentSchema.copy(array = Some(false)),
              Some(inputSchema.copy(array = Some(false))),
              currentPath + "[]"
            ) match {
              case Some(fAttribute) =>
                transform(
                  arrayColumn,
                  elementColumn => {
                    fAttribute(elementColumn)
                  }
                )
              case None =>
                // This means element type has a mismatch issue
                array().cast(inputSchema.sparkType(schemaHandler))
            }
          })
        case Some(inputSchema)
            if currentSchema.`type` == PrimitiveType.variant.value && !currentSchema
              .resolveArray() =>
          val column_as_json = Some((strColumn: Column) => {
            to_json(
              replaceWithDefaultValue(
                currentSchema,
                substituteToNull(applyTrim(currentSchema, strColumn))
              )
            )
          })
          inputSchema.`type` match {
            case PrimitiveType.struct.value =>
              column_as_json
            case _ if inputSchema.resolveArray() =>
              column_as_json
            case _ =>
              Some((strColumn: Column) => {
                replaceWithDefaultValue(
                  currentSchema,
                  substituteToNull(applyTrim(currentSchema, strColumn))
                ).cast(StringType)
              })
          }
        case Some(inputSchema)
            if currentSchema.attributes.nonEmpty && inputSchema.attributes.nonEmpty && inputSchema
              .resolveArray() == currentSchema
              .resolveArray() =>
          // struct case: expect both schemas to be struct and not array (if both were arrays, they would have felt into the first case
          Some((structColumn: Column) =>
            struct(currentSchema.attributes.flatMap { field =>
              prepareData(
                field,
                inputSchema.attributes.find(_.name == field.name),
                if (currentPath.isEmpty) field.name else currentPath + "." + field.name
              ).map(f => f(structColumn.getField(field.name)).as(field.name))
            }: _*)
          )
        case Some(inputSchema)
            if currentSchema.attributes.isEmpty && inputSchema.attributes.isEmpty && inputSchema
              .resolveArray() == currentSchema
              .resolveArray() =>
          // This is the leaf case where we expect primitive types and not containers.
          // at this point we should not have array anymore if expected schema is matched but in some cases, we may still have array in inputSchema so we ignore them.
          Some((strColumn: Column) => {
            // default value is not subject to trim. This means that default value, if column is trimmed, should be trimmed.
            replaceWithDefaultValue(
              currentSchema,
              substituteToNull(applyTrim(currentSchema, strColumn))
            )
          })
        case _ =>
          // We drop unknown columns
          None
      }
    }
    val projectionsList: List[Column] = for {
      field <- attributes
      fAttribute <- prepareData(
        field,
        inputAttributes.find(_.name == field.name),
        field.name
      )
    } yield {
      fAttribute(col(field.name)).as(field.name)
    }
    inputDF.select(projectionsList :+ struct(inputDF("*")).as(SL_INPUT_COL): _*)
  }
}
