package ai.starlake.utils.conversion

import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{PrimitiveType, TableAttribute}
import com.google.cloud.bigquery.{
  Field,
  FieldElementType,
  FieldList,
  Schema => BQSchema,
  StandardSQLTypeName
}
import com.typesafe.scalalogging.LazyLogging

import java.util.Locale
import scala.jdk.CollectionConverters._

/** A BigQuery column type written in a `ddlMapping.bigquery` entry */
case class BigQueryType(
  standardType: StandardSQLTypeName,
  precision: Option[Long] = None,
  scale: Option[Long] = None,
  maxLength: Option[Long] = None,
  rangeElementType: Option[StandardSQLTypeName] = None
) {
  def hasParameters: Boolean =
    precision.isDefined || scale.isDefined || maxLength.isDefined || rangeElementType.isDefined

  /** @return
    *   the type as written in BigQuery DDL, such as STRING(10) or RANGE<DATE>
    */
  def render: String =
    (rangeElementType, maxLength, precision) match {
      case (Some(elementType), _, _) => s"${standardType.name()}<${elementType.name()}>"
      case (_, Some(length), _)      => s"${standardType.name()}($length)"
      case (_, _, Some(p)) => s"${standardType.name()}($p${scale.map(", " + _).getOrElse("")})"
      case _               => standardType.name()
    }

  /** @return
    *   the field with this type, keeping its name, mode and description
    */
  def applyTo(field: Field): Field = {
    val builder = field.toBuilder.setType(standardType)
    builder.setPrecision(precision.map(Long.box).orNull)
    builder.setScale(scale.map(Long.box).orNull)
    builder.setMaxLength(maxLength.map(Long.box).orNull)
    builder.setRangeElementType(
      rangeElementType
        .map(elementType => FieldElementType.newBuilder().setType(elementType.name()).build())
        .orNull
    )
    builder.build()
  }
}

object BigQueryType extends LazyLogging {

  /** @return
    *   the type of a field, with its parameters
    */
  def of(field: Field): BigQueryType =
    BigQueryType(
      field.getType.getStandardType,
      Option(field.getPrecision).map(_.longValue()),
      Option(field.getScale).map(_.longValue()),
      Option(field.getMaxLength).map(_.longValue()),
      Option(field.getRangeElementType)
        .flatMap(elementType => Option(elementType.getType))
        .map(StandardSQLTypeName.valueOf)
    )

  private val aliases: Map[String, StandardSQLTypeName] = Map(
    "INT"        -> StandardSQLTypeName.INT64,
    "INTEGER"    -> StandardSQLTypeName.INT64,
    "SMALLINT"   -> StandardSQLTypeName.INT64,
    "BIGINT"     -> StandardSQLTypeName.INT64,
    "TINYINT"    -> StandardSQLTypeName.INT64,
    "BYTEINT"    -> StandardSQLTypeName.INT64,
    "FLOAT"      -> StandardSQLTypeName.FLOAT64,
    "BOOLEAN"    -> StandardSQLTypeName.BOOL,
    "DECIMAL"    -> StandardSQLTypeName.NUMERIC,
    "BIGDECIMAL" -> StandardSQLTypeName.BIGNUMERIC
  )

  // Types a column can be declared with: STRUCT and ARRAY are built from the attributes
  private val columnTypes: Map[String, StandardSQLTypeName] =
    StandardSQLTypeName
      .values()
      .filterNot(t => t == StandardSQLTypeName.STRUCT || t == StandardSQLTypeName.ARRAY)
      .map(t => t.name() -> t)
      .toMap ++ aliases

  private val Parameterized =
    """^\s*([A-Za-z0-9_]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$""".r
  private val Range = """^\s*RANGE\s*<\s*([A-Za-z]+)\s*>\s*$""".r

  /** @return
    *   the BigQuery type of a `ddlMapping.bigquery` entry, or None when it is not a column type
    *   this parser understands (STRUCT, ARRAY, RANGE without element type, unknown names) or its
    *   parameters are out of the BigQuery bounds
    */
  def parse(ddl: String): Option[BigQueryType] =
    ddl.toUpperCase(Locale.ROOT) match {
      case Range(elementType) =>
        columnTypes
          .get(elementType)
          .filter(t =>
            t == StandardSQLTypeName.DATE || t == StandardSQLTypeName.DATETIME || t == StandardSQLTypeName.TIMESTAMP
          )
          .map(t => BigQueryType(StandardSQLTypeName.RANGE, rangeElementType = Some(t)))
      case Parameterized(name, first, second) =>
        val parameters = List(first, second).map(Option(_).map(_.toLongOption))
        columnTypes.get(name).filter(_ != StandardSQLTypeName.RANGE).flatMap { standardType =>
          (standardType, parameters) match {
            case (_, List(None, None)) =>
              Some(BigQueryType(standardType))
            case (
                  StandardSQLTypeName.STRING | StandardSQLTypeName.BYTES,
                  List(Some(Some(length)), None)
                ) if length >= 1 =>
              Some(BigQueryType(standardType, maxLength = Some(length)))
            case (
                  StandardSQLTypeName.NUMERIC | StandardSQLTypeName.BIGNUMERIC,
                  List(Some(Some(precision)), scaleParameter)
                ) if scaleParameter.forall(_.isDefined) =>
              val scale = scaleParameter.flatten
              // BigQuery bounds: NUMERIC(P[, S]) with S <= 9 and P <= S + 29, BIGNUMERIC with 38
              val (maxScale, maxIntegerDigits) =
                if (standardType == StandardSQLTypeName.NUMERIC) (9L, 29L) else (38L, 38L)
              val effectiveScale = scale.getOrElse(0L)
              val valid = effectiveScale <= maxScale &&
                precision >= math.max(1L, effectiveScale) &&
                precision <= effectiveScale + maxIntegerDigits
              if (valid)
                Some(BigQueryType(standardType, precision = Some(precision), scale = scale))
              else None
            case _ =>
              None
          }
        }
      case _ =>
        None
    }
}

/** Builds the schema of a BigQuery table loaded natively from the types of its attributes */
object BigQueryNativeSchema extends LazyLogging {

  /** @param schema
    *   the schema derived from the Spark types of the attributes, as Starlake built it so far
    * @param attributes
    *   the attributes of the schema, at the same nesting level as its fields
    * @param fieldName
    *   the name of the field of an attribute in this schema: its source name or its final name
    * @param existingTarget
    *   the schema of the target table when it exists and its column types cannot be changed by the
    *   load, that is when it does not replace the table
    * @param enabled
    *   false to keep the derived types for new columns
    * @param log
    *   false to log nothing, when the same schema is built several times by a load
    * @return
    *   the schema where each leaf field whose type has a `ddlMapping.bigquery` with another
    *   BigQuery type than the derived one takes that type. Variant and decimal attributes keep
    *   their derived type. A column of the target table keeps its type when it is the derived or
    *   the mapped one, since BigQuery cannot change the type of a column: the flag and the mapping
    *   then only apply to new columns. Any other type difference is a type change of the attribute,
    *   which BigQuery rejects as before.
    */
  def patch(
    schema: BQSchema,
    attributes: List[TableAttribute],
    fieldName: TableAttribute => String,
    existingTarget: Option[BQSchema],
    schemaHandler: SchemaHandler,
    enabled: Boolean,
    log: Boolean = true
  ): BQSchema =
    if (!enabled && existingTarget.isEmpty) schema
    else
      BQSchema.of(
        patchFields(
          fieldsOf(schema.getFields),
          attributes,
          fieldName,
          existingTarget.map(target => fieldsOf(target.getFields)),
          schemaHandler,
          enabled,
          log,
          ""
        ).asJava
      )

  private def fieldsOf(fields: FieldList): List[Field] =
    Option(fields).map(_.asScala.toList).getOrElse(Nil)

  private def patchFields(
    fields: List[Field],
    attributes: List[TableAttribute],
    fieldName: TableAttribute => String,
    existingFields: Option[List[Field]],
    schemaHandler: SchemaHandler,
    enabled: Boolean,
    log: Boolean,
    prefix: String
  ): List[Field] =
    fields.map { field =>
      attributes.find(fieldName(_) == field.getName) match {
        case None =>
          field
        case Some(attribute) =>
          val columnName = s"$prefix${attribute.getFinalName()}"
          // The target table uses final names
          val existingField =
            existingFields.flatMap(_.find(_.getName.equalsIgnoreCase(attribute.getFinalName())))
          if (attribute.attributes.nonEmpty && field.getSubFields != null) {
            val subFields = patchFields(
              fieldsOf(field.getSubFields),
              attribute.attributes,
              fieldName,
              existingField.map(f => fieldsOf(f.getSubFields)),
              schemaHandler,
              enabled,
              log,
              s"$columnName."
            )
            field.toBuilder.setType(field.getType, FieldList.of(subFields.asJava)).build()
          } else {
            val derivedType = BigQueryType.of(field)
            val mapped = mappedType(attribute, field, schemaHandler, log && enabled)
            val wantedType = if (enabled) mapped.getOrElse(derivedType) else derivedType
            existingField.map(BigQueryType.of) match {
              case Some(existingType)
                  if existingType != wantedType && (existingType == derivedType || mapped
                    .contains(existingType)) =>
                if (log)
                  logger.warn(
                    s"Column $columnName keeps its type ${existingType.render}: it already exists in the target table, and BigQuery cannot change it to ${wantedType.render}. Recreate the column to change its type"
                  )
                existingType.applyTo(field)
              case _ if wantedType != derivedType =>
                if (log)
                  logger.info(
                    s"Column $columnName has the BigQuery type ${wantedType.render} of ${attribute.`type`}"
                  )
                wantedType.applyTo(field)
              case _ =>
                field
            }
          }
      }
    }

  /** @return
    *   the type of each top-level leaf attribute whose column type in the target schema differs
    *   from its raw `ddlMapping.bigquery`, by attribute name: the column kept its type or the
    *   mapping is disabled. POSITION loads cast their values to these types.
    */
  def castTypes(
    targetSchema: BQSchema,
    attributes: List[TableAttribute],
    schemaHandler: SchemaHandler
  ): Map[String, String] =
    attributes.flatMap { attribute =>
      for {
        tpe <- attribute.`type`(schemaHandler)
        if attribute.attributes.isEmpty &&
        tpe.primitiveType != PrimitiveType.variant && tpe.primitiveType != PrimitiveType.decimal
        ddl <- tpe.ddlMapping.flatMap(_.get("bigquery"))
        field <- Option(targetSchema.getFields).flatMap(fields =>
          fields.asScala.find(_.getName.equalsIgnoreCase(attribute.getFinalName()))
        )
        effectiveType = BigQueryType.of(field)
        if !BigQueryType.parse(ddl).contains(effectiveType)
      } yield attribute.name -> effectiveType.render
    }.toMap

  /** @return
    *   the BigQuery type of the attribute type mapping, when it differs from the derived one
    */
  private def mappedType(
    attribute: TableAttribute,
    field: Field,
    schemaHandler: SchemaHandler,
    log: Boolean
  ): Option[BigQueryType] =
    attribute.`type`(schemaHandler).flatMap { tpe =>
      tpe.ddlMapping.flatMap(_.get("bigquery")).flatMap { ddl =>
        val parsed = BigQueryType.parse(ddl)
        tpe.primitiveType match {
          // The variant type is a JSON column
          case PrimitiveType.variant if field.getType.getStandardType == StandardSQLTypeName.JSON =>
            None
          // The decimal parameters come from the zone of the type
          case PrimitiveType.decimal =>
            val derivedType = BigQueryType.of(field)
            val matches = parsed.exists(mapping =>
              mapping == derivedType ||
              mapping.standardType == derivedType.standardType && !mapping.hasParameters
            )
            if (log && !matches)
              logger.warn(
                s"The BigQuery mapping $ddl of the decimal type ${tpe.name} is not applied: the precision and scale of its zone give ${BigQueryType.of(field).render}"
              )
            None
          case _ =>
            parsed match {
              case None =>
                if (log)
                  logger.warn(
                    s"Ignoring the BigQuery mapping $ddl of type ${tpe.name}: it is not a BigQuery column type"
                  )
                None
              case Some(bigQueryType) if bigQueryType == BigQueryType.of(field) =>
                None
              case Some(bigQueryType)
                  if bigQueryType.standardType == field.getType.getStandardType && !bigQueryType.hasParameters =>
                None
              case some =>
                some
            }
        }
      }
    }
}
