package ai.starlake.utils.conversion

import ai.starlake.utils.repackaged.BigQuerySchemaConverters
import com.google.cloud.bigquery.{Field, FieldValue, LegacySQLTypeName, Schema => BQSchema, TableId}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types._
import org.threeten.extra.PeriodDuration

import scala.jdk.CollectionConverters._

/** [X] whatever Conversion between [X] Schema and BigQuery Schema
  */
object BigQueryUtils {

  val sparkToBq: DataFrame => BQSchema = (df: DataFrame) => bqSchema(df.schema)

  /** Compute BigQuery Schema from Spark or PArquet Schema while Schema.bqSchema compute it from YMl
    * File
    * @param schema
    *   Spark DataType
    * @return
    */

  def bqSchema(schema: DataType): BQSchema = {
    BigQuerySchemaConverters.toBigQuerySchema(schema.asInstanceOf[StructType])
  }

  /** Spark BigQuery driver consider integer in BQ as Long. We need to convert the Int DataType to
    * LongType before loading the data. As a good practice, always use long when dealing with big
    * query in your YAML Schema.
    * @param schema
    * @return
    */
  def normalizeSchema(schema: StructType): StructType = {
    val fields = schema.fields.map { field =>
      field.dataType match {
        case dataType: StructType =>
          field.copy(dataType = normalizeSchema(dataType))
        case ArrayType(elementType: StructType, nullable) =>
          field.copy(dataType = ArrayType(normalizeSchema(elementType), nullable))
        case ArrayType(_: IntegerType, nullable) =>
          field.copy(dataType = ArrayType(LongType, nullable))
        case IntegerType => field.copy(dataType = LongType)
        case _           => field
      }
    }
    schema.copy(fields = fields)
  }

  /** This function normalizes the incoming schema to be compatible with the existing schema. It
    * ensures that integer types are converted to long types and that nullable fields in the
    * existing schema remain nullable in the incoming schema.
    *
    * @param incomingSchema
    *   The schema of the incoming data.
    * @param existingSchema
    *   The schema of the existing data.
    * @return
    *   A normalized schema that is compatible with the existing schema.
    */
  def normalizeCompatibleSchema(
    incomingSchema: StructType,
    existingSchema: StructType
  ): StructType = {
    val existingFields = existingSchema.fields.map { field => field.name -> field }.toMap
    val incomingFields = incomingSchema.fields.map { incomingField =>
      val incomingTypedField = incomingField.dataType match {
        case dataType: StructType =>
          incomingField.copy(dataType = normalizeSchema(dataType))
        case ArrayType(elementType: StructType, nullable) =>
          incomingField.copy(dataType = ArrayType(normalizeSchema(elementType), nullable))
        case ArrayType(_: IntegerType, nullable) =>
          incomingField.copy(dataType = ArrayType(LongType, nullable))
        case IntegerType => incomingField.copy(dataType = LongType)
        case _           => incomingField
      }
      existingFields.get(incomingTypedField.name) match {
        case Some(existingField) =>
          if (existingField.nullable && !incomingTypedField.nullable)
            incomingTypedField.copy(nullable = true)
          else
            incomingTypedField
        case None =>
          incomingTypedField
      }
    }
    incomingSchema.copy(fields = incomingFields)
  }

  /** The incoming schema made compatible with the existing one at BigQuery level: a column that is
    * nullable in the existing schema stays nullable. Same rule as `normalizeCompatibleSchema`,
    * without converting the existing schema to Spark, which cannot represent every BigQuery type.
    */
  def compatibleSchema(incomingSchema: BQSchema, existingSchema: BQSchema): BQSchema = {
    val existingFields =
      Option(existingSchema.getFields)
        .map(_.asScala.toList)
        .getOrElse(Nil)
        .map(f => f.getName -> f)
        .toMap
    val fields =
      Option(incomingSchema.getFields).map(_.asScala.toList).getOrElse(Nil).map { field =>
        existingFields.get(field.getName) match {
          case Some(existingField)
              if field.getMode == Field.Mode.REQUIRED && existingField.getMode != Field.Mode.REQUIRED =>
            field.toBuilder.setMode(Field.Mode.NULLABLE).build()
          case _ =>
            field
        }
      }
    BQSchema.of(fields.asJava)
  }

  def computePartitionsToUpdateAfterMerge(
    mergedDF: DataFrame,
    toDeleteDF: DataFrame,
    timestamp: String
  ): List[String] = {
    mergedDF
      .select(col(timestamp))
      .union(toDeleteDF.select(col(timestamp)))
      .select(date_format(col(timestamp), "yyyyMMdd").cast("string"))
      .where(col(timestamp).isNotNull)
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
  }

  def anyRefToAny(fields: Seq[FieldValue], schema: Seq[LegacySQLTypeName]): Seq[Any] = {
    val fieldsWithSchema = fields.zip(schema)
    fieldsWithSchema.map { case (field, schema) =>
      schema match {
        case LegacySQLTypeName.BYTES =>
          Option(field).map(_.getBytesValue).getOrElse(Array[Byte]())
        case LegacySQLTypeName.STRING =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.INTEGER =>
          Option(field).map(_.getLongValue).getOrElse(0L)
        case LegacySQLTypeName.FLOAT =>
          Option(field).map(_.getDoubleValue).getOrElse(0.0)
        case LegacySQLTypeName.NUMERIC =>
          Option(field).map(_.getNumericValue).getOrElse(BigDecimal(0))
        case LegacySQLTypeName.BIGNUMERIC =>
          Option(field).map(_.getNumericValue).getOrElse(BigDecimal(0))
        case LegacySQLTypeName.BOOLEAN =>
          Option(field).map(_.getBooleanValue).getOrElse(false)
        case LegacySQLTypeName.TIMESTAMP =>
          Option(field).map(_.getTimestampValue).getOrElse(0L)
        case LegacySQLTypeName.DATE =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.GEOGRAPHY =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.TIME =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.DATETIME =>
          Option(field).map(_.getTimestampValue).getOrElse(0L)
        case LegacySQLTypeName.RECORD =>
          field.getRecordValue()
        case LegacySQLTypeName.JSON =>
          Option(field).map(_.getStringValue).getOrElse("")
        case LegacySQLTypeName.INTERVAL =>
          Option(field).map(_.getPeriodDuration).getOrElse(PeriodDuration.ZERO)
        case _ =>
          field
      }
    }
  }

  def tableIdToTableName(tableId: TableId): String = {
    (tableId.getProject, tableId.getDataset, tableId.getTable) match {
      case (null, null, null)        => ""
      case (null, null, table)       => table
      case (null, dataset, table)    => s"`$dataset`.`$table`"
      case (project, dataset, table) => s"`$project`.`$dataset`.`$table`"
    }
  }
}
