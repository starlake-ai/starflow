package ai.starlake.dbt.warehouse

import ai.starlake.config.Settings
import ai.starlake.job.sink.bigquery.BigQueryJobBase
import com.google.cloud.bigquery.{
  BigQuery,
  Field,
  FieldValue,
  QueryJobConfiguration,
  StandardSQLTypeName,
  TableDefinition,
  TableId
}

import java.util
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A BigQuery connection of the project: queries and table schemas through the BigQuery client. */
final class BigQueryWarehouse(connectionName: String)(implicit settings: Settings)
    extends DbtWarehouse {
  def name: String = connectionName

  private lazy val bigquery: BigQuery =
    BigQueryJobBase.bigquery(Some(connectionName), accessToken = None, outputDatabase = None)

  private def tableId(r: WarehouseRelation): TableId =
    r.database
      .map(TableId.of(_, r.schema, r.identifier))
      .getOrElse(TableId.of(r.schema, r.identifier))

  def query(sql: String, maxRows: Int): Try[DbtTable] = Try {
    val result =
      bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
    val fields = result.getSchema.getFields.asScala.toList
    val rows = ListBuffer.empty[util.List[Object]]
    // pages are fetched lazily: stop as soon as maxRows rows are read
    val it = result.iterateAll().iterator()
    while (rows.size < maxRows && it.hasNext) {
      val values = it.next()
      val row = new util.ArrayList[Object]()
      fields.zipWithIndex.foreach { case (f, i) =>
        row.add(BigQueryWarehouse.value(f, values.get(i)))
      }
      rows += row
    }
    new DbtTable(fields.map(_.getName), rows.toList)
  }

  def columns(r: WarehouseRelation): Try[List[DbtWarehouseColumn]] = Try {
    Option(bigquery.getTable(tableId(r))).toList.flatMap { t =>
      Option(t.getDefinition[TableDefinition].getSchema).toList.flatMap(
        _.getFields.asScala.map { f =>
          new DbtWarehouseColumn(f.getName, f.getType.getStandardType.name, "`")
        }
      )
    }
  }

  def relation(r: WarehouseRelation): Try[Option[WarehouseRelation]] = Try {
    Option(bigquery.getTable(tableId(r))).map(_ => r)
  }
}

object BigQueryWarehouse {

  /** BigQuery gives every scalar as a string: INT64, FLOAT64 and BOOL are typed, the rest (dates,
    * timestamps, numerics) stays text
    */
  def value(field: Field, v: FieldValue): Object =
    if (v == null || v.isNull) null
    else
      v.getAttribute match {
        case FieldValue.Attribute.PRIMITIVE =>
          Option(field.getType).map(_.getStandardType) match {
            case Some(StandardSQLTypeName.INT64)   => java.lang.Long.valueOf(v.getLongValue)
            case Some(StandardSQLTypeName.FLOAT64) => java.lang.Double.valueOf(v.getDoubleValue)
            case Some(StandardSQLTypeName.BOOL)    => java.lang.Boolean.valueOf(v.getBooleanValue)
            case _                                 => DbtResults.value(v.getStringValue)
          }
        case _ => String.valueOf(v.getValue)
      }
}
