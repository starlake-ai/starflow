package ai.starlake.job.sink.bigquery

import ai.starlake.utils.{JobResult, JsonSerializer}
import com.google.cloud.bigquery._
import com.google.gson.Gson

import scala.jdk.CollectionConverters._

case class BigQueryJobResult(
  tableResult: scala.Option[TableResult],
  totalBytesProcessed: Long,
  job: scala.Option[Job]
) extends JobResult {

  override def sqlSchema(): List[(String, String)] = {
    tableResult
      .map { rows =>
        val headers = rows.getSchema.getFields.iterator().asScala.toList
        val result = headers.map { field =>
          val fieldName = field.getName
          val fieldType = field.getType.toString
          (fieldName, fieldType)
        }
        result
      }
      .getOrElse(Nil)
  }

  /** @return
    *   the schema of the result of a successful dry run of a query, which BigQuery gives in the
    *   statistics of the job (#1950)
    */
  def dryRunSchema(): scala.Option[Schema] =
    job
      .flatMap(job => scala.Option(job.getStatistics[JobStatistics]()))
      .collect { case statistics: JobStatistics.QueryStatistics => statistics }
      .flatMap(statistics => scala.Option(statistics.getSchema))

  private def flatten(fieldList: List[Field], parentPath: String): List[List[(String, String)]] = {
    fieldList.flatMap { field =>
      val level = parentPath.count(_ == '/')
      val space = " " * 4 * level
      val hasSubFields = scala.Option(field.getSubFields).isDefined && !field.getSubFields.isEmpty
      val fieldName = space + field.getName
      val path = if (parentPath.isEmpty) fieldName else parentPath + "/" + fieldName
      val fieldMap =
        List(
          "path"       -> path,
          "field_name" -> fieldName,
          "type"       -> field.getType.toString,
          "mode"       -> field.getMode.toString,
          "default"    -> scala.Option(field.getDefaultValueExpression).getOrElse(""),
          "policy_tags" -> scala
            .Option(field.getPolicyTags)
            .map(_.getNames.asScala.mkString(","))
            .getOrElse(""),
          "description" -> scala.Option(field.getDescription).getOrElse("")
        )
      if (!hasSubFields) {
        List(fieldMap)
      } else {
        List(fieldMap) ++ flatten(field.getSubFields.asScala.toList, path)
      }
    }
  }

  override def asList(): List[List[(String, Any)]] = {
    if (this.totalBytesProcessed < 0) {
      // The result is the schema of the table
      tableResult
        .map { tableResult =>
          val fieldList = tableResult.getSchema.getFields.iterator().asScala.toList
          flatten(fieldList, "")
        }
        .getOrElse(Nil)
    } else {
      tableResult
        .map { rows =>
          val headers: List[Field] = rows.getSchema.getFields.iterator().asScala.toList
          val values = rows.iterateAll().asScala.toList.map { row =>
            val fields = row
              .iterator()
              .asScala
              .toList
            asList(fields, headers)
          }
          values
        }
        .getOrElse(Nil)
      /*
      tableResult
        .map { rows =>
          val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
          val result =
            rows.iterateAll().asScala.toList.map { row =>
              val values = row
                .iterator()
                .asScala
                .toList
                .map { cell =>
                  scala.Option(cell.getValue()).map(_.toString).getOrElse("NULL")
                }
              headers.zip(values).toMap
            }
          result
        }
        .getOrElse(Nil)
       */
    }
  }

  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    if (dryRun) {
      val map = Map("totalBytesProcessed" -> totalBytesProcessed.toString).asJava
      val json = new Gson().toJson(map)
      json
    } else {
      tableResult
        .map { rows =>
          val headers = rows.getSchema.getFields.iterator().asScala.toList.map(_.getName)
          val values =
            rows.iterateAll().asScala.toList.map { row =>
              row
                .iterator()
                .asScala
                .toList
                .map(cell => scala.Option(cell.getValue()).map(_.toString).orNull)
            }
          val result =
            if (format == "json-array") {
              val result =
                tableResult
                  .map { rows =>
                    val headers: List[Field] = rows.getSchema.getFields.iterator().asScala.toList
                    val values = rows.iterateAll().asScala.toList.map { row =>
                      val fields = row
                        .iterator()
                        .asScala
                        .toList
                      asList(fields, headers)
                    }
                    values
                  }
                  .getOrElse(Nil)
              JsonSerializer.mapper.writeValueAsString(result)
            } else {
              prettyPrint(format, headers, values)
            }
          result
        }
        .getOrElse("")
    }
  }

  def asList(fields: List[FieldValue], headers: List[Field]): List[(String, Any)] = {
    val result =
      fields
        .zip(headers)
        .map { case (fieldValue, header) =>
          val attribute = fieldValue.getAttribute
          val headerName = header.getName
          val obj =
            attribute match {
              case FieldValue.Attribute.PRIMITIVE =>
                headerName -> scala.Option(fieldValue.getValue).map(_.toString).orNull
              case FieldValue.Attribute.RECORD =>
                val record = fieldValue.getValue.asInstanceOf[FieldValueList]
                val subFieldValues = record.iterator().asScala.toList
                val subHeaders =
                  scala.Option(header.getSubFields.iterator()).map(_.asScala.toList).getOrElse(Nil)
                val value = asList(subFieldValues, subHeaders)
                headerName -> value
              case FieldValue.Attribute.REPEATED =>
                val record = fieldValue.getValue.asInstanceOf[FieldValueList]
                val subFieldValues = record.iterator().asScala.toList
                val valueList =
                  if (header.getSubFields == null) {
                    val valueList = subFieldValues
                      .map(subField => scala.Option(subField.getValue).map(_.toString).orNull)
                    valueList
                  } else {
                    val subHeaders =
                      scala
                        .Option(header.getSubFields.iterator())
                        .map(_.asScala.toList)
                        .getOrElse(Nil)
                    val valueList = subFieldValues.map { subField =>
                      val record = subField.getValue.asInstanceOf[FieldValueList]
                      val subFieldValues = record.iterator().asScala.toList
                      val value = asList(subFieldValues, subHeaders)
                      value
                    }
                    valueList
                  }
                headerName -> valueList
              case FieldValue.Attribute.RANGE =>
                val value =
                  scala.Option(fieldValue.getRangeValue).map(_.getValues).orNull
                headerName -> value
            }
          obj
        }
    result
  }

  def show(format: String): Unit = {
    println(s"Total Bytes Processed: $totalBytesProcessed bytes.")
    val res = prettyPrint(format)
    println(res)
  }
}
