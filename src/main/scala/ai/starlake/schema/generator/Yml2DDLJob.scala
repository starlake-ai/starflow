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

package ai.starlake.schema.generator

import ai.starlake.config.Settings
import ai.starlake.extract.{ExtractExecutionContext, ExtractTableAttributes, JdbcDbUtils, ParUtils}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.{DomainInfo, JDBCSchema, SchemaInfo}
import ai.starlake.utils.{JinjaUtils, StarlakeNotFoundException, Utils}
import better.files.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path as StoragePath

import scala.util.Try

/** * Infers the schema of a given data path, domain name, schema name.
  */
class Yml2DDLJob(config: Yml2DDLConfig, schemaHandler: SchemaHandler)(implicit
  settings: Settings
) extends LazyLogging {
  implicit class CaseInsensitiveGetMap[V](m: Map[String, V]) {
    def iget(key: String): Option[V] = m
      .get(key.toLowerCase())
      .orElse(m.get(key.toUpperCase())) // you can add more orElse in chain
  }

  def name: String = "InferDDL"

  /** Pre-compute DDL string for an attribute map (handles recursive struct fields) */
  private def attributeToDdl(attr: Map[String, Any]): String = {
    val required = if (attr("required").toString.toBoolean) "NOT NULL" else ""
    val attrName = attr("name")
    val attrType = attr("type")
    attr("nodeType") match {
      case "leaf" => s"$attrName $attrType $required"
      case "node" =>
        val fields = attr("fields").asInstanceOf[List[Map[String, Any]]]
        val ddlFields = fields.map(attributeToDdl).mkString(",")
        s"$attrName $attrType<$ddlFields> $required"
      case _ => s"$attrName $attrType $required"
    }
  }

  /** Enrich attribute maps with pre-computed DDL strings */
  private def withDdl(attrs: List[Map[String, Any]]): List[Map[String, Any]] =
    attrs.map(a => a + ("ddl" -> attributeToDdl(a)))

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Session used for the job
    */
  def run(): Try[Unit] =
    Try {
      val domains = config.domain match {
        case None => schemaHandler.domains()
        case Some(domain) =>
          val res = schemaHandler
            .domains()
            .find(_.name.toLowerCase() == domain)
            .getOrElse(throw new StarlakeNotFoundException(s"Domain $domain not found"))
          List(res)
      }
      val sqlString = new StringBuffer()
      Try(DomainInfo.ddlExtract(config.datawarehouse, "global")) match {
        case scala.util.Success(_) =>
          val customParamMap = Map[String, Any](
            "domains" -> domains
          )
          val result = applyTemplate("global", customParamMap)
          sqlString.append(result)
        case scala.util.Failure(_) =>
        // no global template, do nothing
      }

      domains.map { domain =>
        val domainLabels = Utils.labels(domain.tags)

        Try(DomainInfo.ddlExtract(config.datawarehouse, "domain")) match {
          case scala.util.Success(_) =>
            val customParamMap = Map[String, Any](
              "domain"        -> domain,
              "domainName"    -> domain.finalName,
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels
            )
            val result = applyTemplate("domain", customParamMap)
            sqlString.append(result)
          case scala.util.Failure(_) =>
          // no domain template, do nothing
        }
        val schemas: Seq[SchemaInfo] = config.schemas match {
          case Some(schemas) =>
            schemas.flatMap(schema =>
              domain.tables.find(_.name.toLowerCase() == schema.toLowerCase)
            )
          case None =>
            domain.tables
        }
        val existingTables = config.connectionRef match {
          case Some(connection) =>
            val connectionSettings = settings.appConfig.getConnection(connection)
            ParUtils.withExecutor(config.parallelism) { implicit extractEC =>
              implicit val extractExecutionContext =
                new ExtractExecutionContext(extractEC)
              JdbcDbUtils.extractJDBCTables(
                JDBCSchema(
                  config.catalog,
                  domain.finalName,
                  None,
                  None,
                  Nil,
                  Nil,
                  List("TABLE"),
                  None
                ),
                connectionSettings,
                skipRemarks = true,
                keepOriginalName = true,
                includeColumns = true
              )
            }
          case None => Map.empty[String, ExtractTableAttributes]
        }
        val toDeleteTables = existingTables.keys.filterNot(table =>
          schemas.map(_.finalName.toLowerCase()).contains(table.toLowerCase())
        )

        toDeleteTables.foreach { table =>
          val schema: String = schemas
            .collectFirst {
              case s if s.finalName.toLowerCase() == table.toLowerCase() => s.finalName
            }
            .getOrElse(table)

          val dropParamMap = Map[String, Any](
            "attributes"              -> Nil,
            "newAttributes"           -> Nil,
            "alterAttributes"         -> Nil,
            "alterCommentAttributes"  -> Nil,
            "alterDataTypeAttributes" -> Nil,
            "alterRequiredAttributes" -> Nil,
            "droppedAttributes"       -> Nil,
            "domainName"              -> domain.finalName,
            "domain"                  -> domain.finalName,
            "tableName"               -> schema,
            "schema"                  -> schema,
            "partitions"              -> Nil,
            "clustered"               -> Nil,
            "primaryKeys"             -> Nil,
            "comment"                 -> "",
            "domainComment"           -> ""
          )
          logger.info(s"Dropping table $table")
          val result = applyTemplate("drop", dropParamMap)
          sqlString.append(result)
        }

        schemas.map { schema =>
          val schemaLabels = Utils.labels(schema.tags)

          val ddlFields = schema.ddlMapping(config.datawarehouse, schemaHandler)

          val mergedMetadata = schema.mergedMetadata(domain.metadata)

          val isNew = existingTables.iget(schema.finalName).isEmpty
          if (isNew) {
            val rawAttrs = ddlFields.map(_.toMap())
            val createParamMap = Map[String, Any](
              "attributes"              -> withDdl(rawAttrs),
              "newAttributes"           -> Nil,
              "alterAttributes"         -> Nil,
              "alterCommentAttributes"  -> Nil,
              "alterDataTypeAttributes" -> Nil,
              "alterRequiredAttributes" -> Nil,
              "droppedAttributes"       -> Nil,
              "domainName"              -> domain.finalName,
              "domain"                  -> domain.finalName,
              "tableName"               -> schema.finalName,
              "schema"                  -> schema.finalName,
              "table"                   -> schema,
              "partitions" -> mergedMetadata
                .getSink()
                .toAllSinks()
                .partition
                .getOrElse(Nil),
              "clustered"     -> mergedMetadata.getClustering().getOrElse(Nil),
              "primaryKeys"   -> schema.primaryKey,
              "tableComment"  -> schema.comment.getOrElse(""),
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels,
              "tableLabels"   -> schemaLabels,
              "sink"          -> mergedMetadata.sink,
              "metadata"      -> mergedMetadata
            )
            println(s"Creating new table ${schema.finalName}")
            val result = applyTemplate("create", createParamMap)
            sqlString.append(result)
          } else {
            val tableAttrs =
              existingTables
                .iget(schema.finalName)
                .getOrElse(throw new IllegalStateException("Should never happen"))
            val addColumns =
              schema.attributes.filter(attr =>
                !tableAttrs.columNames
                  .map(_.getFinalName().toLowerCase())
                  .contains(attr.getFinalName().toLowerCase())
              )
            val dropColumns =
              tableAttrs.columNames.filter(attr =>
                !schema.attributes
                  .map(_.getFinalName().toLowerCase())
                  .contains(attr.getFinalName().toLowerCase())
              )
            val alterColumns =
              schema.attributes.filter { attr =>
                tableAttrs.columNames.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  (existingAttr.resolveRequired() != attr.resolveRequired() ||
                  !existingAttr.samePrimitiveType(attr)(schemaHandler) ||
                  existingAttr.comment.getOrElse("") != attr.comment.getOrElse(""))
                )
              }
            val alterDataTypeColumns =
              schema.attributes.filter { attr =>
                tableAttrs.columNames.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  !existingAttr.samePrimitiveType(attr)(schemaHandler)
                )
              }
            val alterDescriptionColumns =
              schema.attributes.filter { attr =>
                tableAttrs.columNames.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  existingAttr.comment.getOrElse("") != attr.comment.getOrElse("")
                )
              }
            val alterRequiredColumns =
              schema.attributes.filter { attr =>
                tableAttrs.columNames.exists(existingAttr =>
                  existingAttr.getFinalName().toLowerCase() == attr.getFinalName().toLowerCase() &&
                  existingAttr.resolveRequired() != attr.resolveRequired()
                )
              }

            val alterParamMap = Map[String, Any](
              "attributes" -> Nil,
              "newAttributes" -> withDdl(
                addColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "alterAttributes" -> withDdl(
                alterColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "alterCommentAttributes" -> withDdl(
                alterDescriptionColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "alterDataTypeAttributes" -> withDdl(
                alterDataTypeColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "alterRequiredAttributes" -> withDdl(
                alterRequiredColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "droppedAttributes" -> withDdl(
                dropColumns.map(
                  _.ddlMapping(isPrimaryKey = false, config.datawarehouse, schemaHandler).toMap()
                )
              ),
              "domainName"    -> domain.finalName,
              "domain"        -> domain.finalName,
              "tableName"     -> schema.finalName,
              "schema"        -> schema.finalName,
              "table"         -> schema,
              "partitions"    -> Nil,
              "clustered"     -> Nil,
              "primaryKeys"   -> Nil,
              "tableComment"  -> schema.comment.getOrElse(""),
              "domainComment" -> domain.comment.getOrElse(""),
              "domainLabels"  -> domainLabels,
              "tableLabels"   -> schemaLabels
            )
            val result = applyTemplate("alter", alterParamMap)
            println(s"Altering existing table ${schema.finalName}")
            sqlString.append(result)
          }
        }
      }

      val sqlScript = sqlString.toString
      logger.debug(s"Final script is:\n $sqlScript")

      val outputPath =
        File(
          config.outputPath.getOrElse(settings.appConfig.metadata),
          "ddl",
          config.datawarehouse + ".sql"
        )
      writeScript(sqlScript, outputPath.pathAsString).toOption

      if (config.apply)
        config.connectionRef.fold(logger.warn("Could not apply script, connection is not defined"))(
          conn => JdbcDbUtils.execute(sqlScript, settings.appConfig.connections(conn).options)
        )
    }

  private def applyTemplate(
    ddlType: String,
    paramMap: Map[String, Any]
  ): String = {
    val (_, templateContent) =
      DomainInfo.ddlExtract(
        config.datawarehouse,
        ddlType
      )
    val javaParams = JinjaUtils.ctx(paramMap.toSeq: _*)
    JinjaUtils.renderJinja(templateContent, javaParams)
  }

  private def writeScript(sqlScript: String, output: String): Try[Unit] = {
    Try(settings.storageHandler().write(sqlScript, new StoragePath(output)))
  }
}
