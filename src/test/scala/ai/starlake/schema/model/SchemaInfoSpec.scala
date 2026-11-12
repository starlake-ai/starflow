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

import ai.starlake.TestHelper
import ai.starlake.schema.model.Severity._

import java.io.{InputStream, StringWriter}
import java.util.regex.Pattern

class SchemaInfoSpec extends TestHelper {

  new WithSettings() {
    val schemaHandler = settings.schemaHandler()

    "Attribute type" should "be valid" in {
      val stream: InputStream =
        getClass.getResourceAsStream("/sample/default.sl.yml")
      val lines =
        scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
      val types = mapper.readValue(lines, classOf[TypesInfo])
      val attr = TableAttribute(
        "attr",
        "invalid-type", // should raise error non existent type
        Some(true),
        required = Some(true),
        Some(
          TransformInput(
            "MD5",
            false
          )
        ) // Should raise an error. Privacy cannot be applied on types other than string
      )

      attr.checkValidity(schemaHandler, "ignore", new SchemaInfo()) shouldBe Left(
        List(
          ValidationMessage(
            Error,
            "Attribute.primitiveType in table ignore.",
            "Invalid Type invalid-type"
          )
        )
      )
    }

    "Attribute privacy" should "applicable to any type" in {
      val attr = TableAttribute(
        "attr",
        "long",
        Some(true),
        required = Some(true),
        Some(
          TransformInput(
            "ApproxLong(20)",
            false
          )
        ) // Should raise an error. Privacy cannot be applied on types other than stringsettings = settings
      )
      attr.checkValidity(schemaHandler, "ignore", new SchemaInfo()) shouldBe Right(true)
    }

    "Sub Attribute" should "be present for struct types only" in {
      val attr = TableAttribute(
        "attr",
        "struct",
        Some(true),
        required = Some(true),
        Some(
          TransformInput(
            "ApproxLong(20)",
            false
          )
        ), // Should raise an error. Privacy cannot be applied on types other than string
        attributes = List[TableAttribute]()
      )
      val expectedErrors = List(
        ValidationMessage(
          Error,
          "Attribute.primitiveType in table ignore.",
          "Attribute TableAttribute(attr,struct,true,true,ApproxLong(20),None,None,None,List(),None,None,Set()) : Struct types must have at least one attribute."
        )
      )

      attr.checkValidity(schemaHandler, "ignore", new SchemaInfo()) shouldBe Left(expectedErrors)
    }

    "Position serialization" should "output all fields" in {
      val yml = loadTextFile(s"/expected/yml/position_serialization.sl.yml")

      val attr =
        TableAttribute(
          "hello",
          position = Some(Position(1, 2)),
          array = Some(false),
          required = Some(false),
          privacy = Some(TransformInput.None),
          ignore = Some(false)
        )
      val writer = new StringWriter()
      mapper.writer().writeValue(writer, attr)
      logger.info("--" + writer.toString + "--")
      logger.info("++" + yml + "++")
      writer.toString.trim should equal(yml)
    }

    "Default value for an attribute" should "only be used for non obligatory fields" in {
      val requiredAttribute =
        TableAttribute("requiredAttribute", "long", required = Some(true), default = Some("10"))
      requiredAttribute.checkValidity(schemaHandler, "ignore", new SchemaInfo()) shouldBe Left(
        List(
          ValidationMessage(
            Error,
            "Attribute.default in table ignore.",
            s"attribute with name ${requiredAttribute.name} - default value valid for optional fields only"
          )
        )
      )

      val optionalAttribute =
        TableAttribute("optionalAttribute", "long", required = Some(false), default = Some("10"))
      optionalAttribute.checkValidity(schemaHandler, "ignore", new SchemaInfo()) shouldBe Right(
        true
      )
    }

    "buildSecondStepSqlSelectOnLoad" should "emit SUBSTR projections for POSITION format" in {
      val schema = SchemaInfo(
        name = "account",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "code0", `type` = "string", position = Some(Position(0, 0))),
          TableAttribute(name = "code1", `type` = "string", position = Some(Position(1, 4)))
        ),
        filter = Some("SUBSTR(value, 1, 2) = '01'")
      )
      val sql = schema.buildSecondStepSqlSelectOnLoad("tmp_table").replaceAll("\\s+", " ")
      sql should include("SUBSTR(value, 1, 1) as code0")
      sql should include("SUBSTR(value, 2, 4) as code1")
      sql should include("FROM tmp_table")
      sql should include("WHERE SUBSTR(value, 1, 2) = '01'")
    }

    "buildSecondStepSqlSelectOnLoad" should "leave DSV projections unchanged" in {
      val schema = SchemaInfo(
        name = "account",
        pattern = Pattern.compile(".*\\.csv"),
        metadata = Some(Metadata(format = Some(Format.DSV))),
        attributes = List(
          TableAttribute(name = "code0", `type` = "string"),
          TableAttribute(name = "code1", `type` = "string")
        )
      )
      val sql = schema.buildSecondStepSqlSelectOnLoad("tmp_table").replaceAll("\\s+", " ")
      sql should include("code0 as code0")
      sql should include("code1 as code1")
      sql should not include "SUBSTR"
    }

    "buildSecondStepSqlSelectOnLoad" should "compute the scripted attributes with the SQL given instead of their script" in {
      val schema = SchemaInfo(
        name = "account",
        pattern = Pattern.compile(".*\\.csv"),
        metadata = Some(Metadata(format = Some(Format.DSV))),
        attributes = List(
          TableAttribute(name = "raw", `type` = "string", ignore = Some(true)),
          TableAttribute(name = "n", `type` = "long", script = Some("CAST(raw AS INT64)")),
          TableAttribute(name = "m", `type` = "long", script = Some("CAST(raw AS INT64) * 2"))
        )
      )
      val sql = schema
        .buildSecondStepSqlSelectOnLoad(
          "tmp_table",
          scripts = Map("n" -> "SAFE_CAST(raw AS INT64)")
        )
        .replaceAll("\\s+", " ")
      sql should include("SAFE_CAST(raw AS INT64) AS n")
      sql should include("CAST(raw AS INT64) * 2 AS m")
      schema.secondStepProjections(None, Map.empty, Map.empty, Map("m" -> "0")) should contain(
        "0 AS m"
      )
    }

    "buildSecondStepSqlSelectOnLoad" should "wrap POSITION projections in SAFE_CAST for non-string DDL types" in {
      val schema = SchemaInfo(
        name = "account",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "code", `type` = "byte", position = Some(Position(0, 0))),
          TableAttribute(name = "amount", `type` = "long", position = Some(Position(1, 10))),
          TableAttribute(name = "label", `type` = "string", position = Some(Position(11, 30)))
        )
      )
      val sql = schema
        .buildSecondStepSqlSelectOnLoad(
          "tmp_table",
          ddlTypesByAttribute = Map(
            "code"   -> "INT64",
            "amount" -> "INT64",
            "label"  -> "STRING"
          )
        )
        .replaceAll("\\s+", " ")
      sql should include("SAFE_CAST(TRIM(SUBSTR(value, 1, 1)) AS INT64) as code")
      sql should include("SAFE_CAST(TRIM(SUBSTR(value, 2, 10)) AS INT64) as amount")
      // STRING is a no-op cast — skipped to keep SQL readable
      sql should include("SUBSTR(value, 12, 20) as label")
      sql should not include "SAFE_CAST(SUBSTR(value, 12, 20) AS STRING)"
    }

    it should "parse the POSITION values of the types that BigQuery cannot cast from a string" in {
      val schema = SchemaInfo(
        name = "places",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "area", `type` = "geography", position = Some(Position(0, 9))),
          TableAttribute(name = "doc", `type` = "variant", position = Some(Position(10, 19))),
          TableAttribute(name = "big", `type` = "bignumeric", position = Some(Position(20, 29)))
        )
      )
      val sql = schema
        .buildSecondStepSqlSelectOnLoad(
          "tmp_table",
          ddlTypesByAttribute = Map("area" -> "GEOGRAPHY", "doc" -> "JSON", "big" -> "BIGNUMERIC")
        )
        .replaceAll("\\s+", " ")
      sql should include("SAFE.ST_GEOGFROM(TRIM(SUBSTR(value, 1, 10))) as area")
      sql should include("SAFE.PARSE_JSON(TRIM(SUBSTR(value, 11, 10))) as doc")
      sql should include("SAFE_CAST(TRIM(SUBSTR(value, 21, 10)) AS BIGNUMERIC) as big")
    }

    it should "cast the POSITION values of parameterized types without their parameters (#1853)" in {
      val schema = SchemaInfo(
        name = "codes",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "code", position = Some(Position(0, 9))),
          TableAttribute(name = "amount", `type` = "decimal", position = Some(Position(10, 19))),
          TableAttribute(name = "small", `type` = "decimal", position = Some(Position(20, 29))),
          TableAttribute(name = "big", `type` = "bignumeric", position = Some(Position(30, 59))),
          TableAttribute(name = "hash", `type` = "binary", position = Some(Position(60, 69)))
        )
      )
      val sql = schema.buildSecondStepSqlSelectOnLoad(
        "tmp_table",
        ddlTypesByAttribute = Map(
          "code"   -> "STRING(10)",
          "amount" -> "NUMERIC(10, 2)",
          "small"  -> "NUMERIC(29)",
          "big"    -> "BIGNUMERIC(50, 20)",
          "hash"   -> "BYTES(16)"
        )
      )
      // BigQuery refuses parameterized types in CAST: NULL beyond the parameters
      sql should include(
        "IF(LENGTH(SUBSTR(value, 1, 10)) <= 10, SUBSTR(value, 1, 10), NULL) as code"
      )
      sql should include(
        "IF(ABS(ROUND(SAFE_CAST(TRIM(SUBSTR(value, 11, 10)) AS NUMERIC), 2)) < CAST('100000000' AS NUMERIC), SAFE_CAST(TRIM(SUBSTR(value, 11, 10)) AS NUMERIC), NULL) as amount"
      )
      // Any NUMERIC has at most 29 integer digits
      sql should include("SAFE_CAST(TRIM(SUBSTR(value, 21, 10)) AS NUMERIC) as small")
      sql should include(
        s"< CAST('1${"0" * 30}' AS BIGNUMERIC), SAFE_CAST(TRIM(SUBSTR(value, 31, 30)) AS BIGNUMERIC), NULL) as big"
      )
      sql should include(
        "IF(BYTE_LENGTH(SAFE_CAST(SUBSTR(value, 61, 10) AS BYTES)) <= 16, SAFE_CAST(SUBSTR(value, 61, 10) AS BYTES), NULL) as hash"
      )
      sql should not include "STRING(10)"
      ai.starlake.sql.SQLUtils.extractColumnNames(sql) shouldBe
      List("code", "amount", "small", "big", "hash")
    }

    it should "build the POSITION RANGE values from their bounds, in SQL that JSqlParser parses" in {
      val schema = SchemaInfo(
        name = "periods",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "days", `type` = "range_date", position = Some(Position(0, 29)))
        )
      )
      val sql = schema.buildSecondStepSqlSelectOnLoad(
        "tmp_table",
        ddlTypesByAttribute = Map("days" -> "RANGE<DATE>")
      )
      sql should include(
        ai.starlake.utils.conversion.BigQueryNativeSchema
          .rangeFromText("TRIM(SUBSTR(value, 1, 30))", "DATE", safe = true) + " as days"
      )
      sql should not include "AS RANGE<DATE>"
      ai.starlake.sql.SQLUtils.extractColumnNames(sql) shouldBe List("days")
    }

    it should "read the POSITION values with the pattern of their type, in SQL that JSqlParser parses (#1886)" in {
      val attributes = List(
        TableAttribute(name = "id", `type` = "long", position = Some(Position(0, 3))),
        TableAttribute(name = "day", `type` = "basic_iso_date", position = Some(Position(4, 11))),
        TableAttribute(name = "flag", `type` = "boolean", position = Some(Position(12, 12))),
        TableAttribute(name = "seen", `type` = "timestamp", position = Some(Position(13, 31))),
        TableAttribute(
          name = "label",
          `type` = "string",
          required = Some(true),
          position = Some(Position(32, 41))
        )
      )
      val schema = SchemaInfo(
        name = "rows",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = attributes
      )
      val types = attributes.flatMap(a => a.`type`(schemaHandler).map(a.name -> _)).toMap
      val ddlTypes = types.flatMap { case (name, tpe) =>
        tpe.ddlMapping.flatMap(_.get("bigquery")).map(name -> _)
      }
      val checks = ai.starlake.job.ingest.loaders.BigQueryNativeLoader
        .positionChecks(attributes, ddlTypes, types, withValue = true)
        .get
      // The second step loads the rows of the first step that the checks accept
      val sql = schema.buildSecondStepSqlSelectOnLoad(
        ai.starlake.job.ingest.loaders.BigQueryNativeLoader
          .withConversions("tmp_table", Nil, Some(checks.condition)),
        ddlTypesByAttribute = ddlTypes,
        typesByAttribute = types
      )
      sql should include(
        "IF(REGEXP_CONTAINS(TRIM(SUBSTR(value, 5, 8)), '^[0-9]{4}[0-9]{2}[0-9]{2}$'), SAFE.PARSE_TIMESTAMP('%Y%m%d', TRIM(SUBSTR(value, 5, 8))), NULL) as day"
      )
      sql should include(
        "CASE WHEN REGEXP_CONTAINS(TRIM(SUBSTR(value, 13, 1)), '(?s)^(?:(?i)true|yes|[y1]|t)$') THEN TRUE"
      )
      sql should include("SAFE_CAST(TRIM(SUBSTR(value, 14, 19)) AS TIMESTAMP) as seen")
      sql should include(s"FROM (SELECT * FROM tmp_table WHERE ${checks.condition})")
      ai.starlake.sql.SQLUtils.extractColumnNames(sql) shouldBe
      List("id", "day", "flag", "seen", "label")
    }
  }
}
