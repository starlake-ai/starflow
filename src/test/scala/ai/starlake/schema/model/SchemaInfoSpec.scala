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
      sql should include("SAFE_CAST(SUBSTR(value, 1, 1) AS INT64) as code")
      sql should include("SAFE_CAST(SUBSTR(value, 2, 10) AS INT64) as amount")
      // STRING is a no-op cast — skipped to keep SQL readable
      sql should include("SUBSTR(value, 12, 20) as label")
      sql should not include "SAFE_CAST(SUBSTR(value, 12, 20) AS STRING)"
    }

    "buildSecondStepSqlSelectOnLoad" should "use the provided cast function for POSITION projections" in {
      val schema = SchemaInfo(
        name = "account",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "code", `type` = "byte", position = Some(Position(0, 0))),
          TableAttribute(name = "label", `type` = "string", position = Some(Position(1, 10)))
        )
      )
      val sql = schema
        .buildSecondStepSqlSelectOnLoad(
          "tmp_table",
          ddlTypesByAttribute = Map("code" -> "TINYINT", "label" -> "VARCHAR"),
          safeCastFunction = "TRY_CAST"
        )
        .replaceAll("\\s+", " ")
      sql should include("TRY_CAST(SUBSTR(value, 1, 1) AS TINYINT) as code")
      // string-like DDL types stay uncast whatever the cast function
      sql should include("SUBSTR(value, 2, 10) as label")
      sql should not include "SAFE_CAST"
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
      sql should include("SAFE.ST_GEOGFROM(SUBSTR(value, 1, 10)) as area")
      sql should include("SAFE.PARSE_JSON(SUBSTR(value, 11, 10)) as doc")
      sql should include("SAFE_CAST(SUBSTR(value, 21, 10) AS BIGNUMERIC) as big")
    }

    it should "cast JSON with the cast function of DuckDB and Snowflake rather than parse it" in {
      val schema = SchemaInfo(
        name = "docs",
        pattern = Pattern.compile(".*TBL\\.POS"),
        metadata = Some(Metadata(format = Some(Format.POSITION))),
        attributes = List(
          TableAttribute(name = "doc", `type` = "variant", position = Some(Position(0, 9)))
        )
      )
      val sql = schema
        .buildSecondStepSqlSelectOnLoad(
          "tmp_table",
          ddlTypesByAttribute = Map("doc" -> "JSON"),
          safeCastFunction = "TRY_CAST"
        )
        .replaceAll("\\s+", " ")
      sql should include("TRY_CAST(SUBSTR(value, 1, 10) AS JSON) as doc")
      sql should not include "SAFE."
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
          .rangeFromText("SUBSTR(value, 1, 30)", "DATE", safe = true) + " as days"
      )
      sql should not include "AS RANGE<DATE>"
      ai.starlake.sql.SQLUtils.extractColumnNames(sql) shouldBe List("days")
    }
  }
}
