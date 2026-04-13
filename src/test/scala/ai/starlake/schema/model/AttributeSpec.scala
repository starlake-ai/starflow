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
import ai.starlake.schema.handlers.SchemaHandler

class AttributeSpec extends TestHelper {
  new WithSettings() {

    implicit val schemaHandler: SchemaHandler = settings.schemaHandler()

    val refAttributes = mapper.readValue[List[TableAttribute]]("""
        |- name: "attr1"
        |  type: "string"
        |  required: true
        |  privacy: A
        |  comment: "my comment"
        |  rename: "rename value"
        |  metricType: "TEXT"
        |  position:
        |    first: 1
        |    last: 2
        |  default: "hohaio"
        |  tags:
        |    - tag-1
        |  trim: BOTH
        |  foreignKey: "fk"
        |  ignore: true
        |  accessPolicy: "accessPolicy"
        |- name: "attr2"
        |  type: "boolean"
        |- name: "struct1"
        |  type: "struct"
        |  attributes:
        |    - name: "struct_attr1"
        |      type: "string"
        |    - name: "struct_attr2"
        |      array: true
        |      type: "struct"
        |      attributes:
        |        - name: "array_struct_struct_attr1"
        |          type: "string"
        |        - name: "array_struct_struct_attr2"
        |          type: "string"
        |          privacy: A
        |          rename: "rename value"
        |          position:
        |            first: 1
        |            last: 2
        |          tags:
        |            - tag-1
        |          foreignKey: "fk"
        |          accessPolicy: "accessPolicy"
        |- name: "array1"
        |  array: true
        |  type: "struct"
        |  attributes:
        |    - name: "array_struct_struct1"
        |      type: "struct"
        |      attributes:
        |        - name: "struct_array_struct_attr1"
        |          type: "long"
        |        - name: "struct_array_struct_attr2"
        |          array: true
        |          type: "boolean"
        |          required: true
        |          comment: "my comment"
        |          metricType: "TEXT"
        |          default: "hohaio"
        |          trim: BOTH
        |          ignore: true
        |    - name: "array_struct_attr2"
        |      type: "long"
        |- name: "array2"
        |  array: true
        |  type: "long"
        |""".stripMargin)

    "Merging with empty" should "return same content" in {
      val sourceAttributes = Nil
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      assert(refAttributes === mergedAttributes)
    }

    "Merging identical content" should "return same content" in {
      val sourceAttributes = refAttributes
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      assert(refAttributes === mergedAttributes)
    }

    "Merging wider content" should "return wider content but not in the same order" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr0"
          |  type: "string"
          |- name: "attr1"
          |  type: "string"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      val expectedAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr1"
          |  type: "string"
          |  required: true
          |  privacy: A
          |  comment: "my comment"
          |  rename: "rename value"
          |  metricType: "TEXT"
          |  position:
          |    first: 1
          |    last: 2
          |  default: "hohaio"
          |  tags:
          |    - tag-1
          |  trim: BOTH
          |  foreignKey: "fk"
          |  ignore: true
          |  accessPolicy: "accessPolicy"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |          privacy: A
          |          rename: "rename value"
          |          position:
          |            first: 1
          |            last: 2
          |          tags:
          |            - tag-1
          |          foreignKey: "fk"
          |          accessPolicy: "accessPolicy"
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |          required: true
          |          comment: "my comment"
          |          metricType: "TEXT"
          |          default: "hohaio"
          |          trim: BOTH
          |          ignore: true
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "attr0"
          |  type: "string"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      assert(expectedAttributes === mergedAttributes)
    }

    it should "return same ref content" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr0"
          |  type: "string"
          |- name: "attr1"
          |  type: "string"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = DropAll,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      val expectedAttributes = refAttributes
      assert(expectedAttributes === mergedAttributes)
    }

    it should "return ref plus source's script but not in the same order" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr0"
          |  type: "string"
          |- name: "attr1"
          |  type: "string"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepOnlyScriptDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      val expectedAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr1"
          |  type: "string"
          |  required: true
          |  privacy: A
          |  comment: "my comment"
          |  rename: "rename value"
          |  metricType: "TEXT"
          |  position:
          |    first: 1
          |    last: 2
          |  default: "hohaio"
          |  tags:
          |    - tag-1
          |  trim: BOTH
          |  foreignKey: "fk"
          |  ignore: true
          |  accessPolicy: "accessPolicy"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |          privacy: A
          |          rename: "rename value"
          |          position:
          |            first: 1
          |            last: 2
          |          tags:
          |            - tag-1
          |          foreignKey: "fk"
          |          accessPolicy: "accessPolicy"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |          required: true
          |          comment: "my comment"
          |          metricType: "TEXT"
          |          default: "hohaio"
          |          trim: BOTH
          |          ignore: true
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      assert(expectedAttributes === mergedAttributes)
    }

    "Merging mismatched container" should "return reference content" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr1"
          |  type: "struct"
          |  attributes:
          |    - name: "ignored_attr"
          |      type: "string"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr2"
          |      type: "struct"
          |      attributes:
          |        - name: "ignored_attr"
          |          type: "string"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "struct_array_struct_attr2"
          |          type: "boolean"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |- name: "array2"
          |  type: "long"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      val expectedAttributes = refAttributes
      assert(expectedAttributes === mergedAttributes)
    }

    it should "fail" in {
      var caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  type: "struct"
            |  attributes:
            |    - name: "struct_attr2"
            |      type: "struct"
            |      attributes:
            |        - name: "ignored_attr"
            |          type: "string"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct_attr2 is not in array in both schema"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  type: "struct"
            |  attributes:
            |    - name: "struct_attr2"
            |      array: true
            |      type: "long"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct_attr2 found with type 'long' where type 'struct' is expected"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "array2"
            |  type: "long"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name array2 is not in array in both schema"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "array1"
            |  type: "struct"
            |  attributes:
            |    - name: "struct_attr2"
            |      array: true
            |      type: "long"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name array1 is not in array in both schema"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  array: true
            |  type: "struct"
            |  attributes:
            |    - name: "struct_attr2"
            |      array: true
            |      type: "long"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct1 is not in array in both schema"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  type: "long"
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = true,
            failOnAttributesEmptinessMismatch = false,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct1 found with type 'long' where type 'struct' is expected"
      )
    }

    "Merging mismatched struct emptiness" should "return reference content" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr1"
          |  type: "struct"
          |  attributes: []
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr2"
          |      type: "struct"
          |      attributes: []
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes: []
          |- name: "array2"
          |  type: "long"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = RefFirst
        )
      )
      val expectedAttributes = refAttributes
      assert(expectedAttributes === mergedAttributes)
    }

    it should "fail" in {
      var caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  type: "struct"
            |  attributes:
            |    - name: "struct_attr2"
            |      type: "struct"
            |      attributes: []
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = false,
            failOnAttributesEmptinessMismatch = true,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct_attr2 has mismatch on attributes emptiness"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "struct1"
            |  type: "struct"
            |  attributes: []
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = false,
            failOnAttributesEmptinessMismatch = true,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name struct1 has mismatch on attributes emptiness"
      )

      caughtError = intercept[IllegalArgumentException] {
        val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
            |- name: "array1"
            |  array: true
            |  type: "struct"
            |  attributes: []
            |""".stripMargin)
        TableAttribute.mergeAll(
          refAttributes,
          sourceAttributes,
          AttributeMergeStrategy(
            failOnContainerMismatch = false,
            failOnAttributesEmptinessMismatch = true,
            keepSourceDiffAttributesStrategy = KeepAllDiff,
            attributePropertiesMergeStrategy = RefFirst
          )
        )
      }
      assert(
        caughtError.getMessage === "requirement failed: attribute with name array1 has mismatch on attributes emptiness"
      )
    }

    "Merging wider content with source properties first" should "return wider content but with mixed properties" in {
      val sourceAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr0"
          |  type: "string"
          |- name: "attr1"
          |  type: "string"
          |  required: false
          |  privacy: B
          |  comment: "other comment"
          |  rename: "other value"
          |  metricType: "DISCRETE"
          |  position:
          |    first: 2
          |    last: 3
          |  default: "hi"
          |  tags:
          |    - tag-2
          |  trim: LEFT
          |  foreignKey: "fk_other"
          |  ignore: false
          |  accessPolicy: "otherAccessPolicy"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |          required: true
          |          comment: "my comment"
          |          metricType: "TEXT"
          |          default: "hohaio"
          |          trim: BOTH
          |          ignore: true
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |          privacy: A
          |          rename: "rename value"
          |          position:
          |            first: 1
          |            last: 2
          |          tags:
          |            - tag-1
          |          foreignKey: "fk"
          |          accessPolicy: "accessPolicy"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      val mergedAttributes = TableAttribute.mergeAll(
        refAttributes,
        sourceAttributes,
        AttributeMergeStrategy(
          failOnContainerMismatch = false,
          failOnAttributesEmptinessMismatch = false,
          keepSourceDiffAttributesStrategy = KeepAllDiff,
          attributePropertiesMergeStrategy = SourceFirst
        )
      )
      val expectedAttributes = mapper.readValue[List[TableAttribute]]("""
          |- name: "attr1"
          |  type: "string"
          |  required: true
          |  privacy: B
          |  comment: "other comment"
          |  rename: "other value"
          |  metricType: "DISCRETE"
          |  position:
          |    first: 2
          |    last: 3
          |  default: "hi"
          |  tags:
          |    - tag-2
          |  trim: LEFT
          |  foreignKey: "fk_other"
          |  ignore: false
          |  accessPolicy: "otherAccessPolicy"
          |- name: "attr2"
          |  type: "boolean"
          |- name: "struct1"
          |  type: "struct"
          |  attributes:
          |    - name: "struct_attr1"
          |      type: "string"
          |    - name: "struct_attr2"
          |      array: true
          |      type: "struct"
          |      attributes:
          |        - name: "array_struct_struct_attr1"
          |          type: "string"
          |        - name: "array_struct_struct_attr2"
          |          type: "string"
          |          privacy: A
          |          comment: "my comment"
          |          rename: "rename value"
          |          metricType: "TEXT"
          |          position:
          |            first: 1
          |            last: 2
          |          default: "hohaio"
          |          tags:
          |            - tag-1
          |          trim: BOTH
          |          foreignKey: "fk"
          |          ignore: true
          |          accessPolicy: "accessPolicy"
          |        - name: "array_struct_struct_attr0"
          |          type: "string"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "struct_attr3"
          |      type: "long"
          |- name: "array1"
          |  array: true
          |  type: "struct"
          |  attributes:
          |    - name: "array_struct_struct1"
          |      type: "struct"
          |      attributes:
          |        - name: "struct_array_struct_attr1"
          |          type: "long"
          |        - name: "struct_array_struct_attr2"
          |          array: true
          |          type: "boolean"
          |          required: true
          |          privacy: A
          |          comment: "my comment"
          |          rename: "rename value"
          |          metricType: "TEXT"
          |          position:
          |            first: 1
          |            last: 2
          |          default: "hohaio"
          |          tags:
          |            - tag-1
          |          trim: BOTH
          |          foreignKey: "fk"
          |          ignore: true
          |          accessPolicy: "accessPolicy"
          |        - name: "script1"
          |          type: "string"
          |          script: "1"
          |        - name: "struct_array_struct_attr3"
          |          array: true
          |          type: "boolean"
          |    - name: "array_struct_attr2"
          |      type: "long"
          |    - name: "script1"
          |      type: "string"
          |      script: "1"
          |    - name: "array_struct_attr1.5"
          |      array: true
          |      type: "long"
          |- name: "array2"
          |  array: true
          |  type: "long"
          |- name: "attr0"
          |  type: "string"
          |- name: "attr3"
          |  type: "boolean"
          |- name: "script1"
          |  type: "string"
          |  script: "1"
          |- name: "script2"
          |  type: "string"
          |  script: "1"
          |""".stripMargin)
      assert(expectedAttributes === mergedAttributes)
    }
  }
}
